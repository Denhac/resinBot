import socket
import json
import subprocess
import threading
import time
import uuid
from flask import Flask, jsonify, Response
from websocket import create_connection, WebSocketTimeoutException

app = Flask(__name__)

# {ip: {"status": {}, "ws": connection, "mainboard_id": str}}
printers = {}
printers_lock = threading.Lock()

def discover_printers():
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_socket.settimeout(5)

    message = "M99999".encode('utf-8')
    udp_socket.sendto(message, ("<broadcast>", 3000))

    try:
        while True:
            response, addr = udp_socket.recvfrom(1024)
            ip = addr[0]
            mainboard_id = name = machine_name = None
            try:
                payload = json.loads(response.decode("utf-8"))
                data = payload.get("Data", payload)
                mainboard_id = data.get("MainboardID")
                name = data.get("Name")
                machine_name = data.get("MachineName")
            except (ValueError, UnicodeDecodeError):
                pass

            with printers_lock:
                if ip not in printers:
                    printers[ip] = {
                        "status": {},
                        "attributes": {},
                        "ws": None,
                        "mainboard_id": mainboard_id,
                        "name": name,
                        "machine_name": machine_name,
                    }
                    print(f"Printer found: {name or machine_name or ip} at {ip} (mainboard: {mainboard_id})")
    except socket.timeout:
        pass
    finally:
        udp_socket.close()

    print(f"Discovery complete. Found {len(printers)} printer(s).")

def _request_payload(mainboard_id, cmd, data=None):
    """Build an SDCP request envelope. Returns (json_string, request_id)."""
    request_id = uuid.uuid4().hex
    payload = json.dumps({
        "Id": "",
        "Data": {
            "Cmd": cmd,
            "Data": data or {},
            "RequestID": request_id,
            "MainboardID": mainboard_id or "",
            "TimeStamp": int(time.time()),
            "From": 0,
        },
        "Topic": f"sdcp/request/{mainboard_id or ''}",
    })
    return payload, request_id

def connect_to_printer(ip):
    try:
        ws = create_connection(f"ws://{ip}:3030/websocket")
        ws.settimeout(5)
        with printers_lock:
            printers[ip]["ws"] = ws
            mb = printers[ip].get("mainboard_id")
        print(f"Connected to printer at {ip}")

        # Request attributes (cmd 1) once so /attributes is populated promptly.
        try:
            payload, _ = _request_payload(mb, 1)
            ws.send(payload)
        except Exception:
            pass

        last_refresh = 0.0
        while True:
            # Ask the printer to push a fresh status (cmd 0). When idle it emits
            # attributes rather than status, so without this the live status would
            # never arrive (or stay stale). Runs on connect and every 30s after.
            if time.time() - last_refresh > 30:
                try:
                    payload, _ = _request_payload(mb, 0)
                    ws.send(payload)
                except Exception:
                    pass
                last_refresh = time.time()

            try:
                message = ws.recv()
            except (WebSocketTimeoutException, socket.timeout):
                continue
            data = json.loads(message)
            topic = data.get("Topic", "")

            # The printer multiplexes message types on one socket. Only treat actual
            # status messages as status — attributes (cmd 1) would otherwise clobber it.
            if "status" in topic or "Status" in data:
                with printers_lock:
                    printers[ip]["status"] = data
                    mb_id = data.get("MainboardID") or data.get("Data", {}).get("MainboardID")
                    if mb_id:
                        printers[ip]["mainboard_id"] = mb_id
                        mb = mb_id
            elif "attributes" in topic or "Attributes" in data:
                attrs = data.get("Attributes", {})
                with printers_lock:
                    printers[ip]["attributes"] = data
                    if attrs.get("Name"):
                        printers[ip]["name"] = attrs["Name"]
                    if attrs.get("MachineName"):
                        printers[ip]["machine_name"] = attrs["MachineName"]
            # other topics (response, error, notice) are ignored
    except Exception as e:
        print(f"WebSocket error for {ip}: {e}")
    finally:
        with printers_lock:
            if ip in printers and printers[ip]["ws"]:
                printers[ip]["ws"].close()
                printers[ip]["ws"] = None

def _send_video_command(ip, mainboard_id, enable):
    """Send SDCP cmd 386 (enable/disable video stream). Returns the parsed response data."""
    payload, request_id = _request_payload(mainboard_id, 386, {"Enable": 1 if enable else 0})
    ws = create_connection(f"ws://{ip}:3030/websocket", timeout=10)
    try:
        ws.send(payload)
        deadline = time.time() + 10
        while time.time() < deadline:
            ws.settimeout(max(0.5, deadline - time.time()))
            msg = ws.recv()
            data = json.loads(msg)
            inner = data.get("Data", {})
            # Match either by RequestID echo or by Cmd 386 response
            if inner.get("RequestID") == request_id or inner.get("Cmd") == 386:
                return inner.get("Data", {})
        return None
    finally:
        try:
            ws.close()
        except Exception:
            pass

def _run_ffmpeg(video_url, timeout):
    """Grab a single JPEG keyframe via ffmpeg. Returns bytes, or None on failure/timeout."""
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-loglevel", "error",
        "-fflags", "+discardcorrupt",    # drop frames ffmpeg flags as corrupt
        "-rtsp_transport", "udp",        # Elegoo's camera rejects TCP ("Nonmatching transport")
        "-buffer_size", "8388608",       # big UDP buffer so a 1080p keyframe burst isn't dropped
                                         # (needs net.core.rmem_max raised; see install.sh)
        "-skip_frame", "nokey",          # decode keyframes only: intra-coded, can't smear
        "-i", video_url,
        "-frames:v", "1",
        "-f", "image2",
        "-c:v", "mjpeg",
        "-q:v", "3",
        "pipe:1",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None
    except FileNotFoundError:
        raise RuntimeError("ffmpeg is not installed")
    return result.stdout or None

# Above this ratio the bottom band is smeared (streaked); clean frames sit well below.
_STREAK_THRESHOLD = 1.2

def _frame_gray(jpg, w=128, h=72):
    """Decode JPEG bytes to a small grayscale buffer via ffmpeg (no numpy/PIL on the Pi).
    Returns a list of h rows, each bytes of length w, or None."""
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "quiet",
        "-i", "pipe:0", "-vf", f"scale={w}:{h},format=gray",
        "-f", "rawvideo", "pipe:1",
    ]
    try:
        result = subprocess.run(cmd, input=jpg, capture_output=True, timeout=10)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    raw = result.stdout
    if len(raw) < w * h:
        return None
    return [raw[r * w:(r + 1) * w] for r in range(h)]

def _streak_score(jpg, w=128, h=72):
    """Vertical-streak score for the bottom band (higher = more streaked).

    UDP slice loss makes the H.264 decoder repeat rows, giving vertical smear:
    low vertical change but sharp horizontal stripes. The ratio of horizontal to
    vertical adjacent-pixel change spikes for streaked frames and stays low both
    for clean content and for genuinely flat regions. Returns None if undecodable.
    """
    rows = _frame_gray(jpg, w, h)
    if rows is None:
        return None
    r0 = int(h * 0.60)  # bottom 40% — where lost slices land
    vsum = vcnt = 0
    for r in range(r0, h):
        if r == 0:
            continue
        prev, cur = rows[r - 1], rows[r]
        vsum += sum(abs(cur[c] - prev[c]) for c in range(w))
        vcnt += w
    hsum = hcnt = 0
    for r in range(r0, h):
        cur = rows[r]
        hsum += sum(abs(cur[c] - cur[c - 1]) for c in range(1, w))
        hcnt += w - 1
    band_vert = vsum / vcnt if vcnt else 0.0
    band_horiz = hsum / hcnt if hcnt else 0.0
    return band_horiz / (band_vert + 1.0)

def _grab_frame(video_url, timeout=30, attempts=4):
    """Grab a keyframe, rejecting bottom-streaked frames caused by UDP slice loss.

    Each candidate is scored for vertical smear; the first clean frame is
    returned, otherwise the least-streaked of N attempts.
    """
    best = None
    best_score = None
    for _ in range(attempts):
        jpg = _run_ffmpeg(video_url, timeout)
        if not jpg:
            continue
        score = _streak_score(jpg)
        if score is None:
            if best is None:        # can't analyze; keep as last-resort fallback
                best, best_score = jpg, float("inf")
            continue
        if score <= _STREAK_THRESHOLD:
            return jpg              # clean
        if best_score is None or score < best_score:
            best, best_score = jpg, score
    if best is None:
        raise RuntimeError(f"ffmpeg produced no frame after {attempts} attempts")
    return best

def capture_screenshot(ip, mainboard_id):
    """Enable the printer's video stream, grab one frame via ffmpeg, disable. Returns JPEG bytes."""
    response = _send_video_command(ip, mainboard_id, enable=True)
    if not response:
        raise RuntimeError("No response to video stream request")

    ack = response.get("Ack")
    if ack not in (0, None):
        # 1: stream limit exceeded, 2: camera doesn't exist, 3: unknown error
        reasons = {1: "stream limit exceeded", 2: "camera not present", 3: "unknown error"}
        raise RuntimeError(f"Printer refused video: {reasons.get(ack, f'ack={ack}')}")

    video_url = response.get("VideoUrl") or response.get("VideoURL") or f"rtsp://{ip}:554/video"

    try:
        return _grab_frame(video_url)
    finally:
        try:
            _send_video_command(ip, mainboard_id, enable=False)
        except Exception as e:
            print(f"Failed to disable video stream for {ip}: {e}")

def _printer_view(printer, field):
    return {
        "name": printer.get("name"),
        "machine_name": printer.get("machine_name"),
        "mainboard_id": printer.get("mainboard_id"),
        field: printer.get(field, {}),
    }

@app.route('/status', methods=['GET'])
def get_all_status():
    with printers_lock:
        if not printers:
            return jsonify({"error": "No printers discovered"}), 503
        result = {ip: _printer_view(p, "status") for ip, p in printers.items()}
    return jsonify(result)

@app.route('/status/<ip>', methods=['GET'])
def get_printer_status(ip):
    with printers_lock:
        printer = printers.get(ip)
        if printer is None:
            return jsonify({"error": f"Printer {ip} not found"}), 404
        view = _printer_view(printer, "status")
    if not view["status"]:
        return jsonify({"error": "No status available"}), 503
    return jsonify(view)

@app.route('/attributes', methods=['GET'])
def get_all_attributes():
    with printers_lock:
        if not printers:
            return jsonify({"error": "No printers discovered"}), 503
        result = {ip: _printer_view(p, "attributes") for ip, p in printers.items()}
    return jsonify(result)

@app.route('/attributes/<ip>', methods=['GET'])
def get_printer_attributes(ip):
    with printers_lock:
        printer = printers.get(ip)
        if printer is None:
            return jsonify({"error": f"Printer {ip} not found"}), 404
        view = _printer_view(printer, "attributes")
    if not view["attributes"]:
        return jsonify({"error": "No attributes available"}), 503
    return jsonify(view)

@app.route('/screenshot/<ip>', methods=['GET'])
def get_screenshot(ip):
    with printers_lock:
        printer = printers.get(ip)
    if printer is None:
        return jsonify({"error": f"Printer {ip} not found"}), 404

    mainboard_id = printer.get("mainboard_id")
    try:
        jpg = capture_screenshot(ip, mainboard_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    if jpg is None:
        return jsonify({"error": "Failed to capture frame from RTSP stream"}), 504

    return Response(jpg, mimetype="image/jpeg")

if __name__ == "__main__":
    discover_printers()
    with printers_lock:
        ips = list(printers.keys())
    for ip in ips:
        threading.Thread(target=connect_to_printer, args=(ip,), daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
