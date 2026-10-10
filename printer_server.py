import contextlib
import socket
import json
import queue
import re
import subprocess
import threading
import time
import uuid
from flask import Flask, jsonify, Response
from websocket import create_connection, WebSocketTimeoutException

app = Flask(__name__)

# {ip: {"status": {}, "attributes": {}, "ws": connection, "thread": Thread,
#       "mainboard_id": str, "name": str, "machine_name": str}}
printers = {}
printers_lock = threading.Lock()
# Serializes discovery so overlapping refreshes can't both reconcile the same replies.
discovery_lock = threading.Lock()

def discover_printers():
    """Broadcast the SDCP probe and collect replies until none arrive for 5s.

    Returns {ip: {"mainboard_id", "name", "machine_name"}} for every responder.
    """
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp_socket.settimeout(5)

    message = "M99999".encode('utf-8')
    udp_socket.sendto(message, ("<broadcast>", 3000))

    found = {}
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

            if ip not in found:
                found[ip] = {
                    "mainboard_id": mainboard_id,
                    "name": name,
                    "machine_name": machine_name,
                }
                print(f"Printer found: {name or machine_name or ip} at {ip} (mainboard: {mainboard_id})")
    except socket.timeout:
        pass
    finally:
        udp_socket.close()

    print(f"Discovery complete. Found {len(found)} printer(s).")
    return found

def _connection_alive(printer):
    thread = printer.get("thread")
    return thread is not None and thread.is_alive()

def _start_connection(ip):
    """Start the websocket thread for a known printer. Caller holds printers_lock."""
    thread = threading.Thread(target=connect_to_printer, args=(ip,), daemon=True)
    printers[ip]["thread"] = thread
    thread.start()

def refresh_printers():
    """Run discovery and reconcile the known printers with whoever answered.

    New responders are added and connected, and responders whose websocket thread
    has exited are reconnected. A printer that neither answered nor still holds a
    live connection is dropped: it's powered off or has moved to another IP.

    Returns {"printers": {ip: view}, "removed": {ip: view}}, where each kept
    printer's view carries a "state": "new", "reconnected", "active", or
    "no_reply" (kept because its connection is still up).
    """
    with discovery_lock:
        found = discover_printers()
        result = {"printers": {}, "removed": {}}
        with printers_lock:
            for ip, info in found.items():
                printer = printers.get(ip)
                if printer is None:
                    printers[ip] = {"status": {}, "attributes": {}, "ws": None, "thread": None, **info}
                    state = "new"
                else:
                    # A reply without names mustn't erase ones learned from attributes.
                    printer.update({k: v for k, v in info.items() if v})
                    state = "active" if _connection_alive(printer) else "reconnected"
                if state != "active":
                    if state == "reconnected":
                        print(f"Reconnecting to printer at {ip}")
                    _start_connection(ip)
                result["printers"][ip] = dict(_printer_view(printers[ip]), state=state)

            for ip in [ip for ip in printers if ip not in found]:
                view = _printer_view(printers[ip])
                if _connection_alive(printers[ip]):
                    result["printers"][ip] = dict(view, state="no_reply")
                else:
                    del printers[ip]
                    result["removed"][ip] = view
                    print(f"Dropped printer at {ip}: no discovery reply and no connection")
    return result

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

# How long one screenshot may watch the stream for an intact keyframe. The camera sends
# one every ~2s, and the first few of a session often arrive torn, so this allows ~25 tries.
_CAPTURE_SECONDS = 60
# ffmpeg sessions per screenshot, in case one dies (e.g. RTSP setup fails) before the deadline.
_CAPTURE_SESSIONS = 3

_SHOWINFO_RE = re.compile(rb"Parsed_showinfo.*\bn:\s*\d+\s+pts:.*\bs:(\d+)x(\d+)")
_CONCEAL_RE = re.compile(rb"concealing (\d+) DC")

def _frame_damage(stderr, damages):
    """Parse ffmpeg's log into each output frame's damage, in order, onto the `damages` queue.

    The decoder logs "concealing N DC, ..." when it patches over missing data, and ffmpeg
    logs "corrupt decoded frame" as it hands such a frame on. showinfo then logs the frame
    itself, so whatever was reported since the previous frame describes this one. Damage is
    the fraction of macroblocks concealed (0.0 = intact). Puts None at EOF.
    """
    concealed, corrupt = 0, False
    for line in stderr:
        if b"corrupt decoded frame" in line:
            corrupt = True
        elif m := _CONCEAL_RE.search(line):
            concealed = int(m.group(1))  # the probe decoder may report first; the last is ours
        elif m := _SHOWINFO_RE.search(line):
            total = (int(m.group(1)) // 16) * (int(m.group(2)) // 16)
            if concealed:
                damages.put(min(1.0, concealed / total))
            else:
                damages.put(1.0 if corrupt else 0.0)  # corrupt with no count: assume the worst
            concealed, corrupt = 0, False
    damages.put(None)

def _read_mpjpeg(stream):
    """Read the next JPEG from ffmpeg's mpjpeg (multipart) output. Returns bytes, or None at EOF."""
    length = None
    for line in iter(stream.readline, b""):
        line = line.strip()
        if line.lower().startswith(b"content-length:"):
            length = int(line.split(b":", 1)[1])
        elif not line and length is not None:
            data = stream.read(length)
            return data if len(data) == length else None
    return None

def _stream_keyframes(video_url, seconds):
    """Yield (jpeg_bytes, damage) for each keyframe one RTSP session delivers within `seconds`."""
    cmd = [
        "ffmpeg",
        "-nostdin", "-hide_banner", "-nostats",
        "-loglevel", "info",             # showinfo and the decoder's concealment reports
        "-rtsp_transport", "udp",        # Elegoo's camera rejects TCP ("Nonmatching transport")
        "-buffer_size", "8388608",       # big UDP buffer so a keyframe burst isn't dropped
                                         # (needs net.core.rmem_max raised; see install.sh)
        "-threads", "1",                 # keeps each decoder report ahead of its own frame
        "-skip_frame", "nokey",          # decode keyframes only: P-frames would inherit damage
        "-fpsprobesize", "0",            # don't probe 3 keyframes (~5s) just to guess the fps
        "-i", video_url,
        "-vf", "showinfo",               # one log line per frame, delimiting the reports
        "-fps_mode", "passthrough",      # one JPEG per keyframe, no duplicates
        "-c:v", "mjpeg", "-q:v", "3",
        "-f", "mpjpeg",                  # length-prefixed JPEGs...
        "-flush_packets", "1",           # ...each written out as soon as it's encoded
        "pipe:1",
    ]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError:
        raise RuntimeError("ffmpeg is not installed")
    damages = queue.Queue()
    threading.Thread(target=_frame_damage, args=(proc.stderr, damages), daemon=True).start()
    # SIGTERM lets ffmpeg send RTSP TEARDOWN, so the camera doesn't hold a dead session.
    timer = threading.Timer(seconds, proc.terminate)
    timer.start()
    try:
        while (jpg := _read_mpjpeg(proc.stdout)) is not None:
            damage = damages.get()
            yield jpg, 1.0 if damage is None else damage
    finally:
        timer.cancel()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

def _grab_frame(video_url):
    """Watch the stream for an intact keyframe. Returns (jpeg_bytes, damage).

    The camera's 720p keyframes are one ~100+ packet UDP burst, and on a weak WiFi link
    losing any packet tears the frame: everything below the gap is concealed as vertical
    streaks. Rather than guess from pixels, trust the decoder's own concealment reports,
    take the first keyframe it decodes whole, and fall back to the least damaged one.
    """
    deadline = time.monotonic() + _CAPTURE_SECONDS
    best, best_damage, tries = None, None, 0
    for _ in range(_CAPTURE_SESSIONS):
        remaining = deadline - time.monotonic()
        if remaining < 5:
            break
        with contextlib.closing(_stream_keyframes(video_url, remaining)) as frames:
            for jpg, damage in frames:
                tries += 1
                if damage == 0:
                    return jpg, 0.0
                if best is None or damage < best_damage:
                    best, best_damage = jpg, damage
    if best is None:
        raise RuntimeError("ffmpeg produced no frame")
    print(f"No intact keyframe from {video_url} in {tries} tries; "
          f"using the least damaged ({best_damage:.0%} concealed)")
    return best, best_damage

def capture_screenshot(ip, mainboard_id):
    """Enable the printer's video stream, grab one frame via ffmpeg, disable.

    Returns (jpeg_bytes, damage), damage being the fraction of the frame concealed.
    """
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

def _printer_view(printer, field=None):
    view = {
        "name": printer.get("name"),
        "machine_name": printer.get("machine_name"),
        "mainboard_id": printer.get("mainboard_id"),
    }
    if field:
        view[field] = printer.get(field, {})
    return view

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
        jpg, damage = capture_screenshot(ip, mainboard_id)
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    # Above 0, no intact frame arrived in time and this is the least damaged one.
    return Response(jpg, mimetype="image/jpeg", headers={"X-Frame-Damage": f"{damage:.3f}"})

@app.route('/refresh', methods=['POST'])
def refresh():
    """Re-run discovery now: pick up new printers, reconnect dropped ones, forget gone ones."""
    return jsonify(refresh_printers())

if __name__ == "__main__":
    refresh_printers()
    app.run(host="0.0.0.0", port=5000)
