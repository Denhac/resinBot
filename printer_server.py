import socket
import json
import subprocess
import threading
import time
import uuid
from flask import Flask, jsonify, Response
from websocket import create_connection

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

def connect_to_printer(ip):
    try:
        ws = create_connection(f"ws://{ip}:3030/websocket")
        with printers_lock:
            printers[ip]["ws"] = ws
        print(f"Connected to printer at {ip}")

        while True:
            message = ws.recv()
            data = json.loads(message)
            mainboard_id = (
                data.get("Data", {}).get("MainboardID")
                or data.get("MainboardID")
            )
            with printers_lock:
                printers[ip]["status"] = data
                if mainboard_id:
                    printers[ip]["mainboard_id"] = mainboard_id
    except Exception as e:
        print(f"WebSocket error for {ip}: {e}")
    finally:
        with printers_lock:
            if ip in printers and printers[ip]["ws"]:
                printers[ip]["ws"].close()
                printers[ip]["ws"] = None

def _send_video_command(ip, mainboard_id, enable):
    """Send SDCP cmd 386 (enable/disable video stream). Returns the parsed response data."""
    request_id = uuid.uuid4().hex
    payload = {
        "Id": "",
        "Data": {
            "Cmd": 386,
            "Data": {"Enable": 1 if enable else 0},
            "RequestID": request_id,
            "MainboardID": mainboard_id or "",
            "TimeStamp": int(time.time()),
            "From": 0,
        },
        "Topic": f"sdcp/request/{mainboard_id or ''}",
    }

    ws = create_connection(f"ws://{ip}:3030/websocket", timeout=10)
    try:
        ws.send(json.dumps(payload))
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

def _grab_frame(video_url, timeout=30, attempts=4):
    """Grab a keyframe, keeping the most complete of N tries.

    A keyframe torn by UDP packet loss compresses smaller, since lost slices
    become flat concealment with little detail. Across keyframes of the same
    near-static print scene the largest is the most intact, so best-of-N by
    byte size is a cheap way to skip the occasional dropped-slice frame.
    """
    best = None
    for _ in range(attempts):
        jpg = _run_ffmpeg(video_url, timeout)
        if jpg and (best is None or len(jpg) > len(best)):
            best = jpg
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

def _printer_view(printer):
    return {
        "name": printer.get("name"),
        "machine_name": printer.get("machine_name"),
        "mainboard_id": printer.get("mainboard_id"),
        "status": printer.get("status", {}),
    }

@app.route('/status', methods=['GET'])
def get_all_status():
    with printers_lock:
        if not printers:
            return jsonify({"error": "No printers discovered"}), 503
        result = {ip: _printer_view(p) for ip, p in printers.items()}
    return jsonify(result)

@app.route('/status/<ip>', methods=['GET'])
def get_printer_status(ip):
    with printers_lock:
        printer = printers.get(ip)
        if printer is None:
            return jsonify({"error": f"Printer {ip} not found"}), 404
        view = _printer_view(printer)
    if not view["status"]:
        return jsonify({"error": "No status available"}), 503
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
