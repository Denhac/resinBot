"""Shared protocol/tunnel helpers for the SDCP printer relay.

The relay bridges two subnets that have **no IP routing between them** — the only
path is a single TCP "tunnel" between ``relay_client`` (on the client subnet, with
printer_server.py and friends) and ``relay_server`` (on the printer subnet).

Everything printer_server.py needs is multiplexed over that one tunnel:

  * SDCP discovery   - UDP broadcast ``M99999`` -> :3000 and the unicast JSON replies
  * SDCP control     - WebSocket TCP :3030
  * camera / video   - RTSP TCP :554 plus its negotiated RTP/RTCP UDP streams

The tunnel carries length-prefixed frames::

    +----------------+--------+------------------+
    | uint32 length  | type   | body (length-1)  |
    +----------------+--------+------------------+

``length`` counts ``type`` + ``body``. Control frames carry JSON bodies; stream
frames carry a uint32 channel id followed by the raw payload bytes.
"""

import json
import re
import socket
import struct
import threading

# --- well-known SDCP ports ---------------------------------------------------
DISCOVERY_PORT = 3000          # UDP: broadcast probe + unicast replies
WS_PORT = 3030                 # TCP: SDCP websocket control channel
RTSP_PORT = 554                # TCP: RTSP control (camera)
PROBE = b"M99999"              # the exact SDCP discovery probe payload

# --- tunnel frame types ------------------------------------------------------
T_DISCOVER_REQ = 0x01          # client->server: please (re)broadcast a probe
T_DISCOVER_REP = 0x02          # server->client: one discovered printer
T_TCP_OPEN = 0x10              # client->server: open a TCP stream to a printer
T_TCP_DATA = 0x11              # both ways: bytes on a TCP stream
T_TCP_CLOSE = 0x12             # both ways: a TCP stream ended
T_UDP_DATA = 0x20              # both ways: one datagram on a media (RTP/RTCP) channel
T_PING = 0x30                  # keepalive


def enable_keepalive(sock, idle=30, intvl=10, cnt=3):
    """Turn on TCP keepalive so a silently-dead tunnel peer is detected.

    Used on the long-lived tunnel sockets, whose reads block indefinitely; without
    keepalive a peer that vanishes without a FIN/RST would never be noticed.
    """
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, intvl)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, cnt)
    except OSError:
        pass


def recvall(sock, n):
    """Read exactly ``n`` bytes from ``sock`` or return None on EOF."""
    chunks = []
    got = 0
    while got < n:
        try:
            chunk = sock.recv(n - got)
        except (OSError, socket.timeout):
            return None
        if not chunk:
            return None
        chunks.append(chunk)
        got += len(chunk)
    return b"".join(chunks)


class Tunnel:
    """Thread-safe framed writer over a single TCP socket.

    Many worker threads (one per proxied connection) write to the same tunnel,
    so every send is guarded by a lock. Reading is done by a single dedicated
    loop in the owner, via :func:`read_frame`.
    """

    def __init__(self, sock):
        self.sock = sock
        self._lock = threading.Lock()

    def _send(self, typ, body=b""):
        frame = struct.pack(">B", typ) + body
        data = struct.pack(">I", len(frame)) + frame
        with self._lock:
            self.sock.sendall(data)

    def send_json(self, typ, obj):
        self._send(typ, json.dumps(obj).encode("utf-8"))

    def send_chan(self, typ, chan, payload):
        self._send(typ, struct.pack(">I", chan) + payload)

    def send_bare(self, typ):
        self._send(typ)

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def read_frame(sock):
    """Read one frame. Returns ``(type, body)`` or None on EOF.

    ``body`` is the bytes after the type byte. Callers decode it per type:
    JSON for control frames, ``uint32 chan + payload`` for stream frames.
    """
    hdr = recvall(sock, 4)
    if hdr is None:
        return None
    (n,) = struct.unpack(">I", hdr)
    if n == 0:
        return None
    frame = recvall(sock, n)
    if frame is None:
        return None
    return frame[0], frame[1:]


def parse_chan(body):
    """Split a stream-frame body into ``(chan_id, payload)``."""
    (chan,) = struct.unpack(">I", body[:4])
    return chan, body[4:]


def media_chan_id(tcp_chan, setup_seq, is_rtcp):
    """Deterministic id for one RTP/RTCP relay path.

    Both relay ends watch the same RTSP byte stream and see SETUP requests in the
    same order, so they can agree on media-channel ids without any extra
    handshake: derive them from the owning RTSP TCP channel, the SETUP sequence
    number on that channel, and whether this is the RTP or RTCP half of the pair.
    """
    return (tcp_chan << 12) | (setup_seq << 1) | (1 if is_rtcp else 0)


# --- RTSP message framing / rewriting ---------------------------------------
#
# RTSP is HTTP-like: a request/status line, headers, a blank line, then an
# optional body sized by Content-Length (the SDP in a DESCRIBE response). We
# only ever need to touch the Transport header and host names, so we frame whole
# messages and rewrite their header block textually.

_CONTENT_LEN_RE = re.compile(rb"(?im)^content-length:\s*(\d+)\s*$")


def split_rtsp(buf):
    """Pull complete RTSP messages out of ``buf``.

    Returns ``(messages, remaining)`` where each message is the full bytes of a
    request/response (headers + body) and ``remaining`` is the partial tail.
    """
    messages = []
    while True:
        idx = buf.find(b"\r\n\r\n")
        if idx == -1:
            break
        head = buf[: idx + 4]
        m = _CONTENT_LEN_RE.search(head)
        blen = int(m.group(1)) if m else 0
        end = idx + 4 + blen
        if len(buf) < end:
            break
        messages.append(buf[:end])
        buf = buf[end:]
    return messages, buf


def split_head_body(msg):
    idx = msg.find(b"\r\n\r\n")
    if idx == -1:
        return msg, b""
    return msg[: idx + 4], msg[idx + 4:]


_RTSP_URI_RE = re.compile(rb"rtsp://([^/:\s]+)")
_CSEQ_RE = re.compile(rb"(?im)^cseq:\s*(\d+)\s*$")
_TRANSPORT_RE = re.compile(rb"(?im)^(transport:.*)$")
_CLIENT_PORT_RE = re.compile(rb"client_port=(\d+)(?:-(\d+))?")
_SERVER_PORT_RE = re.compile(rb"server_port=(\d+)(?:-(\d+))?")
_SOURCE_RE = re.compile(rb"source=([^;\r\n]+)")
_DEST_RE = re.compile(rb"destination=([^;\r\n]+)")


def rtsp_method(msg):
    """First token of the request line (e.g. ``b'SETUP'``), or None for responses."""
    line = msg.split(b"\r\n", 1)[0]
    if line.startswith(b"RTSP/"):
        return None
    return line.split(b" ", 1)[0].upper()


def transport_ports(head, which):
    """Return ``(lo, hi)`` ints for client_port/server_port in a Transport header."""
    rx = _CLIENT_PORT_RE if which == "client" else _SERVER_PORT_RE
    m = rx.search(head)
    if not m:
        return None
    lo = int(m.group(1))
    hi = int(m.group(2)) if m.group(2) else lo + 1
    return lo, hi


def set_client_port(head, lo, hi):
    return _CLIENT_PORT_RE.sub(("client_port=%d-%d" % (lo, hi)).encode(), head)


def set_server_port(head, lo, hi):
    return _SERVER_PORT_RE.sub(("server_port=%d-%d" % (lo, hi)).encode(), head)


def set_source(head, ip):
    rep = ("source=%s" % ip).encode()
    if _SOURCE_RE.search(head):
        return _SOURCE_RE.sub(rep, head)
    # No source param present: append one to the Transport line.
    return _TRANSPORT_RE.sub(lambda m: m.group(1) + b";" + rep, head)


def strip_destination(head):
    """Drop any ``destination=`` param (its value is meaningless across the relay)."""
    return _DEST_RE.sub(b"destination=0.0.0.0", head)


def replace_host(head, old, new):
    """Swap a bare IP literal for another wherever it appears in the header block."""
    if not old:
        return head
    return head.replace(old.encode(), new.encode())


def rewrite_uri_host(head, new_ip):
    """Replace the host in any ``rtsp://host[:port]/path`` URI, keeping port/path."""
    return _RTSP_URI_RE.sub(b"rtsp://" + new_ip.encode(), head)


def get_cseq(head):
    """Return the integer CSeq of an RTSP message, or None.

    Requests and their responses share a CSeq, so it is the reliable way to pair
    a SETUP response back to the SETUP request that allocated its media ports.
    """
    m = _CSEQ_RE.search(head)
    return int(m.group(1)) if m else None


def alloc_udp_pair(bind_ip):
    """Bind a consecutive (even RTP, odd RTCP) UDP port pair on ``bind_ip``.

    RTP requires the RTCP port to be RTP+1, so we keep retrying until the kernel
    hands us an even port whose successor is also free. Returns
    ``((rtp_sock, rtp_port), (rtcp_sock, rtcp_port))``.
    """
    for _ in range(200):
        rtp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rtp.bind((bind_ip, 0))
        port = rtp.getsockname()[1]
        if port % 2 != 0:
            rtp.close()
            continue
        rtcp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            rtcp.bind((bind_ip, port + 1))
        except OSError:
            rtp.close()
            rtcp.close()
            continue
        return (rtp, port), (rtcp, port + 1)
    raise RuntimeError("could not allocate an RTP/RTCP port pair on %s" % bind_ip)


def ws_replace_in_stream(buf, old, new):
    """Rewrite ``old`` -> ``new`` inside unmasked WebSocket *text* frames.

    The printer's SDCP websocket replies (cmd 386) carry a ``VideoUrl`` with the
    printer's real IP — useless to a client that can only reach it via an alias.
    Server->client frames are unmasked, so we can decode each text frame, swap the
    address, and re-emit it with a corrected length. Masked (client->server),
    binary, control, and fragmented frames pass through untouched. Returns
    ``(output_bytes, remaining_buf)``; ``remaining_buf`` holds a partial frame.

    Assumes no permessage-deflate (SDCP printers negotiate none); a compressed
    payload would simply pass through unrewritten rather than corrupt.
    """
    out = bytearray()
    while True:
        if len(buf) < 2:
            break
        b0, b1 = buf[0], buf[1]
        masked = b1 & 0x80
        ln = b1 & 0x7F
        idx = 2
        if ln == 126:
            if len(buf) < 4:
                break
            ln = struct.unpack(">H", buf[2:4])[0]
            idx = 4
        elif ln == 127:
            if len(buf) < 10:
                break
            ln = struct.unpack(">Q", buf[2:10])[0]
            idx = 10
        if masked:
            idx += 4
        total = idx + ln
        if len(buf) < total:
            break
        opcode = b0 & 0x0F
        if not masked and opcode == 0x1 and old:               # unmasked text frame
            payload = bytes(buf[idx:total])                     # no mask key when unmasked
            new_payload = payload.replace(old.encode(), new.encode())
            out += _ws_text_header(b0, len(new_payload)) + new_payload
        else:
            out += buf[:total]
        buf = buf[total:]
    return bytes(out), buf


def _ws_text_header(b0, length):
    if length < 126:
        return bytes([b0, length])
    if length < 65536:
        return bytes([b0, 126]) + struct.pack(">H", length)
    return bytes([b0, 127]) + struct.pack(">Q", length)


class MediaChan:
    """One relayed UDP path (an RTP or RTCP stream).

    A datagram arriving on the local ``sock`` is forwarded over the tunnel tagged
    with ``chan_id``; a datagram arriving from the tunnel is sent out ``sock`` to
    ``peer``. ``peer`` may be known up front (the side facing ffmpeg knows where
    to send) or learned from the first inbound datagram (the side facing the
    printer learns the printer's source).
    """

    def __init__(self, tunnel, chan_id, sock, peer=None, learn=True):
        self.tunnel = tunnel
        self.chan_id = chan_id
        self.sock = sock
        self.peer = peer
        self.learn = learn
        self._stop = False
        self._thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._thread.start()

    def _recv_loop(self):
        while not self._stop:
            try:
                data, addr = self.sock.recvfrom(65535)
            except OSError:
                return
            if not data:
                continue
            if self.learn:
                self.peer = addr
            try:
                self.tunnel.send_chan(T_UDP_DATA, self.chan_id, data)
            except OSError:
                return

    def on_tunnel_data(self, data):
        if self.peer is None:
            return
        try:
            self.sock.sendto(data, self.peer)
        except OSError:
            pass

    def close(self):
        self._stop = True
        try:
            self.sock.close()
        except OSError:
            pass
