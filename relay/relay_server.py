#!/usr/bin/env python3
"""Relay server — runs on the *printer* subnet.

It listens for a single tunnel connection from ``relay_client`` (on the client
subnet) and, over that one TCP link:

  * answers SDCP discovery by broadcasting ``M99999`` on the local subnet and
    returning each printer's real reply (cached, so clients get an instant
    answer well inside printer_server.py's 5 s discovery timeout);
  * opens TCP streams to printers (:3030 websocket, :554 RTSP) on demand and
    proxies their bytes;
  * for RTSP it rewrites SETUP so the printer streams RTP/RTCP to *this* host,
    then tunnels those datagrams back to the client.

Run::

    ./relay_server.py --listen 0.0.0.0:7000 --broadcast 192.168.20.255

Only UDP/3000 ``M99999`` traffic is touched, so it never reflects unrelated
broadcasts.
"""

import argparse
import base64
import json
import socket
import threading
import time

from relay_common import (
    DISCOVERY_PORT, PROBE, RTSP_PORT,
    T_DISCOVER_REQ, T_DISCOVER_REP, T_TCP_OPEN, T_TCP_DATA, T_TCP_CLOSE,
    T_UDP_DATA, T_PING,
    T_STATIC_TARGETS,
    Tunnel, MediaChan, read_frame, parse_chan, media_chan_id, alloc_udp_pair,
    enable_keepalive, split_rtsp, split_head_body, rtsp_method, get_cseq,
    transport_ports, set_client_port, strip_destination, rewrite_uri_host,
)


class PrinterChannel:
    """A TCP stream from the tunnel to one printer (websocket or RTSP)."""

    def __init__(self, server, chan_id, dst_ip, dst_port):
        self.server = server
        self.tunnel = server.tunnel
        self.chan_id = chan_id
        self.dst_ip = dst_ip
        self.is_rtsp = dst_port == RTSP_PORT
        self.sock = socket.create_connection((dst_ip, dst_port), timeout=10)
        self.sock.settimeout(None)
        # RTSP rewriting state (requests flow tunnel->printer, responses back).
        self._req_buf = b""        # bytes from tunnel awaiting full RTSP messages
        self._resp_buf = b""       # bytes from printer awaiting full RTSP messages
        self._setup_seq = 0
        self._cseq_to_seq = {}     # CSeq -> setup sequence number
        self._media = {}           # setup_seq -> {"rtcp_id":..} for printer-addr learning
        self._reader = threading.Thread(target=self._read_printer, daemon=True)
        self._reader.start()

    # -- printer -> tunnel ----------------------------------------------------
    def _read_printer(self):
        try:
            while True:
                data = self.sock.recv(65535)
                if not data:
                    break
                if self.is_rtsp:
                    self._on_printer_rtsp(data)
                else:
                    self.tunnel.send_chan(T_TCP_DATA, self.chan_id, data)
        except OSError:
            pass
        finally:
            self.server.close_channel(self.chan_id, notify=True)

    def _on_printer_rtsp(self, data):
        self._resp_buf += data
        msgs, self._resp_buf = split_rtsp(self._resp_buf)
        for msg in msgs:
            head, body = split_head_body(msg)
            # Learn where the printer wants RTCP sent (its server_port + source).
            cseq = get_cseq(head)
            seq = self._cseq_to_seq.get(cseq)
            sp = transport_ports(head, "server")
            if seq is not None and sp is not None:
                rtcp = self._media.get(seq, {}).get("rtcp")
                if rtcp is not None and rtcp.peer is None:
                    rtcp.peer = (self.dst_ip, sp[1])
            self.tunnel.send_chan(T_TCP_DATA, self.chan_id, msg)

    # -- tunnel -> printer ----------------------------------------------------
    def feed_from_tunnel(self, data):
        if not self.is_rtsp:
            self.sock.sendall(data)
            return
        self._req_buf += data
        msgs, self._req_buf = split_rtsp(self._req_buf)
        for msg in msgs:
            self.sock.sendall(self._rewrite_request(msg))

    def _rewrite_request(self, msg):
        head, body = split_head_body(msg)
        head = rewrite_uri_host(head, self.dst_ip)      # point the URL at the real printer
        if rtsp_method(msg) == b"SETUP":
            cp = transport_ports(head, "client")
            if cp is not None:
                seq = self._setup_seq
                self._setup_seq += 1
                cseq = get_cseq(head)
                if cseq is not None:
                    self._cseq_to_seq[cseq] = seq
                # Allocate our own RTP/RTCP ports and make the printer stream to us.
                (rtp_sock, rtp_port), (rtcp_sock, rtcp_port) = alloc_udp_pair(self.server.bind_ip)
                rtp_id = media_chan_id(self.chan_id, seq, False)
                rtcp_id = media_chan_id(self.chan_id, seq, True)
                rtp = MediaChan(self.tunnel, rtp_id, rtp_sock, peer=None, learn=True)
                rtcp = MediaChan(self.tunnel, rtcp_id, rtcp_sock, peer=None, learn=True)
                self.server.register_media(rtp_id, rtp)
                self.server.register_media(rtcp_id, rtcp)
                self._media[seq] = {"rtp": rtp, "rtcp": rtcp, "ids": (rtp_id, rtcp_id)}
                head = set_client_port(head, rtp_port, rtcp_port)
                head = strip_destination(head)
        return head + body

    def close(self):
        for st in self._media.values():
            for key in ("rtp", "rtcp"):
                self.server.unregister_media(st["ids"][0 if key == "rtp" else 1])
                st[key].close()
        try:
            self.sock.close()
        except OSError:
            pass


class RelayServer:
    def __init__(self, args):
        self.listen_host, self.listen_port = _split_hostport(args.listen, 7000)
        self.broadcast = args.broadcast
        self.bind_ip = args.bind_ip
        self.probe_interval = args.discovery_interval
        self.cache_ttl = args.cache_ttl
        self.tunnel = None
        self.channels = {}
        self.media = {}
        self.lock = threading.Lock()
        self.cache = {}            # ip -> (raw_reply_bytes, last_seen)
        self.cache_lock = threading.Lock()
        self.static_targets = set()  # printer IPs to unicast-probe (set by the client)

    # -- discovery ------------------------------------------------------------
    def discovery_loop(self):
        while True:
            try:
                self.probe_once(timeout=2.0)
            except Exception as exc:           # keep the loop alive no matter what
                print("discovery probe failed:", exc)
            self._expire_cache()
            time.sleep(self.probe_interval)

    def probe_once(self, timeout=2.0):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(timeout)
        try:
            try:
                sock.sendto(PROBE, (self.broadcast, DISCOVERY_PORT))
            except OSError as exc:
                print("broadcast probe failed:", exc)
            # Unicast-probe any explicitly-mapped printers (reachable even when
            # broadcast can't get to them, e.g. a different subnet on this side).
            for ip in list(self.static_targets):
                try:
                    sock.sendto(PROBE, (ip, DISCOVERY_PORT))
                except OSError:
                    pass
            deadline = time.time() + timeout
            while time.time() < deadline:
                sock.settimeout(max(0.05, deadline - time.time()))
                try:
                    resp, addr = sock.recvfrom(2048)
                except socket.timeout:
                    break
                if not _looks_like_sdcp(resp):
                    continue           # ignore anything that isn't a printer reply
                with self.cache_lock:
                    self.cache[addr[0]] = (resp, time.time())
        finally:
            sock.close()

    def _expire_cache(self):
        now = time.time()
        with self.cache_lock:
            stale = [ip for ip, (_, ts) in self.cache.items() if now - ts > self.cache_ttl]
            for ip in stale:
                del self.cache[ip]

    def handle_discover_req(self, req_id):
        # Kick a fresh probe so newly-powered printers show up, but answer the
        # client immediately from cache to stay inside its discovery timeout.
        threading.Thread(target=self._probe_and_resend, args=(req_id,), daemon=True).start()
        self._send_cached(req_id)

    def _probe_and_resend(self, req_id):
        try:
            self.probe_once(timeout=2.0)
        except Exception:
            return
        self._send_cached(req_id)

    def _send_cached(self, req_id):
        with self.cache_lock:
            items = list(self.cache.items())
        if not self.tunnel:
            return
        for ip, (raw, _) in items:
            self.tunnel.send_json(T_DISCOVER_REP, {
                "req_id": req_id,
                "ip": ip,
                "raw": base64.b64encode(raw).decode("ascii"),
            })

    # -- channel / media registries ------------------------------------------
    def register_media(self, chan_id, mc):
        with self.lock:
            self.media[chan_id] = mc

    def unregister_media(self, chan_id):
        with self.lock:
            self.media.pop(chan_id, None)

    def close_channel(self, chan_id, notify):
        with self.lock:
            ch = self.channels.pop(chan_id, None)
        if ch is None:
            return
        if notify and self.tunnel:
            try:
                self.tunnel.send_chan(T_TCP_CLOSE, chan_id, b"")
            except OSError:
                pass
        ch.close()

    # -- tunnel ---------------------------------------------------------------
    def serve(self):
        threading.Thread(target=self.discovery_loop, daemon=True).start()
        ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ls.bind((self.listen_host, self.listen_port))
        ls.listen(1)
        print("relay_server listening for tunnel on %s:%d" % (self.listen_host, self.listen_port))
        while True:
            conn, addr = ls.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # Blocking reads with keepalive: notice a dead client without a plain
            # recv timeout (which would otherwise tear down a healthy idle tunnel).
            enable_keepalive(conn)
            print("tunnel connected from", addr)
            self.tunnel = Tunnel(conn)
            try:
                self._tunnel_loop(conn)
            except Exception as exc:
                print("tunnel error:", exc)
            finally:
                self._teardown()
                print("tunnel closed")

    def _tunnel_loop(self, conn):
        while True:
            frame = read_frame(conn)
            if frame is None:
                break
            typ, body = frame
            if typ == T_DISCOVER_REQ:
                obj = json.loads(body)
                self.handle_discover_req(obj.get("req_id"))
            elif typ == T_STATIC_TARGETS:
                obj = json.loads(body)
                self.static_targets = set(obj.get("ips", []))
                # Probe them right away so they're cached before the next request.
                threading.Thread(target=self._safe_probe, daemon=True).start()
            elif typ == T_TCP_OPEN:
                obj = json.loads(body)
                self._open(obj["chan"], obj["ip"], obj["port"])
            elif typ == T_TCP_DATA:
                chan, payload = parse_chan(body)
                with self.lock:
                    ch = self.channels.get(chan)
                if ch:
                    try:
                        ch.feed_from_tunnel(payload)
                    except OSError:
                        self.close_channel(chan, notify=True)
            elif typ == T_TCP_CLOSE:
                chan, _ = parse_chan(body)
                self.close_channel(chan, notify=False)
            elif typ == T_UDP_DATA:
                chan, payload = parse_chan(body)
                with self.lock:
                    mc = self.media.get(chan)
                if mc:
                    mc.on_tunnel_data(payload)
            elif typ == T_PING:
                pass

    def _open(self, chan, ip, port):
        try:
            ch = PrinterChannel(self, chan, ip, port)
        except OSError as exc:
            print("connect to %s:%d failed: %s" % (ip, port, exc))
            if self.tunnel:
                self.tunnel.send_chan(T_TCP_CLOSE, chan, b"")
            return
        with self.lock:
            self.channels[chan] = ch

    def _safe_probe(self):
        try:
            self.probe_once(timeout=2.0)
        except Exception as exc:
            print("static probe failed:", exc)

    def _teardown(self):
        with self.lock:
            chans = list(self.channels.values())
            self.channels.clear()
            self.media.clear()
        for ch in chans:
            ch.close()
        self.static_targets = set()
        self.tunnel = None


def _looks_like_sdcp(resp):
    try:
        payload = json.loads(resp.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return False
    data = payload.get("Data", payload) if isinstance(payload, dict) else {}
    return isinstance(data, dict) and (
        "MainboardID" in data or "MachineName" in data or "Name" in data
    )


def _split_hostport(s, default_port):
    if ":" in s:
        host, port = s.rsplit(":", 1)
        return host or "0.0.0.0", int(port)
    return s, default_port


def main():
    ap = argparse.ArgumentParser(description="SDCP printer relay — printer-subnet side")
    ap.add_argument("--listen", default="0.0.0.0:7000",
                    help="host:port to accept the tunnel on (default 0.0.0.0:7000)")
    ap.add_argument("--broadcast", required=True,
                    help="broadcast address of the printer subnet, e.g. 192.168.20.255")
    ap.add_argument("--bind-ip", default="0.0.0.0",
                    help="local IP to bind relayed media (RTP) sockets to (default 0.0.0.0)")
    ap.add_argument("--discovery-interval", type=float, default=10.0,
                    help="seconds between background discovery probes (default 10)")
    ap.add_argument("--cache-ttl", type=float, default=60.0,
                    help="drop a printer from the cache after this many seconds unseen")
    RelayServer(ap.parse_args()).serve()


if __name__ == "__main__":
    main()
