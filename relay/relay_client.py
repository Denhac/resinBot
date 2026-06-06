#!/usr/bin/env python3
"""Relay client — runs on the *client* subnet (with printer_server.py).

It makes printers on a remote subnet look local. For each printer discovered
through the tunnel it claims a distinct **alias IP** on the local subnet and
presents that printer there: discovery replies appear to come from the alias,
and the printer's websocket (:3030) and RTSP camera (:554) are served on the
alias and tunneled to the real printer. printer_server.py keys everything off the
alias IP and never needs to know the printer is on another subnet.

Run (needs root for :554 and for adding alias IPs)::

    sudo ./relay_client.py --server 10.0.0.9:7000 \
        --iface eth0 --alias-cidr 192.168.1.200-192.168.1.209/24 --manage-aliases

Only UDP/3000 ``M99999`` probes are intercepted, so unrelated broadcasts are
ignored.
"""

import argparse
import atexit
import base64
import itertools
import json
import signal
import socket
import subprocess
import threading
import time

from relay_common import (
    DISCOVERY_PORT, PROBE, WS_PORT, RTSP_PORT,
    T_DISCOVER_REQ, T_DISCOVER_REP, T_STATIC_TARGETS, T_TCP_OPEN, T_TCP_DATA,
    T_TCP_CLOSE, T_UDP_DATA, T_PING,
    Tunnel, MediaChan, read_frame, parse_chan, media_chan_id, alloc_udp_pair,
    enable_keepalive, split_rtsp, split_head_body, rtsp_method, get_cseq,
    transport_ports, set_client_port, set_server_port, set_source,
    strip_destination, ws_replace_in_stream,
)


class ClientChannel:
    """A local TCP connection from a client app, tunneled to one printer.

    For websocket channels the printer->client direction is reframed so the
    printer's real IP becomes the alias (the cmd 386 ``VideoUrl``). For RTSP the
    SETUP handshake is rewritten so the camera's RTP/RTCP arrive here and reach
    the app at the alias.
    """

    def __init__(self, client, chan_id, sock, real_ip, alias_ip, is_rtsp):
        self.client = client
        self.tunnel = client.tunnel
        self.chan_id = chan_id
        self.sock = sock
        self.real_ip = real_ip
        self.alias_ip = alias_ip
        self.is_rtsp = is_rtsp
        self.peer_ip = sock.getpeername()[0]      # the app (e.g. printer_server) host
        self._ws_buf = b""                        # printer->app, awaiting full WS frames
        self._ws_ready = False                    # seen end of the HTTP 101 upgrade yet?
        self._req_buf = b""                       # app->printer RTSP requests
        self._resp_buf = b""                      # printer->app RTSP responses
        self._setup_seq = 0
        self._cseq_to_seq = {}
        self._state = {}                          # setup_seq -> ports + media ids
        self._reader = threading.Thread(target=self._read_local, daemon=True)
        self._reader.start()

    # -- app -> tunnel --------------------------------------------------------
    def _read_local(self):
        try:
            while True:
                data = self.sock.recv(65535)
                if not data:
                    break
                if self.is_rtsp:
                    self._on_app_rtsp(data)
                else:
                    self.tunnel.send_chan(T_TCP_DATA, self.chan_id, data)
        except OSError:
            pass
        finally:
            self.client.close_channel(self.chan_id, notify=True)

    def _on_app_rtsp(self, data):
        # Parse requests to allocate our alias-side RTP ports, but forward the
        # bytes unchanged — relay_server rewrites client_port for the printer.
        self._req_buf += data
        msgs, self._req_buf = split_rtsp(self._req_buf)
        for msg in msgs:
            if rtsp_method(msg) == b"SETUP":
                self._alloc_app_media(msg)
            self.tunnel.send_chan(T_TCP_DATA, self.chan_id, msg)

    def _alloc_app_media(self, msg):
        head, _ = split_head_body(msg)
        cp = transport_ports(head, "client")
        if cp is None:
            return
        seq = self._setup_seq
        self._setup_seq += 1
        cseq = get_cseq(head)
        if cseq is not None:
            self._cseq_to_seq[cseq] = seq
        (rtp_sock, rtp_port), (rtcp_sock, rtcp_port) = alloc_udp_pair(self.alias_ip)
        rtp_id = media_chan_id(self.chan_id, seq, False)
        rtcp_id = media_chan_id(self.chan_id, seq, True)
        # RTP flows printer->app only; RTCP is two-way. Targets are the app's
        # advertised client_port at the app host — known now, so no learning.
        rtp = MediaChan(self.tunnel, rtp_id, rtp_sock, peer=(self.peer_ip, cp[0]), learn=False)
        rtcp = MediaChan(self.tunnel, rtcp_id, rtcp_sock, peer=(self.peer_ip, cp[1]), learn=False)
        self.client.register_media(rtp_id, rtp)
        self.client.register_media(rtcp_id, rtcp)
        self._state[seq] = {
            "app": cp, "local": (rtp_port, rtcp_port), "ids": (rtp_id, rtcp_id),
        }

    # -- tunnel -> app --------------------------------------------------------
    def feed_from_tunnel(self, data):
        if not self.is_rtsp:
            self._ws_buf += data
            if not self._ws_ready:
                # WebSocket frames only begin after the printer's HTTP "101
                # Switching Protocols" response. Forward that handshake verbatim;
                # if we fed it through the frame rewriter it would mis-read the
                # HTTP text as frames and desync from the real ones that follow.
                end = self._ws_buf.find(b"\r\n\r\n")
                if end == -1:
                    return
                self.sock.sendall(self._ws_buf[:end + 4])
                self._ws_buf = self._ws_buf[end + 4:]
                self._ws_ready = True
            out, self._ws_buf = ws_replace_in_stream(self._ws_buf, self.real_ip, self.alias_ip)
            if out:
                self.sock.sendall(out)
            return
        self._resp_buf += data
        msgs, self._resp_buf = split_rtsp(self._resp_buf)
        for msg in msgs:
            self.sock.sendall(self._rewrite_response(msg))

    def _rewrite_response(self, msg):
        # Make the whole reply (headers + SDP) refer to the alias, then fix the
        # SETUP Transport ports back to what the app expects to talk to us on.
        msg = msg.replace(self.real_ip.encode(), self.alias_ip.encode())
        head, body = split_head_body(msg)
        cseq = get_cseq(head)
        seq = self._cseq_to_seq.get(cseq)
        if seq is not None and transport_ports(head, "server") is not None:
            st = self._state.get(seq)
            if st:
                head = set_client_port(head, st["app"][0], st["app"][1])
                head = set_server_port(head, st["local"][0], st["local"][1])
                head = set_source(head, self.alias_ip)
                head = strip_destination(head)
        return head + body

    def close(self):
        for st in self._state.values():
            for i, key in ((0, "rtp"), (1, "rtcp")):
                self.client.unregister_media(st["ids"][i])
        try:
            self.sock.close()
        except OSError:
            pass


class RelayClient:
    def __init__(self, args):
        self.server_host, self.server_port = _split_hostport(args.server, 7000)
        self.iface = args.iface
        self.manage = args.manage_aliases
        self.local_broadcast = args.local_broadcast
        self.alias_pool, self.prefix = _parse_alias_cidr(args.alias_cidr) if args.alias_cidr else ([], 24)
        self.static_maps = _parse_maps(args.map)   # {real_ip: alias or None}
        self.tunnel = None
        self.tunnel_lock = threading.Lock()
        self.channels = {}
        self.media = {}
        self.lock = threading.Lock()
        self._chan_ids = itertools.count(1)
        self.real_to_alias = {}
        self.alias_to_real = {}
        self.free_aliases = list(self.alias_pool)
        self.added_aliases = []    # alias IPs we put on the interface (to remove on exit)
        self.pending = {}          # req_id -> (requester_ip, requester_port, ts)
        self._req_ids = itertools.count(1)
        if self.manage:
            # Take the alias IPs back down on exit (Ctrl-C, kill, or normal).
            atexit.register(self._remove_aliases)
            signal.signal(signal.SIGINT, self._signal_exit)
            signal.signal(signal.SIGTERM, self._signal_exit)

    # -- discovery interception ----------------------------------------------
    def _on_probe(self, ip, port):
        """A client sent an SDCP probe from ip:port — forward a discovery request."""
        req_id = next(self._req_ids)
        self.pending[req_id] = (ip, port, time.time())
        self._prune_pending()
        with self.tunnel_lock:
            t = self.tunnel
        if t:
            try:
                t.send_json(T_DISCOVER_REQ, {"req_id": req_id})
            except OSError:
                pass

    def discovery_listener(self):
        """Catch probes that arrive over the wire (clients on other hosts)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", DISCOVERY_PORT))
        print("intercepting SDCP probes on udp/%d" % DISCOVERY_PORT)
        while True:
            data, addr = sock.recvfrom(2048)
            if data.strip() != PROBE:
                continue           # only the SDCP probe — never other broadcasts
            self._on_probe(addr[0], addr[1])

    def local_broadcast_sniffer(self):
        """Catch probes sent by software on THIS host.

        A broadcast a host transmits is never looped back to its own udp/3000
        listener, so the socket above can't see a same-box client's probe. An
        AF_PACKET tap sees frames at the device, including ones we send, so we
        pick the probe off the wire as it leaves and learn the sender's port.
        """
        PACKET_OUTGOING = getattr(socket, "PACKET_OUTGOING", 4)
        ETH_P_IP = 0x0800
        try:
            s = socket.socket(socket.AF_PACKET, socket.SOCK_DGRAM, socket.htons(ETH_P_IP))
            if self.iface:
                s.bind((self.iface, 0))
        except (AttributeError, OSError) as exc:
            print("same-host probe capture unavailable (%s) — need root/Linux; "
                  "same-box discovery disabled" % exc)
            return
        print("capturing locally-sent SDCP probes on %s (same-host mode)"
              % (self.iface or "all interfaces"))
        while True:
            try:
                data, addr = s.recvfrom(65535)
            except OSError:
                return
            if addr[2] != PACKET_OUTGOING:     # only probes WE transmit
                continue
            probe = _parse_udp_probe(data)
            if probe is not None:
                self._on_probe(*probe)

    def _prune_pending(self):
        now = time.time()
        for rid in [r for r, (_, _, ts) in self.pending.items() if now - ts > 15]:
            del self.pending[rid]

    def handle_discover_rep(self, obj):
        req_id = obj.get("req_id")
        ip = obj["ip"]
        if ip in self.alias_to_real:
            return         # one of our own alias IPs reflected back — not a printer
        raw = base64.b64decode(obj["raw"])
        alias = self.ensure_alias(ip)
        if alias is None:
            print("alias pool exhausted; cannot map printer", ip)
            return
        requester = self.pending.get(req_id)
        if not requester:
            return
        # Deliver the printer's real reply, but sourced from the alias so the app
        # records the alias as the printer's address. Send from :3000 like a real
        # printer where possible, falling back to an ephemeral port.
        out = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        out.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            try:
                out.bind((alias, DISCOVERY_PORT))
            except OSError:
                out.bind((alias, 0))
            out.sendto(raw, (requester[0], requester[1]))
        except OSError as exc:
            print("failed to deliver discovery reply via %s: %s" % (alias, exc))
        finally:
            out.close()

    # -- alias management -----------------------------------------------------
    def ensure_alias(self, real_ip, preferred=None):
        """Map a printer's real IP to an alias (allocating one if needed).

        ``preferred`` pins a specific alias (from an explicit --map); otherwise
        the next IP from the --alias-cidr pool is used.
        """
        with self.lock:
            if real_ip in self.real_to_alias:
                return self.real_to_alias[real_ip]
            if preferred is not None:
                alias = preferred
                if alias in self.free_aliases:
                    self.free_aliases.remove(alias)
            elif self.free_aliases:
                alias = self.free_aliases.pop(0)
            else:
                return None
            self.real_to_alias[real_ip] = alias
            self.alias_to_real[alias] = real_ip
        if self.manage:
            self._add_alias(alias)
        self._start_listeners(alias)
        print("mapped printer %s -> alias %s" % (real_ip, alias))
        return alias

    def _setup_static_maps(self):
        """Pin the explicitly-mapped printers at startup (no discovery needed)."""
        for real_ip, alias in self.static_maps.items():
            if self.ensure_alias(real_ip, preferred=alias) is None:
                print("could not map %s: no alias given and pool exhausted" % real_ip)

    def _add_alias(self, alias):
        subprocess.run(
            ["ip", "addr", "add", "%s/%d" % (alias, self.prefix), "dev", self.iface],
            check=False, stderr=subprocess.DEVNULL,
        )
        self.added_aliases.append(alias)

    def _remove_aliases(self):
        """Take down the alias IPs we added (idempotent; safe to call twice)."""
        if not self.manage:
            return
        while self.added_aliases:
            alias = self.added_aliases.pop()
            subprocess.run(
                ["ip", "addr", "del", "%s/%d" % (alias, self.prefix), "dev", self.iface],
                check=False, stderr=subprocess.DEVNULL,
            )
            print("removed alias %s" % alias)

    def _signal_exit(self, signum, frame):
        # Raise SystemExit so the interpreter shuts down cleanly and the atexit
        # handler removes the aliases (handlers run only in the main thread).
        raise SystemExit(0)

    def _start_listeners(self, alias):
        for port, is_rtsp in ((WS_PORT, False), (RTSP_PORT, True)):
            t = threading.Thread(target=self._listen, args=(alias, port, is_rtsp), daemon=True)
            t.start()

    def _listen(self, alias, port, is_rtsp):
        ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            ls.bind((alias, port))
        except OSError as exc:
            print("cannot bind %s:%d (%s) — alias up? root for :554?" % (alias, port, exc))
            return
        ls.listen(8)
        while True:
            try:
                conn, _ = ls.accept()
            except OSError:
                return
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._accept(conn, alias, port, is_rtsp)

    def _accept(self, conn, alias, port, is_rtsp):
        real_ip = self.alias_to_real.get(alias)
        with self.tunnel_lock:
            t = self.tunnel
        if real_ip is None or t is None:
            conn.close()
            return
        chan = next(self._chan_ids)
        try:
            t.send_json(T_TCP_OPEN, {"chan": chan, "ip": real_ip, "port": port})
        except OSError:
            conn.close()
            return
        ch = ClientChannel(self, chan, conn, real_ip, alias, is_rtsp)
        with self.lock:
            self.channels[chan] = ch

    # -- registries -----------------------------------------------------------
    def register_media(self, chan_id, mc):
        with self.lock:
            self.media[chan_id] = mc

    def unregister_media(self, chan_id):
        with self.lock:
            mc = self.media.pop(chan_id, None)
        if mc:
            mc.close()

    def close_channel(self, chan_id, notify):
        with self.lock:
            ch = self.channels.pop(chan_id, None)
        if ch is None:
            return
        if notify:
            with self.tunnel_lock:
                t = self.tunnel
            if t:
                try:
                    t.send_chan(T_TCP_CLOSE, chan_id, b"")
                except OSError:
                    pass
        ch.close()

    # -- tunnel ---------------------------------------------------------------
    def run(self):
        self._setup_static_maps()        # pin explicit mappings before anything else
        threading.Thread(target=self.discovery_listener, daemon=True).start()
        if self.local_broadcast:
            threading.Thread(target=self.local_broadcast_sniffer, daemon=True).start()
        while True:
            try:
                conn = socket.create_connection((self.server_host, self.server_port), timeout=10)
            except OSError as exc:
                print("tunnel connect failed (%s); retrying in 3s" % exc)
                time.sleep(3)
                continue
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # create_connection() leaves a 10s timeout on the socket; that would
            # make the idle read loop below treat any quiet stretch as EOF and
            # tear the tunnel down. Go back to blocking and lean on TCP keepalive
            # to notice a genuinely dead peer.
            conn.settimeout(None)
            enable_keepalive(conn)
            print("tunnel connected to %s:%d" % (self.server_host, self.server_port))
            with self.tunnel_lock:
                self.tunnel = Tunnel(conn)
            if self.static_maps:
                # Ask the server to unicast-probe our explicit printers so they
                # land in discovery too (not just reachable via the alias).
                try:
                    self.tunnel.send_json(T_STATIC_TARGETS, {"ips": list(self.static_maps)})
                except OSError:
                    pass
            threading.Thread(target=self._keepalive, daemon=True).start()
            try:
                self._tunnel_loop(conn)
            except Exception as exc:
                print("tunnel error:", exc)
            finally:
                self._teardown()
                try:
                    conn.close()        # don't leak the old socket on reconnect
                except OSError:
                    pass
                print("tunnel closed; reconnecting")
                time.sleep(2)

    def _keepalive(self):
        while True:
            time.sleep(20)
            with self.tunnel_lock:
                t = self.tunnel
            if t is None:
                return
            try:
                t.send_bare(T_PING)
            except OSError:
                return

    def _tunnel_loop(self, conn):
        while True:
            frame = read_frame(conn)
            if frame is None:
                break
            typ, body = frame
            if typ == T_DISCOVER_REP:
                self.handle_discover_rep(json.loads(body))
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

    def _teardown(self):
        with self.tunnel_lock:
            self.tunnel = None
        with self.lock:
            chans = list(self.channels.values())
            self.channels.clear()
            media = list(self.media.values())
            self.media.clear()
        for ch in chans:
            ch.close()
        for mc in media:
            mc.close()


def _parse_maps(entries):
    """Parse ``--map`` values into ``{real_ip: alias or None}``.

    Each value is ``real_ip`` or ``real_ip=alias``, and may be a comma list.
    """
    maps = {}
    for entry in entries or []:
        for part in entry.split(","):
            part = part.strip()
            if not part:
                continue
            if "=" in part:
                real_ip, alias = part.split("=", 1)
                maps[real_ip.strip()] = alias.strip()
            else:
                maps[part] = None
    return maps


def _parse_udp_probe(pkt):
    """If ``pkt`` (an IPv4 packet) is a UDP SDCP probe, return (src_ip, src_port).

    AF_PACKET with SOCK_DGRAM hands us the packet starting at the IP header.
    """
    if len(pkt) < 20 or pkt[0] >> 4 != 4:      # IPv4 only
        return None
    ihl = (pkt[0] & 0x0F) * 4
    if pkt[9] != 17 or len(pkt) < ihl + 8:     # UDP only
        return None
    dport = int.from_bytes(pkt[ihl + 2:ihl + 4], "big")
    if dport != DISCOVERY_PORT:
        return None
    if pkt[ihl + 8:].strip() != PROBE:
        return None
    src_ip = socket.inet_ntoa(pkt[12:16])
    src_port = int.from_bytes(pkt[ihl:ihl + 2], "big")
    return src_ip, src_port


def _split_hostport(s, default_port):
    if ":" in s:
        host, port = s.rsplit(":", 1)
        return host, int(port)
    return s, default_port


def _parse_alias_cidr(spec):
    """Parse ``A.B.C.D-A.B.C.E/prefix`` or ``A.B.C.D/prefix`` into (ip_list, prefix).

    Also accepts a comma list ``ip1,ip2,.../prefix``. The prefix is the subnet's
    so the alias IPs are added correctly when --manage-aliases is set.
    """
    body, _, pfx = spec.partition("/")
    prefix = int(pfx) if pfx else 24
    ips = []
    if "-" in body and "," not in body:
        lo, hi = body.split("-", 1)
        a = list(map(int, lo.split(".")))
        b = list(map(int, hi.split(".")))
        start = (a[0] << 24) | (a[1] << 16) | (a[2] << 8) | a[3]
        end = (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]
        for n in range(start, end + 1):
            ips.append("%d.%d.%d.%d" % ((n >> 24) & 255, (n >> 16) & 255, (n >> 8) & 255, n & 255))
    else:
        ips = [p for p in body.split(",") if p]
    return ips, prefix


def main():
    ap = argparse.ArgumentParser(description="SDCP printer relay — client-subnet side")
    ap.add_argument("--server", required=True,
                    help="relay_server tunnel address, host:port (e.g. 10.0.0.9:7000)")
    ap.add_argument("--alias-cidr", default=None,
                    help="alias IP pool for auto-discovered printers: 'A-B/prefix' "
                         "range, 'ip1,ip2/prefix' list, or single 'ip/prefix'. "
                         "Optional if every printer is given with --map=ip=alias.")
    ap.add_argument("--map", action="append", default=[], metavar="REAL_IP[=ALIAS]",
                    help="explicitly map a known printer IP to an alias without "
                         "discovery (repeatable, or comma-separated). With =ALIAS the "
                         "alias is pinned; without it one is taken from --alias-cidr.")
    ap.add_argument("--iface", default=None,
                    help="local interface for alias IPs (required with --manage-aliases)")
    ap.add_argument("--manage-aliases", action="store_true",
                    help="add the alias IPs to --iface via 'ip addr add' (needs root); "
                         "removed again on exit")
    ap.add_argument("--local-broadcast", action="store_true",
                    help="also capture SDCP probes sent by software on THIS host "
                         "(needs root). Use when the relay client runs on the same "
                         "box as printer_server.py; sniffs --iface, or all interfaces.")
    args = ap.parse_args()
    if not args.alias_cidr and not args.map:
        ap.error("need --alias-cidr (a pool) and/or --map (explicit printers)")
    if args.manage_aliases and not args.iface:
        ap.error("--manage-aliases requires --iface")
    maps = _parse_maps(args.map)
    if not args.alias_cidr and any(a is None for a in maps.values()):
        ap.error("--map without =ALIAS needs --alias-cidr to draw an alias from")
    RelayClient(args).run()


if __name__ == "__main__":
    main()
