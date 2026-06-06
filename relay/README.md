# SDCP printer relay

Lets `printer_server.py` (and any other SDCP client) on one subnet discover and
drive Elegoo/SDCP resin printers on a **different** subnet, when the only thing
connecting the two subnets is this relay — no IP routing between them.

It forwards exactly the printer traffic and nothing else:

| Purpose            | Protocol            | What the relay does                              |
|--------------------|---------------------|--------------------------------------------------|
| Discovery          | UDP `:3000` `M99999`| Intercepts the probe, rebroadcasts on the far subnet, returns each printer's real reply |
| Control / status   | TCP `:3030` (WS)    | Tunnels the websocket; rewrites the cmd 386 `VideoUrl` to point at the alias |
| Camera / video     | TCP `:554` (RTSP)   | Tunnels RTSP and relays the negotiated RTP/RTCP UDP streams |

Unrelated broadcasts are never touched — the client only acts on UDP `:3000`
packets whose payload is exactly `M99999`.

## How it works

```
   client subnet (A)                                printer subnet (B)
 ┌───────────────────┐                            ┌────────────────────┐
 │ printer_server.py │                            │   printer  .20.5   │
 │  other clients    │                            │   printer  .20.6   │
 └─────────┬─────────┘                            └─────────┬──────────┘
           │ broadcast :3000, ws :3030, rtsp :554           │ broadcast/unicast
 ┌─────────┴─────────┐      one TCP tunnel        ┌──────────┴─────────┐
 │   relay_client    │◄──────────────────────────►│    relay_server    │
 │  (this subnet)    │      (the only link)       │  (printer subnet)  │
 └───────────────────┘                            └────────────────────┘
```

`printer_server.py` identifies a printer by the **source IP of its discovery
reply** and then connects to `<that ip>:3030` / `<that ip>:554`. Since the real
printer IP isn't routable from subnet A, `relay_client` gives each discovered
printer its own **alias IP on subnet A** and presents the printer there:

* the discovery reply is delivered *from the alias IP*, so the client records
  the alias as the printer's address;
* `relay_client` listens on `alias:3030` and `alias:554` and tunnels those
  connections to the real printer via `relay_server`;
* the printer's real IP that leaks back inside the websocket `VideoUrl` and in
  RTSP `Transport`/SDP is rewritten to the alias on the way through.

No source-IP spoofing and no kernel routing changes are needed — `relay_client`
only ever uses addresses it legitimately owns.

## Requirements

* Python 3 standard library only (no extra pip packages).
* `relay_client` needs:
  * one spare IP per printer on the client subnet (the alias pool);
  * **root** (or `CAP_NET_BIND_SERVICE` + `CAP_NET_ADMIN`) — RTSP binds the
    privileged port `:554`, and `--manage-aliases` adds the alias IPs.
* `relay_server` needs to be able to broadcast on the printer subnet (any normal
  host there will do).

## Running

On the **printer subnet** (replace the broadcast address with that subnet's):

```sh
./relay_server.py --listen 0.0.0.0:7000 --broadcast 192.168.20.255
```

On the **client subnet** (same subnet as `printer_server.py`):

```sh
sudo ./relay_client.py \
    --server 192.168.20.9:7000 \
    --iface eth0 \
    --alias-cidr 192.168.1.200-192.168.1.209/24 \
    --manage-aliases
```

That reserves `192.168.1.200…209` as aliases (10 printers) and adds them to
`eth0` as printers are discovered, and **removes them again on exit** (Ctrl-C,
`kill`/SIGTERM, or normal shutdown). If you'd rather provision the alias IPs
yourself (e.g. statically), drop `--manage-aliases` and just pass the pool with
`--alias-cidr` — then they are left untouched on exit. A pool can be a range
(`A-B/prefix`), a comma list (`ip1,ip2/prefix`), or a single `ip/prefix`.

Now point `printer_server.py` at the client subnet as usual and run its
discovery — the remote printers appear at their alias IPs and status/screenshots
work end-to-end.

### Mapping printers explicitly, without discovery

When a printer's IP is known and you'd rather not rely on broadcast discovery
(e.g. broadcast doesn't reach it, or it's on yet another subnet behind the
server), map it directly with `--map REAL_IP[=ALIAS]`:

```sh
sudo ./relay_client.py --server 192.168.20.9:7000 --iface eth0 --manage-aliases \
    --map 192.168.20.41=192.168.1.200 \
    --map 192.168.20.55=192.168.1.201
```

Each mapping is pinned at startup, so:

* a client can talk to the printer at its **alias** immediately, with no
  discovery at all (point any SDCP client straight at `192.168.1.200`); and
* `relay_client` tells the server to **unicast-probe** those IPs, so the
  printers also show up in normal discovery (with their real `MainboardID`),
  letting discovery-based clients like `printer_server.py` find them at the
  pinned alias too.

`--map` is repeatable and accepts comma-separated lists. Drop the `=ALIAS` to
draw an alias from the `--alias-cidr` pool instead of pinning one. With every
printer mapped explicitly, `--alias-cidr` is optional.

### Which way does the tunnel dial?

`relay_server` listens and `relay_client` dials out, so the client subnet only
needs **outbound** access to the server's `:7000`. `relay_client` reconnects
automatically if the link drops.

### Running the relay client on the same box as the client software

By default `relay_client` hears probes that arrive **over the wire**, which is
exactly what happens when the SDCP client (e.g. `printer_server.py`) is on a
*different* host on the subnet. But a broadcast a host transmits is never looped
back to that same host's `udp/3000` listener, so if `printer_server.py` runs on
the **same box** as `relay_client`, its probe would be missed. Add
`--local-broadcast` to also tap probes leaving this host:

```sh
sudo ./relay_client.py --server 192.168.20.9:7000 --iface eth0 \
    --alias-cidr 192.168.1.200-192.168.1.209/24 --manage-aliases --local-broadcast
```

This opens an `AF_PACKET` tap on `--iface` (needs root) that picks the probe off
the wire as it leaves and learns the sender's port to address the reply. It only
acts on probes this host *sends*, so it never double-counts wire probes. If the
tap can't be created it logs and continues — over-the-wire discovery still works.

## Notes & limits

* One tunnel (one `relay_client`) at a time per `relay_server`. Multiple clients
  on subnet A all share the single `relay_client` and its alias pool.
* The alias pool must be at least as large as the number of printers; mappings
  are stable for the lifetime of the `relay_client` process.
* Discovery answers come from a cache that `relay_server` refreshes every
  `--discovery-interval` seconds (default 10), so clients get an instant reply
  inside their discovery timeout. A printer unseen for `--cache-ttl` seconds
  (default 60) is dropped.
* The websocket rewrite assumes the SDCP socket uses no `permessage-deflate`
  compression (Elegoo printers negotiate none). A compressed frame would pass
  through unrewritten rather than be corrupted.
* RTSP video is relayed over UDP (RTP/RTCP); the printer's camera rejects RTSP
  interleaved/TCP, matching `printer_server.py`'s `-rtsp_transport udp`.
