#!/usr/bin/env python3
"""Query the Chitu OTA endpoint (mainboardVersionUpdate/getInfo.do7), print the
JSON response, and download the firmware package it points to."""
import argparse
import hashlib
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_HOST = "https://mms.chituiot.com"
ENDPOINT = "/mainboardVersionUpdate/getInfo.do7"


def get_info(host, machine_type, machine_id, version, lan, firmware_type, timeout=30):
    # Param order mirrors what the firmware builds.
    params = [
        ("machineType", machine_type),
        ("machineId", machine_id),
        ("version", version),
        ("lan", lan),
        ("firmwareType", firmware_type),
    ]
    query = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params)
    url = f"{host}{ENDPOINT}?{query}"
    req = urllib.request.Request(url, headers={"Accept-Language": lan})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return url, json.loads(body)


def download(url, dest, expected_md5=None, timeout=120):
    md5 = hashlib.md5()
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        done = 0
        with open(dest, "wb") as f:
            while True:
                chunk = resp.read(1 << 16)
                if not chunk:
                    break
                f.write(chunk)
                md5.update(chunk)
                done += len(chunk)
                if sys.stderr.isatty():
                    bar = f"\r  {done:,}" + (f" / {total:,} bytes ({done * 100 // total}%)" if total else " bytes")
                    print(bar, end="", file=sys.stderr, flush=True)
    if sys.stderr.isatty():
        print(file=sys.stderr)
    digest = md5.hexdigest()
    if expected_md5:
        ok = digest.lower() == expected_md5.lower()
        print(f"  MD5 {digest} -> {'OK' if ok else 'MISMATCH (expected ' + expected_md5 + ')'}", file=sys.stderr)
        return ok
    print(f"  MD5 {digest}", file=sys.stderr)
    return True


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--machine-type", default="ELEGOO SATURN 4 Ultra")
    p.add_argument("--machine-id", default="ec4cc265b10e0100", help="SoC id / MainboardID (16 hex chars)")
    p.add_argument("--version", default="00.00.01",
                   help="version to report; a low value makes the server return the latest package")
    p.add_argument("--lan", default="en")
    p.add_argument("--firmware-type", default="1", help="1 = mainboard/SYS, 2 = AIC/camera")
    p.add_argument("-o", "--output-dir", default=".")
    p.add_argument("--no-download", action="store_true", help="print JSON only")
    args = p.parse_args()

    url, info = get_info(args.host, args.machine_type, args.machine_id,
                         args.version, args.lan, args.firmware_type)
    print(f"# GET {url}\n", file=sys.stderr)
    print(json.dumps(info, indent=2, ensure_ascii=False))

    if args.no_download:
        return

    data = info.get("data") or {}
    pkg = data.get("packageUrl")
    if not pkg:
        print("\nNo packageUrl (no update for the reported version).", file=sys.stderr)
        sys.exit(1)

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    name = Path(urllib.parse.urlparse(pkg).path).name or "firmware.zip"
    dest = outdir / name
    print(f"\nDownloading {pkg}\n  -> {dest}", file=sys.stderr)
    ok = download(pkg, dest, data.get("packageHash"))
    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()
