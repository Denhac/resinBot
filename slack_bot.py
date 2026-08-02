import io
import os
import re
import requests
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

PRINTER_SERVER_URL = os.environ.get("PRINTER_SERVER_URL", "http://localhost:5000")

app = App(token=os.environ["SLACK_BOT_TOKEN"])


def fetch_printer_statuses():
    try:
        resp = requests.get(f"{PRINTER_SERVER_URL}/status", timeout=5)
        resp.raise_for_status()
        return resp.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Could not reach the printer server."
    except requests.exceptions.Timeout:
        return None, "Printer server timed out."
    except Exception as e:
        return None, str(e)


def fetch_screenshot(ip):
    """Returns (jpeg_bytes, error_message)."""
    try:
        # Capture enables video, then grabs/scores up to 4 keyframes (rejecting streaked
        # ones) before disabling. On the single-core Pi a weak-link printer can need
        # several retries, so this must exceed the server's worst case (4 x 30s + overhead).
        resp = requests.get(f"{PRINTER_SERVER_URL}/screenshot/{ip}", timeout=180)
        if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("image/"):
            return resp.content, None
        try:
            err = resp.json().get("error", f"HTTP {resp.status_code}")
        except ValueError:
            err = f"HTTP {resp.status_code}"
        return None, err
    except requests.exceptions.Timeout:
        return None, "Screenshot request timed out."
    except Exception as e:
        return None, str(e)


# SDCP v3.0.0 enums
MACHINE_STATUS = {
    0: "Idle",
    1: "Printing",
    2: "Transferring file",
    3: "Exposure test",
    4: "Self-check",
}

PRINT_STATUS = {
    0: "Idle",
    1: "Homing",
    2: "Descending",
    3: "Exposing",
    4: "Lifting",
    5: "Pausing",
    6: "Paused",
    7: "Stopping",
    8: "Stopped",
    9: "Complete",
    10: "File checking",
}

PRINT_ERRORS = {
    1: "File MD5 check failed",
    2: "File read failed",
    3: "Resolution mismatch",
    4: "Format mismatch",
    5: "Machine model mismatch",
}


def printer_label(ip: str, printer: dict) -> str:
    name = printer.get("name")
    machine = printer.get("machine_name")
    if name and machine:
        return f"{name} ({machine} @ {ip})"
    if name:
        return f"{name} ({ip})"
    if machine:
        return f"{machine} ({ip})"
    return ip


def format_printer_text(ip: str, printer: dict) -> str:
    """Build a compact text summary for a single printer."""
    label = printer_label(ip, printer)
    raw = printer.get("status") or {}
    # Status push nests the live fields under a "Status" key.
    inner = raw.get("Status", raw)

    if not inner:
        return f"*{label}*\n_No data received yet._"

    machine_codes = inner.get("CurrentStatus")
    if isinstance(machine_codes, list):
        machine_str = ", ".join(
            MACHINE_STATUS.get(c, f"Unknown ({c})") for c in machine_codes
        ) or "Unknown"
    elif machine_codes is not None:
        machine_str = MACHINE_STATUS.get(machine_codes, f"Unknown ({machine_codes})")
    else:
        machine_str = "Unknown"

    lines = [f"*{label}*", f"*Status:* {machine_str}"]

    print_info = inner.get("PrintInfo") or {}
    if print_info:
        sub = print_info.get("Status")
        if sub is not None:
            lines.append(f"*Stage:* {PRINT_STATUS.get(sub, f'Unknown ({sub})')}")

        filename = print_info.get("Filename")
        if filename:
            lines.append(f"*File:* {filename}")

        current_layer = print_info.get("CurrentLayer")
        total_layers = print_info.get("TotalLayer")
        if current_layer is not None and total_layers:
            try:
                pct = round(int(current_layer) / int(total_layers) * 100, 1)
                lines.append(f"*Progress:* {current_layer}/{total_layers} layers ({pct}%)")
            except (ValueError, ZeroDivisionError):
                pass

        # No explicit remaining-time field; derive it from tick counters (milliseconds).
        current_ticks = print_info.get("CurrentTicks")
        total_ticks = print_info.get("TotalTicks")
        if current_ticks is not None and total_ticks:
            try:
                remaining_min = max(0, int(total_ticks) - int(current_ticks)) // 1000 // 60
                hours, mins = divmod(remaining_min, 60)
                time_str = f"{hours}h {mins}m" if hours else f"{mins}m"
                lines.append(f"*Time remaining:* ~{time_str}")
            except ValueError:
                pass

        err = print_info.get("ErrorNumber")
        if err:
            lines.append(f":warning: *Error:* {PRINT_ERRORS.get(err, f'code {err}')}")

    temp = inner.get("TempOfUVLED")
    if temp is not None:
        try:
            lines.append(f"*UV LED temp:* {float(temp):.1f}°C")
        except (ValueError, TypeError):
            pass

    return "\n".join(lines)


# Any of these ask for a camera frame alongside the status.
SCREENSHOT_KEYWORDS = {"screenshot", "webcam", "cam", "snapshot", "picture"}

# Words that just name the default action; recognized so they aren't mistaken
# for printer-name filters.
STATUS_KEYWORDS = {"status", "all"}

HELP_KEYWORDS = {"help", "usage", "commands"}

COMMAND_KEYWORDS = SCREENSHOT_KEYWORDS | STATUS_KEYWORDS | HELP_KEYWORDS

# Slack renders mentions/links/channels as <...> entities; none are printer names.
_ENTITY_RE = re.compile(r"<[^>]*>")
# Keep inner dots and dashes so an IP or a hyphenated name survives as one
# token, but never trailing ones ("left." must still match "saturn4uleft").
_TOKEN_RE = re.compile(r"[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?")


def parse_command(text: str):
    """Split mention text into (show_help, with_screenshot, name_filters).

    Every token that isn't a recognized command keyword is treated as a
    case-insensitive substring filter on printer names.
    """
    body = _ENTITY_RE.sub(" ", text or "").lower()
    tokens = _TOKEN_RE.findall(body)

    show_help = any(t in HELP_KEYWORDS for t in tokens)
    with_screenshot = any(t in SCREENSHOT_KEYWORDS for t in tokens)

    filters = []
    for token in tokens:
        if token not in COMMAND_KEYWORDS and token not in filters:
            filters.append(token)
    return show_help, with_screenshot, filters


def _keyword_list(keywords) -> str:
    return ", ".join(f"`{k}`" for k in sorted(keywords))


def help_text() -> str:
    """Usage summary, generated from the keyword sets so it can't drift."""
    return "\n".join([
        ":printer: *resinBot* — mention me with optional keywords.",
        "",
        f"*Status* — just mention me (or say {_keyword_list(STATUS_KEYWORDS)}) "
        "for every printer's status.",
        f"*Snapshot* — add {_keyword_list(SCREENSHOT_KEYWORDS)} to include a camera frame.",
        "*Pick printers* — any other word filters by printer name, model, or IP. "
        "Matching is case-insensitive and partial, and multiple terms are combined "
        "(any match wins).",
        f"*Help* — {_keyword_list(HELP_KEYWORDS)}.",
        "",
        "_Examples:_",
        "• `@resinBot` — status for all printers",
        "• `@resinBot left` — status for printers whose name contains \"left\"",
        "• `@resinBot cam left right` — snapshots from both printers",
    ])


def matches_filters(ip: str, printer: dict, filters) -> bool:
    """True if any filter term appears in the printer's name, model, or IP."""
    if not filters:
        return True
    haystack = " ".join(
        str(v).lower()
        for v in (printer.get("name"), printer.get("machine_name"), ip)
        if v
    )
    return any(f in haystack for f in filters)


def select_printers(statuses: dict, filters) -> dict:
    return {
        ip: printer
        for ip, printer in statuses.items()
        if matches_filters(ip, printer, filters)
    }


def post_printer(client, channel, thread_ts, ip, printer, with_screenshot):
    text = format_printer_text(ip, printer)

    if not with_screenshot:
        client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=text)
        return

    jpg, err = fetch_screenshot(ip)

    if jpg:
        client.files_upload_v2(
            channel=channel,
            thread_ts=thread_ts,
            file=io.BytesIO(jpg),
            filename=f"{ip.replace('.', '_')}.jpg",
            title=printer_label(ip, printer),
            initial_comment=text,
        )
    else:
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=f"{text}\n_:camera_with_flash: Screenshot unavailable: {err}_",
        )


@app.event("app_mention")
def handle_mention(event, say, client):
    channel = event["channel"]
    # Reply in-thread if the mention was in a thread, else start a new thread on our header.
    parent_ts = event.get("thread_ts") or event["ts"]

    show_help, with_screenshot, filters = parse_command(event.get("text", ""))

    if show_help:
        say(text=help_text(), thread_ts=parent_ts)
        return

    statuses, error = fetch_printer_statuses()

    if error:
        say(text=f":warning: {error}", thread_ts=parent_ts)
        return

    if not statuses:
        say(text=":printer: No printers have been discovered yet.", thread_ts=parent_ts)
        return

    selected = select_printers(statuses, filters)

    if not selected:
        known = ", ".join(printer_label(ip, p) for ip, p in statuses.items())
        say(
            text=(
                f":mag: No printers matched {', '.join(filters)}.\n"
                f"_Known printers: {known}_\n"
                "_Say `help` for usage._"
            ),
            thread_ts=parent_ts,
        )
        return

    what = "status and snapshots" if with_screenshot else "status"
    header = f":printer: Fetching {what} for {len(selected)} printer(s)"
    if filters:
        header += f" matching {', '.join(filters)}"
    say(text=f"{header}…", thread_ts=parent_ts)

    # Capture serially: the Pi Zero W is single-core, so concurrent ffmpeg decodes
    # would contend and slow every capture. One printer at a time keeps each fast.
    for ip, printer in selected.items():
        post_printer(client, channel, parent_ts, ip, printer, with_screenshot)


if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    print("Slack bot started.")
    handler.start()
