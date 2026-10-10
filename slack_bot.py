import io
import os
import re
import threading
import time
import requests
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

PRINTER_SERVER_URL = os.environ.get("PRINTER_SERVER_URL", "http://localhost:5000")

# Where print started/paused/resumed/completed/cancelled/error posts go. Set it
# empty to turn them off. The bot must be a member of the channel.
NOTIFY_CHANNEL = os.environ.get("NOTIFY_CHANNEL", "#bots")
# The server caches each printer's latest status, so polling it is cheap.
NOTIFY_POLL_SECONDS = 10

app = App(token=os.environ["SLACK_BOT_TOKEN"])


def fetch_printer_statuses():
    try:
        resp = requests.get(f"{PRINTER_SERVER_URL}/status", timeout=5)
        if resp.status_code == 503:  # server is up but hasn't discovered any printers
            return {}, None
        resp.raise_for_status()
        return resp.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Could not reach the printer server."
    except requests.exceptions.Timeout:
        return None, "Printer server timed out."
    except Exception as e:
        return None, str(e)


def request_refresh():
    """Have the server re-run discovery. Returns (result, error_message)."""
    try:
        # Discovery listens until replies stop for 5s, and waits out any discovery
        # already running, so allow well beyond that.
        resp = requests.post(f"{PRINTER_SERVER_URL}/refresh", timeout=60)
        resp.raise_for_status()
        return resp.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Could not reach the printer server."
    except requests.exceptions.Timeout:
        return None, "Printer discovery timed out."
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


def format_duration(ms) -> str:
    """Render a millisecond tick count as "2h 5m" / "5m"; None if it isn't a number."""
    try:
        total_min = max(0, int(ms)) // 1000 // 60
    except (TypeError, ValueError):
        return None
    hours, mins = divmod(total_min, 60)
    return f"{hours}h {mins}m" if hours else f"{mins}m"


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
                time_str = format_duration(int(total_ticks) - int(current_ticks))
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


# How each refresh state from the server is annotated; "active" needs no note.
REFRESH_NOTES = {
    "new": "newly found",
    "reconnected": "reconnecting",
    "no_reply": "didn't answer, but its connection is still up",
}


def format_refresh_text(result: dict) -> str:
    kept = result.get("printers") or {}
    removed = result.get("removed") or {}

    if kept:
        lines = [f":resin-printer: Discovery done — tracking {len(kept)} printer(s):"]
        for ip, printer in kept.items():
            note = REFRESH_NOTES.get(printer.get("state"))
            lines.append(f"• {printer_label(ip, printer)}" + (f" — _{note}_" if note else ""))
    else:
        lines = [":resin-printer: Discovery found no printers."]

    if removed:
        gone = ", ".join(printer_label(ip, p) for ip, p in removed.items())
        lines.append(f"_Dropped {len(removed)} printer(s) that stopped responding: {gone}_")
    return "\n".join(lines)


# Any of these ask for a camera frame alongside the status.
SCREENSHOT_KEYWORDS = {"screenshot", "webcam", "cam", "snapshot", "picture"}

# Words that just name the default action; recognized so they aren't mistaken
# for printer-name filters.
STATUS_KEYWORDS = {"status", "all"}

HELP_KEYWORDS = {"help", "usage", "commands"}

REFRESH_KEYWORDS = {"refresh", "rediscover", "rescan"}

COMMAND_KEYWORDS = SCREENSHOT_KEYWORDS | STATUS_KEYWORDS | HELP_KEYWORDS | REFRESH_KEYWORDS

# Slack renders mentions/links/channels as <...> entities; none are printer names.
_ENTITY_RE = re.compile(r"<[^>]*>")
# Keep inner dots and dashes so an IP or a hyphenated name survives as one
# token, but never trailing ones ("left." must still match "saturn4uleft").
_TOKEN_RE = re.compile(r"[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?")


def parse_command(text: str):
    """Split mention text into (show_help, refresh, with_screenshot, name_filters).

    Every token that isn't a recognized command keyword is treated as a
    case-insensitive substring filter on printer names.
    """
    body = _ENTITY_RE.sub(" ", text or "").lower()
    tokens = _TOKEN_RE.findall(body)

    show_help = any(t in HELP_KEYWORDS for t in tokens)
    refresh = any(t in REFRESH_KEYWORDS for t in tokens)
    with_screenshot = any(t in SCREENSHOT_KEYWORDS for t in tokens)

    filters = []
    for token in tokens:
        if token not in COMMAND_KEYWORDS and token not in filters:
            filters.append(token)
    return show_help, refresh, with_screenshot, filters


def _keyword_list(keywords) -> str:
    return ", ".join(f"`{k}`" for k in sorted(keywords))


def help_text() -> str:
    """Usage summary, generated from the keyword sets so it can't drift."""
    return "\n".join([
        ":resin-printer: *resinBot* — mention me with optional keywords.",
        "",
        f"*Status* — just mention me (or say {_keyword_list(STATUS_KEYWORDS)}) "
        "for every printer's status.",
        f"*Snapshot* — add {_keyword_list(SCREENSHOT_KEYWORDS)} to include a camera frame.",
        "*Pick printers* — any other word filters by printer name, model, or IP. "
        "Matching is case-insensitive and partial, and multiple terms are combined "
        "(any match wins).",
        f"*Refresh* — {_keyword_list(REFRESH_KEYWORDS)} re-scans the network for printers: "
        "finds ones turned on since startup, reconnects dropped ones, and forgets ones "
        "that are gone.",
        f"*Help* — {_keyword_list(HELP_KEYWORDS)}.",
        "",
        "_Examples:_",
        "• `@resinBot` — status for all printers",
        "• `@resinBot left` — status for printers whose name contains \"left\"",
        "• `@resinBot cam left right` — snapshots from both printers",
        "• `@resinBot refresh` — look for new or missing printers",
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

    show_help, refresh, with_screenshot, filters = parse_command(event.get("text", ""))

    if show_help:
        say(text=help_text(), thread_ts=parent_ts)
        return

    if refresh:
        say(text=":mag: Scanning the network for printers…", thread_ts=parent_ts)
        result, error = request_refresh()
        say(text=f":warning: {error}" if error else format_refresh_text(result), thread_ts=parent_ts)
        return

    statuses, error = fetch_printer_statuses()

    if error:
        say(text=f":warning: {error}", thread_ts=parent_ts)
        return

    if not statuses:
        say(
            text=":resin-printer: No printers have been discovered yet. Say `refresh` to scan again.",
            thread_ts=parent_ts,
        )
        return

    selected = select_printers(statuses, filters)

    if not selected:
        known = ", ".join(printer_label(ip, p) for ip, p in statuses.items())
        say(
            text=(
                f":mag: No printers matched {', '.join(filters)}.\n"
                f"_Known printers: {known}_\n"
                "_Say `refresh` to scan for missing printers, or `help` for usage._"
            ),
            thread_ts=parent_ts,
        )
        return

    what = "status and snapshots" if with_screenshot else "status"
    header = f":resin-printer: Fetching {what} for {len(selected)} printer(s)"
    if filters:
        header += f" matching {', '.join(filters)}"
    say(text=f"{header}…", thread_ts=parent_ts)

    # Capture serially: the Pi Zero W is single-core, so concurrent ffmpeg decodes
    # would contend and slow every capture. One printer at a time keeps each fast.
    for ip, printer in selected.items():
        post_printer(client, channel, parent_ts, ip, printer, with_screenshot)


# PrintInfo.Status codes that decide a print's phase. The v3.0.0 spec stops at 10,
# but Saturn firmware also sends codes the open-source clients disagree on: 10 is
# "file checking" (spec) or "stopping" (mslicer), 13 "canceled" or "printing", 16
# "complete" or "preheating". Only codes every source agrees on are trusted here;
# anything else counts as printing, and the layer count settles how a print ended.
# 7 (stopping) isn't a cancel: Saturn 4 Ultra V1.5.2 sends it briefly on a normal
# finish (7 at 200/200 layers, then idle with 9).
PAUSED_CODES = {5, 6}  # pausing, paused
CANCELLED_CODES = {8}  # stopped
COMPLETED_CODES = {9}
FILE_CHECKING = 10
ACTIVE_PHASES = {"printing", "paused"}


def layers_done(info: dict) -> bool:
    try:
        total = int(info.get("TotalLayer") or 0)
        return total > 0 and int(info.get("CurrentLayer") or 0) >= total
    except (TypeError, ValueError):
        return False


def print_phase(inner: dict):
    """Classify a status as idle, busy, printing, paused, completed, or cancelled.

    "busy" is machine work that isn't a print (file transfer, exposure test,
    self-check) plus the pre-print file check. Returns None before any status
    has arrived.
    """
    codes = inner.get("CurrentStatus")
    if codes is None:
        return None
    codes = set(codes) if isinstance(codes, list) else {codes}
    # A list can pair printing (1) with other work, e.g. a file upload mid-print.
    if 1 not in codes:
        return "idle" if codes <= {0} else "busy"
    info = inner.get("PrintInfo") or {}
    sub = info.get("Status")
    if sub in PAUSED_CODES:
        return "paused"
    if sub in COMPLETED_CODES or layers_done(info):
        return "completed"
    if sub in CANCELLED_CODES:
        return "cancelled"
    if sub == FILE_CHECKING:
        return "busy"
    return "printing"


class PrintWatcher:
    """Turns successive /status snapshots into the events worth posting.

    A printer's first snapshot only records where it is, so restarts never
    announce prints already underway. Completed/cancelled post only when a print
    was in progress, and idle never posts on its own.
    """

    def __init__(self):
        # key -> {"active": "printing" | "paused" | None, "error": int}
        self._printers = {}

    def update(self, statuses: dict) -> list:
        """Returns [(event, name, print_info)] for this snapshot's transitions."""
        events = []
        for ip, printer in statuses.items():
            raw = printer.get("status") or {}
            inner = raw.get("Status", raw)
            phase = print_phase(inner) if inner else None
            if phase is None:
                continue
            info = inner.get("PrintInfo") or {}
            error = info.get("ErrorNumber") or 0
            key = printer.get("mainboard_id") or ip
            name = printer.get("name") or printer.get("machine_name") or ip

            state = self._printers.get(key)
            if state is None:
                # A busy printer could be mid-print or about to start one; wait
                # for a phase that says which before taking the baseline.
                if phase != "busy":
                    self._printers[key] = {
                        "active": phase if phase in ACTIVE_PHASES else None,
                        "error": error,
                    }
                continue

            event = self._transition(state, phase, info)
            if event:
                events.append((event, name, info))
            if error and error != state["error"]:
                events.append(("error", name, info))
            state["error"] = error
        return events

    @staticmethod
    def _transition(state: dict, phase: str, info: dict):
        active = state["active"]
        if phase in ACTIVE_PHASES:
            state["active"] = phase
            if active is None:
                return "started"
            if active != phase:
                return "paused" if phase == "paused" else "resumed"
            return None
        if phase == "busy":
            return None
        state["active"] = None
        if active is None:
            return None
        if phase == "idle":
            # The printer can drop straight to idle, skipping a completed state
            # we'd see; the leftover print info says how it ended.
            done = info.get("Status") in COMPLETED_CODES or layers_done(info)
            return "completed" if done else "cancelled"
        return phase


def format_event_text(event: str, name: str, info: dict) -> str:
    filename = info.get("Filename")
    what = f" `{filename}`" if filename else ""
    current, total = info.get("CurrentLayer"), info.get("TotalLayer")
    at_layer = f" at layer {current}/{total}" if current is not None and total else ""

    if event == "started":
        details = []
        if total:
            details.append(f"{total} layers")
        try:
            remaining = int(info.get("TotalTicks")) - int(info.get("CurrentTicks"))
            if remaining > 0:
                details.append(f"~{format_duration(remaining)}")
        except (TypeError, ValueError):
            pass
        suffix = f" ({', '.join(details)})" if details else ""
        return f":resin-printer: *{name}* started printing{what}{suffix}"
    if event == "paused":
        return f":double_vertical_bar: *{name}* paused{what}{at_layer}"
    if event == "resumed":
        return f":arrow_forward: *{name}* resumed{what}{at_layer}"
    if event == "completed":
        took = format_duration(info.get("CurrentTicks"))
        return f":white_check_mark: *{name}* finished{what}" + (f" in ~{took}" if took else "")
    if event == "cancelled":
        return f":octagonal_sign: *{name}* cancelled{what}{at_layer}"
    err = info.get("ErrorNumber")
    with_file = f" with{what}" if what else ""
    return f":warning: *{name}* error{with_file}: {PRINT_ERRORS.get(err, f'code {err}')}"


def watch_printers(client):
    """Poll the server forever, posting major print transitions to NOTIFY_CHANNEL."""
    watcher = PrintWatcher()
    while True:
        events = []
        try:
            statuses, _ = fetch_printer_statuses()
            # Server down or no printers: keep the last-known state and retry.
            events = watcher.update(statuses or {})
        except Exception as e:
            print(f"Printer watch error: {e}")
        for event in events:
            text = format_event_text(*event)
            print(f"Posting to {NOTIFY_CHANNEL}: {text}")
            try:
                client.chat_postMessage(channel=NOTIFY_CHANNEL, text=text)
            except Exception as e:
                print(f"Failed to post to {NOTIFY_CHANNEL}: {e}")
        time.sleep(NOTIFY_POLL_SECONDS)


if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    if NOTIFY_CHANNEL:
        threading.Thread(target=watch_printers, args=(app.client,), daemon=True).start()
        print(f"Posting print transitions to {NOTIFY_CHANNEL}.")
    print("Slack bot started.")
    handler.start()
