"""
Health reporting for long-running feed services, read by finhisaab-be's feed watchdog
(finhisaab-be docs/market-data/feed-watchdog.md).

Two parts:

* A /health HTTP endpoint (daemon thread) serving the shared schema: state, uptime,
  last attempt/success, consecutive failures, last error, and deliberate sleeps.
  The state follows the main loop's progress, not just whether this thread answers:
  if the loop hasn't attempted work within `stall_after_s`, it reports "failing".
* Pushed events: "started" on boot (so the watchdog can spot crash loops) and
  "fatal" with the traceback just before the process dies.

Reporting must never break the service: every network or bind failure is logged
and swallowed. Messages are scrubbed of credentials before they leave the process.

Env vars:
    HEALTH_PORT               port for /health (default: PORT, else 8080; 0 disables)
    FEED_HEALTH_REPORT_URL    finhisaab-be base URL; unset disables pushed events
    FEED_HEALTH_REPORT_KEY    X-API-Key for the events endpoint

Keep this file identical in every service that uses it.
"""

import json
import logging
import os
import re
import socket
import threading
import traceback as traceback_module
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger(__name__)

REPORT_TIMEOUT_S = 3
_URI_CREDENTIALS = re.compile(r"([a-zA-Z][a-zA-Z0-9+.\-]*://)[^/\s:@]+:[^@\s]+@")
_SECRET_ENV_NAME = re.compile(r"(URI|URL|KEY|TOKEN|SECRET|PASSWORD|PASS)", re.IGNORECASE)


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.isoformat() if dt else None


def redact(text):
    """Strip credentials from URIs and the values of secret-looking env vars."""
    if text is None:
        return None
    text = _URI_CREDENTIALS.sub(r"\1<redacted>@", str(text))
    for name, value in os.environ.items():
        if value and len(value) >= 8 and _SECRET_ENV_NAME.search(name) and value in text:
            text = text.replace(value, f"<{name}>")
    return text


class FeedHealth:
    def __init__(self, service, feed=None, stall_after_s=600, failing_after=2, version=None):
        """
        service:        name shown in /health
        feed:           key on finhisaab-be's events endpoint (default: service)
        stall_after_s:  no attempt for this long (outside a sleep) reports "failing"
        failing_after:  consecutive failures before reporting "failing"
        """
        self.service = service
        self.feed = feed or service
        self.stall_after_s = stall_after_s
        self.failing_after = failing_after
        self.version = version or os.getenv("RAILWAY_GIT_COMMIT_SHA") or os.getenv("GIT_COMMIT")
        self.instance = os.getenv("RAILWAY_DEPLOYMENT_ID") or socket.gethostname()
        self.started_at = _now()
        self._lock = threading.Lock()
        self._last_attempt_at = None
        self._last_success_at = None
        self._consecutive_failures = 0
        self._last_error = None
        self._sleeping_until = None
        self._sleep_reason = None
        self._details = {}
        self._server = None

    # -- main-loop hooks ------------------------------------------------------

    def attempt(self):
        """Call at the start of each unit of work. Ends any sleep."""
        with self._lock:
            self._last_attempt_at = _now()
            self._sleeping_until = None
            self._sleep_reason = None

    def success(self, **details):
        with self._lock:
            self._last_success_at = _now()
            self._consecutive_failures = 0
            self._details = details

    def failure(self, code, message):
        with self._lock:
            self._consecutive_failures += 1
            self._last_error = {"code": code, "message": (redact(str(message)) or "")[:500], "at": _iso(_now())}

    def sleeping(self, until, reason):
        """Call before a deliberate idle period (market closed, holiday backoff)."""
        with self._lock:
            self._sleeping_until = until
            self._sleep_reason = reason

    # -- /health --------------------------------------------------------------

    def snapshot(self, now=None):
        now = now or _now()
        with self._lock:
            last_attempt = self._last_attempt_at
            sleeping_until = self._sleeping_until
            body = {
                "service": self.service,
                "version": self.version,
                "started_at": _iso(self.started_at),
                "uptime_s": round((now - self.started_at).total_seconds()),
                "last_attempt_at": _iso(last_attempt),
                "last_success_at": _iso(self._last_success_at),
                "consecutive_failures": self._consecutive_failures,
                "last_error": self._last_error,
                "sleeping_until": _iso(sleeping_until),
                "sleep_reason": self._sleep_reason,
                "details": dict(self._details),
            }
            failures = self._consecutive_failures

        if sleeping_until and sleeping_until > now:
            body["state"] = "sleeping"
            return body

        # A sleep that should have ended counts from its end, not from the last attempt.
        since = max(t for t in (last_attempt, sleeping_until, self.started_at) if t is not None)
        if (now - since).total_seconds() > self.stall_after_s:
            body["state"] = "failing"
            body["last_error"] = {
                "code": "STALLED",
                "message": f"No work attempted for {round((now - since).total_seconds())}s",
                "at": _iso(now),
            }
        elif failures >= self.failing_after:
            body["state"] = "failing"
        else:
            body["state"] = "running"
        return body

    def start_server(self, port=None):
        """Serve /health on a daemon thread. Never raises."""
        if port is None:
            port = int(os.getenv("HEALTH_PORT") or os.getenv("PORT") or 8080)
        if not port:
            return None

        health = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                if self.path.split("?")[0] != "/health":
                    self.send_response(404)
                    self.end_headers()
                    return
                body = health.snapshot()
                payload = json.dumps(body).encode()
                self.send_response(503 if body["state"] == "failing" else 200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        try:
            # Railway's private network is IPv6; bind dual-stack so IPv4 works too.
            class DualStackServer(ThreadingHTTPServer):
                address_family = socket.AF_INET6
                daemon_threads = True

                def server_bind(self):
                    self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
                    super().server_bind()

            self._server = DualStackServer(("::", port), Handler)
        except OSError as exc:
            logger.warning("Health endpoint not started on port %s: %s", port, exc)
            return None

        threading.Thread(target=self._server.serve_forever, name="health", daemon=True).start()
        logger.info("Health endpoint listening on :%s/health", port)
        return self._server

    # -- pushed events --------------------------------------------------------

    def report(self, event, message=None, traceback=None):
        """POST an event to finhisaab-be. Blocking (3s max), never raises."""
        base = (os.getenv("FEED_HEALTH_REPORT_URL") or "").rstrip("/")
        key = os.getenv("FEED_HEALTH_REPORT_KEY") or ""
        if not base or not key:
            return False

        payload = {
            "event": event,
            "instance": self.instance,
            "version": self.version,
            "message": redact(message),
            "traceback": redact(traceback),
            "occurredAt": _iso(_now()),
        }
        request = urllib.request.Request(
            f"{base}/api/v1/internal/feed-health/{self.feed}/events",
            data=json.dumps({k: v for k, v in payload.items() if v is not None}).encode(),
            headers={"Content-Type": "application/json", "X-API-Key": key},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=REPORT_TIMEOUT_S) as response:
                return 200 <= response.status < 300
        except Exception as exc:  # noqa: BLE001 - reporting must never break the service
            logger.warning("Could not report %s event: %s", event, exc)
            return False

    def run(self, main):
        """
        Run `main` with crash reporting: report "started", then "fatal" with the
        traceback if it raises (Ctrl-C excepted), and re-raise so the process
        still exits non-zero and the platform restarts it.
        """
        self.report("started")
        try:
            return main()
        except KeyboardInterrupt:
            raise
        except BaseException as exc:
            if isinstance(exc, SystemExit) and exc.code in (None, 0):
                raise
            self.report(
                "fatal",
                message=f"{type(exc).__name__}: {exc}",
                traceback=traceback_module.format_exc(),
            )
            raise
