import json
import os
import socket
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

from psx import feed_health
from psx.feed_health import FeedHealth, redact


def free_port():
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
        s.bind(("::", 0))
        return s.getsockname()[1]


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.health = FeedHealth("psx-intraday", stall_after_s=600, failing_after=2)
        self.t0 = self.health.started_at

    def test_running_after_success(self):
        self.health.attempt()
        self.health.success(symbols=492)
        body = self.health.snapshot()
        self.assertEqual(body["state"], "running")
        self.assertEqual(body["details"], {"symbols": 492})
        self.assertEqual(body["consecutive_failures"], 0)

    def test_failing_after_consecutive_failures_with_last_error(self):
        for _ in range(2):
            self.health.attempt()
            self.health.failure("PSX_FETCH_FAILED", "RemoteDisconnected")
        body = self.health.snapshot()
        self.assertEqual(body["state"], "failing")
        self.assertEqual(body["last_error"]["code"], "PSX_FETCH_FAILED")

    def test_success_resets_failures(self):
        self.health.failure("PSX_FETCH_FAILED", "x")
        self.health.failure("PSX_FETCH_FAILED", "x")
        self.health.success()
        self.assertEqual(self.health.snapshot()["state"], "running")

    def test_sleeping_then_stalled_when_the_sleep_overruns(self):
        until = self.t0 + timedelta(hours=1)
        self.health.sleeping(until, "market closed")
        self.assertEqual(self.health.snapshot(self.t0 + timedelta(minutes=30))["state"], "sleeping")
        # Woke at `until` but never attempted work: stalled 600s after the sleep ended.
        self.assertEqual(self.health.snapshot(until + timedelta(minutes=5))["state"], "running")
        stalled = self.health.snapshot(until + timedelta(minutes=11))
        self.assertEqual(stalled["state"], "failing")
        self.assertEqual(stalled["last_error"]["code"], "STALLED")

    def test_hung_loop_reports_stalled(self):
        self.health.attempt()
        body = self.health.snapshot(datetime.now(timezone.utc) + timedelta(minutes=15))
        self.assertEqual(body["state"], "failing")
        self.assertEqual(body["last_error"]["code"], "STALLED")

    def test_attempt_ends_a_sleep(self):
        self.health.sleeping(self.t0 + timedelta(hours=1), "market closed")
        self.health.attempt()
        self.assertEqual(self.health.snapshot()["state"], "running")


class RedactTests(unittest.TestCase):
    def test_strips_uri_credentials(self):
        text = "connect to mongodb://fh_app:s3cretpass@mongo.internal:27017/db failed"
        self.assertEqual(redact(text), "connect to mongodb://<redacted>@mongo.internal:27017/db failed")

    def test_strips_secret_env_values(self):
        with patch.dict(os.environ, {"SOME_API_KEY": "abcdef123456"}):
            self.assertEqual(redact("key abcdef123456 rejected"), "key <SOME_API_KEY> rejected")

    def test_failure_messages_are_redacted(self):
        health = FeedHealth("x")
        health.failure("MONGO", "mongodb://u:pw123456@h/db")
        self.assertNotIn("pw123456", health.snapshot()["last_error"]["message"])


class ServerTests(unittest.TestCase):
    def test_serves_health_json_and_503_when_failing(self):
        health = FeedHealth("psx-intraday", failing_after=1)
        port = free_port()
        server = health.start_server(port)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read())["service"], "psx-intraday")

        health.failure("PSX_FETCH_FAILED", "boom")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(f"http://[::1]:{port}/health", timeout=3)
        self.assertEqual(ctx.exception.code, 503)
        self.assertEqual(json.loads(ctx.exception.read())["state"], "failing")

    def test_bind_failure_does_not_raise(self):
        health = FeedHealth("x")
        port = free_port()
        first = health.start_server(port)
        self.addCleanup(first.server_close)
        self.addCleanup(first.shutdown)
        self.assertIsNone(FeedHealth("y").start_server(port))


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.received = []
        received = self.received

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers["Content-Length"]))
                received.append((self.path, self.headers["X-API-Key"], json.loads(body)))
                self.send_response(202)
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        env = {
            "FEED_HEALTH_REPORT_URL": f"http://127.0.0.1:{self.server.server_port}/",
            "FEED_HEALTH_REPORT_KEY": "feed-key-123",
        }
        patcher = patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_run_reports_started_then_fatal_and_reraises(self):
        health = FeedHealth("psx-intraday")

        def crash():
            raise KeyError("generation")

        with self.assertRaises(KeyError):
            health.run(crash)

        self.assertEqual([r[2]["event"] for r in self.received], ["started", "fatal"])
        path, key, fatal = self.received[1]
        self.assertEqual(path, "/api/v1/internal/feed-health/psx-intraday/events")
        self.assertEqual(key, "feed-key-123")
        self.assertEqual(fatal["message"], "KeyError: 'generation'")
        self.assertIn("Traceback", fatal["traceback"])

    def test_clean_exit_reports_only_started(self):
        FeedHealth("psx-intraday").run(lambda: None)
        self.assertEqual([r[2]["event"] for r in self.received], ["started"])

    def test_unreachable_backend_never_raises(self):
        with patch.dict(os.environ, {"FEED_HEALTH_REPORT_URL": "http://127.0.0.1:1"}):
            self.assertFalse(FeedHealth("x").report("started"))

    def test_disabled_without_url(self):
        with patch.dict(os.environ, {"FEED_HEALTH_REPORT_URL": ""}):
            self.assertFalse(FeedHealth("x").report("started"))
        self.assertEqual(self.received, [])


if __name__ == "__main__":
    unittest.main()
