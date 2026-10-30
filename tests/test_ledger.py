import concurrent.futures
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from hookledger import Ledger, sign, LeaseLost
from hookledger.ledger import http_sender
from hookledger.server import make_server


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 1000
        self.path = Path(self.temp.name) / "hooks.db"
        self.ledger = Ledger(self.path, "synthetic-ingestion-secret", clock=lambda: self.now)
        self.ledger.subscribe("one", "http://127.0.0.1:9999/hooks", "synthetic-subscriber-secret")

    def event(self, event_id="evt-one", **overrides):
        body = json.dumps({"id": event_id, "type": "example.created", "data": {"demo": True}}).encode()
        timestamp = overrides.get("timestamp", self.now)
        signature = overrides.get("signature", sign(self.ledger.secret, timestamp, body))
        return self.ledger.ingest(body, timestamp, signature)

    def test_signature_and_signed_bytes_are_enforced(self):
        with self.assertRaises(ValueError):
            self.event(signature="0" * 64)
        body = b'{"id":"evt","type":"demo","data":1}'
        signature = sign(self.ledger.secret, self.now, body)
        with self.assertRaises(ValueError):
            self.ledger.ingest(body.replace(b":1", b":2"), self.now, signature)
        self.assertEqual(self.ledger.deliveries(), [])

    def test_expired_future_and_invalid_timestamps(self):
        for timestamp in (699, 1301, True, "bad", -1):
            with self.subTest(timestamp=timestamp), self.assertRaises(ValueError):
                self.event(timestamp=timestamp)
        self.assertFalse(self.event(timestamp=700)["duplicate"])

    def test_duplicate_events_are_idempotent_but_conflicts_fail(self):
        self.assertFalse(self.event()["duplicate"])
        self.assertTrue(self.event()["duplicate"])
        self.assertEqual(len(self.ledger.deliveries()), 1)
        body = b'{"id":"evt-one","type":"different","data":1}'
        with self.assertRaises(ValueError):
            self.ledger.ingest(body, self.now, sign(self.ledger.secret, self.now, body))

    def test_concurrent_ingestion_deduplicates(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.event(), range(12)))
        self.assertEqual(sum(not r["duplicate"] for r in results), 1)
        self.assertEqual(len(self.ledger.deliveries()), 1)

    def test_outbound_signature_and_success_history(self):
        self.event()
        def sender(url, body, headers):
            self.assertEqual(headers["X-HookLedger-Signature"], sign("synthetic-subscriber-secret", headers["X-HookLedger-Timestamp"], body))
            self.assertEqual(headers["Idempotency-Key"], "evt-one:one")
            return 204
        self.assertEqual(self.ledger.deliver_one(sender), "done")
        self.assertIsNone(self.ledger.deliver_one(sender))
        self.assertEqual([r["event"] for r in self.ledger.history(1)], ["claimed", "delivered"])

    def test_failures_backoff_and_exhaust_budget(self):
        self.event()
        self.assertEqual(self.ledger.deliver_one(lambda *_: 500), "queued")
        self.assertIsNone(self.ledger.claim())
        self.now += 2
        self.ledger.deliver_one(lambda *_: 500)
        self.now += 4
        self.assertEqual(self.ledger.deliver_one(lambda *_: 500), "dead")
        self.assertEqual(self.ledger.deliveries()[0]["attempts"], 3)

    def test_sender_exception_is_recorded(self):
        self.event()
        def sender(*_):
            raise TimeoutError("synthetic timeout")
        self.ledger.deliver_one(sender)
        self.assertIn("TimeoutError", self.ledger.deliveries()[0]["error"])

    def test_lease_recovery_fences_stale_worker(self):
        self.event()
        old = self.ledger.claim()
        self.now += 60
        fresh = self.ledger.claim()
        with self.assertRaises(LeaseLost):
            self.ledger.finish(old, status=204)
        self.assertEqual(self.ledger.finish(fresh, status=204), "done")

    def test_replay_limits_are_explicit(self):
        self.event()
        self.ledger.deliver_one(lambda *_: 204)
        for _ in range(3):
            self.ledger.replay(1)
            self.ledger.deliver_one(lambda *_: 204)
        with self.assertRaises(ValueError):
            self.ledger.replay(1)

    def test_disabling_subscriber_pauses_pending_delivery(self):
        self.event()
        self.ledger.enable("one", False)
        self.assertIsNone(self.ledger.claim())
        self.ledger.enable("one", True)
        self.assertIsNotNone(self.ledger.claim())

    def test_later_subscriber_can_be_queued_once(self):
        self.event()
        self.ledger.subscribe("two", "https://example.invalid/hooks", "another-synthetic-secret")
        self.ledger.queue("evt-one", "two")
        self.ledger.queue("evt-one", "two")
        self.assertEqual(len(self.ledger.deliveries()), 2)

    def test_restart_preserves_delivery_state(self):
        self.event()
        self.ledger.deliver_one(lambda *_: 204)
        again = Ledger(self.path, "synthetic-ingestion-secret")
        self.assertEqual(again.deliveries()[0]["state"], "done")

    def test_concurrent_claimers_do_not_share_deliveries(self):
        for i in range(10):
            self.event("evt-" + str(i))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            jobs = list(pool.map(lambda _: self.ledger.claim(), range(15)))
        ids = [job["id"] for job in jobs if job]
        self.assertEqual(len(ids), 10)
        self.assertEqual(len(set(ids)), 10)

    def test_invalid_event_and_subscriber_inputs(self):
        for url in ("file:///etc/passwd", "http://user:password@localhost", "https://example.invalid/#fragment"):
            with self.assertRaises(ValueError):
                self.ledger.subscribe("bad", url, "synthetic-secret-long")
        for body in (b"[]", b'{"id":"evt","type":"demo","data":NaN}', b"x" * 65537):
            with self.assertRaises(ValueError):
                self.ledger.ingest(body, self.now, sign(self.ledger.secret, self.now, body))

    def test_real_http_inbox_accepts_signed_event_and_rejects_invalid_signature(self):
        server = make_server(self.ledger, 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            body = b'{"id":"http-demo","type":"demo","data":1}'
            headers = {"X-HookLedger-Timestamp": str(self.now), "X-HookLedger-Signature": sign(self.ledger.secret, self.now, body)}
            request = urllib.request.Request(f"http://127.0.0.1:{server.server_port}/events", data=body, headers=headers)
            with urllib.request.urlopen(request, timeout=3) as response:
                self.assertEqual(response.status, 201)
            request.headers["X-hookledger-signature"] = "0" * 64
            with self.assertRaises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(request, timeout=3)
            self.assertEqual(error.exception.code, 400)
        finally:
            server.shutdown()
            thread.join()
            server.server_close()

    def test_real_outbound_transport_does_not_follow_redirects(self):
        seen = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_POST(self):
                seen.append(self.path)
                self.send_response(302)
                self.send_header("Location", "/unexpected")
                self.end_headers()
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with self.assertRaises(urllib.error.HTTPError):
                http_sender(f"http://127.0.0.1:{server.server_port}/hooks", b"{}", {})
            self.assertEqual(seen, ["/hooks"])
        finally:
            server.shutdown()
            thread.join()
            server.server_close()
