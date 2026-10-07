"""The loopback HTTP adapter to the independent Sentinel court organ.

Nothing here is mocked. Every case runs a real HTTP server on a real loopback
socket and a real urllib round trip, because the thing under test IS the
transport. A stubbed transport would prove only that the stub agrees with
itself -- and a seam that was never actually connected is the defect this
module exists to close.

The organ is never imported. That is asserted, not assumed: importing it would
be the in-process wiring that was deliberately ruled out, and it would collapse
the process boundary that makes "independent" true at runtime.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from neuruh_workers.adjudicator_http import (
    ADJUDICATE_PATH,
    COURT_TIMEOUT_ENV,
    DEFAULT_TIMEOUT_SECONDS,
    ORGAN_ROUND_TRIP_CEILING_SECONDS,
    COURT_URL_ENV,
    DEFAULT_COURT_URL,
    SentinelCourtHTTPAdjudicator,
    court_timeout,
    court_url,
    sentinel_court_adjudicator,
)
from neuruh_workers.switchboard import AdjudicatorUnavailable

ENVELOPE = {"event_id": "evt-1", "event_type": "court.requested",
            "payload_ref_or_payload": {"court_job_id": "job-1"}}


def _closed_port() -> int:
    """A port that is bound, read, then released -- so connections are refused."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class _Organ:
    """Stands in for sentinel_court's loopback service at the HTTP level only."""

    def __init__(self, *, status: int = 200, body: Any = None, delay: float = 0.0,
                 raw: bytes | None = None) -> None:
        self.received: list[dict] = []
        organ = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # noqa: A002
                return

            def _respond(self):
                if delay:
                    time.sleep(delay)
                payload = raw if raw is not None else json.dumps(
                    body if body is not None else {}).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):  # noqa: N802
                self._respond()

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length") or "0")
                organ.received.append(json.loads(self.rfile.read(length).decode("utf-8")))
                organ.path = self.path
                self._respond()

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.path = ""
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()



class CourtURLTests(unittest.TestCase):
    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in (COURT_URL_ENV, COURT_TIMEOUT_ENV)}
        self.addCleanup(self._restore)

    def _restore(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_default_is_the_organs_own_bind(self):
        os.environ.pop(COURT_URL_ENV, None)
        self.assertEqual(court_url(), DEFAULT_COURT_URL)
        # Pinned to sentinel_court/service.py serve_forever's default.
        self.assertEqual(DEFAULT_COURT_URL, "http://127.0.0.1:18731")

    def test_env_overrides_the_default(self):
        os.environ[COURT_URL_ENV] = "http://localhost:19999/"
        self.assertEqual(court_url(), "http://localhost:19999")
        self.assertEqual(sentinel_court_adjudicator().base_url, "http://localhost:19999")

    def test_a_public_court_url_is_refused(self):
        """The organ refuses a public bind; workers refuses to dial one.

        A court reached over a public network is not the court this seam was
        authorised to consult, and it fails at construction rather than on the
        first candidate to come along."""
        for url in ("http://10.0.0.5:18731", "http://sentinel.example.com"):
            with self.assertRaises(AdjudicatorUnavailable) as caught:
                SentinelCourtHTTPAdjudicator(url)
            self.assertIn("loopback", str(caught.exception))

    def test_a_nonsense_timeout_is_refused_rather_than_defaulted(self):
        for bad in ("0", "-5", "soon"):
            os.environ[COURT_TIMEOUT_ENV] = bad
            with self.assertRaises(AdjudicatorUnavailable):
                court_timeout()


class RoundTripTests(unittest.TestCase):
    def test_the_envelope_crosses_verbatim_and_the_record_returns_verbatim(self):
        record = {"kind": "independent_sentinel_court", "verdict": "PASS",
                  "switchboard_ingest": {"switchboard_passed": True, "ingest_verdict": "PASS"}}
        organ = _Organ(body=record)
        self.addCleanup(organ.close)

        got = SentinelCourtHTTPAdjudicator(organ.url, timeout=5.0)(ENVELOPE)

        self.assertEqual(organ.path, ADJUDICATE_PATH)
        self.assertEqual(organ.received, [ENVELOPE], "the packet must cross unchanged")
        self.assertEqual(got, record, "the record must arrive unchanged")

    def test_health_reports_the_organ(self):
        organ = _Organ(body={"organ": "independent-sentinel-court"})
        self.addCleanup(organ.close)
        self.assertEqual(
            SentinelCourtHTTPAdjudicator(organ.url, timeout=5.0).health()["organ"],
            "independent-sentinel-court")


class BlockedNotJudgedTests(unittest.TestCase):
    """Every failure mode must be a named block. None may become a verdict."""

    def test_connection_refused(self):
        client = SentinelCourtHTTPAdjudicator(f"http://127.0.0.1:{_closed_port()}", timeout=2.0)
        with self.assertRaises(AdjudicatorUnavailable) as caught:
            client(ENVELOPE)
        self.assertIn("unreachable", str(caught.exception))

    def test_timeout_is_bounded_and_named(self):
        organ = _Organ(body={}, delay=2.0)
        self.addCleanup(organ.close)
        client = SentinelCourtHTTPAdjudicator(organ.url, timeout=0.25)
        started = time.monotonic()
        with self.assertRaises(AdjudicatorUnavailable):
            client(ENVELOPE)
        self.assertLess(time.monotonic() - started, 1.5, "the client must not wait forever")

    def test_a_refusal_names_the_organs_own_code(self):
        """422 carries the organ's refusal (PACKET_INVALID, ORGAN_FAULT...).

        It is a real answer about the REQUEST, surfaced verbatim -- and still not
        a verdict the candidate earned, so it blocks rather than being ingested."""
        organ = _Organ(status=422, body={"code": "PACKET_INVALID", "detail": "no evidence"})
        self.addCleanup(organ.close)
        with self.assertRaises(AdjudicatorUnavailable) as caught:
            SentinelCourtHTTPAdjudicator(organ.url, timeout=5.0)(ENVELOPE)
        message = str(caught.exception)
        self.assertIn("422", message)
        self.assertIn("PACKET_INVALID", message)
        self.assertIn("no verdict was sealed", message)

    def test_a_500_is_not_a_fail_verdict(self):
        organ = _Organ(status=500, body={"error": "boom"})
        self.addCleanup(organ.close)
        with self.assertRaises(AdjudicatorUnavailable) as caught:
            SentinelCourtHTTPAdjudicator(organ.url, timeout=5.0)(ENVELOPE)
        self.assertIn("500", str(caught.exception))

    def test_a_non_json_body_is_a_block(self):
        organ = _Organ(raw=b"<html>gateway</html>")
        self.addCleanup(organ.close)
        with self.assertRaises(AdjudicatorUnavailable) as caught:
            SentinelCourtHTTPAdjudicator(organ.url, timeout=5.0)(ENVELOPE)
        self.assertIn("non-JSON", str(caught.exception))


class IndependenceTests(unittest.TestCase):
    def test_workers_never_imports_the_organ(self):
        """The boundary is the whole point. If sentinel_court is importable in
        this process, the adapter has become the in-process wiring it replaced."""
        import neuruh_workers.adjudicator_http  # noqa: F401
        import neuruh_workers.organ_consumers  # noqa: F401
        import neuruh_workers.switchboard  # noqa: F401
        self.assertNotIn("sentinel_court", sys.modules)


if __name__ == "__main__":
    unittest.main()


class DefaultTimeoutOutlastsTheOrgan(unittest.TestCase):
    """A bound shorter than the organ's own IAR round trip can never carry a PASS:
    the client gives up, the court stays open, the replay re-requests it forever.
    Observed 2026-09-14 at the old 30s bound against a live organ that sealed a
    genuine PASS at 58s (court-e309e9b41486414c5d42d166)."""

    def test_default_bound_covers_the_organ_round_trip(self):
        saved = os.environ.pop(COURT_TIMEOUT_ENV, None)
        try:
            self.assertEqual(court_timeout(), DEFAULT_TIMEOUT_SECONDS)
            self.assertGreaterEqual(DEFAULT_TIMEOUT_SECONDS, ORGAN_ROUND_TRIP_CEILING_SECONDS)
            self.assertGreaterEqual(DEFAULT_TIMEOUT_SECONDS, 58.0 * 2)
        finally:
            if saved is not None:
                os.environ[COURT_TIMEOUT_ENV] = saved

    def test_environment_still_overrides_the_default(self):
        saved = os.environ.get(COURT_TIMEOUT_ENV)
        os.environ[COURT_TIMEOUT_ENV] = "5"
        try:
            self.assertEqual(court_timeout(), 5.0)
        finally:
            if saved is None:
                os.environ.pop(COURT_TIMEOUT_ENV, None)
            else:
                os.environ[COURT_TIMEOUT_ENV] = saved
