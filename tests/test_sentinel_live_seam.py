"""The seam, end to end, against a REAL independent Sentinel court organ.

Everything in tests/test_switchboard_court_request_contract.py drives the
consumer with a StubAdjudicator that was hand-written to satisfy this board's
own ingest gate -- so it could never detect that the producer disagreed, and for
five days it did not (handoff/2026-09-09/12_stub_drift_finding.md:
STUB_GREEN != CONTRACT_HONORED). This test is the one that can.

The organ is started as a separate PROCESS from a neuruh-sentinel-court
checkout (NEURUH_SENTINEL_COURT_ROOT, else the sibling checkout) and reached
only over loopback through the production HTTP client. Workers never imports
it; that is asserted. When no checkout is present the test SKIPS and says so --
it never substitutes a double.

What it proves: the board's own consumer leases court.requested, the organ seals
a record that echoes all sixteen identity fields the ingest gate binds on, and
the verdict lands on the ledger as court.passed|court.failed with no deadletter
and no block. With an offline canonical remote the honest verdict is FAIL
(CANONICAL_REPO_LOCAL_REFUSED): a FAIL the candidate earned, ingested through
the same gate a PASS would be.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import unittest
import urllib.request

from neuruh_workers.adjudicator_http import SentinelCourtHTTPAdjudicator
from neuruh_workers.events import EventType
from neuruh_workers.switchboard import INDEPENDENT_COURT_KIND
from neuruh_workers.switchboard_jobs import SENTINEL_MODEL_EXECUTION

from tests.test_switchboard_court_request_contract import _Court

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORGAN_ROOT_ENV = "NEURUH_SENTINEL_COURT_ROOT"


def _organ_root() -> str | None:
    named = os.environ.get(ORGAN_ROOT_ENV, "").strip()
    candidates = [named] if named else [
        os.path.join(os.path.dirname(REPO_ROOT), "neuruh-sentinel-court"),
    ]
    for root in candidates:
        if root and os.path.isfile(os.path.join(root, "sentinel_court", "service.py")):
            return os.path.abspath(root)
    return None


def _git_head(root: str) -> str:
    try:
        return subprocess.run(["git", "-C", root, "rev-parse", "HEAD"], capture_output=True,
                              text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class SentinelLiveSeamTests(_Court):
    def setUp(self):
        super().setUp()
        root = _organ_root()
        if root is None:
            self.skipTest(f"no neuruh-sentinel-court checkout ({ORGAN_ROOT_ENV} or sibling); "
                          "the live seam is not proven in this environment")
        self.organ_root = root
        self.organ_head = _git_head(root)
        self.port = _free_port()
        self.organ = subprocess.Popen(
            [sys.executable, "-c",
             "from sentinel_court.service import serve_forever; "
             f"serve_forever(host='127.0.0.1', port={self.port})"],
            cwd=root, env={**os.environ, "PYTHONPATH": root},
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        self.addCleanup(self._stop_organ)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.organ.poll() is not None:
                self.fail("organ exited: " + self.organ.stderr.read().decode("utf-8")[-800:])
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/health", timeout=1) as r:
                    self.health = json.loads(r.read().decode("utf-8"))
                    break
            except OSError:
                time.sleep(0.1)
        else:
            self.fail("organ did not answer /health within 15s")

    def _stop_organ(self):
        if self.organ.poll() is None:
            self.organ.terminate()
            try:
                self.organ.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.organ.kill()

    def test_real_organ_verdict_is_ingested_through_the_identity_gate(self):
        self.assertNotIn("sentinel_court", sys.modules, "workers must never import the organ")
        self.assertEqual(self.health.get("kind"), INDEPENDENT_COURT_KIND)

        world = self.world()
        client = SentinelCourtHTTPAdjudicator(f"http://127.0.0.1:{self.port}", timeout=120.0)
        board = self.board(world, adjudicator=client)
        board.tick()
        court = self.court(board)
        sealed = board.events(EventType.RECEIPT_SEALED)[0]
        # The request names the repository the receipt names (never this board's own).
        self.assertEqual(court["source_repo"], sealed["source_repo"])

        report = board.tick()
        context = f"organ {self.organ_root} @ {self.organ_head}"
        self.assertIn(court["event_id"], report.acked, context)
        self.assertEqual(report.blocked, [], context)
        self.assertEqual(board.deadletters(), [], context)

        outcomes = board.events(EventType.COURT_FAILED) + board.events(EventType.COURT_PASSED)
        self.assertEqual(len(outcomes), 1, context)
        outcome = outcomes[0]
        self.assertEqual(outcome["causation_id"], court["event_id"])

        courts_dir = os.path.join(board.root, "courts")
        records = sorted(os.listdir(courts_dir))
        self.assertEqual(len(records), 1)
        with open(os.path.join(courts_dir, records[0]), encoding="utf-8") as handle:
            record = json.load(handle)
        inner = court["payload_ref_or_payload"]
        expected = {
            "kind": INDEPENDENT_COURT_KIND, "source_role": "SENTINEL", "independent": True,
            "mission_id": court["mission_id"], "job_id": inner["court_job_id"],
            "receipt_ref": inner["receipt_ref"],
            "receipt_sha": sealed["payload_ref_or_payload"]["receipt_sha"],
            "candidate_sha": court["source_head"], "plan_id": court["plan_id"],
            "task_id": court["task_id"], "correlation_id": court["correlation_id"],
            "causation_id": court["event_id"], "source_head": court["source_head"],
            "source_repo": court["source_repo"], "authority_ref": court["authority_ref"],
            "workers_sentinel_model_execution": SENTINEL_MODEL_EXECUTION,
        }
        for field, value in expected.items():
            self.assertEqual(record.get(field), value, f"{field} ({context})")
        self.assertIn(record["verdict"], {"PASS", "PASS_WITH_EXCEPTIONS", "FAIL"})
        # An offline bare remote is not a canonical remote; the organ says so and
        # the board records the FAIL the candidate earned -- not a block, not a
        # deadletter, not a PASS.
        self.assertEqual(outcome["event_type"], EventType.COURT_FAILED.value, context)
        self.assertIn("CANONICAL_REPO_LOCAL_REFUSED", [f["code"] for f in record["findings"]])
        self.assertEqual(SENTINEL_MODEL_EXECUTION, "BLOCKED")


if __name__ == "__main__":
    unittest.main()
