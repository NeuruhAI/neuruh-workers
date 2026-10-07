"""The dispatch bridge names, in its own court.requested, the two facts the
independent Sentinel will not PASS without.

sentinel_court.verify.FR_REQUIRED_FACTS is receipt_sha, receipt_created,
receipt_sealed, execution_job, job_succeeded_sha and authority_record: every
court the organ seals is a claim that an AUTHORISED FR dispatch EXECUTION
happened, and each leg must be bound by evidence the request names. Until
2026-09-14 the bridge's Switchboard was composed with neither resolver the
board reads those from (switchboard.Switchboard authority_resolver /
execution_ledger_resolver, both present since #12 and both left None by
organ_consumers.emit_receipt_sealed_and_switcharoo), so every production
request named the gaps "execution_ledger" and "authority_record" and the live
organ sealed FAIL INSUFFICIENT_EVIDENCE "missing authority_record,
execution_job, job_succeeded_sha" (court-1cec8bc25ec85b6b02036d46, organ
3f2cc79b, workers bec67a66).

Nothing here invents evidence. The execution ledger the request names IS the
bridge's own queue ledger -- the rows it enqueued, ran and sealed the job in --
and the Governance record is read from the governance organ's append-only
authorize-receipts log, never written by workers. Without a Governance log the
request still names the gap and the court still fails closed.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.parse
import urllib.request
from unittest.mock import patch

from neuruh_workers import ExecutionLedger, JobQueue, WorkflowRunner
from neuruh_workers.adjudicator_http import SentinelCourtHTTPAdjudicator
from neuruh_workers.founder_runtime import (
    DispatchBridge,
    RuntimeStatus,
    map_founder_runtime_dispatch,
)
from neuruh_workers.ledger import split_lines_exact
from neuruh_workers.organ_consumers import (
    FR_API_VERSION,
    FR_HEALTH_URL,
    GOVERNANCE_AUTHORIZE_RECEIPTS_LOG_ENV,
    execution_ledger_resolver_for,
    governance_authority_resolver_for,
)
from neuruh_workers.switchboard import INDEPENDENT_COURT_KIND
from neuruh_workers.workflows import build_catalog, build_registry
from tests.canonical_fixture import OfflineCanonical
from tests.test_founder_runtime import _fixture_git
from tests.test_sentinel_live_seam import ORGAN_ROOT_ENV, _free_port, _organ_root
from tests.test_switchboard_court_request_contract import StubAdjudicator, _gov_entry

MISSION = "msn-dispatch-court-evidence"
OTHER_MISSION = "msn-somebody-else"
APPROVAL = "appr-dispatch-court-evidence"
AUTH_ID = "auth_0000court0evidence"
KIND_RECEIPT = "prime_receipt_txt"
KIND_EVENTS = "switchboard_events_jsonl"
KIND_LEDGER = "workers_execution_ledger_jsonl"
KIND_GOVERNANCE = "governance_authorize_receipts_jsonl"
TYPED_KINDS = {KIND_RECEIPT, KIND_EVENTS, KIND_LEDGER, KIND_GOVERNANCE}
#: Findings that would mean the evidence itself did not bind. None may appear.
EVIDENCE_FINDINGS = {
    "INSUFFICIENT_EVIDENCE", "RANGE_NOT_ANCHORED", "EVIDENCE_HASH_MISMATCH",
    "LEDGER_TAMPERED", "BROKEN_HASH_CHAIN", "MALFORMED_EVIDENCE", "AUTHORITY_MISMATCH",
    "RETRIEVAL_FAILED", "APPROVAL_MISMATCH", "WORK_SPEC_MISMATCH", "SOURCE_REPO_MISMATCH",
    "SOURCE_SHA_MISMATCH", "CAUSAL_CHAIN_MISMATCH", "RECEIPT_INCOMPLETE",
}


def _sha_lines(path: str, line_start: int, line_end: int) -> str:
    with open(path, "rb") as handle:
        lines = split_lines_exact(handle.read())
    return hashlib.sha256(b"".join(lines[line_start - 1:line_end])).hexdigest()


def write_governance_log(path: str, *, allow_for: str | None, deny_after: bool = False) -> dict | None:
    """A hash-chained Governance segment as the organ writes it: a GENESIS row,
    an allow for someone else, optionally the allow under test, optionally a
    later deny for the same mission. Returns the allow row, if written."""
    rows = []
    prior = _gov_entry("GENESIS", {
        "kind": "governance_authorization", "authorization_id": "auth_prior",
        "decision": "founder_required", "target": {"type": "mission", "id": MISSION},
        "seq": 0, "prev_hash": "GENESIS"})
    rows.append(prior)
    other = _gov_entry(prior["entry_hash"], {
        "kind": "governance_authorization", "authorization_id": "auth_other_mission",
        "decision": "allow", "target": {"type": "mission", "id": OTHER_MISSION},
        "seq": 1, "prev_hash": prior["entry_hash"]})
    rows.append(other)
    allow = None
    if allow_for:
        allow = _gov_entry(other["entry_hash"], {
            "kind": "governance_authorization", "schema_version": "gov.authorization.v1",
            "authorization_id": AUTH_ID, "component": "founder_runtime",
            "action_class": "artifact_write", "target": {"type": "mission", "id": allow_for},
            "decision": "allow", "reason_code": "FOUNDER_APPROVED_SCOPE_ONCE",
            "approver_ids": ["founder"], "seq": 2, "prev_hash": other["entry_hash"]})
        rows.append(allow)
    if deny_after:
        prev = rows[-1]["entry_hash"]
        rows.append(_gov_entry(prev, {
            "kind": "governance_authorization", "authorization_id": "auth_later_deny",
            "decision": "deny", "reason_code": "SOURCE_SHA_MISMATCH",
            "target": {"type": "mission", "id": MISSION},
            "seq": len(rows), "prev_hash": prev}))
    with open(path, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")
    return allow


class _BridgeWorld(unittest.TestCase):
    """The bridge exactly as production composes it, over a fixture source tree:
    real ledger, queue, runner, catalog; GitHub-named origin (the identity gate
    pins it) with the canonical base observed on an offline mirror."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.source_root = os.path.join(self.tmp.name, "workers-source")
        os.makedirs(self.source_root)
        _fixture_git("init", "-q", "-b", "main", self.source_root)
        with open(os.path.join(self.source_root, "README"), "w", encoding="utf-8") as handle:
            handle.write("workers\n")
        _fixture_git("-C", self.source_root, "add", "README")
        _fixture_git("-C", self.source_root, "commit", "-qm", "source")
        _fixture_git("-C", self.source_root, "remote", "add", "origin",
                     "https://github.com/NeuruhAI/neuruh-workers.git")
        self.canonical = OfflineCanonical.mirror(
            self.tmp.name, self.source_root, remote_name="canonical-fixture")
        self.source_sha = _fixture_git(
            "-C", self.source_root, "rev-parse", "HEAD", capture=True).strip()
        _fixture_git("-C", self.source_root, "update-ref",
                     "refs/remotes/origin/main", self.source_sha)
        remote_head = patch("neuruh_workers.organ_consumers._remote_main_head",
                            return_value=self.source_sha)
        remote_head.start()
        self.addCleanup(remote_head.stop)
        env = patch.dict(os.environ)
        env.start()
        self.addCleanup(env.stop)
        os.environ.pop(GOVERNANCE_AUTHORIZE_RECEIPTS_LOG_ENV, None)

        self.work_spec = {
            "schema": "fr.work_spec.v1",
            "candidate_id": "candidate-court-evidence",
            "workflow_name": "property-research",
            "params": {"address": "123 Main St, Benton Harbor, MI", "county": "Berrien"},
            "intelligence_layer": "L0_DETERMINISTIC",
            "evidence_refs": ["workflow:property-research@1.0.0"],
            "production_mutation": False, "external_message": False, "money_move": False,
            "irreversible": False, "private_ip_risk": False, "code_custody_required": False,
        }
        self.work_spec_sha = hashlib.sha256(json.dumps(
            self.work_spec, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        ).encode("utf-8")).hexdigest()

        self.ledger_path = os.path.join(self.tmp.name, "workers", "execution-ledger.jsonl")
        self.ledger = ExecutionLedger(self.ledger_path)
        self.registry = build_registry()
        self.catalog = build_catalog()
        self.queue = JobQueue(self.ledger, self.registry)
        self.runner = WorkflowRunner(self.queue, self.registry, self.catalog)
        self.receipts_dir = os.path.join(self.tmp.name, "receipts")
        self.switchboard_root = os.path.join(self.tmp.name, "switchboard")
        self.gov_path = os.path.join(self.tmp.name, "governance", "authorize-receipts.jsonl")
        os.makedirs(os.path.dirname(self.gov_path))
        self.allow = write_governance_log(self.gov_path, allow_for=MISSION, deny_after=True)

    def envelope(self) -> dict:
        return {
            "api_version": FR_API_VERSION, "mission_id": MISSION, "version": 3,
            "state": "QUEUED", "objective": "Read-only public-record research.",
            "work_spec": self.work_spec, "work_spec_sha256": self.work_spec_sha,
            "authority": {
                "state": "APPROVED", "execution_authorized": True,
                "work_spec_execution_authorized": True,
                "dispatch_approval_id": APPROVAL, "work_spec_sha256": self.work_spec_sha,
            },
        }

    def opener(self, request, timeout=None):
        class _Response:
            status = 200

            def __init__(self, body):
                self.body = body

            def read(self):
                return self.body

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        if request.get_method() != "GET":
            raise OSError("non-health endpoint unavailable in court")
        if request.full_url == FR_HEALTH_URL:
            return _Response(b'{"status":"ok","api_version":"fr-api-v1","auth_configured":true}')
        if "/v1/missions/" in request.full_url:
            mission_id = urllib.parse.unquote(request.full_url.rsplit("/", 1)[-1])
            self.assertEqual(mission_id, MISSION)
            return _Response(json.dumps({"envelope": self.envelope()}).encode("utf-8"))
        raise OSError("unknown endpoint unavailable in court")

    def bridge(self, *, adjudicator=None, governance_authorize_ledger="fixture", **kw) -> DispatchBridge:
        if governance_authorize_ledger == "fixture":
            governance_authorize_ledger = self.gov_path
        return DispatchBridge(
            self.queue, self.catalog, self.runner,
            receipts_dir=self.receipts_dir, switchboard_root=self.switchboard_root,
            source_head=self.source_sha, source_root=self.source_root,
            http_open=self.opener, fr_token="court-token",
            canonical_observer=self.canonical.observe,
            adjudicator=adjudicator if adjudicator is not None else StubAdjudicator(verdict="PASS"),
            governance_authorize_ledger=governance_authorize_ledger,
            **kw,
        )

    def dispatch(self, bridge: DispatchBridge):
        ticket, candidate = map_founder_runtime_dispatch(self.envelope(), catalog=self.catalog)
        return bridge.dispatch(ticket, [candidate], auto_execute_read_only=True)

    def succeeded_line(self, job_id: str) -> int:
        with open(self.ledger_path, "rb") as handle:
            lines = split_lines_exact(handle.read())
        found = 0
        for lineno, raw in enumerate(lines, start=1):
            row = json.loads(raw)
            if row["job_id"] == job_id and row["receipt_type"] == "job.succeeded":
                found = lineno
        return found


class ProductionDefaultsCourt(_BridgeWorld):
    def test_execution_ledger_default_names_the_bridge_own_ledger_only_for_jobs_it_holds(self):
        bridge = DispatchBridge(self.queue, self.catalog, self.runner)
        self.assertIsNone(bridge.execution_ledger_resolver("job-nobody-ever-enqueued"))
        self.assertIsNone(bridge.execution_ledger_resolver(""))
        result = self.dispatch(self.bridge())
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        resolved = execution_ledger_resolver_for(self.queue)(result.job_id)
        self.assertEqual(resolved, os.path.abspath(self.ledger_path))
        self.assertEqual(bridge.execution_ledger_resolver(result.job_id), resolved)

    def test_authority_default_is_absent_without_a_governance_log_and_reads_the_named_one(self):
        self.assertIsNone(DispatchBridge(self.queue, self.catalog, self.runner).authority_resolver)
        self.assertIsNone(governance_authority_resolver_for(None))
        self.assertIsNone(governance_authority_resolver_for(""))
        with patch.dict(os.environ, {GOVERNANCE_AUTHORIZE_RECEIPTS_LOG_ENV: self.gov_path}):
            from_env = DispatchBridge(self.queue, self.catalog, self.runner).authority_resolver
        self.assertIsNotNone(from_env)
        explicit = self.bridge().authority_resolver
        for resolver in (from_env, explicit):
            found = resolver(MISSION)
            self.assertEqual(found, {
                "authorization_id": AUTH_ID, "entry_hash": self.allow["entry_hash"],
                "ledger_ref": os.path.abspath(self.gov_path), "line_start": 1, "line_end": 3,
            })
            # The later deny for the same mission is not the authority; it is not a binding either.
            self.assertIsNone(resolver("msn-never-authorised"))
            self.assertIsNone(resolver(""))
        # A missing file is a gap, not an error.
        self.assertIsNone(governance_authority_resolver_for(
            os.path.join(self.tmp.name, "absent.jsonl"))(MISSION))

    def test_only_an_allow_for_this_mission_is_an_authority_record(self):
        write_governance_log(self.gov_path, allow_for=OTHER_MISSION, deny_after=True)
        self.assertIsNone(governance_authority_resolver_for(self.gov_path)(MISSION))
        write_governance_log(self.gov_path, allow_for=None, deny_after=True)
        self.assertIsNone(governance_authority_resolver_for(self.gov_path)(MISSION))
        # An injected resolver always wins over the production default.
        injected = lambda mission_id: {"authorization_id": "x"}  # noqa: E731
        self.assertIs(self.bridge(authority_resolver=injected).authority_resolver, injected)
        ledger = lambda job_id: "/nowhere"  # noqa: E731
        self.assertIs(self.bridge(execution_ledger_resolver=ledger).execution_ledger_resolver, ledger)


class CourtRequestEvidenceCourt(_BridgeWorld):
    def test_the_request_binds_the_real_ledger_and_the_governance_record(self):
        stub = StubAdjudicator(verdict="PASS")
        result = self.dispatch(self.bridge(adjudicator=stub))
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertEqual(result.sealed_receipt["event_type"], "receipt.sealed")
        self.assertEqual(result.switchboard_emit["bind_status"], "SWITCHBOARD_CONSUMED")
        court = result.court_request
        self.assertEqual(court["event_type"], "court.requested")
        payload = court["payload_ref_or_payload"]

        by_kind = {r["kind"]: r for r in payload["evidence_refs"]}
        self.assertEqual(set(by_kind), TYPED_KINDS)
        self.assertNotIn("execution_ledger", payload["binding_gaps"])
        self.assertNotIn("authority_record", payload["binding_gaps"])

        # The execution ledger is THIS bridge's queue ledger, anchored at line 1 and
        # closed at the job.succeeded row, hashed exactly as the organ re-hashes it.
        ledger_ref = by_kind[KIND_LEDGER]
        self.assertEqual(ledger_ref["ref"], os.path.abspath(self.ledger_path))
        self.assertEqual(ledger_ref["line_start"], 1)
        self.assertEqual(ledger_ref["line_end"], self.succeeded_line(result.job_id))
        self.assertGreater(ledger_ref["line_end"], 1)
        self.assertEqual(ledger_ref["sha256"], _sha_lines(self.ledger_path, 1, ledger_ref["line_end"]))
        rows = [json.loads(line) for line in split_lines_exact(open(self.ledger_path, "rb").read())]
        self.assertEqual(rows[0]["receipt_type"], "job.enqueued")
        self.assertEqual(rows[0]["payload"]["requested_by"], f"jgi:{MISSION}")
        self.assertEqual(rows[ledger_ref["line_end"] - 1]["receipt_type"], "job.succeeded")
        self.assertEqual(rows[ledger_ref["line_end"] - 1]["job_id"], result.job_id)

        # The Governance record is named by id AND entry hash, from the log, line 1 onward.
        self.assertEqual(payload["authority_record_ref"], f"{AUTH_ID}:{self.allow['entry_hash']}")
        gov_ref = by_kind[KIND_GOVERNANCE]
        self.assertEqual(gov_ref["ref"], os.path.abspath(self.gov_path))
        self.assertEqual((gov_ref["line_start"], gov_ref["line_end"]), (1, 3))
        self.assertEqual(gov_ref["sha256"], _sha_lines(self.gov_path, 1, 3))

        # The court is carried on the board's next tick (the adjudicate consumer
        # leases court.requested); the first dispatch reports it pending.
        self.assertFalse(result.sentinel["judged"])
        self.assertEqual(result.note, "FR ticket execution sealed; independent court is pending")
        self.assertEqual(stub.packets, [])

        # What the organ is handed is the event, nothing assembled on the way.
        replay = self.dispatch(self.bridge(adjudicator=stub))
        self.assertEqual(replay.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertEqual(len(stub.packets), 1)
        self.assertEqual(stub.packets[0]["event_id"], court["event_id"])
        self.assertEqual(stub.packets[0]["payload_ref_or_payload"]["evidence_refs"], payload["evidence_refs"])
        self.assertEqual(stub.packets[0]["payload_ref_or_payload"]["authority_record_ref"],
                         payload["authority_record_ref"])
        self.assertEqual(replay.sentinel["verdict"], "PASS")
        self.assertEqual(replay.note, "FR ticket dispatched, independently courted, and sealed")

    def test_the_ledger_digest_is_final_at_emission_and_survives_the_replay(self):
        stub = StubAdjudicator(verdict="PASS")
        first = self.dispatch(self.bridge(adjudicator=stub))
        self.assertEqual(first.status, RuntimeStatus.SUCCEEDED)
        ledger_ref = next(r for r in first.court_request["payload_ref_or_payload"]["evidence_refs"]
                          if r["kind"] == KIND_LEDGER)
        replay = self.dispatch(self.bridge(adjudicator=stub))
        self.assertEqual(replay.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertEqual(replay.job_id, first.job_id)
        self.assertEqual(replay.sentinel["verdict"], "PASS")
        rows = [json.loads(line) for line in split_lines_exact(open(self.ledger_path, "rb").read())]
        self.assertEqual(rows[-1]["receipt_type"], "job.enqueue_deduplicated")
        self.assertGreater(len(rows), ledger_ref["line_end"])
        self.assertEqual(ledger_ref["sha256"], _sha_lines(self.ledger_path, 1, ledger_ref["line_end"]))
        self.assertEqual(len(stub.packets), 1, "the same court request is not re-requested")

    def test_without_a_governance_log_the_request_names_the_gap_and_invents_nothing(self):
        stub = StubAdjudicator(verdict="PASS")
        result = self.dispatch(self.bridge(adjudicator=stub, governance_authorize_ledger=None))
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        payload = result.court_request["payload_ref_or_payload"]
        kinds = {r["kind"] for r in payload["evidence_refs"]}
        self.assertEqual(kinds, TYPED_KINDS - {KIND_GOVERNANCE})
        self.assertNotIn("authority_record_ref", payload)
        self.assertIn("authority_record", payload["binding_gaps"])
        self.assertNotIn("execution_ledger", payload["binding_gaps"])

    def test_a_governance_log_that_never_allowed_this_mission_is_a_gap(self):
        write_governance_log(self.gov_path, allow_for=OTHER_MISSION, deny_after=True)
        result = self.dispatch(self.bridge())
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        payload = result.court_request["payload_ref_or_payload"]
        self.assertNotIn("authority_record_ref", payload)
        self.assertIn("authority_record", payload["binding_gaps"])


class SentinelLiveEvidenceCourt(_BridgeWorld):
    """The organ itself, as a separate process, handed the bridge's real request:
    it must bind execution_job, job_succeeded_sha and authority_record from the
    evidence the request names. Without an IAR client the organ cannot PASS
    (MODEL_UNAVAILABLE) and with an offline canonical mirror it refuses the base
    (CANONICAL_REPO_LOCAL_REFUSED); neither is an evidence finding, and no
    evidence finding may appear."""

    def setUp(self):
        super().setUp()
        root = _organ_root()
        if root is None:
            self.skipTest(f"no neuruh-sentinel-court checkout ({ORGAN_ROOT_ENV} or sibling)")
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

    def test_the_real_organ_binds_every_required_fact_from_the_bridge_request(self):
        self.assertNotIn("sentinel_court", sys.modules, "workers must never import the organ")
        self.assertEqual(self.health.get("kind"), INDEPENDENT_COURT_KIND)
        client = SentinelCourtHTTPAdjudicator(f"http://127.0.0.1:{self.port}", timeout=120.0)
        result = self.dispatch(self.bridge(adjudicator=client))
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        packet = json.loads(json.dumps(result.court_request))

        record = client(packet)
        self.assertEqual(record.get("kind"), INDEPENDENT_COURT_KIND)
        self.assertTrue(record.get("independent"))
        self.assertEqual(record.get("candidate_sha"), self.source_sha)
        codes = {str(f.get("code")) for f in record.get("findings") or []}
        self.assertEqual(codes & EVIDENCE_FINDINGS, set(), (codes, record.get("findings")))
        items = {item.get("kind"): item for item in record.get("evidence_items") or []}
        self.assertEqual(set(items), TYPED_KINDS, items)
        for kind, item in items.items():
            self.assertEqual(item.get("sha256"), item.get("declared_sha256"), kind)
        bound = {fact for item in items.values() for fact in (item.get("bound") or [])}
        for fact in ("receipt_sha", "receipt_created", "receipt_sealed",
                     "execution_job", "job_succeeded_sha", "authority_record"):
            self.assertIn(fact, bound, (fact, bound))


if __name__ == "__main__":
    unittest.main()
