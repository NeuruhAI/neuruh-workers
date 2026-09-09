"""court.requested must name its evidence and bind it; a consumer must carry it to
the independent Sentinel with no operator hand-assembly; the outcome verifier must
accept an OBSERVED base and refuse an asserted one (H01 / A04 §6, §11, §12).

The live T02 court proved the gap: Switchboard emitted court.requested with no
evidence_refs, no receipt_sha, no canonical base, no work_spec_sha256 and no
Governance record, and no consumer ever leased it. A person had to build the
organ's packet by hand -- and the packet they built asserted canonical_sha ==
candidate_sha, which the organ accepted as a relation. Nothing here may be
hand-assembled: the packet the organ receives IS the emitted event.

The organ is never imported here. Workers must not depend on the Sentinel; the
adjudicator is injected, exactly as git_custodian and promotion_executor are,
and without one court.requested stays unroutable -- fail closed, as today.

Observer contract (closed 2026-09-06): a board that did not itself observe the
canonical base may not accept a PASS merely because the judged-side record is
self-consistent. Both production composition roots now inject the git observer
(ProductionWiringTests), and _require_observed_relation refuses a PASS whose
request named the canonical_base gap or declared the unobserved sentinel.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest

from neuruh_workers import Switchboard
from neuruh_workers.contracts import content_sha
from neuruh_workers.events import AckState, DeadletterReason, EventType
from neuruh_workers.ledger import ExecutionLedger
from neuruh_workers.organ_consumers import _fr_dispatch_receipt_text
from neuruh_workers.switchboard import (
    INDEPENDENT_COURT_KIND, SwitchboardError, observe_canonical_base_via_git,
    _UNOBSERVED_BASE,
)
from neuruh_workers.switchboard_jobs import HONEY_POT_WORKFLOW, REMEDIATION_WORKFLOW
from tests.helpers import fixed_clock

GIT = os.environ.get("NEURUH_GIT") or shutil.which("git") or "git"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MISSION = "msn_0000000000000000000000000000c0e1"
JOB_ID = "job-000000000000000000000c0e1"
APPROVAL = "appr_0000000000000000000000000000c0e1"
WORK_SPEC = "7" * 64
AUTHORITY = f"fr-api-v1:approval:{APPROVAL}:work-spec:{WORK_SPEC}"
AUTH_ID = "auth_00000000000c0e1"
CONSUMER_ID = "switchboard.adjudicate-on-court-requested.v1"
TYPED_KINDS = {
    "switchboard_events_jsonl", "workers_execution_ledger_jsonl",
    "prime_receipt_txt", "governance_authorize_receipts_jsonl",
}


def _git(repo: str, *args: str) -> str:
    return subprocess.run([GIT, "-C", repo, *args], capture_output=True, text=True,
                          check=True).stdout.strip()


def _sha_file(path: str, line_start: int = 0, line_end: int = 0) -> str:
    with open(path, "rb") as handle:
        body = handle.read()
    if line_start or line_end:
        lines = body.splitlines(keepends=True)
        body = b"".join(lines[line_start - 1:line_end])
    return hashlib.sha256(body).hexdigest()


def _aggregate(shas) -> str:
    return hashlib.sha256(json.dumps(sorted(shas), separators=(",", ":")).encode("utf-8")).hexdigest()


def _gov_entry(prev: str, body: dict) -> dict:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return {**body, "entry_hash": hashlib.sha256((prev + canonical).encode("utf-8")).hexdigest()}


class _World:
    """A worktree with an offline canonical remote, a real execution ledger for the
    job, a Governance segment, and the receipt the bridge actually emits."""

    def __init__(self, tmp: str, *, ahead_of_main: bool = False) -> None:
        self.tmp = tmp
        self.worktree = os.path.join(tmp, "worktree")
        self.remote = os.path.join(tmp, "canonical.git")
        os.makedirs(self.worktree)
        _git(self.worktree, "init", "-q", "-b", "main")
        _git(self.worktree, "config", "user.email", "court@test")
        _git(self.worktree, "config", "user.name", "court")
        _git(self.worktree, "commit", "-q", "--allow-empty", "-m", "base")
        self.base = _git(self.worktree, "rev-parse", "HEAD").lower()
        subprocess.run([GIT, "init", "-q", "--bare", self.remote], check=True)
        _git(self.worktree, "remote", "add", "origin", self.remote)
        _git(self.worktree, "push", "-q", "origin", "main")
        if ahead_of_main:
            _git(self.worktree, "checkout", "-q", "-b", "candidate")
            _git(self.worktree, "commit", "-q", "--allow-empty", "-m", "candidate")
        self.head = _git(self.worktree, "rev-parse", "HEAD").lower()

        # Workers execution ledger: the real class, three receipts for the job.
        self.ledger_path = os.path.join(tmp, "execution-ledger.jsonl")
        ledger = ExecutionLedger(self.ledger_path, clock=fixed_clock())
        ledger.append("job.enqueued", JOB_ID, {"dedupe_key": "d" * 64, "dry_run": False,
                                               "params": {"address": "77 Orchard Ln"},
                                               "requested_by": f"jgi:{MISSION}"})
        ledger.append("job.started", JOB_ID, {})
        succeeded = ledger.append("job.succeeded", JOB_ID, {"dry_run": False, "results_sha": "e" * 64})
        self.job_succeeded_sha = succeeded.receipt_sha
        self.ledger_tail_sha = ledger.verify()["tail_sha"]

        # Governance segment: a prior entry and the allow record, hash-chained.
        prior = _gov_entry("0" * 64, {"kind": "governance_authorization", "authorization_id": "auth_prior",
                                       "decision": "founder_required",
                                       "target": {"type": "mission", "id": MISSION},
                                       "seq": 210, "prev_hash": "0" * 64})
        self.allow = _gov_entry(prior["entry_hash"], {
            "kind": "governance_authorization", "schema_version": "gov.authorization.v1",
            "authorization_id": AUTH_ID, "component": "founder_runtime",
            "action_class": "artifact_write", "target": {"type": "mission", "id": MISSION},
            "decision": "allow", "reason_code": "FOUNDER_APPROVED_SCOPE_ONCE",
            "approver_ids": ["founder"], "scope_key": "5" * 64,
            "seq": 211, "prev_hash": prior["entry_hash"]})
        self.gov_path = os.path.join(tmp, "authorize-receipts.jsonl")
        with open(self.gov_path, "w", encoding="utf-8") as handle:
            for row in (prior, self.allow):
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")

    def receipt_text(self) -> str:
        return _fr_dispatch_receipt_text(
            mission_id=MISSION, mission_version=15, work_spec_sha256=WORK_SPEC,
            idempotency_key="b" * 64, dispatch_result="SUCCEEDED", job_id=JOB_ID,
            ledger_tail_sha=self.ledger_tail_sha, context_digest="c" * 64,
            evidence_refs=["workflow:property-research@1.0.0"],
            job_succeeded_sha=self.job_succeeded_sha, source_head=self.head,
            source_branch="main", source_worktree=self.worktree,
            authority_ref=AUTHORITY, code_custody_required=False,
        )

    # The two facts the board cannot know by itself, supplied by injection.
    def authority_resolver(self, mission_id: str):
        assert mission_id == MISSION
        return {"authorization_id": AUTH_ID, "entry_hash": self.allow["entry_hash"],
                "ledger_ref": self.gov_path, "line_start": 1, "line_end": 2}

    def execution_ledger_resolver(self, job_id: str):
        assert job_id == JOB_ID
        return self.ledger_path


class StubAdjudicator:
    """Stands in for the organ. Derives EVERYTHING from the packet it is handed,
    so a test can prove the packet came from the event and nowhere else."""

    def __init__(self, *, verdict: str = "PASS", findings=None, exceptions=None,
                 relation: str = "candidate_is_canonical_base", observed_remote: bool = True,
                 canonical_sha: str | None = None, evidence_sha256: str | None = None,
                 evidence_refs=None, observed_sha: str | None = None) -> None:
        self.verdict = verdict
        self.findings = list(findings or [])
        self.exceptions = list(exceptions or [])
        self.relation = relation
        self.observed_remote = observed_remote
        self.canonical_sha = canonical_sha
        self.evidence_sha256 = evidence_sha256
        self.evidence_refs = evidence_refs
        self.observed_sha = observed_sha
        self.packets: list[dict] = []

    def __call__(self, packet: dict) -> dict:
        self.packets.append(json.loads(json.dumps(packet)))
        inner = packet.get("payload_ref_or_payload") or {}
        refs = self.evidence_refs if self.evidence_refs is not None else [
            r["ref"] for r in inner.get("evidence_refs") or [] if isinstance(r, dict)]
        evidence_sha256 = (self.evidence_sha256 if self.evidence_sha256 is not None
                           else inner.get("evidence_sha256") or "")
        observed = self.observed_sha if self.observed_sha is not None else hashlib.sha256(
            evidence_sha256.encode("utf-8")).hexdigest()
        canonical = self.canonical_sha or inner.get("canonical_sha") or packet["source_head"]
        court_id = "court-" + hashlib.sha256(
            f"{packet['mission_id']}|{inner.get('court_job_id')}|{evidence_sha256}|{observed}".encode()
        ).hexdigest()[:24]
        passed = self.verdict == "PASS"
        return {
            "kind": INDEPENDENT_COURT_KIND, "source_role": "SENTINEL", "independent": True,
            "court_id": court_id, "verdict": self.verdict,
            "receipt_ref": inner.get("receipt_ref"), "receipt_sha": inner.get("receipt_sha"),
            "evidence_refs": refs, "evidence_sha256": evidence_sha256, "observed_sha": observed,
            "candidate_sha": packet["source_head"], "canonical_sha": canonical,
            "canonical_base_sha": canonical, "base_relation": self.relation,
            "base_binding": {"relation": self.relation, "independently_observed": self.observed_remote,
                             "canonical_base_observed_on_remote": self.observed_remote,
                             "observed_base_sha": canonical if self.observed_remote else "",
                             "detail": "stub"},
            "findings": self.findings, "exceptions": self.exceptions,
            "required_repairs": [] if passed else ["do not treat this organ output as a silent PASS"],
            "sealed_at": "2026-09-05T00:00:00Z", "ref": "",
            "mission_id": packet["mission_id"], "job_id": inner.get("court_job_id"),
            "plan_id": packet["plan_id"], "task_id": packet["task_id"],
            "correlation_id": packet["correlation_id"], "causation_id": packet["event_id"],
            "source_head": packet["source_head"], "source_repo": packet["source_repo"],
            "authority_ref": packet["authority_ref"],
            "workers_sentinel_model_execution": "BLOCKED",
            "iar_provenance": {
                "authorized": passed, "authorization_ref": "iar:stub:test" if passed else "",
                "provenance_intact": passed,
                "binding": {"repo_sha": packet["source_head"], "ledger_line_count": 1,
                            "ledger_sha256_prefix": "beef"},
            } if passed else {"authorized": False, "authorization_ref": ""},
        }


class _Court(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="neuruh-court-contract-")
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))

    def world(self, **kw) -> _World:
        return _World(os.path.join(self.tmp, "world"), **kw)

    def board(self, world: _World, *, adjudicator=None, resolvers: bool = True,
              observe: bool = True, **kw) -> Switchboard:
        receipts = os.path.join(self.tmp, "receipts")
        os.makedirs(receipts, exist_ok=True)
        receipt_path = os.path.join(receipts, f"{JOB_ID}.txt")
        with open(receipt_path, "w", encoding="utf-8") as handle:
            handle.write(world.receipt_text())
        # The bridge seals dispatch receipts immutable (mode 0444); the outcome
        # verifier refuses a writable one. The fixture honours the same contract.
        os.chmod(receipt_path, 0o444)
        extra = {}
        if resolvers:
            extra["authority_resolver"] = world.authority_resolver
            extra["execution_ledger_resolver"] = world.execution_ledger_resolver
        if observe:
            # The git observer is INJECTED. It reads the worktree's own remote,
            # which in this court is an offline bare repository.
            extra["canonical_observer"] = observe_canonical_base_via_git
        if adjudicator is not None:
            extra["adjudicator"] = adjudicator
        extra.update(kw)
        return Switchboard(os.path.join(self.tmp, "state"), receipts_dir=receipts,
                           known_missions=frozenset({MISSION}), clock=fixed_clock(), **extra)

    def court(self, board: Switchboard) -> dict:
        events = board.events(EventType.COURT_REQUESTED)
        self.assertEqual(len(events), 1)
        return events[0]


# ================================================================ emitter (A04 §6)
class CourtRequestContractTests(_Court):
    def test_court_request_names_its_evidence_and_bindings(self):
        world = self.world()
        board = self.board(world)
        board.tick()
        court = self.court(board)
        payload = court["payload_ref_or_payload"]
        sealed = board.events(EventType.RECEIPT_SEALED)[0]

        for key in ("receipt_ref", "receipt_sha", "candidate_sha", "canonical_sha", "canonical_repo",
                    "canonical_base_ref", "authority_ref", "authority_record_ref", "work_spec_sha256",
                    "task_id", "source_repo_path", "evidence_refs", "evidence_sha256"):
            self.assertIn(key, payload, key)

        # Identity and authority are copied from immutable facts, never invented.
        self.assertEqual(payload["receipt_sha"], sealed["payload_ref_or_payload"]["receipt_sha"])
        self.assertEqual(payload["candidate_sha"], world.head)
        self.assertEqual(payload["task_id"], JOB_ID)
        self.assertEqual(payload["source_repo_path"], world.worktree)
        self.assertEqual(payload["work_spec_sha256"], WORK_SPEC)
        self.assertTrue(payload["authority_ref"].endswith(f":work-spec:{WORK_SPEC}"))
        self.assertEqual(payload["authority_record_ref"], f"{AUTH_ID}:{world.allow['entry_hash']}")

        # The canonical base is OBSERVED on the worktree's own remote, not asserted.
        self.assertEqual(payload["canonical_repo"], world.remote)
        self.assertEqual(payload["canonical_base_ref"], "refs/heads/main")
        self.assertEqual(payload["canonical_sha"], world.base)
        self.assertTrue(payload["canonical_base_observed"])

        # Typed, hash-bound evidence; the receipt only under its kind; never jobs.jsonl.
        refs = payload["evidence_refs"]
        self.assertTrue(refs)
        kinds = [r["kind"] for r in refs]
        self.assertEqual(set(kinds), TYPED_KINDS)
        self.assertEqual(len(kinds), len(set(kinds)))
        for ref in refs:
            self.assertTrue(os.path.isabs(ref["ref"]))
            self.assertTrue(os.path.isfile(ref["ref"]))
            self.assertNotEqual(os.path.basename(ref["ref"]), "jobs.jsonl")
            self.assertEqual(ref["sha256"], _sha_file(ref["ref"], ref.get("line_start", 0), ref.get("line_end", 0)))
        by_kind = {r["kind"]: r for r in refs}
        self.assertEqual(os.path.realpath(by_kind["prime_receipt_txt"]["ref"]),
                         os.path.realpath(payload["receipt_ref"]))
        self.assertEqual(by_kind["workers_execution_ledger_jsonl"]["ref"], world.ledger_path)
        self.assertEqual(by_kind["governance_authorize_receipts_jsonl"]["ref"], world.gov_path)
        self.assertEqual(payload["evidence_sha256"], _aggregate(r["sha256"] for r in refs))

    def test_evidence_digest_is_final_at_emission(self):
        """events.jsonl is named only up to the sealed line, so the digest it carries
        still reproduces AFTER the court.requested line itself has been appended."""
        world = self.world()
        board = self.board(world)
        board.tick()
        payload = self.court(board)["payload_ref_or_payload"]
        events_ref = next(r for r in payload["evidence_refs"] if r["kind"] == "switchboard_events_jsonl")
        self.assertEqual(events_ref["line_start"], 1)
        sealed_seq = next(r.seq for r in board.event_ledger.receipts("switchboard-events")
                          if r.payload.get("event_type") == "receipt.sealed")
        self.assertEqual(events_ref["line_end"], sealed_seq)
        self.assertEqual(events_ref["sha256"], _sha_file(events_ref["ref"], 1, sealed_seq))
        # and the file now holds more than that range
        with open(events_ref["ref"], "rb") as handle:
            self.assertGreater(len(handle.read().splitlines()), sealed_seq)

    def test_without_resolvers_nothing_is_fabricated(self):
        world = self.world()
        board = self.board(world, resolvers=False)
        board.tick()
        payload = self.court(board)["payload_ref_or_payload"]
        self.assertNotIn("authority_record_ref", payload)
        kinds = {r["kind"] for r in payload["evidence_refs"]}
        self.assertNotIn("governance_authorize_receipts_jsonl", kinds)
        self.assertNotIn("workers_execution_ledger_jsonl", kinds)
        self.assertIn("authority_record", payload["binding_gaps"])
        self.assertIn("execution_ledger", payload["binding_gaps"])

    def test_default_board_observes_nothing_and_says_so(self):
        """No observer injected: no remote is read (no network from a tick), the
        base is not asserted from the packet, and the gap is named."""
        world = self.world()
        board = self.board(world, observe=False)
        board.tick()
        payload = self.court(board)["payload_ref_or_payload"]
        self.assertFalse(payload["canonical_base_observed"])
        self.assertNotIn("canonical_repo", payload)
        self.assertIn("canonical_base", payload["binding_gaps"])
        # The gap must still be READABLE by the organ: its packet gate REQUIRES
        # canonical_sha, so an omitted key is an unparseable request, not a
        # fail-closed one -- the court would deadletter with no verdict at all.
        self.assertEqual(payload["canonical_sha"], _UNOBSERVED_BASE)

    def test_unobservable_remote_is_reported_not_asserted(self):
        world = self.world()
        shutil.rmtree(world.remote)
        board = self.board(world)
        board.tick()
        payload = self.court(board)["payload_ref_or_payload"]
        self.assertFalse(payload["canonical_base_observed"])
        self.assertIn("canonical_base", payload["binding_gaps"])
        self.assertEqual(payload["canonical_sha"], _UNOBSERVED_BASE)


# ================================================================ consumer (A04 §12)
class CourtRequestConsumerTests(_Court):
    def test_without_adjudicator_court_requested_is_not_consumed(self):
        world = self.world()
        board = self.board(world)
        board.tick()
        board.tick()
        self.assertNotIn(CONSUMER_ID, [c.consumer_id for c in board.consumers])
        self.assertEqual(board.events(EventType.COURT_PASSED), [])
        self.assertEqual(board.events(EventType.COURT_FAILED), [])

    def test_consumer_hands_the_event_itself_to_the_adjudicator_and_ingests_pass(self):
        world = self.world()
        stub = StubAdjudicator()
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        self.assertIn(CONSUMER_ID, [c.consumer_id for c in board.consumers])
        board.tick()

        self.assertEqual(len(stub.packets), 1)
        self.assertEqual(stub.packets[0], court, "the packet must be the emitted event, nothing more")

        passed = board.events(EventType.COURT_PASSED)
        self.assertEqual(len(passed), 1)
        self.assertEqual(passed[0]["causation_id"], court["event_id"])
        self.assertEqual(passed[0]["correlation_id"], court["correlation_id"])
        evidence = passed[0]["payload_ref_or_payload"]["evidence"]
        self.assertEqual(evidence["kind"], INDEPENDENT_COURT_KIND)
        self.assertTrue(os.path.isabs(evidence["ref"]) and os.path.isfile(evidence["ref"]))
        self.assertTrue(evidence["ref"].startswith(board.root))
        with open(evidence["ref"], encoding="utf-8") as handle:
            record = json.load(handle)
        self.assertEqual(record["ref"], evidence["ref"])
        self.assertEqual(record["evidence_sha256"], court["payload_ref_or_payload"]["evidence_sha256"])
        # CODE_CUSTODY_REQUIRED=NO: no honey pot, no remediation
        self.assertEqual(board.events(EventType.CANDIDATE_PUSH_REQUESTED), [])
        self.assertEqual(board.jobs_for(REMEDIATION_WORKFLOW), [])

    def test_fail_seal_ingests_court_failed_and_remediation(self):
        world = self.world()
        stub = StubAdjudicator(verdict="FAIL", findings=[
            {"code": "UNSUPPORTED_EVIDENCE_KIND", "detail": "prose", "severity": "blocking"},
            {"code": "NO_EVIDENCE", "detail": "nothing bound", "severity": "blocking"}])
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(len(board.events(EventType.COURT_FAILED)), 1)
        self.assertEqual(board.events(EventType.COURT_PASSED), [])
        # court.failed is delivered to the remediation consumer on the next tick
        # (at-least-once delivery over a per-tick snapshot), never in the same one.
        board.tick()
        self.assertEqual(len(board.jobs_for(REMEDIATION_WORKFLOW)), 1)
        self.assertEqual(board.jobs_for(HONEY_POT_WORKFLOW), [])
        self.assertEqual(len(board.events(EventType.COURT_FAILED)), 1)

    def test_model_unavailable_alone_is_not_a_terminal_verdict(self):
        world = self.world()
        stub = StubAdjudicator(verdict="FAIL", findings=[
            {"code": "MODEL_UNAVAILABLE", "detail": "IAR down", "severity": "blocking"}])
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_FAILED), [])
        self.assertEqual(board.events(EventType.COURT_PASSED), [])
        lease = board._consumer_state(CONSUMER_ID)["lease"]
        self.assertIsNotNone(lease)
        self.assertEqual(lease["state"], AckState.RETRY.value)
        for _ in range(5):
            board.tick()
        self.assertEqual(board.events(EventType.COURT_FAILED), [])
        dead = [d for d in board.deadletters()
                if d["payload_ref_or_payload"]["reason"] == DeadletterReason.MAX_RETRIES.value]
        self.assertEqual(len(dead), 1)
        self.assertTrue(dead[0]["payload_ref_or_payload"]["original_retained"])

    def test_replay_and_restart_ingest_exactly_one_verdict(self):
        world = self.world()
        stub = StubAdjudicator()
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        board.tick()
        again = board.reopen()
        again.tick()
        self.assertEqual(len(again.events(EventType.COURT_PASSED)), 1)
        self.assertEqual(len(again.events(EventType.COURT_REQUESTED)), 1)
        courts = os.listdir(os.path.join(again.root, "courts"))
        self.assertEqual(len(courts), 1)
        self.assertEqual(len({p["payload_ref_or_payload"]["court_job_id"] for p in stub.packets}), 1)


# ================================================================ outcome verifier (A04 §11)
class RecordExpectedTests(_Court):
    def test_observed_base_that_differs_from_head_is_accepted(self):
        world = self.world(ahead_of_main=True)
        self.assertNotEqual(world.head, world.base)
        stub = StubAdjudicator(relation="candidate_descends_from_canonical_base", canonical_sha=world.base)
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        passed = board.events(EventType.COURT_PASSED)
        self.assertEqual(len(passed), 1, board.deadletters())
        self.assertEqual(board.events(EventType.COURT_REQUESTED)[0]["payload_ref_or_payload"]["canonical_sha"],
                         world.base)

    def test_asserted_identity_without_observation_is_refused(self):
        """relation must be a WHITELISTED one, or the pre-existing "unverified"
        check refuses first and the guard this test is named for never runs."""
        world = self.world()
        stub = StubAdjudicator(relation="candidate_is_canonical_base", observed_remote=False)
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_PASSED), [])
        # refused at ingest, so it is a consumer failure -> retry/deadletter, never PASS
        self.assertEqual(board.events(EventType.COURT_FAILED), [])

    def test_record_evidence_sha256_must_match_the_request(self):
        world = self.world()
        stub = StubAdjudicator(evidence_sha256="0" * 64)
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_PASSED), [])

    def test_content_free_pass_is_refused(self):
        """A PASS that names no bound evidence is the 09-04 verdict. Refused."""
        world = self.world()
        stub = StubAdjudicator(evidence_refs=[], observed_sha="")
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_PASSED), [])

    def test_direct_record_court_outcome_still_refuses_asserted_identity(self):
        """The verifier itself, not just the consumer: canonical_sha == source_head
        with no observed base is no longer accepted by record_court_outcome."""
        world = self.world()
        board = self.board(world)
        board.tick()
        court = self.court(board)
        sealed = board.events(EventType.RECEIPT_SEALED)[0]
        record = StubAdjudicator(relation="candidate_is_canonical_base", observed_remote=False)(court)
        ref = os.path.join(board.root, "asserted.json")
        record["ref"] = ref
        with open(ref, "w", encoding="utf-8") as handle:
            json.dump(record, handle, sort_keys=True)
        with self.assertRaises(SwitchboardError):
            board.record_court_outcome(
                passed=True,
                court_job_id=court["payload_ref_or_payload"]["court_job_id"],
                mission_id=sealed["mission_id"], plan_id=sealed["plan_id"], task_id=sealed["task_id"],
                correlation_id=sealed["correlation_id"], causation_id=court["event_id"],
                source_head=sealed["source_head"], authority_ref=sealed["authority_ref"],
                source_repo=sealed["source_repo"],
                receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
                evidence={"kind": INDEPENDENT_COURT_KIND, "source_role": "SENTINEL",
                          "independent": True, "ref": ref},
            )
        self.assertEqual(board.events(EventType.COURT_PASSED), [])



# ============================================ independent-court repairs (round 2)
class _Reseal:
    """An organ that re-adjudicates on redelivery: same court_id, fresh sealed_at.
    That is the canonical organ's own behaviour when constructed without a ledger
    (CourtOrgan.state_dir is optional), and court_id_for deliberately excludes
    sealed_at."""

    def __init__(self, inner) -> None:
        self.inner = inner
        self.n = 0

    def __call__(self, packet: dict) -> dict:
        self.n += 1
        record = self.inner(packet)
        record["sealed_at"] = f"2026-09-05T00:00:{self.n:02d}Z"
        return record


class CourtRepairTests(_Court):
    """Each case pins one hole an independent court found in the first candidate.

    Every one was reproduced against that tree before it was closed here, and each
    test fails if its guard is removed -- the earlier suite left five such mutants
    alive because two of its tests never reached the function they were named for.
    """

    def test_a_bare_observation_claim_is_no_longer_the_whole_proof(self):
        """CLOSED HERE: a record that merely SETS canonical_base_observed_on_remote
        and names no observed sha is refused. That boolean was the entire proof of
        an observed base, on a board that had observed nothing. Since 09-06 such a
        board is refused earlier, on its own gap, so this runs on a board that DID
        observe: the claim guard must be the one that fires."""
        world = self.world()
        stub = StubAdjudicator(relation="candidate_is_canonical_base", observed_remote=True)
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        record = stub(court)
        record["base_binding"]["observed_base_sha"] = ""
        with self.assertRaises(SwitchboardError) as caught:
            board._require_observed_relation(record, court=court, source_head=world.head)
        self.assertIn("names no observed sha", str(caught.exception))

    def test_open_defect_a_board_that_observed_nothing_still_accepts_a_coherent_record(self):
        """CLOSED 2026-09-06 -- the one open H01 defect, pinned here since 09-05
        (H01_INDEPENDENT_COURT_20260905.md). The name is kept so the receipt
        trail stays traceable; the assertion is the opposite of what it pinned.

        When this board observed nothing its request carries _UNOBSERVED_BASE and
        names the canonical_base gap, so nothing corroborates the record. A
        self-consistent record used to earn a PASS on a base only the judged party
        vouched for. Now the request itself is the refusal: the PASS is refused at
        ingest, retried, deadlettered, and no court.passed ever exists -- and no
        court.failed either, because the record was never judged, only refused."""
        world = self.world()
        stub = StubAdjudicator(relation="candidate_is_canonical_base", observed_remote=True)
        board = self.board(world, adjudicator=stub, observe=False)
        for _ in range(6):
            board.tick()
        payload = self.court(board)["payload_ref_or_payload"]
        self.assertIn("canonical_base", payload["binding_gaps"])
        self.assertEqual(payload["canonical_sha"], _UNOBSERVED_BASE)
        self.assertEqual(board.events(EventType.COURT_PASSED), [])
        self.assertEqual(board.events(EventType.COURT_FAILED), [])
        details = [d["payload_ref_or_payload"].get("detail", "") for d in board.deadletters()]
        self.assertTrue(any("never observed" in d for d in details), details)

    def test_a_board_that_observed_nothing_can_never_certify_a_pass(self):
        """The guard itself, reached directly: a request naming the canonical_base
        gap refuses every verified relation, however coherent the record -- here
        one that is observed, sha-bearing and identity by claim."""
        world = self.world()
        stub = StubAdjudicator(relation="candidate_is_canonical_base", observed_remote=True)
        board = self.board(world, adjudicator=stub, observe=False)
        board.tick()
        court = self.court(board)
        record = stub(court)
        record["canonical_sha"] = record["canonical_base_sha"] = world.head
        record["base_binding"]["observed_base_sha"] = world.head
        with self.assertRaises(SwitchboardError) as caught:
            board._require_observed_relation(record, court=court, source_head=world.head)
        self.assertIn("never observed", str(caught.exception))

    def test_a_request_that_names_the_gap_refuses_a_pass_even_with_a_base_beside_it(self):
        """Each clause of the refusal stands alone. A request that names the gap
        AND carries a 40-hex base is a contradiction; the gap wins."""
        world = self.world()
        stub = StubAdjudicator()
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        record = stub(court)
        contradicted = json.loads(json.dumps(court))
        contradicted["payload_ref_or_payload"]["binding_gaps"] = ["canonical_base"]
        self.assertEqual(contradicted["payload_ref_or_payload"]["canonical_sha"], world.base)
        with self.assertRaises(SwitchboardError) as caught:
            board._require_observed_relation(record, court=contradicted, source_head=world.head)
        self.assertIn("never observed", str(caught.exception))

    def test_the_unobserved_sentinel_refuses_a_pass_even_when_no_gap_is_named(self):
        """The sentinel without a gap is still no observation."""
        world = self.world()
        stub = StubAdjudicator()
        board = self.board(world, adjudicator=stub, observe=False)
        board.tick()
        court = json.loads(json.dumps(self.court(board)))
        court["payload_ref_or_payload"]["binding_gaps"] = []
        record = stub(court)
        self.assertEqual(record["canonical_sha"], _UNOBSERVED_BASE)
        with self.assertRaises(SwitchboardError) as caught:
            board._require_observed_relation(record, court=court, source_head=world.head)
        self.assertIn("never observed", str(caught.exception))

    def test_a_pass_that_names_no_canonical_base_at_all_is_refused(self):
        """The final corroboration (record canonical == the base this board read)
        must be load-bearing on its own. _require_request_binding already refuses
        a DIFFERENT canonical for every verdict, so the one shape only this clause
        catches is a record that names NO canonical at all -- empty, unobserved,
        a whitelisted relation -- on a board that did observe. Reached directly
        and end to end."""
        world = self.world(ahead_of_main=True)
        stub = StubAdjudicator(relation="candidate_descends_from_canonical_base",
                               observed_remote=False)
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        record = stub(court)
        record["canonical_sha"] = record["canonical_base_sha"] = ""
        record["base_binding"]["observed_base_sha"] = ""
        with self.assertRaises(SwitchboardError) as caught:
            board._require_observed_relation(record, court=court, source_head=world.head)
        self.assertIn("different canonical base", str(caught.exception))
        sealed = board.events(EventType.RECEIPT_SEALED)[0]
        ref = os.path.join(board.root, "no-base.json")
        record["ref"] = ref
        with open(ref, "w", encoding="utf-8") as handle:
            json.dump(record, handle, sort_keys=True)
        with self.assertRaises(SwitchboardError) as refused:
            board.record_court_outcome(
                passed=True,
                court_job_id=court["payload_ref_or_payload"]["court_job_id"],
                mission_id=sealed["mission_id"], plan_id=sealed["plan_id"], task_id=sealed["task_id"],
                correlation_id=sealed["correlation_id"], causation_id=court["event_id"],
                source_head=sealed["source_head"], authority_ref=sealed["authority_ref"],
                source_repo=sealed["source_repo"],
                receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
                evidence={"kind": INDEPENDENT_COURT_KIND, "source_role": "SENTINEL",
                          "independent": True, "ref": ref},
            )
        self.assertIn("different canonical base", str(refused.exception))
        self.assertEqual(board.events(EventType.COURT_PASSED), [])

    def test_a_request_with_a_malformed_base_refuses_a_pass(self):
        """A base that is not 40-hex was never read off any remote."""
        world = self.world()
        stub = StubAdjudicator()
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        record = stub(court)
        malformed = json.loads(json.dumps(court))
        malformed["payload_ref_or_payload"]["canonical_sha"] = "g" * 40
        with self.assertRaises(SwitchboardError) as caught:
            board._require_observed_relation(record, court=malformed, source_head=world.head)
        self.assertIn("never observed", str(caught.exception))

    def test_a_pass_on_a_base_the_board_did_not_read_is_refused(self):
        """Observer injected, but the record names a base the board never saw."""
        world = self.world()
        stub = StubAdjudicator(relation="candidate_descends_from_canonical_base",
                               canonical_sha="c" * 40, observed_remote=True)
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_PASSED), [])

    def test_a_pass_claiming_observation_without_an_observed_sha_is_refused(self):
        world = self.world()
        stub = StubAdjudicator()
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        record = stub(court)
        record["base_binding"]["observed_base_sha"] = ""
        with self.assertRaises(SwitchboardError):
            board._require_observed_relation(record, court=court, source_head=world.head)

    def test_a_request_that_named_no_evidence_digest_refuses_every_verdict(self):
        """A legacy court.requested -- the shape already sitting on live ledgers --
        corroborates nothing, so absence must be fatal, not permissive."""
        world = self.world()
        stub = StubAdjudicator()
        board = self.board(world, adjudicator=stub)
        board._court_bindings = lambda *a, **k: {}
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_PASSED), [])
        self.assertEqual(board.events(EventType.COURT_FAILED), [])

    def test_a_fail_is_request_bound_too(self):
        """A terminal FAIL forecloses the correct verdict forever, so it may not be
        ingested against evidence the court never asked about."""
        world = self.world()
        stub = StubAdjudicator(verdict="FAIL", evidence_sha256="0" * 64,
                               findings=[{"code": "NO_EVIDENCE", "detail": "x", "severity": "blocking"}])
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_FAILED), [])

    def test_a_fail_that_names_nothing_at_all_is_refused(self):
        world = self.world()
        stub = StubAdjudicator(verdict="FAIL")
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        record = stub(court)
        record["required_repairs"] = []
        with self.assertRaises(SwitchboardError):
            board._require_request_binding(record, court=court, passed=False)

    def test_binding_a_fail_does_not_add_a_new_refusal_for_the_no_evidence_verdict(self):
        """PRE-EXISTING GAP, pinned here so it is not mistaken for this change's.

        The canonical organ emits observed_sha="" for its NO_EVIDENCE FAIL
        (sentinel_court/verify.py: `digest.hexdigest() if bound else ""`), and the
        board has required a 64-hex observed_sha on EVERY record since before this
        change (the "findings and observed digest are malformed" check). So the
        organ's honest no-evidence verdict is refused, retried and deadlettered
        rather than recorded: fail-closed, never a wrong PASS, but the seam cannot
        record that answer. Closing it means relaxing a main-line invariant and
        belongs to its own change, not to this one. What this test guarantees is
        that the FAIL-side binding added here is not the thing refusing it."""
        world = self.world()
        stub = StubAdjudicator(verdict="FAIL", evidence_refs=[], observed_sha="",
                               findings=[{"code": "NO_EVIDENCE", "detail": "nothing bound",
                                          "severity": "blocking"}])
        board = self.board(world, adjudicator=stub)
        board.tick()
        court = self.court(board)
        record = stub(court)
        # The FAIL-side binding added by this change ACCEPTS the organ's answer.
        board._require_request_binding(record, court=court, passed=False)
        # What refuses it is the pre-existing observed-digest rule, end to end.
        for _ in range(6):
            board.tick()
        self.assertEqual(board.events(EventType.COURT_FAILED), [])
        details = [d["payload_ref_or_payload"].get("detail", "") for d in board.deadletters()]
        self.assertTrue(any("malformed" in d for d in details), details)

    def test_a_pass_with_no_bound_evidence_is_refused_on_its_own(self):
        """Isolates the content-free-PASS guard. The shipped
        test_content_free_pass_is_refused also blanks observed_sha, so the
        pre-existing 64-hex digest rule refuses first and the evidence-refs guard
        is never reached -- deleting it left that test green. Here observed_sha is
        a valid digest, so only the boundness check can refuse."""
        world = self.world()
        stub = StubAdjudicator(evidence_refs=[], observed_sha="a" * 64)
        board = self.board(world, adjudicator=stub)
        board.tick()
        board.tick()
        self.assertEqual(board.events(EventType.COURT_PASSED), [])
        court = self.court(board)
        with self.assertRaises(SwitchboardError) as caught:
            board._require_request_binding(stub(court), court=court, passed=True)
        self.assertIn("no bound evidence", str(caught.exception))

    def test_the_receipt_cannot_choose_the_canonical_remote(self):
        """`REMOTE=origin <url>` would let the artefact under judgement pick both the
        canonical repository and the host this tick reaches."""
        world = self.world()
        self.assertEqual(
            observe_canonical_base_via_git(world.worktree, "origin", "refs/heads/main"),
            (world.remote, world.base))
        self.assertEqual(
            observe_canonical_base_via_git(world.worktree, f"origin {world.remote}", "refs/heads/main"),
            ("", ""))
        self.assertEqual(
            observe_canonical_base_via_git(world.worktree, "zzz file:///tmp/elsewhere.git",
                                           "refs/heads/main"),
            ("", ""))

    def test_a_transient_ingest_fault_does_not_wedge_the_court(self):
        """The sealed file is written AFTER the verdict is ingested; a re-seal that
        differs only in sealed_at must not foreclose the court."""
        world = self.world()
        stub = _Reseal(StubAdjudicator())
        board = self.board(world, adjudicator=stub)
        original = board._record_court_outcome_locked
        state = {"n": 0}

        def flaky(**kwargs):
            state["n"] += 1
            if state["n"] == 1:
                raise SwitchboardError("transient ingest fault")
            return original(**kwargs)

        board._record_court_outcome_locked = flaky
        for _ in range(4):
            board.tick()
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 1, board.deadletters())
        self.assertEqual([d["payload_ref_or_payload"]["reason"] for d in board.deadletters()], [])

    def test_the_unobserved_packet_still_satisfies_the_organ_packet_gate(self):
        """The organ requires receipt_ref, mission_id, job_id, candidate_sha and
        canonical_sha, and validates the two shas as 40-hex. A gap-naming request
        must still clear that gate so the organ can fail closed ON THE EVIDENCE
        rather than refusing the envelope and leaving the court with no verdict."""
        world = self.world()
        board = self.board(world, observe=False)
        board.tick()
        payload = self.court(board)["payload_ref_or_payload"]
        for key in ("receipt_ref", "canonical_sha"):
            self.assertIn(key, payload, key)
        for key in ("candidate_sha", "canonical_sha"):
            value = str(payload[key]).lower()
            self.assertEqual(len(value), 40, key)
            self.assertTrue(all(c in "0123456789abcdef" for c in value), key)
        self.assertFalse(payload["canonical_base_observed"])
        self.assertIn("canonical_base", payload["binding_gaps"])


# ==================================================== production wiring (2026-09-06)
class ProductionWiringTests(_Court):
    """scripts/run_switchboard.py is the composition root the 09-05 court found
    observer-less. It must inject observe_canonical_base_via_git, and the board
    it builds must be the loopback-only first seam. The dispatch bridge's wiring
    is pinned beside the bridge (tests/test_founder_runtime.py)."""

    def test_the_production_board_carries_the_git_observer(self):
        import argparse
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "run_switchboard_under_court",
            os.path.join(REPO_ROOT, "scripts", "run_switchboard.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        args = argparse.Namespace(root=os.path.join(self.tmp, "prod-state"),
                                  receipts=os.path.join(self.tmp, "prod-receipts"))
        board = module._board(args)
        self.assertIs(board.canonical_observer, observe_canonical_base_via_git)
        self.assertEqual(board.bind_host, "127.0.0.1")
        self.assertIsNone(board.adjudicator)


if __name__ == "__main__":
    unittest.main()
