import hashlib
import inspect
import json
import os
import stat
import subprocess
import tempfile
import threading
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import patch

from neuruh_workers.contracts import JobStatus, ReceiptType
from neuruh_workers.convergence import (
    THIS_PR_MAY_OWN,
    THIS_PR_MUST_NOT_OWN,
    owner_of,
    this_pr_must_not_own,
    this_pr_owns,
)
from neuruh_workers import founder_runtime as founder_runtime_mod
from neuruh_workers.founder_runtime import (
    DispatchBridge,
    DispatchTicket,
    FounderRuntime,
    IntelligenceLayer,
    MissionIntent,
    RuntimeStatus,
    WorkCandidate,
    compile_context,
    map_founder_runtime_dispatch,
    rank_candidates,
)
from neuruh_workers.events import EventType
from neuruh_workers.git_custodian.constants import WRITE_BAN_REPOS
from neuruh_workers.ledger import ExecutionLedger
from neuruh_workers.organ_consumers import (
    AXON_EXECUTE_URL,
    FR_API_VERSION,
    FR_HEALTH_URL,
    GOVERNANCE_CONSUME_URL,
    GOVERNANCE_EVALUATE_URL,
    bind_founder_runtime,
    honey_pot_bind,
    pull_dispatchable_tickets,
    switchboard_present,
    _canonical_observer_for,
    _write_immutable,
)
from neuruh_workers.prime_receipts import parse_prime_receipt
from neuruh_workers.queue import JobQueue
from neuruh_workers.runner import WorkflowRunner
from neuruh_workers.switchboard import (
    AdjudicatorUnavailable,
    INDEPENDENT_COURT_KIND, SwitchboardError, observe_canonical_base_via_git,
)
from neuruh_workers.switchboard_jobs import (
    GIT_PUSH,
    HONEY_POT_WORKFLOW,
    SENTINEL_MODEL_EXECUTION,
    SENTINEL_WORKFLOW,
)
from neuruh_workers.workflows import build_catalog, build_registry
from tests.canonical_fixture import OfflineCanonical

REPO_ROOT = Path(__file__).resolve().parents[1]

# The source-identity court builds throwaway repositories.  A developer's own global
# git config must not decide whether they pass: commit signing with no pinentry, or a
# global hooks path, made every one of these fixtures fail on a signing host while CI
# stayed green.  Same isolation the custodian fixtures already use.
_FIXTURE_GIT_ENV = {
    "GIT_AUTHOR_NAME": "Court",
    "GIT_AUTHOR_EMAIL": "court@local",
    "GIT_COMMITTER_NAME": "Court",
    "GIT_COMMITTER_EMAIL": "court@local",
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_CONFIG_NOSYSTEM": "1",
}


def _fixture_git(*args: str, capture: bool = False):
    """Run git against a fixture repository with the host's identity kept out."""
    env = os.environ.copy()
    env.update(_FIXTURE_GIT_ENV)
    argv = [
        "git",
        "-c", "commit.gpgsign=false",
        "-c", "tag.gpgsign=false",
        "-c", "core.hooksPath=",
        "-c", "init.defaultBranch=main",
        *args,
    ]
    if capture:
        return subprocess.check_output(argv, text=True, env=env)
    subprocess.run(argv, check=True, env=env)
    return None


class FounderRuntimeCourt(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.source_root = os.path.join(self.tmp.name, "workers-source")
        os.makedirs(self.source_root)
        _fixture_git("init", "-q", "-b", "main", self.source_root)
        Path(self.source_root, "README").write_text("workers\n", encoding="utf-8")
        _fixture_git("-C", self.source_root, "add", "README")
        _fixture_git("-C", self.source_root, "commit", "-qm", "source")
        _fixture_git(
            "-C", self.source_root, "remote", "add", "origin",
            "https://github.com/NeuruhAI/neuruh-workers.git",
        )
        # The identity check pins origin to GitHub, which no court tick may read.
        # The canonical base is observed on an offline mirror of that origin.
        self.canonical = OfflineCanonical.mirror(
            self.tmp.name, self.source_root, remote_name="canonical-fixture")
        self.default_work_spec = {
            "schema": "fr.work_spec.v1",
            "candidate_id": "property-research-existing-v1",
            "workflow_name": "property-research",
            "params": {
                "address": "123 Main St, Benton Harbor, MI",
                "county": "Berrien",
            },
            "intelligence_layer": "L0_DETERMINISTIC",
            "evidence_refs": ["workflow:property-research@1.0.0"],
            "production_mutation": False,
            "external_message": False,
            "money_move": False,
            "irreversible": False,
            "private_ip_risk": False,
            "code_custody_required": False,
        }
        self.default_work_spec_sha = hashlib.sha256(json.dumps(
            self.default_work_spec,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")).hexdigest()
        self.authoritative_envelopes = {}
        self.source_sha = _fixture_git(
            "-C", self.source_root, "rev-parse", "HEAD", capture=True).strip()
        _fixture_git(
            "-C", self.source_root, "update-ref",
            "refs/remotes/origin/main", self.source_sha,
        )
        remote_head_patch = patch(
            "neuruh_workers.organ_consumers._remote_main_head",
            return_value=self.source_sha,
        )
        remote_head_patch.start()
        self.addCleanup(remote_head_patch.stop)

        class _HealthResponse:
            status = 200

            def __init__(self, body):
                self.body = body

            def read(self):
                return self.body

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        def health_opener(request, timeout=None):
            if request.get_method() != "GET":
                raise OSError("non-health endpoint unavailable in court")
            if request.full_url == FR_HEALTH_URL:
                return _HealthResponse(
                    b'{"status":"ok","api_version":"fr-api-v1","auth_configured":true}')
            if "/v1/missions/" in request.full_url:
                mission_id = urllib.parse.unquote(
                    request.full_url.rsplit("/", 1)[-1])
                authoritative = self.authoritative_envelopes.get(mission_id)
                if authoritative is None:
                    authoritative = {
                        "api_version": FR_API_VERSION,
                        "mission_id": mission_id,
                        "version": 1,
                        "state": "QUEUED",
                        "objective": (
                            "finish a safe existing workflow without a clerical handoff"
                        ),
                        "work_spec": self.default_work_spec,
                        "work_spec_sha256": self.default_work_spec_sha,
                        "authority": {
                            "state": "APPROVED",
                            "execution_authorized": True,
                            "work_spec_execution_authorized": True,
                            "dispatch_approval_id": "appr-jgi-court-001",
                            "work_spec_sha256": self.default_work_spec_sha,
                        },
                    }
                return _HealthResponse(json.dumps({"envelope": {
                    **authoritative,
                }}).encode("utf-8"))
            raise OSError("unknown endpoint unavailable in court")

        self.health_opener = health_opener
        self.ledger_path = os.path.join(self.tmp.name, "ledger.jsonl")
        self.ledger = ExecutionLedger(self.ledger_path)
        self.registry = build_registry()
        self.catalog = build_catalog()
        self.queue = JobQueue(self.ledger, self.registry)
        self.runner = WorkflowRunner(self.queue, self.registry, self.catalog)
        self.receipts_dir = os.path.join(self.tmp.name, "switcharoo-receipts")
        self.switchboard_root = os.path.join(self.tmp.name, "switchboard")
        self.runtime = DispatchBridge(
            self.queue,
            self.catalog,
            self.runner,
            receipts_dir=self.receipts_dir,
            switchboard_root=self.switchboard_root,
            source_head=self.source_sha,
            source_root=self.source_root,
            http_open=self.health_opener,
            fr_token="court-token",
            canonical_observer=self.canonical.observe,
            # No organ is consulted in this court. Without an injected adjudicator
            # the bridge's Switchboard gets the production one -- a loopback HTTP
            # client to whatever organ is LIVE on this host -- and this suite's
            # verdicts then depend on that process (2026-09-14: a live organ at
            # 127.0.0.1:18731 sealed a real FAIL for the fixture and the
            # idempotency court returned BLOCKED). The court stays open, named,
            # exactly as with an unreachable organ; nothing is scored.
            adjudicator=self.no_organ,
        )

    @staticmethod
    def no_organ(envelope):
        raise AdjudicatorUnavailable(
            "no independent Sentinel organ is consulted in the founder-runtime court")

    def tearDown(self):
        self.tmp.cleanup()

    def mission(self, **kwargs):
        base = dict(
            mission_id="JGI-COURT-001",
            objective="finish a safe existing workflow without a clerical handoff",
            mission_version=1,
            dispatch_approval_id="appr-jgi-court-001",
            work_spec_sha256=self.default_work_spec_sha,
            execution_authorized=True,
            work_spec_execution_authorized=True,
            fr_state="QUEUED",
            fr_api_version=FR_API_VERSION,
            expected_value_usd=500.0,
            uncertainty=0.2,
            relevance=1.0,
            model_budget_usd=1.0,
            canonical_refs=("repo:NeuruhAI/neuruh-workers@0923725",),
            do_not_rebuild=("JobQueue", "WorkflowRunner", "ExecutionLedger"),
        )
        base.update(kwargs)
        return DispatchTicket(**base)

    def property_candidate(self, **kwargs):
        base = dict(
            candidate_id="property-research-existing-v1",
            workflow_name="property-research",
            params={"address": "123 Main St, Benton Harbor, MI", "county": "Berrien"},
            layer=IntelligenceLayer.L0,
            success_probability=1.0,
            estimated_execution_cost_usd=0.0,
            estimated_model_cost_usd=0.0,
            estimated_founder_minutes=0.0,
            estimated_latency_minutes=1.0,
            evidence_refs=("workflow:property-research@1.0.0",),
        )
        base.update(kwargs)
        return WorkCandidate(**base)

    def sentinel_evidence(self, sealed, court, *, passed=True):
        path = os.path.join(self.tmp.name, "independent-sentinel.json")
        request = court["payload_ref_or_payload"] or {}
        # The base is the one the BOARD read (or the unobserved sentinel, stated
        # as unobserved); the stub never invents an observation.
        canonical = str(request.get("canonical_sha") or sealed["source_head"]).lower()
        observed = request.get("canonical_base_observed") is True
        relation = ("candidate_is_canonical_base" if canonical == sealed["source_head"]
                    else "candidate_descends_from_canonical_base")
        record = {
            "kind": INDEPENDENT_COURT_KIND,
            "source_role": "SENTINEL",
            "independent": True,
            "verdict": "PASS" if passed else "FAIL",
            "court_id": "court-" + hashlib.sha256(
                court["payload_ref_or_payload"]["court_job_id"].encode("utf-8")
            ).hexdigest()[:24],
            "ref": path,
            "mission_id": sealed["mission_id"],
            "job_id": court["payload_ref_or_payload"]["court_job_id"],
            "receipt_ref": sealed["payload_ref_or_payload"]["receipt_ref"],
            "receipt_sha": sealed["payload_ref_or_payload"]["receipt_sha"],
            "candidate_sha": sealed["source_head"],
            "canonical_sha": canonical,
            "plan_id": sealed["plan_id"],
            "task_id": sealed["task_id"],
            "correlation_id": sealed["correlation_id"],
            "causation_id": court["event_id"],
            "source_head": sealed["source_head"],
            "source_repo": sealed["source_repo"],
            "authority_ref": sealed["authority_ref"],
            "workers_sentinel_model_execution": "BLOCKED",
            "observed_sha": hashlib.sha256(
                Path(sealed["payload_ref_or_payload"]["receipt_ref"]).read_bytes()
            ).hexdigest(),
            "base_relation": relation,
            "base_binding": {
                "relation": relation,
                "independently_observed": observed,
                "canonical_base_observed_on_remote": observed,
                "observed_base_sha": canonical if observed else "",
                "detail": "test fixture: base taken from the court request",
            },
            "evidence_refs": [sealed["payload_ref_or_payload"]["receipt_ref"]],
            "evidence_sha256": (court["payload_ref_or_payload"] or {}).get("evidence_sha256", ""),
            "findings": [] if passed else [{"code": "TEST_FAIL"}],
            "exceptions": [],
            "required_repairs": [] if passed else ["repair test failure"],
            "iar_provenance": {
                "authorized": passed,
                "authorization_ref": "iar:test" if passed else "",
                "provenance_intact": passed,
                "binding": {
                    "repo_sha": sealed["source_head"],
                    "ledger_sha256_prefix": "beef",
                    "ledger_line_count": 1,
                },
            },
        }
        Path(path).write_text(json.dumps(record, sort_keys=True), encoding="utf-8")
        return {
            "kind": INDEPENDENT_COURT_KIND,
            "source_role": "SENTINEL",
            "independent": True,
            "ref": path,
        }

    def record_outcome(self, result, *, passed=True):
        board = self.runtime.last_switchboard
        sealed = result.sealed_receipt
        court = result.court_request
        outcome = board.record_court_outcome(
            passed=passed,
            court_job_id=court["payload_ref_or_payload"]["court_job_id"],
            mission_id=sealed["mission_id"],
            plan_id=sealed["plan_id"],
            task_id=sealed["task_id"],
            correlation_id=sealed["correlation_id"],
            causation_id=court["event_id"],
            source_head=sealed["source_head"],
            authority_ref=sealed["authority_ref"],
            source_repo=sealed["source_repo"],
            receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
            evidence=self.sentinel_evidence(sealed, court, passed=passed),
        )
        board.tick()
        return outcome

    def authorized_pair(self, mission_id, candidate, *, parent_mission_id=None):
        work_spec = {
            "schema": "fr.work_spec.v1",
            "candidate_id": candidate.candidate_id,
            "workflow_name": candidate.workflow_name,
            "params": dict(candidate.params),
            "intelligence_layer": candidate.layer.value,
            "evidence_refs": list(candidate.evidence_refs),
            "production_mutation": candidate.production_mutation,
            "external_message": candidate.external_message,
            "money_move": candidate.money_move,
            "irreversible": candidate.irreversible,
            "private_ip_risk": candidate.private_ip_risk,
            "code_custody_required": False,
        }
        work_spec_sha = hashlib.sha256(json.dumps(
            work_spec,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")).hexdigest()
        envelope = {
            "api_version": FR_API_VERSION,
            "mission_id": mission_id,
            "version": 1,
            "state": "QUEUED",
            "objective": "finish a safe existing workflow without a clerical handoff",
            "parent_mission_id": parent_mission_id,
            "work_spec": work_spec,
            "work_spec_sha256": work_spec_sha,
            "authority": {
                "state": "APPROVED",
                "execution_authorized": True,
                "work_spec_execution_authorized": True,
                "dispatch_approval_id": "appr-jgi-court-001",
                "work_spec_sha256": work_spec_sha,
            },
        }
        self.authoritative_envelopes[mission_id] = envelope
        return map_founder_runtime_dispatch(envelope, catalog=self.catalog)

    def fr_envelope(self, **updates):
        work_spec = {
            "schema": "fr.work_spec.v1",
            "candidate_id": "candidate-fr-boundary",
            "workflow_name": "property-research",
            "params": {
                "address": "123 Main St, Benton Harbor, MI",
                "county": "Berrien",
            },
            "intelligence_layer": "L0_DETERMINISTIC",
            "evidence_refs": ["workflow:property-research@1.0.0"],
            "production_mutation": False,
            "external_message": False,
            "money_move": False,
            "irreversible": False,
            "private_ip_risk": False,
            "code_custody_required": False,
        }
        work_spec_sha256 = hashlib.sha256(
            json.dumps(
                work_spec,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
        ).hexdigest()
        envelope = {
            "api_version": FR_API_VERSION,
            "mission_id": "msn-fr-boundary",
            "state": "QUEUED",
            "version": 12,
            "objective": "Read-only public-record research.",
            "work_spec": work_spec,
            "work_spec_sha256": work_spec_sha256,
            "authority": {
                "state": "APPROVED",
                "work_spec_sha256": work_spec_sha256,
                "work_spec_execution_authorized": True,
                "execution_authorized": True,
                "dispatch_approval_id": "appr-fr-boundary",
            },
        }
        envelope.update(updates)
        return envelope

    def rebind_work_spec(self, envelope):
        work_spec_sha256 = hashlib.sha256(
            json.dumps(
                envelope["work_spec"],
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
        ).hexdigest()
        envelope["work_spec_sha256"] = work_spec_sha256
        envelope["authority"]["work_spec_sha256"] = work_spec_sha256
        return envelope

    def test_maps_authority_bound_fr_envelope_to_real_workers_types(self):
        ticket, candidate = map_founder_runtime_dispatch(
            self.fr_envelope(),
            catalog=self.catalog,
        )
        self.assertIsInstance(ticket, DispatchTicket)
        self.assertIsInstance(candidate, WorkCandidate)
        self.assertIs(candidate.layer, IntelligenceLayer.L0)
        self.assertEqual(ticket.dispatch_approval_id, "appr-fr-boundary")
        self.assertEqual(ticket.work_spec_sha256, self.fr_envelope()["work_spec_sha256"])
        self.assertTrue(ticket.execution_authorized)
        self.assertTrue(ticket.work_spec_execution_authorized)
        self.assertTrue(self.catalog.get(candidate.workflow_name).read_only)
        context = compile_context(ticket, candidate)
        self.assertEqual(context["candidate"]["layer"], "L0_DETERMINISTIC")
        self.assertEqual(context["fr_state"], "QUEUED")
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_multiline_founder_objective_survives_mapping_byte_for_byte(self):
        # The live T02 mission carries its "Done when:" clause on a second line.
        # Founder Runtime owns mission intent; Workers transports it and must not
        # reinterpret, join, or truncate it.
        objective = (
            "T02 close, supervised mission B: read-only property research dispatched "
            "through the Workers seam under the scoped R1 ratification, to prove Founder "
            "Runtime -> Governance -> QUEUED -> Workers exactly once -> ExecutionLedger -> "
            "typed receipt -> receipt.sealed -> SWITCHAROO -> Sentinel -> Honey Pot -> "
            "parent continuation.\n"
            "Done when: One sealed nrh.receipt.fr-dispatch.v1, one court request, no "
            "duplicate dispatch, receipt, court or custody."
        )
        ticket, candidate = map_founder_runtime_dispatch(
            self.fr_envelope(objective=objective),
            catalog=self.catalog,
        )
        self.assertIsInstance(ticket, DispatchTicket)
        self.assertIsInstance(candidate, WorkCandidate)
        self.assertIs(candidate.layer, IntelligenceLayer.L0)
        # Byte-for-byte: not joined, not first-line-only, not stripped.
        self.assertEqual(ticket.objective, objective)
        self.assertIn("\n", ticket.objective)
        self.assertEqual(ticket.objective.count("\n"), 1)
        context = compile_context(ticket, candidate)
        self.assertEqual(context["objective"], objective)
        self.assertIn("\n", context["objective"])
        # The transported objective is still the mission's, not an inferred one.
        self.assertEqual(candidate.workflow_name, "property-research")
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_identifier_fields_still_refuse_embedded_newlines(self):
        # Widening `objective` must not widen identity. These stay single-line.
        for update in (
            {"mission_id": "msn-fr-boundary\ninjected"},
            {"mission_id": "msn-fr-boundary\rinjected"},
        ):
            with self.subTest(update=update):
                with self.assertRaises(ValueError):
                    map_founder_runtime_dispatch(
                        self.fr_envelope(**update),
                        catalog=self.catalog,
                    )
        for approval_id in ("appr-fr-boundary\ninjected", "appr-fr-boundary\rinjected"):
            with self.subTest(dispatch_approval_id=approval_id):
                envelope = self.fr_envelope()
                envelope["authority"] = dict(envelope["authority"])
                envelope["authority"]["dispatch_approval_id"] = approval_id
                with self.assertRaises(ValueError):
                    map_founder_runtime_dispatch(envelope, catalog=self.catalog)
        # An empty objective is still no objective at all -- and widening the
        # newline rule must not let whitespace impersonate mission intent.
        for empty in ("", " ", "\t", "\n", "\r\n", "\n\n\n"):
            with self.subTest(objective=repr(empty)):
                with self.assertRaises(ValueError):
                    map_founder_runtime_dispatch(
                        self.fr_envelope(objective=empty),
                        catalog=self.catalog,
                    )
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_pulled_fr_envelope_reaches_production_mapping_without_loss(self):
        envelope = self.fr_envelope()

        class _Response:
            status = 200

            def __init__(self, url):
                self.url = url

            def read(self):
                if self.url.endswith("/v1/health"):
                    return b'{"status":"ok","api_version":"fr-api-v1","auth_configured":true}'
                return json.dumps({
                    "count": 1,
                    "missions": [envelope],
                }).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        pulled = pull_dispatchable_tickets(
            opener=lambda request, timeout=None: _Response(request.full_url),
            token="court",
        )
        self.assertTrue(pulled["ok"])
        self.assertEqual(pulled["tickets"], [envelope])
        ticket, candidate = map_founder_runtime_dispatch(
            pulled["tickets"][0],
            catalog=self.catalog,
        )
        self.assertIsInstance(ticket, DispatchTicket)
        self.assertIsInstance(candidate, WorkCandidate)
        self.assertIs(candidate.layer, IntelligenceLayer.L0)
        compile_context(ticket, candidate)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_fr_envelope_mapping_fails_closed_without_execution_authority(self):
        for update in (
            {"state": "APPROVED_NO_EXECUTION"},
            {"state": "PAUSED"},
            {"authority": {
                "state": "PLAN_APPROVED_NO_EXECUTION",
                "work_spec_sha256": self.fr_envelope()["work_spec_sha256"],
                "work_spec_execution_authorized": False,
                "execution_authorized": False,
                "dispatch_approval_id": None,
            }},
        ):
            with self.subTest(update=update):
                with self.assertRaises(ValueError):
                    map_founder_runtime_dispatch(
                        self.fr_envelope(**update),
                        catalog=self.catalog,
                    )
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_fr_envelope_mapping_rejects_invalid_work_spec_material(self):
        cases = []
        unknown_workflow = self.fr_envelope()
        unknown_workflow["work_spec"]["workflow_name"] = "unknown-workflow"
        cases.append(self.rebind_work_spec(unknown_workflow))
        malformed_params = self.fr_envelope()
        malformed_params["work_spec"]["params"] = ["not", "an", "object"]
        cases.append(self.rebind_work_spec(malformed_params))
        nested_float = self.fr_envelope()
        nested_float["work_spec"]["params"] = {"nested": [{"confidence": 1.5}]}
        cases.append(self.rebind_work_spec(nested_float))
        unknown_layer = self.fr_envelope()
        unknown_layer["work_spec"]["intelligence_layer"] = "L5_UNKNOWN"
        cases.append(self.rebind_work_spec(unknown_layer))
        wrong_hash = self.fr_envelope(work_spec_sha256="0" * 64)
        cases.append(wrong_hash)
        injected_receipt_field = self.fr_envelope()
        injected_receipt_field["work_spec"]["evidence_refs"] = [
            "workflow:property-research@1.0.0\nSOURCE_HEAD=" + "0" * 40,
        ]
        cases.append(self.rebind_work_spec(injected_receipt_field))
        for envelope in cases:
            with self.subTest(envelope=envelope):
                with self.assertRaises(ValueError):
                    map_founder_runtime_dispatch(
                        envelope,
                        catalog=self.catalog,
                    )
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_aliases_are_dispatch_bridge_not_a_new_fr(self):
        self.assertIs(FounderRuntime, DispatchBridge)
        self.assertIs(MissionIntent, DispatchTicket)
        source = inspect.getsource(founder_runtime_mod)
        self.assertIn("FOUNDER_RUNTIME_TO_WORKERS_DISPATCH_BRIDGE", source)
        self.assertNotIn("class FounderRuntime", source.split("FounderRuntime = DispatchBridge")[0])
        self.assertFalse(hasattr(founder_runtime_mod, "MissionState"))
        self.assertFalse((REPO_ROOT / "missions.sqlite").exists())

    def test_safe_l0_workflow_runs_to_receipted_success_without_handoff(self):
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertIsNotNone(result.job_id)
        self.assertEqual(result.job_status, "SUCCEEDED")
        self.assertIsNotNone(result.ledger_tail_sha)
        self.assertEqual(result.compiled_context["jeramie_manual_handoffs_target"], 0)
        self.assertEqual(result.compiled_context["mission_authority"], "recipe-engine.founder_runtime.fr-api-v1")
        receipts = self.ledger.receipts(result.job_id)
        self.assertTrue(any(r.receipt_type == "job.succeeded" for r in receipts))
        receipt = parse_prime_receipt(
            result.sealed_receipt["payload_ref_or_payload"]["receipt_ref"])
        self.assertEqual(
            receipt.get("RECEIPT_SCHEMA"), "nrh.receipt.fr-dispatch.v1")
        self.assertEqual(receipt.get("MISSION_VERSION"), "1")
        self.assertEqual(receipt.get("WORK_SPEC_SHA256"), self.default_work_spec_sha)
        self.assertEqual(receipt.get("DISPATCH_RESULT"), "SUCCEEDED")
        self.assertEqual(receipt.get("CODE_CUSTODY_REQUIRED"), "NO")

    def test_dispatch_rejects_caller_state_delta_and_candidate_multiplicity(self):
        result = self.runtime.dispatch(
            self.mission(),
            [self.property_candidate()],
            state_delta={"caller": "changes-dedupe-identity"},
        )
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)
        with self.assertRaises(ValueError):
            self.runtime.dispatch(
                self.mission(),
                [self.property_candidate(), self.property_candidate()],
            )

    def test_repeated_dispatch_is_idempotent_and_does_not_reexecute_success(self):
        first = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        receipt_path = first.sealed_receipt["payload_ref_or_payload"]["receipt_ref"]
        first_receipt = Path(receipt_path).read_bytes()
        first_count = len(self.ledger.receipts(first.job_id))
        second = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(second.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertTrue(second.switchboard_emit["consumed"])
        self.assertEqual(second.switchboard_emit["deadlettered"], [])
        self.assertEqual(Path(receipt_path).read_bytes(), first_receipt)
        board = self.runtime.last_switchboard
        self.assertEqual(board.deadletters(), [])
        self.assertEqual(len(board.events(EventType.RECEIPT_SEALED)), 1)
        self.assertEqual(len(board.events(EventType.COURT_REQUESTED)), 1)
        reopened_ledger = ExecutionLedger(self.ledger_path)
        reopened_queue = JobQueue(reopened_ledger, self.registry)
        reopened_runtime = DispatchBridge(
            reopened_queue,
            self.catalog,
            WorkflowRunner(reopened_queue, self.registry, self.catalog),
            receipts_dir=self.receipts_dir,
            switchboard_root=self.switchboard_root,
            source_head=self.source_sha,
            source_root=self.source_root,
            http_open=self.health_opener,
            fr_token="court-token",
            canonical_observer=self.canonical.observe,
            adjudicator=self.no_organ,
        )
        third = reopened_runtime.dispatch(
            self.mission(), [self.property_candidate()])
        self.assertEqual(third.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertTrue(third.switchboard_emit["consumed"])
        self.assertEqual(third.switchboard_emit["deadlettered"], [])
        self.assertEqual(
            len(reopened_runtime.last_switchboard.events(EventType.RECEIPT_SEALED)), 1)
        self.assertEqual(
            len(reopened_runtime.last_switchboard.events(EventType.COURT_REQUESTED)), 1)
        receipts = self.ledger.receipts(first.job_id)
        self.assertGreater(len(receipts), first_count)
        self.assertEqual(sum(r.receipt_type == "job.succeeded" for r in receipts), 1)

    def test_replay_after_independent_sentinel_pass_is_idempotent_success(self):
        first = self.runtime.dispatch(
            self.mission(), [self.property_candidate()])
        board = self.runtime.last_switchboard
        evidence = self.sentinel_evidence(
            first.sealed_receipt, first.court_request)
        board.record_court_outcome(
            passed=True,
            court_job_id=first.court_request[
                "payload_ref_or_payload"]["court_job_id"],
            mission_id=first.sealed_receipt["mission_id"],
            plan_id=first.sealed_receipt["plan_id"],
            task_id=first.sealed_receipt["task_id"],
            correlation_id=first.sealed_receipt["correlation_id"],
            causation_id=first.court_request["event_id"],
            source_head=first.sealed_receipt["source_head"],
            authority_ref=first.sealed_receipt["authority_ref"],
            source_repo=first.sealed_receipt["source_repo"],
            receipt_ref=first.sealed_receipt[
                "payload_ref_or_payload"]["receipt_ref"],
            evidence=evidence,
        )
        succeeded_before = sum(
            receipt.receipt_type == "job.succeeded"
            for receipt in self.ledger.receipts(first.job_id)
        )
        replay = self.runtime.dispatch(
            self.mission(), [self.property_candidate()])
        self.assertEqual(replay.status, RuntimeStatus.ALREADY_SUCCEEDED)
        succeeded_after = sum(
            receipt.receipt_type == "job.succeeded"
            for receipt in self.ledger.receipts(first.job_id)
        )
        self.assertEqual(succeeded_after, succeeded_before)
        self.assertTrue(replay.sentinel["judged"])
        self.assertEqual(replay.sentinel["verdict"], "PASS")
        self.assertEqual(
            replay.sentinel["bind_status"], "SENTINEL_ALREADY_COURTED_PASS")
        self.assertEqual(
            len(self.runtime.last_switchboard.events(EventType.COURT_REQUESTED)), 1)
        self.assertEqual(
            len(self.runtime.last_switchboard.events(EventType.COURT_PASSED)), 1)

    def test_sentinel_fail_blocks_parent_and_fr_update_on_replay(self):
        parent_def = self.catalog.get("property-research")
        parent_job = self.queue.enqueue(
            parent_def,
            {"address": "1 Blocked Parent St", "county": "Berrien"},
            requested_by="jgi:JGI-PARENT-FAIL",
        )
        self.queue.pause(parent_job)
        ticket, candidate = self.authorized_pair(
            "JGI-CHILD-FAIL",
            self.property_candidate(candidate_id="child-fail"),
            parent_mission_id="JGI-PARENT-FAIL",
        )
        first = self.runtime.dispatch(ticket, [candidate])
        self.assertIsNone(first.parent_resume)
        self.assertIsNone(first.fr_evidence_update)
        self.record_outcome(first, passed=False)
        replay = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(replay.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(replay.parent_resume)
        self.assertIsNone(replay.fr_evidence_update)
        self.assertEqual(self.queue.load(parent_job).status, JobStatus.PAUSED)

    def test_sentinel_pass_without_custody_intent_emits_no_honey_pot(self):
        first = self.runtime.dispatch(
            self.mission(), [self.property_candidate()])
        self.record_outcome(first)
        board = self.runtime.last_switchboard
        self.assertEqual(board.jobs_for(HONEY_POT_WORKFLOW), [])
        self.assertEqual(
            board.events(EventType.CANDIDATE_PUSH_REQUESTED), [])

    def test_rejects_ticket_that_is_not_fr_approved_or_queued(self):
        result = self.runtime.dispatch(
            self.mission(fr_state="AWAITING_APPROVAL"),
            [self.property_candidate()],
        )
        self.assertEqual(result.status, RuntimeStatus.FR_STATE_NOT_DISPATCHABLE)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)
        self.assertFalse(result.fr_bind["this_pr_duplicates_founder_runtime"])

    def test_safe_execution_fails_closed_if_fr_becomes_unavailable(self):
        runtime = DispatchBridge(
            self.queue,
            self.catalog,
            self.runner,
            receipts_dir=self.receipts_dir,
            switchboard_root=self.switchboard_root,
            source_head=self.source_sha,
            source_root=self.source_root,
            http_open=lambda _request, timeout=None: (_ for _ in ()).throw(
                OSError("Founder Runtime unavailable")),
            fr_token="court-token",
            canonical_observer=self.canonical.observe,
            adjudicator=self.no_organ,
        )
        result = runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_safe_execution_rechecks_exact_authenticated_fr_authority(self):
        for forged in (
            self.mission(dispatch_approval_id="forged-approval"),
            self.mission(work_spec_sha256="b" * 64),
            self.mission(mission_version=2),
        ):
            with self.subTest(forged=forged):
                result = self.runtime.dispatch(
                    forged, [self.property_candidate()])
                self.assertEqual(result.status, RuntimeStatus.BLOCKED)
                self.assertIsNone(result.job_id)
        result = self.runtime.dispatch(
            self.mission(),
            [self.property_candidate(params={
                "address": "attacker-selected",
                "county": "Elsewhere",
            })],
        )
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_execution_success_is_not_dispatch_success_when_sealing_refuses(self):
        refused = {
            "sealed": {"bind_status": "SWITCHBOARD_EMIT_REFUSED"},
            "court_request": None,
            "emit": {
                "consumed": False,
                "bind_status": "SWITCHBOARD_EMIT_REFUSED",
            },
            "sentinel": {"bind_status": "SENTINEL_NOT_JOBBED"},
            "honey_pot": {"activated": False},
            "board": None,
        }
        with patch.object(
            founder_runtime_mod,
            "emit_receipt_sealed_and_switcharoo",
            return_value=refused,
        ):
            result = self.runtime.dispatch(
                self.mission(),
                [self.property_candidate()],
            )
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertEqual(result.job_status, JobStatus.SUCCEEDED.value)
        self.assertIsNone(result.parent_resume)
        self.assertIsNone(result.fr_evidence_update)

    def test_safe_execution_fails_closed_on_source_provenance_drift(self):
        source_file = Path(self.source_root, "README")
        source_file.write_text("dirty\n", encoding="utf-8")
        try:
            result = self.runtime.dispatch(
                self.mission(), [self.property_candidate()])
        finally:
            source_file.write_text("workers\n", encoding="utf-8")
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

        self.runtime.source_head = "0" * 40
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

        with patch(
            "neuruh_workers.organ_consumers._remote_main_head",
            return_value="f" * 40,
        ):
            result = self.runtime.dispatch(
                self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

        _fixture_git(
            "-C", self.source_root, "remote", "set-url", "origin",
            "https://github.com/attacker/neuruh-workers.git",
        )
        try:
            result = self.runtime.dispatch(
                self.mission(), [self.property_candidate()])
        finally:
            _fixture_git(
                "-C", self.source_root, "remote", "set-url", "origin",
                "https://github.com/NeuruhAI/neuruh-workers.git",
            )
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_source_drift_during_execution_refuses_receipt_and_dispatch_success(self):
        source_file = Path(self.source_root, "README")
        original_execute = self.runner.execute

        def execute_then_drift(job_id):
            state = original_execute(job_id)
            source_file.write_text("dirty during execution\n", encoding="utf-8")
            return state

        try:
            with patch.object(self.runner, "execute", side_effect=execute_then_drift):
                result = self.runtime.dispatch(
                    self.mission(), [self.property_candidate()])
        finally:
            source_file.write_text("workers\n", encoding="utf-8")
        self.assertEqual(result.status, RuntimeStatus.BLOCKED)
        self.assertEqual(result.job_status, JobStatus.SUCCEEDED.value)
        self.assertFalse(result.switchboard_emit["consumed"])
        self.assertIsNone(result.fr_evidence_update)

    def test_immutable_receipt_write_is_race_safe_and_read_only(self):
        path = os.path.join(self.tmp.name, "immutable", "receipt.txt")
        barrier = threading.Barrier(4)
        errors = []

        def write():
            try:
                barrier.wait()
                _write_immutable(path, "sealed\n")
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(Path(path).read_text(encoding="utf-8"), "sealed\n")
        self.assertEqual(stat.S_IMODE(os.lstat(path).st_mode), 0o444)

    def test_material_authority_gate_never_enqueues(self):
        candidate = self.property_candidate(production_mutation=True)
        ticket, candidate = self.authorized_pair("JGI-MATERIAL", candidate)
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED)
        self.assertNotEqual(result.status, RuntimeStatus.FOUNDER_GATE_REQUIRED)
        self.assertIn("PRODUCTION_MUTATION", result.founder_gate_reasons)
        self.assertEqual(result.authority_path, "governance->sentinel->axon")
        self.assertEqual(result.broker_bind["authorized"], False)
        self.assertEqual(result.broker_bind["executed"], False)
        self.assertEqual(result.broker_bind["enqueued"], False)
        self.assertEqual(result.broker_bind["caller_approved"], False)
        self.assertTrue(result.broker_bind["attempted"])
        self.assertFalse(result.broker_bind["may_execute"])
        methods = {(c["organ"], c["method"]) for c in result.broker_bind["calls"]}
        self.assertIn(("governance", "evaluate"), methods)
        self.assertIn(("sentinel", "switchboard_court_job"), methods)
        self.assertEqual(result.broker_bind["sentinel"]["bind_status"], "SWITCHBOARD_PRESENT_NO_SEALED_RECEIPT")
        axon_call = next(c for c in result.broker_bind["calls"] if c["organ"] == "axon")
        self.assertFalse(axon_call["attempted"])
        self.assertEqual(axon_call["error"], "AXON_WRITE_BANNED")
        self.assertTrue(result.broker_bind["axon"]["write_banned"])
        self.assertIn("8848", result.broker_bind["calls"][0]["url"])
        self.assertIn("/v1/evaluate", result.broker_bind["calls"][0]["url"])
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_consequential_flags_all_fail_closed_to_gsa(self):
        for flag in (
            "money_move",
            "external_message",
            "production_mutation",
            "irreversible",
            "private_ip_risk",
        ):
            with self.subTest(flag=flag):
                ticket, candidate = self.authorized_pair(
                    f"JGI-{flag}",
                    self.property_candidate(**{flag: True}),
                )
                result = self.runtime.dispatch(ticket, [candidate])
                self.assertEqual(result.status, RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED)
                self.assertIsNone(result.job_id)
                self.assertFalse(result.broker_bind["caller_approved"])
                self.assertTrue(result.broker_bind["attempted"])
                self.assertEqual(result.broker_bind["calls"][0]["url"], GOVERNANCE_EVALUATE_URL)

    def test_landos_hunt_is_never_auto_executed(self):
        # POST /hunt mutates LandOS. An APPROVED + execution_authorized ticket
        # with every consequential flag false must still stop at GSA: the
        # workflow's own read_only=False is the gate, not the caller's flags.
        candidate = self.property_candidate(
            candidate_id="landos-hunt-court-v1",
            workflow_name="landos-hunt",
            params={"territory": [{"fips": "26021"}]},
            evidence_refs=("workflow:landos-hunt@1.0.0",),
        )
        ticket, candidate = self.authorized_pair("JGI-LANDOS-HUNT", candidate)
        with patch(
            "neuruh_workers.landos_hunt.http_post_json",
            side_effect=AssertionError("POST /hunt must not run without approval"),
        ):
            result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED)
        self.assertEqual(result.founder_gate_reasons, ("NON_READ_ONLY_WORKFLOW",))
        self.assertIsNone(result.job_id)
        self.assertFalse(result.broker_bind["caller_approved"])
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_model_layer_routes_to_broker_instead_of_self_authorizing(self):
        candidate = self.property_candidate(
            layer=IntelligenceLayer.L4,
            estimated_model_cost_usd=0.5,
            ambiguity=0.95,
        )
        ticket, candidate = self.authorized_pair("JGI-L4", candidate)
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.REASONING_BROKER_REQUIRED)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)
        self.assertIn("authorization", result.broker_bind["module"])
        self.assertEqual(result.broker_bind["symbol"], "InferenceAuthorization")
        self.assertEqual(result.broker_bind["authorized"], False)
        self.assertEqual(result.broker_bind["model_call"], False)
        self.assertTrue(result.broker_bind["organ_absent"])
        self.assertIsNone(result.broker_bind["required_authorization_id"])
        self.assertEqual(result.broker_bind["iar_path"], "iar_v2")
        self.assertIn("127.0.0.1:8090", result.broker_bind["iar_contract"])

    def test_l2_l3_l4_fail_closed_without_iar_rf(self):
        for layer in (IntelligenceLayer.L2, IntelligenceLayer.L3, IntelligenceLayer.L4):
            with self.subTest(layer=layer.value):
                ticket, candidate = self.authorized_pair(
                    f"JGI-{layer.value}",
                    self.property_candidate(
                        layer=layer, estimated_model_cost_usd=0.1),
                )
                result = self.runtime.dispatch(ticket, [candidate])
                self.assertEqual(result.status, RuntimeStatus.REASONING_BROKER_REQUIRED)
                self.assertTrue(result.broker_bind["organ_absent"])
                self.assertFalse(result.broker_bind["authorized"])
                self.assertFalse(result.broker_bind["model_call"])
                self.assertIsNone(result.job_id)

    def test_l2_still_fail_closed_when_iar_health_is_up_but_rf_absent(self):
        runtime = DispatchBridge(
            self.queue,
            self.catalog,
            self.runner,
            iar_probe=lambda: {
                "present": True,
                "url": "http://127.0.0.1:8090/health",
                "health": {"status": "ok"},
                "authorized": False,
            },
            receipts_dir=self.receipts_dir,
            switchboard_root=self.switchboard_root,
            source_head=self.source_sha,
            source_root=self.source_root,
            http_open=self.health_opener,
            fr_token="court-token",
            canonical_observer=self.canonical.observe,
            adjudicator=self.no_organ,
        )
        ticket, candidate = self.authorized_pair(
            "JGI-L3-IAR-UP",
            self.property_candidate(
                layer=IntelligenceLayer.L3, estimated_model_cost_usd=0.1),
        )
        result = runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.REASONING_BROKER_REQUIRED)
        self.assertTrue(result.broker_bind["iar_health"]["present"])
        self.assertFalse(result.broker_bind["rf_present"])
        self.assertTrue(result.broker_bind["organ_absent"])
        self.assertFalse(result.broker_bind["authorized"])
        self.assertIsNone(result.job_id)

    def test_usd_score_candidate_is_deleted(self):
        source = inspect.getsource(founder_runtime_mod)
        self.assertFalse(hasattr(founder_runtime_mod, "score_candidate"))
        self.assertFalse(hasattr(founder_runtime_mod, "CandidateScore"))
        self.assertNotIn("founder_minute_value_usd", source)
        self.assertNotIn("latency_minute_value_usd", source)
        self.assertNotIn("2.0", source)
        self.assertNotIn("0.05", source)
        cheap = self.property_candidate(candidate_id="cheap")
        expensive = self.property_candidate(
            candidate_id="frontier",
            layer=IntelligenceLayer.L4,
            estimated_model_cost_usd=0.75,
        )
        ranked = rank_candidates(self.mission(), [expensive, cheap])
        self.assertEqual(ranked[0].candidate_id, "cheap")
        self.assertEqual(owner_of("CHEAPEST_CAPABLE_ROUTER_L0_L1")["role"], "DELETE")
        self.assertNotIn("CHEAPEST_CAPABLE_ROUTER_L0_L1", THIS_PR_MAY_OWN)

    def test_context_is_pointer_heavy_bounded_and_has_no_chat_field(self):
        mission = self.mission(
            canonical_refs=tuple(f"repo:thing@{i:04d}" for i in range(40)),
            known_failures=tuple(f"failure-{i}" for i in range(20)),
            blockers=tuple(f"blocker-{i}" for i in range(20)),
        )
        packet = compile_context(mission, self.property_candidate(), max_bytes=1800)
        raw = json.dumps(packet, sort_keys=True).encode("utf-8")
        self.assertLessEqual(len(raw), 1800)
        self.assertNotIn("chat_history", packet)
        self.assertNotIn("transcript", packet)
        self.assertEqual(packet["jeramie_manual_handoffs_target"], 0)
        self.assertEqual(packet["authority_path"], "governance->sentinel->axon")
        self.assertEqual(packet["reasoning_broker"], "revenue_factory.authorization+aegis.iar_v2")
        self.assertEqual(packet["spend_authority"], "iar_rf")
        self.assertEqual(packet["fr_api_version"], FR_API_VERSION)

    def test_receipt_sealed_consumed_by_live_switchboard_jobs_independent_sentinel(self):
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertTrue(switchboard_present())
        self.assertEqual(result.switchboard_emit["bind_status"], "SWITCHBOARD_CONSUMED")
        self.assertTrue(result.switchboard_emit["consumed"])
        self.assertEqual(result.switchboard_emit["deadlettered"], [])
        self.assertEqual(result.switchboard_emit["matching_sealed_count"], 1)
        self.assertEqual(result.switchboard_emit["matching_court_count"], 1)
        self.assertEqual(result.sealed_receipt["schema_version"], "nrh.event.v1")
        self.assertEqual(result.sealed_receipt["source_role"], "DISPATCH_BRIDGE")
        self.assertNotEqual(result.sealed_receipt.get("source_role"), "PRIME")
        self.assertIsNotNone(result.court_request)
        self.assertEqual(result.court_request["event_type"], EventType.COURT_REQUESTED.value)
        request = result.court_request["payload_ref_or_payload"]
        # The bridge's board carried the injected offline observer and read the
        # canonical base off it: no gap, and the base is the mirror's main.
        self.assertEqual(self.runtime.last_switchboard.canonical_observer, self.canonical.observe)
        self.assertEqual(request["canonical_sha"], self.canonical.base)
        self.assertEqual(request["canonical_sha"], self.source_sha)
        self.assertEqual(request["canonical_repo"], self.canonical.bare)
        self.assertTrue(request["canonical_base_observed"])
        self.assertNotIn("canonical_base", request["binding_gaps"])
        self.assertEqual(result.court_request["source_role"], "SWITCHAROO")
        self.assertEqual(result.sentinel["model_execution"], SENTINEL_MODEL_EXECUTION)
        self.assertEqual(result.sentinel["bind_status"], "SENTINEL_COURT_JOBBED_INDEPENDENT")
        self.assertTrue(result.sentinel["independent"])
        self.assertFalse(result.sentinel["judged"])
        self.assertIsNone(result.sentinel["verdict"])
        self.assertTrue(result.sentinel["ci_job_named_court_is_not_sentinel"])
        board = self.runtime.last_switchboard
        self.assertEqual(board.deadletters(), [])
        self.assertEqual(len(board.events(EventType.RECEIPT_SEALED)), 1)
        self.assertEqual(len(board.events(EventType.COURT_REQUESTED)), 1)
        receipt_text = Path(
            result.sealed_receipt["payload_ref_or_payload"]["receipt_ref"]
        ).read_text(encoding="utf-8")
        self.assertIn("AUTHORITY_REFERENCE=fr-api-v1:approval:appr-jgi-court-001", receipt_text)
        self.assertIn("work-spec:" + self.default_work_spec_sha, receipt_text)
        board = self.runtime.last_switchboard
        self.assertIsNotNone(board)
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 0)
        jobs = board.jobs_for(SENTINEL_WORKFLOW)
        self.assertEqual(len(jobs), 1)
        artifact = board.queue.load(jobs[0]).step_results["materialize"]
        self.assertEqual(artifact["sentinel_model_execution"], "BLOCKED")

    def test_bridge_board_carries_the_production_observer_unless_a_court_injects_one(self):
        """The dispatch bridge is a production composition root. With nothing
        injected its board observes the canonical base through read-only git
        against the worktree's own remote; a court injects an offline observer so
        no tick here reaches GitHub. Pinned at the resolver -- never by ticking a
        board that carries the production observer from inside a test."""
        import neuruh_workers.switchboard as switchboard_mod
        self.assertIs(_canonical_observer_for(switchboard_mod, None),
                      observe_canonical_base_via_git)
        self.assertEqual(_canonical_observer_for(switchboard_mod, self.canonical.observe),
                         self.canonical.observe)
        self.assertIsNone(_canonical_observer_for(None, None))
        self.assertIsNone(DispatchBridge(self.queue, self.catalog, self.runner).canonical_observer)
        # Same contract for the Sentinel: None means the production HTTP adjudicator.
        self.assertIsNone(DispatchBridge(self.queue, self.catalog, self.runner).adjudicator)
        self.assertEqual(self.runtime.canonical_observer, self.canonical.observe)

    def test_a_bridge_board_that_observed_nothing_cannot_certify_a_pass(self):
        """An observer that reads nothing leaves the request naming the gap, and a
        coherent PASS against it is refused at ingest -- the H01 defect, closed
        on the bridge path as well as on scripts/run_switchboard.py."""
        runtime = DispatchBridge(
            self.queue,
            self.catalog,
            self.runner,
            receipts_dir=self.receipts_dir,
            switchboard_root=self.switchboard_root,
            source_head=self.source_sha,
            source_root=self.source_root,
            http_open=self.health_opener,
            fr_token="court-token",
            canonical_observer=lambda worktree, remote, base_ref: ("", ""),
        )
        result = runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        request = result.court_request["payload_ref_or_payload"]
        self.assertIn("canonical_base", request["binding_gaps"])
        self.assertFalse(request["canonical_base_observed"])
        board = runtime.last_switchboard
        sealed, court = result.sealed_receipt, result.court_request
        with self.assertRaises(SwitchboardError) as caught:
            board.record_court_outcome(
                passed=True,
                court_job_id=court["payload_ref_or_payload"]["court_job_id"],
                mission_id=sealed["mission_id"],
                plan_id=sealed["plan_id"],
                task_id=sealed["task_id"],
                correlation_id=sealed["correlation_id"],
                causation_id=court["event_id"],
                source_head=sealed["source_head"],
                authority_ref=sealed["authority_ref"],
                source_repo=sealed["source_repo"],
                receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
                evidence=self.sentinel_evidence(sealed, court),
            )
        self.assertIn("never observed", str(caught.exception))
        self.assertEqual(board.events(EventType.COURT_PASSED), [])

    def test_fake_sentinel_pass_from_receipt_or_reconstruction_is_refused(self):
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        board = self.runtime.last_switchboard
        self.assertIsNotNone(board)
        sealed = result.sealed_receipt
        court = result.court_request
        with self.assertRaises(SwitchboardError):
            board.record_court_outcome(
                passed=True,
                court_job_id=court["payload_ref_or_payload"]["court_job_id"],
                mission_id=sealed["mission_id"],
                plan_id=sealed["plan_id"],
                task_id=sealed["task_id"],
                correlation_id=sealed["correlation_id"],
                causation_id=court["event_id"],
                source_head=sealed["source_head"],
                authority_ref=sealed["authority_ref"],
                source_repo=sealed["source_repo"],
                receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
                evidence={
                    "kind": "reconstruction",
                    "source_role": "SENTINEL",
                    "independent": False,
                    "ref": "fake://ci-court-pass",
                },
            )
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 0)
        with self.assertRaises(SwitchboardError):
            board.record_court_outcome(
                passed=True,
                court_job_id=court["payload_ref_or_payload"]["court_job_id"],
                mission_id=sealed["mission_id"],
                plan_id=sealed["plan_id"],
                task_id=sealed["task_id"],
                correlation_id=sealed["correlation_id"],
                causation_id=court["event_id"],
                source_head=sealed["source_head"],
                authority_ref=sealed["authority_ref"],
                source_repo=sealed["source_repo"],
                receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
                evidence={
                    "kind": "prime_verdict",
                    "source_role": "PRIME",
                    "independent": True,
                    "ref": "fake://prime-verdict",
                },
            )
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 0)

    def test_honey_pot_consumed_after_independent_sentinel_when_code_custody_required(self):
        envelope = self.fr_envelope(mission_id="msn-code-custody")
        envelope["work_spec"]["code_custody_required"] = True
        self.rebind_work_spec(envelope)
        self.authoritative_envelopes[envelope["mission_id"]] = envelope
        ticket, candidate = map_founder_runtime_dispatch(
            envelope, catalog=self.catalog)
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertEqual(result.honey_pot["bind_status"], "GIT_CUSTODIAN_CONSUMED")
        self.assertTrue(result.honey_pot["present"])
        self.assertFalse(result.honey_pot["this_pr_copies_custodian"])
        self.assertEqual(result.honey_pot["custody"], "HONEY_POT")
        self.assertTrue(result.honey_pot["required"])
        self.assertFalse(result.honey_pot["activated"])
        self.assertEqual(result.honey_pot["jobs"], [])
        board = self.runtime.last_switchboard
        sealed = result.sealed_receipt
        court = result.court_request
        board.record_court_outcome(
            passed=True,
            court_job_id=court["payload_ref_or_payload"]["court_job_id"],
            mission_id=sealed["mission_id"],
            plan_id=sealed["plan_id"],
            task_id=sealed["task_id"],
            correlation_id=sealed["correlation_id"],
            causation_id=court["event_id"],
            source_head=sealed["source_head"],
            authority_ref=sealed["authority_ref"],
            source_repo=sealed["source_repo"],
            receipt_ref=sealed["payload_ref_or_payload"]["receipt_ref"],
            evidence=self.sentinel_evidence(sealed, court),
        )
        board.tick()
        passed = board.events(EventType.COURT_PASSED)
        self.assertEqual(len(passed), 1)
        self.assertEqual(passed[0]["source_role"], "SENTINEL")
        self.assertTrue(passed[0]["payload_ref_or_payload"]["evidence"]["independent"])
        self.assertEqual(passed[0]["payload_ref_or_payload"]["sentinel_model_execution"], "BLOCKED")
        honey_jobs = board.jobs_for(HONEY_POT_WORKFLOW)
        self.assertEqual(len(honey_jobs), 1)
        artifact = board.queue.load(honey_jobs[0]).step_results["materialize"]
        self.assertEqual(artifact["git_push"], GIT_PUSH)
        self.assertEqual(artifact["repo"], "NeuruhAI/neuruh-workers")
        self.assertEqual(artifact["branch"], "main")
        self.assertEqual(artifact["head"], self.source_sha)
        self.assertEqual(artifact["worktree"], os.path.realpath(self.source_root))
        self.assertNotEqual(artifact["repo"], "NeuruhAI/neuruh-axon")

    def test_parent_resume_from_paused_parent_job(self):
        parent_def = self.catalog.get("property-research")
        parent_job = self.queue.enqueue(
            parent_def,
            {"address": "1 Parent St, Benton Harbor, MI", "county": "Berrien"},
            requested_by="jgi:JGI-PARENT-001",
        )
        self.queue.pause(parent_job)
        self.assertEqual(self.queue.load(parent_job).status, JobStatus.PAUSED)

        ticket, candidate = self.authorized_pair(
            "JGI-CHILD-001",
            self.property_candidate(
                candidate_id="child",
                params={"address": "2 Child St, Benton Harbor, MI", "county": "Berrien"},
            ),
            parent_mission_id="JGI-PARENT-001",
        )
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertIsNone(result.parent_resume)
        self.assertEqual(self.queue.load(parent_job).status, JobStatus.PAUSED)
        self.record_outcome(result)
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertTrue(result.parent_resume["automatic"])
        self.assertTrue(result.parent_resume["proven"])
        self.assertEqual(result.parent_resume["status"], "PARENT_RESUME=PROVEN")
        self.assertEqual(result.parent_resume["parent_mission_id"], "JGI-PARENT-001")
        self.assertEqual(self.queue.load(parent_job).status, JobStatus.SUCCEEDED)
        types = [r.receipt_type for r in self.ledger.receipts(parent_job)]
        self.assertIn(ReceiptType.JOB_RESUMED, types)

    def test_parent_resume_uses_job_recovered_for_open_steps(self):
        parent_def = self.catalog.get("property-research")
        parent_job = self.queue.enqueue(
            parent_def,
            {"address": "9 Recover St, Benton Harbor, MI", "county": "Berrien"},
            requested_by="jgi:JGI-PARENT-RECOVER",
        )
        self.ledger.append(ReceiptType.JOB_STARTED, parent_job, {})
        self.ledger.append(ReceiptType.STEP_STARTED, parent_job, {
            "step_id": "normalize_subject",
            "attempt": 1,
            "capability": "research.normalize_subject",
            "dry_run": False,
        })
        self.assertEqual(self.queue.load(parent_job).open_steps, {"normalize_subject": 1})

        ticket, candidate = self.authorized_pair(
            "JGI-CHILD-RECOVER",
            self.property_candidate(
                candidate_id="child-recover",
                params={"address": "8 Child Recover St", "county": "Berrien"},
            ),
            parent_mission_id="JGI-PARENT-RECOVER",
        )
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertIsNone(result.parent_resume)
        self.record_outcome(result)
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertTrue(result.parent_resume["automatic"])
        self.assertTrue(result.parent_resume["proven"])
        self.assertEqual(result.parent_resume["status"], "PARENT_RESUME=PROVEN")
        self.assertEqual(self.queue.load(parent_job).status, JobStatus.SUCCEEDED)
        types = [r.receipt_type for r in self.ledger.receipts(parent_job)]
        self.assertIn(ReceiptType.JOB_RECOVERED, types)

    def test_parent_resume_absent_parent_is_precise_gap(self):
        ticket, candidate = self.authorized_pair(
            "JGI-COURT-001",
            self.property_candidate(),
            parent_mission_id="JGI-PARENT-MISSING",
        )
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertIsNone(result.parent_resume)
        self.record_outcome(result)
        result = self.runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertFalse(result.parent_resume["automatic"])
        self.assertFalse(result.parent_resume["proven"])
        self.assertEqual(result.parent_resume["status"], "PARENT_JOB_ABSENT_IN_LEDGER")
        self.assertEqual(result.parent_resume["parent_mission_id"], "JGI-PARENT-MISSING")

    def test_fr_execution_outcome_writeback_is_attempted_and_fails_closed_here(self):
        # R02D: the bridge reports DISPATCHED then SUCCEEDED (from the sealed
        # receipt, not gated on a Sentinel verdict). This court's opener refuses
        # every POST, so both write-backs are attempted and recorded fail-closed;
        # the dispatch itself is untouched. The legacy /evidence route is gone.
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertIsNone(result.fr_evidence_update)
        writeback = result.fr_outcome_writeback
        self.assertEqual(sorted(writeback), ["DISPATCHED", "SUCCEEDED"])
        for outcome, report in writeback.items():
            self.assertTrue(report["attempted"])
            self.assertFalse(report["ok"])
            self.assertEqual(report["bind_status"], "FR_OUTCOME_WRITEBACK_FAIL_CLOSED")
            self.assertEqual(report["outcome"], outcome)
            self.assertEqual(report["execution_id"], result.job_id)
            self.assertIn("/execution-outcomes", report["url"])
            self.assertEqual(report["owner"], "NeuruhAI/recipe-engine Founder Runtime")
        self.assertEqual(
            writeback["DISPATCHED"]["idempotency_key"],
            f"execution_outcome:{result.job_id}:DISPATCHED:-")
        receipt_sha = result.sealed_receipt["payload_ref_or_payload"]["receipt_sha"]
        self.assertEqual(
            writeback["SUCCEEDED"]["idempotency_key"],
            f"execution_outcome:{result.job_id}:SUCCEEDED:{receipt_sha}")
        self.record_outcome(result)
        replay = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(replay.status, RuntimeStatus.ALREADY_SUCCEEDED)
        self.assertIsNone(replay.fr_evidence_update)
        # A replayed dispatch re-reports only the sealed SUCCEEDED evidence
        # (same idempotency key => FR replay by construction); no second DISPATCHED.
        self.assertEqual(sorted(replay.fr_outcome_writeback), ["SUCCEEDED"])
        self.assertEqual(
            replay.fr_outcome_writeback["SUCCEEDED"]["idempotency_key"],
            writeback["SUCCEEDED"]["idempotency_key"])

    def test_fr_api_health_without_auth_configuration_fails_closed(self):
        class _Resp:
            status = 200

            def read(self):
                return (
                    b'{"status":"ok","api_version":"fr-api-v1",'
                    b'"auth_configured":false}'
                )

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        bind = bind_founder_runtime(opener=lambda request, timeout=None: _Resp())
        self.assertFalse(bind["present"])
        self.assertEqual(bind["bind_status"], "FR_API_ABSENT_FAIL_CLOSED")

    def test_fr_api_bind_consumes_fr_api_v1_identity(self):
        class _Resp:
            status = 200

            def read(self):
                return b'{"status":"ok","api_version":"fr-api-v1","auth_configured":true}'

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        bind = bind_founder_runtime(opener=lambda request, timeout=None: _Resp())
        self.assertEqual(bind["bind_status"], "FR_API_CONSUMED")
        self.assertEqual(bind["api_version"], FR_API_VERSION)
        self.assertFalse(bind["this_pr_duplicates_founder_runtime"])
        self.assertFalse(bind["mission_persistence_local"])
        self.assertFalse(bind["approval_state_local"])
        self.assertEqual(bind["url"], FR_HEALTH_URL)

        class _List:
            status = 200

            def __init__(self, url):
                self.url = url

            def read(self):
                if self.url.endswith("/v1/health"):
                    return b'{"status":"ok","api_version":"fr-api-v1","auth_configured":true}'
                return json.dumps({
                    "count": 2,
                    "missions": [
                        {
                            "mission_id": "FR-1",
                            "state": "QUEUED",
                            "api_version": "fr-api-v1",
                            "objective": "read-only research",
                            "work_spec": {"schema": "fr.work_spec.v1"},
                            "work_spec_sha256": "a" * 64,
                            "authority": {
                                "state": "APPROVED",
                                "execution_authorized": True,
                            },
                        },
                        {
                            "mission_id": "FR-PLAN",
                            "state": "APPROVED_NO_EXECUTION",
                            "api_version": "fr-api-v1",
                            "authority": {
                                "state": "PLAN_APPROVED_NO_EXECUTION",
                                "execution_authorized": False,
                            },
                        },
                    ],
                }).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        pulled = pull_dispatchable_tickets(
            opener=lambda request, timeout=None: _List(request.full_url),
            token="t",
        )
        self.assertEqual(pulled["bind_status"], "FR_TICKETS_CONSUMED")
        self.assertEqual(len(pulled["tickets"]), 1)
        self.assertEqual(pulled["tickets"][0]["mission_id"], "FR-1")
        self.assertEqual(pulled["tickets"][0]["objective"], "read-only research")
        self.assertEqual(
            pulled["tickets"][0]["work_spec"]["schema"],
            "fr.work_spec.v1",
        )
        self.assertTrue(pulled["tickets"][0]["authority"]["execution_authorized"])
        self.assertFalse(pulled["this_pr_persists_missions"])

    def test_ownership_seam_does_not_claim_foreign_organs(self):
        for organ in THIS_PR_MAY_OWN:
            self.assertTrue(this_pr_owns(organ), organ)
        for organ in THIS_PR_MUST_NOT_OWN:
            self.assertTrue(this_pr_must_not_own(organ), organ)
            self.assertFalse(this_pr_owns(organ), organ)
        self.assertIn("CONTEXT_COMPILER", THIS_PR_MAY_OWN)
        self.assertIn("DISPATCH_BRIDGE", THIS_PR_MAY_OWN)
        self.assertNotIn("INTENT", THIS_PR_MAY_OWN)
        self.assertTrue(this_pr_must_not_own("INTENT"))
        self.assertTrue(this_pr_must_not_own("MISSION_PERSISTENCE"))
        self.assertTrue(this_pr_must_not_own("MISSION_IDENTITY"))
        self.assertTrue(this_pr_must_not_own("APPROVAL_STATE"))
        self.assertTrue(this_pr_must_not_own("DECISION_PLAN"))
        self.assertTrue(this_pr_must_not_own("MODEL_ROUTER"))
        self.assertTrue(this_pr_must_not_own("FOUNDER_RUNTIME"))
        self.assertTrue(this_pr_must_not_own("ETHER"))
        self.assertEqual(owner_of("ETHER")["role"], "ABSENT")
        self.assertIn("switchboard", owner_of("SWITCHBOARD")["owner"])
        self.assertIn("git_custodian", owner_of("GIT_CUSTODIAN")["owner"])
        self.assertIn("recipe-engine", owner_of("FOUNDER_RUNTIME")["owner"])
        self.assertIn("19090", owner_of("FOUNDER_RUNTIME")["owner"])
        self.assertEqual(owner_of("AUTOMATIC_PARENT_RESUME")["role"], "CONSUME")
        self.assertEqual(owner_of("MISSION_RECOVERY")["role"], "PARTIAL")
        self.assertEqual(owner_of("CHEAPEST_CAPABLE_ROUTER_L0_L1")["role"], "DELETE")
        self.assertTrue(this_pr_must_not_own("CHEAPEST_CAPABLE_ROUTER_L0_L1"))

    def test_consequential_path_invokes_gsa_call_sites(self):
        seen: list[str] = []

        def opener(request, timeout=None):
            seen.append(request.full_url)
            if request.full_url.startswith("http://127.0.0.1:19090/"):
                return self.health_opener(request, timeout=timeout)
            raise OSError("organ unreachable")

        runtime = DispatchBridge(
            self.queue,
            self.catalog,
            self.runner,
            receipts_dir=self.receipts_dir,
            switchboard_root=self.switchboard_root,
            source_head=self.source_sha,
            source_root=self.source_root,
            http_open=opener,
            fr_token="court-token",
            canonical_observer=self.canonical.observe,
            adjudicator=self.no_organ,
        )
        ticket, candidate = self.authorized_pair(
            "JGI-CONSEQUENTIAL-CALLS",
            self.property_candidate(money_move=True),
        )
        result = runtime.dispatch(ticket, [candidate])
        self.assertEqual(result.status, RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED)
        self.assertIsNone(result.job_id)
        self.assertIn(GOVERNANCE_EVALUATE_URL, seen)
        self.assertTrue(any("8848" in url and "/v1/evaluate" in url for url in seen))
        self.assertNotIn(AXON_EXECUTE_URL, seen)
        self.assertFalse(any(":8300" in url for url in seen))
        self.assertFalse(any("/guards" in url for url in seen))
        self.assertNotIn(GOVERNANCE_CONSUME_URL, seen)
        self.assertEqual(result.broker_bind["sentinel"]["bind_status"], "SWITCHBOARD_PRESENT_NO_SEALED_RECEIPT")
        axon_call = next(c for c in result.broker_bind["calls"] if c["organ"] == "axon")
        self.assertFalse(axon_call["attempted"])
        self.assertEqual(axon_call["error"], "AXON_WRITE_BANNED")
        self.assertTrue(result.broker_bind["attempted"])
        self.assertEqual(result.broker_bind["caller_approved"], False)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_organ_consumers_has_no_invented_gsa_fabric(self):
        source = Path(REPO_ROOT / "neuruh_workers" / "organ_consumers.py").read_text(encoding="utf-8")
        self.assertNotIn("8100", source)
        self.assertNotIn("8101", source)
        self.assertNotIn("/guards", source)
        self.assertIn("8848", source)
        self.assertIn("/v1/evaluate", source)
        self.assertIn("IAR_HEALTH_PORT = 8090", source)
        self.assertIn("FR_API_PORT = 19090", source)
        self.assertIn("FR_API_VERSION = \"fr-api-v1\"", source)
        self.assertNotIn("NRH_ENVELOPE_FIELDS", source)
        self.assertNotIn("def build_nrh_event_v1", source)
        self.assertNotIn("class Switchboard", source)
        self.assertNotIn("class GitCustodian", source)
        self.assertNotIn("Ether", source)

    def test_consumes_live_switchboard_and_custodian_from_main(self):
        self.assertTrue((REPO_ROOT / "neuruh_workers" / "switchboard.py").exists())
        self.assertTrue((REPO_ROOT / "neuruh_workers" / "events.py").exists())
        self.assertTrue((REPO_ROOT / "neuruh_workers" / "git_custodian").exists())
        self.assertTrue(switchboard_present())
        honey = honey_pot_bind()
        self.assertFalse(honey["this_pr_copies_custodian"])
        self.assertEqual(honey["custody"], "HONEY_POT")
        self.assertEqual(honey["bind_status"], "GIT_CUSTODIAN_CONSUMED")
        self.assertTrue(honey["present"])
        self.assertTrue(honey["axon_write_banned"])
        self.assertIn("NeuruhAI/neuruh-axon", WRITE_BAN_REPOS)
        added = Path(REPO_ROOT / "neuruh_workers" / "organ_consumers.py").read_text(encoding="utf-8")
        self.assertIn("neuruh_workers.switchboard", added)
        self.assertIn("neuruh_workers.events", added)
        self.assertIn("neuruh_workers.git_custodian", added)


if __name__ == "__main__":
    unittest.main()
