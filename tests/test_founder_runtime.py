import inspect
import os
import tempfile
import unittest
from pathlib import Path

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
    FounderRuntime,
    IntelligenceLayer,
    MissionIntent,
    RuntimeStatus,
    WorkCandidate,
    compile_context,
    rank_candidates,
)
from neuruh_workers.ledger import ExecutionLedger
from neuruh_workers.organ_consumers import (
    AXON_GUARDS_URL,
    GOVERNANCE_CONSUME_URL,
    GOVERNANCE_EVALUATE_URL,
    NRH_ENVELOPE_FIELDS,
    SENTINEL_COURT_URL,
    honey_pot_bind,
    switchboard_present,
)
from neuruh_workers.queue import JobQueue
from neuruh_workers.runner import WorkflowRunner
from neuruh_workers.workflows import build_catalog, build_registry

REPO_ROOT = Path(__file__).resolve().parents[1]


class FounderRuntimeCourt(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = ExecutionLedger(os.path.join(self.tmp.name, "ledger.jsonl"))
        self.registry = build_registry()
        self.catalog = build_catalog()
        self.queue = JobQueue(self.ledger, self.registry)
        self.runner = WorkflowRunner(self.queue, self.registry, self.catalog)
        self.receipts_dir = os.path.join(self.tmp.name, "switcharoo-receipts")
        self.runtime = FounderRuntime(
            self.queue,
            self.catalog,
            self.runner,
            receipts_dir=self.receipts_dir,
            source_head="a" * 40,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def mission(self, **kwargs):
        base = dict(
            mission_id="JGI-COURT-001",
            objective="finish a safe existing workflow without a clerical handoff",
            expected_value_usd=500.0,
            uncertainty=0.2,
            relevance=1.0,
            model_budget_usd=1.0,
            canonical_refs=("repo:NeuruhAI/neuruh-workers@05c669a",),
            do_not_rebuild=("JobQueue", "WorkflowRunner", "ExecutionLedger"),
        )
        base.update(kwargs)
        return MissionIntent(**base)

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

    def test_safe_l0_workflow_runs_to_receipted_success_without_handoff(self):
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertIsNotNone(result.job_id)
        self.assertEqual(result.job_status, "SUCCEEDED")
        self.assertIsNotNone(result.ledger_tail_sha)
        self.assertEqual(result.compiled_context["jeramie_manual_handoffs_target"], 0)
        receipts = self.ledger.receipts(result.job_id)
        self.assertTrue(any(r.receipt_type == "job.succeeded" for r in receipts))

    def test_repeated_dispatch_is_idempotent_and_does_not_reexecute_success(self):
        first = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        first_count = len(self.ledger.receipts(first.job_id))
        second = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(second.status, RuntimeStatus.ALREADY_SUCCEEDED)
        receipts = self.ledger.receipts(first.job_id)
        self.assertGreater(len(receipts), first_count)  # dedupe receipt is explicit
        self.assertEqual(sum(r.receipt_type == "job.succeeded" for r in receipts), 1)

    def test_material_authority_gate_never_enqueues(self):
        candidate = self.property_candidate(production_mutation=True)
        result = self.runtime.dispatch(self.mission(), [candidate])
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
        self.assertIn(("sentinel", "court"), methods)
        self.assertIn(("axon", "guards"), methods)
        self.assertTrue(any(c["attempted"] and c.get("error") for c in result.broker_bind["calls"]))
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
                result = self.runtime.dispatch(
                    self.mission(mission_id=f"JGI-{flag}"),
                    [self.property_candidate(**{flag: True})],
                )
                self.assertEqual(result.status, RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED)
                self.assertIsNone(result.job_id)
                self.assertFalse(result.broker_bind["caller_approved"])
                self.assertTrue(result.broker_bind["attempted"])
                self.assertTrue(result.broker_bind["calls"][0]["attempted"])
                self.assertEqual(result.broker_bind["calls"][0]["url"], GOVERNANCE_EVALUATE_URL)

    def test_model_layer_routes_to_broker_instead_of_self_authorizing(self):
        candidate = self.property_candidate(
            layer=IntelligenceLayer.L4,
            estimated_model_cost_usd=0.5,
            ambiguity=0.95,
        )
        result = self.runtime.dispatch(self.mission(), [candidate])
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
                result = self.runtime.dispatch(
                    self.mission(mission_id=f"JGI-{layer.value}"),
                    [self.property_candidate(layer=layer, estimated_model_cost_usd=0.1)],
                )
                self.assertEqual(result.status, RuntimeStatus.REASONING_BROKER_REQUIRED)
                self.assertTrue(result.broker_bind["organ_absent"])
                self.assertFalse(result.broker_bind["authorized"])
                self.assertFalse(result.broker_bind["model_call"])
                self.assertIsNone(result.job_id)

    def test_l2_still_fail_closed_when_iar_health_is_up_but_rf_absent(self):
        runtime = FounderRuntime(
            self.queue,
            self.catalog,
            self.runner,
            iar_probe=lambda: {
                "present": True,
                "url": "http://127.0.0.1:8090/health",
                "health": {"status": "ok"},
                "authorized": False,
            },
        )
        result = runtime.dispatch(
            self.mission(),
            [self.property_candidate(layer=IntelligenceLayer.L3, estimated_model_cost_usd=0.1)],
        )
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
        packet = compile_context(mission, self.property_candidate(), max_bytes=1500)
        import json
        raw = json.dumps(packet, sort_keys=True).encode("utf-8")
        self.assertLessEqual(len(raw), 1500)
        self.assertNotIn("chat_history", packet)
        self.assertNotIn("transcript", packet)
        self.assertEqual(packet["jeramie_manual_handoffs_target"], 0)
        self.assertEqual(packet["authority_path"], "governance->sentinel->axon")
        self.assertEqual(packet["reasoning_broker"], "revenue_factory.authorization+aegis.iar_v2")
        self.assertEqual(packet["spend_authority"], "iar_rf")

    def test_receipt_sealed_and_court_request_emitted(self):
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertEqual(tuple(result.sealed_receipt), NRH_ENVELOPE_FIELDS)
        self.assertEqual(result.sealed_receipt["schema_version"], "nrh.event.v1")
        self.assertEqual(result.sealed_receipt["event_type"], "receipt.sealed")
        self.assertEqual(result.court_request["event_type"], "court.requested")
        self.assertEqual(tuple(result.court_request), NRH_ENVELOPE_FIELDS)
        payload = result.sealed_receipt["payload_ref_or_payload"]
        self.assertEqual(payload["job_id"], result.job_id)
        self.assertEqual(payload["ledger_tail_sha"], result.ledger_tail_sha)
        self.assertIn("workflow:property-research@1.0.0", payload["evidence_refs"])
        self.assertIsNotNone(payload["job_succeeded_sha"])
        self.assertIn("required_invariants", payload)
        self.assertEqual(payload["sentinel_closure_in_receipt"], "NOT_JUDGED")
        self.assertIsNone(result.court_request["payload_ref_or_payload"]["verdict"])
        self.assertEqual(
            result.court_request["payload_ref_or_payload"]["sentinel_model_execution"],
            "BLOCKED",
        )
        self.assertTrue(result.switchboard_emit["attempted"])
        if not switchboard_present():
            self.assertEqual(
                result.switchboard_emit["bind_status"],
                "SWITCHBOARD_ABSENT_FAIL_CLOSED",
            )
            self.assertEqual(result.switchboard_emit["error"], "ModuleNotFoundError")
        receipt_path = os.path.join(self.receipts_dir, f"{result.job_id}.txt")
        self.assertTrue(os.path.exists(receipt_path))
        text = Path(receipt_path).read_text(encoding="utf-8")
        self.assertIn("EVENT_TARGET=receipt.sealed", text)
        self.assertIn("SENTINEL_CLOSURE=NOT_JUDGED", text)
        self.assertNotIn("SENTINEL_CLOSURE=PASS", text)

    def test_parent_resume_from_paused_parent_job(self):
        parent_def = self.catalog.get("property-research")
        parent_job = self.queue.enqueue(
            parent_def,
            {"address": "1 Parent St, Benton Harbor, MI", "county": "Berrien"},
            requested_by="jgi:JGI-PARENT-001",
        )
        self.queue.pause(parent_job)
        self.assertEqual(self.queue.load(parent_job).status, JobStatus.PAUSED)

        result = self.runtime.dispatch(
            self.mission(mission_id="JGI-CHILD-001", parent_mission_id="JGI-PARENT-001"),
            [self.property_candidate(
                candidate_id="child",
                params={"address": "2 Child St, Benton Harbor, MI", "county": "Berrien"},
            )],
        )
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertTrue(result.parent_resume["automatic"])
        self.assertEqual(result.parent_resume["status"], "PARENT_RESUMED_FROM_RECEIPTS")
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

        result = self.runtime.dispatch(
            self.mission(mission_id="JGI-CHILD-RECOVER", parent_mission_id="JGI-PARENT-RECOVER"),
            [self.property_candidate(
                candidate_id="child-recover",
                params={"address": "8 Child Recover St", "county": "Berrien"},
            )],
        )
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertTrue(result.parent_resume["automatic"])
        self.assertEqual(self.queue.load(parent_job).status, JobStatus.SUCCEEDED)
        types = [r.receipt_type for r in self.ledger.receipts(parent_job)]
        self.assertIn(ReceiptType.JOB_RECOVERED, types)

    def test_parent_resume_absent_parent_is_precise_gap(self):
        result = self.runtime.dispatch(
            self.mission(parent_mission_id="JGI-PARENT-MISSING"),
            [self.property_candidate()],
        )
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertFalse(result.parent_resume["automatic"])
        self.assertEqual(result.parent_resume["status"], "PARENT_JOB_ABSENT_IN_LEDGER")
        self.assertEqual(result.parent_resume["parent_mission_id"], "JGI-PARENT-MISSING")

    def test_ownership_seam_does_not_claim_foreign_organs(self):
        for organ in THIS_PR_MAY_OWN:
            self.assertTrue(this_pr_owns(organ), organ)
        for organ in THIS_PR_MUST_NOT_OWN:
            self.assertTrue(this_pr_must_not_own(organ), organ)
            self.assertFalse(this_pr_owns(organ), organ)
        self.assertIn("neuruh-workers#3", owner_of("SWITCHBOARD")["owner"])
        self.assertIn("git_custodian", owner_of("GIT_CUSTODIAN")["owner"])
        self.assertIn("authorization.py", owner_of("REVENUE_FACTORY")["owner"] + owner_of("CHEAPEST_CAPABLE_ROUTER_L2_L4")["owner"])
        self.assertIn("aegis", owner_of("IAR")["owner"])
        self.assertIn("iar_v2", owner_of("IAR")["owner"])
        self.assertIn("autonomous-governance-core", owner_of("GOVERNANCE")["owner"])
        self.assertIn("neuruh-axon", owner_of("AXON")["owner"])
        self.assertEqual(owner_of("AUTOMATIC_PARENT_RESUME")["role"], "CONSUME")
        self.assertEqual(owner_of("MISSION_RECOVERY")["role"], "PARTIAL")
        self.assertEqual(owner_of("CHEAPEST_CAPABLE_ROUTER_L0_L1")["role"], "DELETE")
        self.assertTrue(this_pr_must_not_own("CHEAPEST_CAPABLE_ROUTER_L0_L1"))

    def test_consequential_path_invokes_gsa_call_sites(self):
        seen: list[str] = []

        def opener(request, timeout=None):
            seen.append(request.full_url)
            raise OSError("organ unreachable")

        runtime = FounderRuntime(
            self.queue, self.catalog, self.runner, http_open=opener,
        )
        result = runtime.dispatch(
            self.mission(),
            [self.property_candidate(money_move=True)],
        )
        self.assertEqual(result.status, RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED)
        self.assertIsNone(result.job_id)
        self.assertIn(GOVERNANCE_EVALUATE_URL, seen)
        self.assertIn(SENTINEL_COURT_URL, seen)
        self.assertIn(AXON_GUARDS_URL, seen)
        self.assertNotIn(GOVERNANCE_CONSUME_URL, seen)
        self.assertTrue(result.broker_bind["attempted"])
        self.assertEqual(result.broker_bind["caller_approved"], False)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

    def test_no_duplicate_switchboard_or_custodian_modules(self):
        self.assertFalse((REPO_ROOT / "neuruh_workers" / "switchboard.py").exists())
        self.assertFalse((REPO_ROOT / "neuruh_workers" / "events.py").exists())
        self.assertFalse((REPO_ROOT / "neuruh_workers" / "git_custodian").exists())
        self.assertFalse((REPO_ROOT / "neuruh_workers" / "prime_receipts.py").exists())
        honey = honey_pot_bind()
        self.assertFalse(honey["this_pr_copies_custodian"])
        self.assertEqual(honey["custody"], "HONEY_POT")
        if not honey["present"]:
            self.assertEqual(honey["bind_status"], "GIT_CUSTODIAN_ABSENT_FAIL_CLOSED")


if __name__ == "__main__":
    unittest.main()
