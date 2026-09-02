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
    DispatchBridge,
    DispatchTicket,
    FounderRuntime,
    IntelligenceLayer,
    MissionIntent,
    RuntimeStatus,
    WorkCandidate,
    compile_context,
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
)
from neuruh_workers.queue import JobQueue
from neuruh_workers.runner import WorkflowRunner
from neuruh_workers.switchboard import INDEPENDENT_COURT_KIND, SwitchboardError
from neuruh_workers.switchboard_jobs import (
    GIT_PUSH,
    HONEY_POT_WORKFLOW,
    SENTINEL_MODEL_EXECUTION,
    SENTINEL_WORKFLOW,
)
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
        self.switchboard_root = os.path.join(self.tmp.name, "switchboard")
        self.runtime = DispatchBridge(
            self.queue,
            self.catalog,
            self.runner,
            receipts_dir=self.receipts_dir,
            switchboard_root=self.switchboard_root,
            source_head="a" * 40,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def mission(self, **kwargs):
        base = dict(
            mission_id="JGI-COURT-001",
            objective="finish a safe existing workflow without a clerical handoff",
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

    def test_repeated_dispatch_is_idempotent_and_does_not_reexecute_success(self):
        first = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        first_count = len(self.ledger.receipts(first.job_id))
        second = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(second.status, RuntimeStatus.ALREADY_SUCCEEDED)
        receipts = self.ledger.receipts(first.job_id)
        self.assertGreater(len(receipts), first_count)
        self.assertEqual(sum(r.receipt_type == "job.succeeded" for r in receipts), 1)

    def test_rejects_ticket_that_is_not_fr_approved_or_queued(self):
        result = self.runtime.dispatch(
            self.mission(fr_state="AWAITING_APPROVAL"),
            [self.property_candidate()],
        )
        self.assertEqual(result.status, RuntimeStatus.FR_STATE_NOT_DISPATCHABLE)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)
        self.assertFalse(result.fr_bind["this_pr_duplicates_founder_runtime"])

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
                result = self.runtime.dispatch(
                    self.mission(mission_id=f"JGI-{flag}"),
                    [self.property_candidate(**{flag: True})],
                )
                self.assertEqual(result.status, RuntimeStatus.GOVERNANCE_SENTINEL_AXON_REQUIRED)
                self.assertIsNone(result.job_id)
                self.assertFalse(result.broker_bind["caller_approved"])
                self.assertTrue(result.broker_bind["attempted"])
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
        packet = compile_context(mission, self.property_candidate(), max_bytes=1800)
        import json
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
        self.assertEqual(result.sealed_receipt["schema_version"], "nrh.event.v1")
        self.assertEqual(result.sealed_receipt["source_role"], "DISPATCH_BRIDGE")
        self.assertNotEqual(result.sealed_receipt.get("source_role"), "SWITCHAROO")
        self.assertIsNotNone(result.court_request)
        self.assertEqual(result.court_request["event_type"], EventType.COURT_REQUESTED.value)
        self.assertEqual(result.court_request["source_role"], "SWITCHAROO")
        self.assertEqual(result.sentinel["model_execution"], SENTINEL_MODEL_EXECUTION)
        self.assertEqual(result.sentinel["bind_status"], "SENTINEL_COURT_JOBBED_INDEPENDENT")
        self.assertTrue(result.sentinel["independent"])
        self.assertFalse(result.sentinel["judged"])
        self.assertIsNone(result.sentinel["verdict"])
        self.assertTrue(result.sentinel["ci_job_named_court_is_not_sentinel"])
        board = self.runtime.last_switchboard
        self.assertIsNotNone(board)
        self.assertEqual(len(board.events(EventType.COURT_PASSED)), 0)
        jobs = board.jobs_for(SENTINEL_WORKFLOW)
        self.assertEqual(len(jobs), 1)
        artifact = board.queue.load(jobs[0]).step_results["materialize"]
        self.assertEqual(artifact["sentinel_model_execution"], "BLOCKED")

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
        result = self.runtime.dispatch(
            self.mission(code_custody_required=True),
            [self.property_candidate()],
        )
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
            evidence={
                "kind": INDEPENDENT_COURT_KIND,
                "source_role": "SENTINEL",
                "independent": True,
                "ref": "test://sentinel-independent-dispatch-bridge",
            },
            extra_payload={
                "honey_pot_expected": {
                    "repo": "NeuruhAI/neuruh-workers",
                    "branch": "feature/jgi-founder-runtime-v0-20260901",
                    "head": "a" * 40,
                    "worktree": "/tmp/dispatch-bridge-worktree",
                    "remote": "origin",
                },
            },
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

        result = self.runtime.dispatch(
            self.mission(mission_id="JGI-CHILD-001", parent_mission_id="JGI-PARENT-001"),
            [self.property_candidate(
                candidate_id="child",
                params={"address": "2 Child St, Benton Harbor, MI", "county": "Berrien"},
            )],
        )
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
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

        result = self.runtime.dispatch(
            self.mission(mission_id="JGI-CHILD-RECOVER", parent_mission_id="JGI-PARENT-RECOVER"),
            [self.property_candidate(
                candidate_id="child-recover",
                params={"address": "8 Child Recover St", "county": "Berrien"},
            )],
        )
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        self.assertTrue(result.parent_resume["automatic"])
        self.assertTrue(result.parent_resume["proven"])
        self.assertEqual(result.parent_resume["status"], "PARENT_RESUME=PROVEN")
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
        self.assertFalse(result.parent_resume["proven"])
        self.assertEqual(result.parent_resume["status"], "PARENT_JOB_ABSENT_IN_LEDGER")
        self.assertEqual(result.parent_resume["parent_mission_id"], "JGI-PARENT-MISSING")

    def test_fr_evidence_update_is_attempted_and_not_persisted_here(self):
        result = self.runtime.dispatch(self.mission(), [self.property_candidate()])
        self.assertEqual(result.status, RuntimeStatus.SUCCEEDED)
        update = result.fr_evidence_update
        self.assertTrue(update["attempted"])
        self.assertFalse(update["persisted_locally"])
        self.assertEqual(update["bind_status"], "FR_EVIDENCE_UPDATE_FAIL_CLOSED")
        self.assertTrue(update["fr_write"]["attempted"])
        self.assertFalse(update["fr_write"]["ok"])
        self.assertIn("/v1/missions/", update["fr_write"]["url"])
        self.assertEqual(update["owner"], "NeuruhAI/recipe-engine Founder Runtime")

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
                return (
                    b'{"count":1,"missions":[{"mission_id":"FR-1","state":"QUEUED",'
                    b'"api_version":"fr-api-v1","authority":{"state":"APPROVED"}}]}'
                )

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        pulled = pull_dispatchable_tickets(
            opener=lambda request, timeout=None: _List(request.full_url),
            token="t",
        )
        self.assertEqual(pulled["bind_status"], "FR_TICKETS_CONSUMED")
        self.assertEqual(pulled["tickets"][0]["mission_id"], "FR-1")
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
            raise OSError("organ unreachable")

        runtime = DispatchBridge(
            self.queue, self.catalog, self.runner, http_open=opener,
        )
        result = runtime.dispatch(
            self.mission(),
            [self.property_candidate(money_move=True)],
        )
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
