import os
import tempfile
import unittest

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
from neuruh_workers.queue import JobQueue
from neuruh_workers.runner import WorkflowRunner
from neuruh_workers.workflows import build_catalog, build_registry


class FounderRuntimeCourt(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = ExecutionLedger(os.path.join(self.tmp.name, "ledger.jsonl"))
        self.registry = build_registry()
        self.catalog = build_catalog()
        self.queue = JobQueue(self.ledger, self.registry)
        self.runner = WorkflowRunner(self.queue, self.registry, self.catalog)
        self.runtime = FounderRuntime(self.queue, self.catalog, self.runner)

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
        self.assertEqual(result.status, RuntimeStatus.FOUNDER_GATE_REQUIRED)
        self.assertIn("PRODUCTION_MUTATION", result.founder_gate_reasons)
        self.assertIsNone(result.job_id)
        self.assertEqual(self.ledger.verify()["receipts"], 0)

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

    def test_economic_rank_can_choose_cheaper_path_without_model_bias(self):
        cheap = self.property_candidate(candidate_id="cheap", estimated_latency_minutes=2)
        expensive = self.property_candidate(
            candidate_id="frontier",
            layer=IntelligenceLayer.L4,
            estimated_model_cost_usd=0.75,
            success_probability=1.0,
            estimated_latency_minutes=2,
        )
        ranked = rank_candidates(self.mission(), [expensive, cheap])
        self.assertEqual(ranked[0][0].candidate_id, "cheap")

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


if __name__ == "__main__":
    unittest.main()
