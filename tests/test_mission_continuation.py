"""Durable mission continuation court (O4).

Every predicate the O4 mission names is asserted here against the real
ledger primitives — no mocks, no in-memory state, "restart" is a fresh
MissionContinuation over the same ledger file.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from neuruh_workers import JobStatus, ReceiptType
from neuruh_workers.contracts import content_sha
from neuruh_workers.continuation import (
    AWAIT_CAPABILITY,
    GATED_PARENT_WORKFLOW,
    PHASE_BLOCKED_ON_APPROVAL,
    PHASE_READY_FOR_REVIEW,
    PHASE_WAITING_ON_CHILD,
    ContinuationRefused,
    MissionContinuation,
    expected_child_job_id,
)
from neuruh_workers.ledger import ExecutionLedger
from tests.helpers import fixed_clock

MISSION = "O4-COURT-001"
CHILD_PARAMS = {"components": [{"name": "qdrant", "status": "healthy"},
                               {"name": "laric", "status": "degraded"}]}


def mission_params(child_params=CHILD_PARAMS, workflow="cockpit-health-snapshot"):
    return {
        "objective": "prove durable parent/child continuation",
        "done_predicate": "parent READY_FOR_REVIEW with the child's job.succeeded sha bound",
        "economic_link": "first-paid-tape acquisition proof",
        "children": {"specialist": {"workflow": workflow, "params": child_params}},
    }


class ContinuationCourt(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="neuruh-continuation-")
        self.addCleanup(self._cleanup)
        self.path = os.path.join(self.tmp, "ledger.jsonl")

    def _cleanup(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def process(self) -> MissionContinuation:
        """A 'new process' over the same ledger file."""
        return MissionContinuation(ExecutionLedger(self.path, clock=fixed_clock()))

    # -- identity ------------------------------------------------------------

    def test_mission_identity_survives_restart_and_reopen_is_receipted_noop(self):
        p1 = self.process()
        opened = p1.open_mission(MISSION, mission_params())
        self.assertEqual(opened["status"], "OPENED")
        p2 = self.process()
        again = p2.open_mission(MISSION, mission_params())
        self.assertEqual(again["status"], "ALREADY_OPEN")
        self.assertEqual(again["parent_job_id"], opened["parent_job_id"])
        self.assertEqual(p2.parent_job_id(MISSION), opened["parent_job_id"])
        types = [r.receipt_type for r in p2.ledger.receipts(opened["parent_job_id"])]
        self.assertEqual(types, [ReceiptType.JOB_ENQUEUED, ReceiptType.JOB_ENQUEUE_DEDUPLICATED])

    def test_reopening_a_mission_with_different_work_is_refused(self):
        p = self.process()
        p.open_mission(MISSION, mission_params())
        with self.assertRaises(ContinuationRefused):
            p.open_mission(MISSION, mission_params(child_params={"components": []}))

    def test_child_identity_is_deterministic_from_parent_job_and_role(self):
        p = self.process()
        parent = p.open_mission(MISSION, mission_params())["parent_job_id"]
        report = p.tick(MISSION, run_children=False)
        self.assertEqual(report["phase"], PHASE_WAITING_ON_CHILD)
        child = p.status(MISSION)["children"]["specialist"]
        expected = expected_child_job_id(
            parent, "specialist", p.catalog.get("cockpit-health-snapshot"), CHILD_PARAMS)
        self.assertEqual(child["child_job_id"], expected)
        self.assertEqual(child["child_mission_id"], f"{MISSION}::specialist")
        # A second ledger with the same mission yields the same child id.
        other = MissionContinuation(ExecutionLedger(
            os.path.join(self.tmp, "other.jsonl"), clock=fixed_clock()))
        other.open_mission(MISSION, mission_params())
        other.tick(MISSION, run_children=False)
        self.assertEqual(other.status(MISSION)["children"]["specialist"]["child_job_id"], expected)

    # -- duplicate dispatch --------------------------------------------------

    def test_duplicate_dispatch_is_refused_while_waiting_and_after_completion(self):
        p = self.process()
        p.open_mission(MISSION, mission_params())
        p.tick(MISSION, run_children=False)
        child_id = p.status(MISSION)["children"]["specialist"]["child_job_id"]
        dup = self.process().dispatch_child(MISSION, "specialist")
        self.assertEqual(dup["status"], "DUPLICATE_REFUSED")
        self.assertEqual(dup["child_job_id"], child_id)
        # exactly one child enqueue, one dedupe receipt, one child.dispatched
        ledger = self.process().ledger
        child_types = [r.receipt_type for r in ledger.receipts(child_id)]
        self.assertEqual(child_types.count(ReceiptType.JOB_ENQUEUED), 1)
        self.assertEqual(child_types.count(ReceiptType.JOB_ENQUEUE_DEDUPLICATED), 1)
        parent_types = [r.receipt_type for r in ledger.receipts(p.parent_job_id(MISSION))]
        self.assertEqual(parent_types.count(ReceiptType.CHILD_DISPATCHED), 1)
        # ticking again while waiting does not dispatch a second child
        self.process().tick(MISSION, run_children=False)
        self.assertEqual(len(self.process().queue.job_ids()), 2)
        # after completion the parent is terminal: dispatch is refused outright
        self.process().run_until_settled(MISSION)
        after = self.process().dispatch_child(MISSION, "specialist")
        self.assertEqual(after["status"], "DISPATCH_REFUSED_PARENT_TERMINAL")
        self.assertEqual(len(self.process().queue.job_ids()), 2)

    # -- continuation --------------------------------------------------------

    def test_child_completion_resumes_parent_across_processes_without_founder(self):
        p1 = self.process()
        parent = p1.open_mission(MISSION, mission_params())["parent_job_id"]
        r1 = p1.tick(MISSION, run_children=False)
        self.assertEqual((r1["before"], r1["after"]), ("QUEUED", "PAUSED"))
        self.assertEqual(r1["phase"], PHASE_WAITING_ON_CHILD)
        child = p1.status(MISSION)["children"]["specialist"]["child_job_id"]
        self.assertEqual(p1.queue.load(child).status, JobStatus.QUEUED)

        p2 = self.process()  # restart: the child runs in another process
        p2.runner.execute(child)
        self.assertEqual(p2.queue.load(child).status, JobStatus.SUCCEEDED)
        self.assertEqual(p2.status(MISSION)["phase"], PHASE_WAITING_ON_CHILD)

        p3 = self.process()  # restart: ingestion + resume
        r3 = p3.tick(MISSION)
        self.assertEqual(r3["action"], "PARENT_RESUMED")
        self.assertEqual((r3["before"], r3["after"]), ("PAUSED", "SUCCEEDED"))
        status = p3.status(MISSION)
        self.assertEqual(status["phase"], PHASE_READY_FOR_REVIEW)
        self.assertFalse(status["review_packet"]["published"])
        succeeded = [r for r in p3.ledger.receipts(child)
                     if r.receipt_type == ReceiptType.JOB_SUCCEEDED][-1]
        self.assertEqual(status["review_packet"]["child_job_succeeded_sha"], succeeded.receipt_sha)
        types = [r.receipt_type for r in p3.ledger.receipts(parent)]
        self.assertNotIn(ReceiptType.APPROVAL_REQUESTED, types)
        self.assertNotIn(ReceiptType.APPROVAL_GRANTED, types)
        self.assertEqual(types.count(ReceiptType.CHILD_RESULT_INGESTED), 1)
        self.assertEqual(types.count(ReceiptType.JOB_RESUMED), 1)
        # the declare step was served from the ledger, not re-executed
        self.assertIn(ReceiptType.STEP_REPLAY_SKIPPED, types)
        self.assertEqual(types.count(ReceiptType.STEP_STARTED), 3)

    def test_single_tick_loop_settles_without_any_human_receipt(self):
        p = self.process()
        p.open_mission(MISSION, mission_params())
        reports = p.run_until_settled(MISSION)
        self.assertEqual([r["action"] for r in reports],
                         ["PARENT_ADVANCED", "PARENT_RESUMED", "PARENT_TERMINAL"])
        self.assertEqual(self.process().status(MISSION)["phase"], PHASE_READY_FOR_REVIEW)

    # -- ingestion idempotency / sealed replay --------------------------------

    def test_result_ingestion_is_idempotent(self):
        p = self.process()
        p.open_mission(MISSION, mission_params())
        p.tick(MISSION)  # dispatch + run child
        first = self.process().ingest_child(MISSION, "specialist")
        self.assertEqual(first["status"], "INGESTED")
        tail = self.process().ledger.verify()
        second = self.process().ingest_child(MISSION, "specialist")
        self.assertEqual(second["status"], "ALREADY_INGESTED")
        self.assertEqual(second["child_job_succeeded_sha"], first["child_job_succeeded_sha"])
        self.assertEqual(self.process().ledger.verify(), tail)  # nothing appended

    def test_sealed_child_receipt_replays_safely_and_tamper_is_refused(self):
        p = self.process()
        p.open_mission(MISSION, mission_params())
        p.tick(MISSION)
        receipt = self.process().export_child_receipt(MISSION, "specialist")
        self.assertEqual(receipt["receipt_sha"],
                         content_sha({k: v for k, v in receipt.items() if k != "receipt_sha"}))
        path = os.path.join(self.tmp, "COMMONS_RECEIPT.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(receipt, fh)
        with open(path, encoding="utf-8") as fh:
            loaded = json.load(fh)
        r1 = self.process().ingest_sealed_receipt(loaded)
        self.assertEqual(r1["status"], "INGESTED")
        tail = self.process().ledger.verify()
        r2 = self.process().ingest_sealed_receipt(loaded)  # replay
        self.assertEqual(r2["status"], "ALREADY_INGESTED")
        self.assertEqual(self.process().ledger.verify(), tail)
        # tampered receipts never ingest
        forged = {**loaded, "child_results_sha": "0" * 64}
        with self.assertRaises(ContinuationRefused):
            self.process().ingest_sealed_receipt(forged)
        forged = {**loaded, "child_job_succeeded_sha": "0" * 64}
        body = {k: v for k, v in forged.items() if k != "receipt_sha"}
        forged["receipt_sha"] = content_sha(body)  # re-sealed but unbound
        with self.assertRaises(ContinuationRefused):
            self.process().ingest_sealed_receipt(forged)
        self.assertEqual(self.process().ledger.verify(), tail)
        # the parent then resumes from the replayed ingestion on the next tick
        self.assertEqual(self.process().tick(MISSION)["action"], "PARENT_RESUMED")

    # -- blocked stays blocked -----------------------------------------------

    def test_failed_child_leaves_parent_waiting_and_never_succeeds(self):
        p = self.process()
        # invalid child params: cockpit.collect refuses -> child FAILED
        p.open_mission(MISSION, mission_params(child_params={"components": "nope"}))
        reports = p.run_until_settled(MISSION)
        self.assertEqual(reports[-1]["action"], "CHILD_FAILED_PARENT_STAYS_PAUSED")
        status = self.process().status(MISSION)
        self.assertEqual(status["phase"], PHASE_WAITING_ON_CHILD)
        self.assertEqual(status["children"]["specialist"]["status"], "FAILED")
        with self.assertRaises(ContinuationRefused):
            self.process().ingest_child(MISSION, "specialist")
        with self.assertRaises(ContinuationRefused):
            self.process().export_child_receipt(MISSION, "specialist")
        # more ticks change nothing
        tail = self.process().ledger.verify()
        self.assertEqual(self.process().tick(MISSION)["action"], "CHILD_FAILED_PARENT_STAYS_PAUSED")
        self.assertEqual(self.process().ledger.verify(), tail)

    def test_gated_parent_stays_blocked_on_approval_after_child_returns(self):
        p = self.process()
        p.open_mission(MISSION, mission_params(), workflow=GATED_PARENT_WORKFLOW)
        reports = p.run_until_settled(MISSION)
        self.assertEqual([r["action"] for r in reports],
                         ["PARENT_ADVANCED", "PARENT_RESUMED", "PARENT_BLOCKED_ON_APPROVAL"])
        status = self.process().status(MISSION)
        self.assertEqual(status["phase"], PHASE_BLOCKED_ON_APPROVAL)
        self.assertIsNone(status["review_packet"])
        self.assertTrue(status["children"]["specialist"]["ingested"])
        tail = self.process().ledger.verify()
        for _ in range(3):
            self.assertEqual(self.process().tick(MISSION)["action"], "PARENT_BLOCKED_ON_APPROVAL")
        self.assertEqual(self.process().ledger.verify(), tail)
        self.assertEqual(self.process().dispatch_child(MISSION, "specialist")["status"],
                         "DUPLICATE_REFUSED")

    # -- structure -----------------------------------------------------------

    def test_await_step_fails_closed_without_ingestion(self):
        p = self.process()
        p.open_mission(MISSION, mission_params())
        p.tick(MISSION, run_children=False)
        parent = p.parent_job_id(MISSION)
        with self.assertRaises(ContinuationRefused):
            p.registry.resolve(AWAIT_CAPABILITY).execute(
                type("Ctx", (), {"job_id": parent})(), {"child_role": "specialist"})

    def test_parent_and_child_are_read_only_and_no_external_capability_exists(self):
        p = self.process()
        for name in (p.catalog.get("mission-continuation-parent"),
                     p.catalog.get(GATED_PARENT_WORKFLOW)):
            self.assertTrue(name.read_only)
        caps = p.registry.declarations()["mission-continuation-v1"]["capabilities"]
        self.assertTrue(all(c["read_only"] and not c["external"] for c in caps))
        with self.assertRaises(ContinuationRefused):
            p.open_mission(MISSION, mission_params(workflow="no-such-workflow"))
            p.tick(MISSION)


if __name__ == "__main__":
    unittest.main()
