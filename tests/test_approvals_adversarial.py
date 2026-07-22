"""Adversarial approval tests: every forgery, replay and bypass path must be
rejected with a receipt, and only the exact content-bound approval advances."""
import unittest

from neuruh_workers import ApprovalInvalid, JobStatus, ReceiptType
from tests.helpers import make_stack, pending_approval, run_brief_to_gate


class ApprovalGateTests(unittest.TestCase):
    def setUp(self):
        self.stack = make_stack(self)
        self.job = run_brief_to_gate(self.stack)
        self.pending = pending_approval(self.stack, self.job)

    def _grant(self, **overrides):
        args = {
            "approver": "founder",
            "nonce": "nonce-1",
            "challenge": self.pending["challenge"],
            "params_sha": self.pending["params_sha"],
        }
        args.update(overrides)
        return self.stack.runner.grant_approval(
            self.job, overrides.get("step_id", "finalize_brief"), **{
                k: v for k, v in args.items() if k != "step_id"})

    def _rejected_attempts(self):
        return [r for r in self.stack.ledger.receipts(self.job)
                if r.receipt_type == ReceiptType.APPROVAL_REJECTED_ATTEMPT]

    def test_gate_blocks_job_with_clear_state(self):
        state = self.stack.queue.load(self.job)
        self.assertEqual(state.status, JobStatus.BLOCKED_ON_APPROVAL)
        self.assertEqual(state.pending_approval["step_id"], "finalize_brief")
        self.assertNotIn("finalize_brief", state.step_results)

    def test_valid_approval_unblocks_and_completes(self):
        self._grant()
        state = self.stack.runner.execute(self.job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        brief = state.step_results["finalize_brief"]["brief"]
        self.assertEqual(brief["status"], "FINAL_APPROVED")
        self.assertIn("not authorized", brief["distribution"])

    def test_wrong_approver_rejected(self):
        with self.assertRaises(ApprovalInvalid):
            self._grant(approver="intruder")
        self.assertEqual(self._rejected_attempts()[-1].payload["reason"],
                         "approver is not on this gate's approver list")

    def test_self_approval_forbidden(self):
        # The gate's approver list could name the requester; even then the
        # requester cannot approve their own job.
        with self.assertRaises(ApprovalInvalid):
            self.stack.runner.grant_approval(
                self.job, "finalize_brief", approver="operator",
                nonce="n", challenge=self.pending["challenge"],
                params_sha=self.pending["params_sha"])
        reasons = {r.payload["reason"] for r in self._rejected_attempts()}
        self.assertTrue(any("approver" in r or "self-approval" in r
                            for r in reasons))

    def test_forged_challenge_rejected(self):
        with self.assertRaises(ApprovalInvalid):
            self._grant(challenge="0" * 64)
        self.assertIn("challenge", self._rejected_attempts()[-1].payload["reason"])

    def test_tampered_params_binding_rejected(self):
        with self.assertRaises(ApprovalInvalid):
            self._grant(params_sha="f" * 64)
        self.assertIn("parameters", self._rejected_attempts()[-1].payload["reason"])

    def test_wrong_step_rejected(self):
        with self.assertRaises(ApprovalInvalid):
            self._grant(step_id="draft_brief")

    def test_nonce_replay_rejected(self):
        self._grant(nonce="nonce-used")
        # Force a second gate pass on a second identical-shaped job.
        job2 = self.stack.queue.enqueue(
            self.stack.catalog.get("daily-opportunity-brief"),
            {"date": "2026-07-23", "opportunities": []},
            requested_by="operator")
        self.stack.runner.execute(job2)
        pending2 = pending_approval(self.stack, job2)
        # Same-job replay: gate is no longer pending.
        with self.assertRaises(ApprovalInvalid):
            self._grant(nonce="nonce-used")
        # A *fresh* nonce on the second job works; the used nonce is per-job
        # tracked and refused on the first job forever.
        self.stack.runner.grant_approval(
            job2, "finalize_brief", approver="founder", nonce="nonce-fresh",
            challenge=pending2["challenge"], params_sha=pending2["params_sha"])

    def test_missing_nonce_rejected(self):
        with self.assertRaises(ApprovalInvalid):
            self._grant(nonce="")

    def test_approval_of_unblocked_job_rejected(self):
        other = self.stack.queue.enqueue(
            self.stack.catalog.get("cockpit-health-snapshot"),
            {"components": [{"name": "iar", "status": "healthy"}]},
            requested_by="operator")
        with self.assertRaises(ApprovalInvalid):
            self.stack.runner.grant_approval(
                other, "render_snapshot", approver="founder", nonce="n",
                challenge="x", params_sha="y")

    def test_denial_fails_job_terminally_and_blocks_late_grant(self):
        self.stack.runner.deny_approval(
            self.job, "finalize_brief", approver="founder",
            nonce="deny-nonce", reason="not today")
        state = self.stack.queue.load(self.job)
        self.assertEqual(state.status, JobStatus.FAILED)
        self.assertEqual(state.failure_reason, "approval_denied")
        with self.assertRaises(ApprovalInvalid):
            self._grant()  # too late — the job already failed

    def test_dry_run_does_not_bypass_the_gate(self):
        job = run_brief_to_gate(self.stack.reopen(), dry_run=True)
        # even a dry-run blocks; there is no approval bypass path
        fresh = self.stack.reopen()
        self.assertEqual(fresh.queue.load(job).status,
                         JobStatus.BLOCKED_ON_APPROVAL)

    def test_every_rejected_attempt_is_receipted(self):
        for bad in ({"approver": "intruder"}, {"challenge": "0" * 64},
                    {"nonce": ""}):
            with self.assertRaises(ApprovalInvalid):
                self._grant(**bad)
        self.assertEqual(len(self._rejected_attempts()), 3)


if __name__ == "__main__":
    unittest.main()
