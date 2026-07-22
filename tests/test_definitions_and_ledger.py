"""Unit tests: definitions, retry math, catalog, ledger chain integrity."""
import json
import os
import unittest

from neuruh_workers import (
    DefinitionError, ExecutionLedger, LedgerCorrupt, RetryPolicy,
    StepDefinition, WorkflowCatalog, WorkflowDefinition,
)
from tests.helpers import fixed_clock, make_stack


def _step(step_id="s1", **kw):
    return StepDefinition(step_id=step_id, capability="cap.one", **kw)


class DefinitionTests(unittest.TestCase):
    def test_valid_definition(self):
        wf = WorkflowDefinition(
            name="ok-flow", version="1.0.0", description="d",
            steps=(_step("a"), _step("b")))
        self.assertEqual(wf.step("a").step_id, "a")
        self.assertEqual(len(wf.spec()["steps"]), 2)

    def test_rejects_duplicate_step_ids(self):
        with self.assertRaises(DefinitionError):
            WorkflowDefinition(name="dup", version="1", description="d",
                               steps=(_step("a"), _step("a")))

    def test_rejects_empty_steps_and_bad_names(self):
        with self.assertRaises(DefinitionError):
            WorkflowDefinition(name="empty", version="1", description="d", steps=())
        with self.assertRaises(DefinitionError):
            WorkflowDefinition(name="Bad Name!", version="1", description="d",
                               steps=(_step(),))
        with self.assertRaises(DefinitionError):
            _step("UPPER")

    def test_rejects_approval_gate_without_approvers(self):
        with self.assertRaises(DefinitionError):
            _step("g", requires_approval=True)
        with self.assertRaises(DefinitionError):
            _step("g", approvers=("someone",))  # approvers without a gate

    def test_rejects_nonpositive_timeout(self):
        with self.assertRaises(DefinitionError):
            _step("t", timeout_s=0)

    def test_retry_policy_validation_and_backoff_math(self):
        with self.assertRaises(DefinitionError):
            RetryPolicy(max_attempts=0)
        with self.assertRaises(DefinitionError):
            RetryPolicy(multiplier=0.5)
        policy = RetryPolicy(max_attempts=5, base_delay_s=1.0,
                             multiplier=2.0, max_delay_s=5.0)
        self.assertEqual([policy.delay_for_attempt(n) for n in (1, 2, 3, 4)],
                         [1.0, 2.0, 4.0, 5.0])  # capped at max_delay_s

    def test_catalog_refuses_unknown_and_duplicates(self):
        catalog = WorkflowCatalog()
        wf = WorkflowDefinition(name="one", version="1", description="d",
                                steps=(_step(),))
        catalog.register(wf)
        with self.assertRaises(DefinitionError):
            catalog.register(wf)
        with self.assertRaises(DefinitionError):
            catalog.get("missing")


class LedgerTests(unittest.TestCase):
    def test_append_and_verify(self):
        stack = make_stack(self)
        r1 = stack.ledger.append("job.enqueued", "job-x", {"k": 1})
        r2 = stack.ledger.append("job.started", "job-x", {})
        self.assertEqual(r2.prev_sha, r1.receipt_sha)
        facts = stack.ledger.verify()
        self.assertEqual(facts["receipts"], 2)
        self.assertEqual(facts["tail_sha"], r2.receipt_sha)

    def test_tampered_payload_is_refused(self):
        stack = make_stack(self)
        stack.ledger.append("job.enqueued", "job-x", {"amount": 1})
        with open(stack.ledger_path) as fh:
            line = fh.read()
        with open(stack.ledger_path, "w") as fh:
            fh.write(line.replace('"amount":1', '"amount":999'))
        with self.assertRaises(LedgerCorrupt):
            ExecutionLedger(stack.ledger_path)

    def test_truncated_chain_link_is_refused(self):
        stack = make_stack(self)
        stack.ledger.append("job.enqueued", "job-x", {})
        stack.ledger.append("job.started", "job-x", {})
        with open(stack.ledger_path) as fh:
            first, _second = fh.read().strip().split("\n")
        record = json.loads(first)
        record["seq"] = 2  # break sequencing
        with open(stack.ledger_path, "w") as fh:
            fh.write(json.dumps(record) + "\n")
        with self.assertRaises(LedgerCorrupt):
            ExecutionLedger(stack.ledger_path)

    def test_corrupt_ledger_refuses_appends_no_silent_fallback(self):
        stack = make_stack(self)
        stack.ledger.append("job.enqueued", "job-x", {})
        with open(stack.ledger_path, "a") as fh:
            fh.write("{not json}\n")
        with self.assertRaises(LedgerCorrupt):
            stack.ledger.append("job.started", "job-x", {})

    def test_unexpected_fields_are_refused(self):
        stack = make_stack(self)
        r = stack.ledger.append("job.enqueued", "job-x", {})
        record = json.loads(r.to_line())
        record["injected"] = True
        with open(stack.ledger_path, "w") as fh:
            fh.write(json.dumps(record) + "\n")
        with self.assertRaises(LedgerCorrupt):
            ExecutionLedger(stack.ledger_path)

    def test_ledger_is_append_only_on_disk(self):
        stack = make_stack(self)
        stack.ledger.append("job.enqueued", "job-x", {})
        size_before = os.path.getsize(stack.ledger_path)
        stack.ledger.append("job.started", "job-x", {})
        self.assertGreater(os.path.getsize(stack.ledger_path), size_before)
        with open(stack.ledger_path) as fh:
            self.assertEqual(len(fh.read().strip().split("\n")), 2)


if __name__ == "__main__":
    unittest.main()
