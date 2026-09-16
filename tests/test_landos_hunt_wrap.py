"""Targeted unit tests for the LandOS Hunt JobQueue WRAP."""
from __future__ import annotations

import json
import os
import unittest
from unittest import mock

from neuruh_workers.contracts import JobStatus, ReceiptType
from neuruh_workers.landos_hunt import (
    CAPABILITY_ID,
    LANDOS_HUNT_JOB_ID_FIELD,
    WORKFLOW_NAME,
    create_hunt,
    landos_hunt_job_id_from_step_results,
    post_hunt,
)
from neuruh_workers.registry import StepContext
from tests.helpers import make_stack


class _FakeResponse:
    def __init__(self, payload: dict, status: int = 202):
        self._raw = json.dumps(payload).encode("utf-8")
        self.status = status

    def read(self, _n: int = -1) -> bytes:
        return self._raw

    def getcode(self) -> int:
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class LandosHuntWrapTests(unittest.TestCase):
    def test_capability_and_workflow_registered(self):
        stack = make_stack(self)
        cap = stack.registry.resolve(CAPABILITY_ID)
        self.assertTrue(cap.read_only)
        self.assertFalse(cap.external)
        wf = stack.catalog.get(WORKFLOW_NAME)
        self.assertTrue(wf.read_only)
        self.assertEqual(wf.steps[0].capability, CAPABILITY_ID)
        census = stack.registry.declarations()
        self.assertIn("landos-hunt-wrap-v1", census)
        self.assertEqual(
            census["landos-hunt-wrap-v1"]["capabilities"][0]["name"], CAPABILITY_ID
        )

    def test_enqueue_emits_capability_in_job_spec(self):
        stack = make_stack(self)
        params = {
            "territory": [{"fips": "26021"}],
            "recipe": {"signal_types": ["taxsale"]},
        }
        job_id = stack.queue.enqueue(
            stack.catalog.get(WORKFLOW_NAME), params, requested_by="test"
        )
        enqueued = [
            r for r in stack.ledger.receipts(job_id)
            if r.receipt_type == ReceiptType.JOB_ENQUEUED
        ]
        self.assertEqual(len(enqueued), 1)
        spec = enqueued[0].payload["spec"]
        self.assertEqual(spec["name"], WORKFLOW_NAME)
        self.assertEqual(spec["steps"][0]["capability"], CAPABILITY_ID)

    @mock.patch.dict(os.environ, {"NVE_LAND_TOKEN": "test-token"}, clear=False)
    def test_execute_persists_landos_job_id_in_fr_jobqueue_outcome_shape(self):
        """LandOS job_id lands in JobQueue STEP_SUCCEEDED result (FR outcome vocabulary)."""
        landos_jid = "a91adc8d-a541-48dc-b48e-681c237a5cf9"

        def fake_open(request, timeout=None):
            self.assertEqual(request.get_method(), "POST")
            self.assertIn("/hunt", request.full_url)
            self.assertEqual(
                request.get_header("Authorization"), "Bearer test-token"
            )
            body = json.loads(request.data.decode("utf-8"))
            self.assertEqual(body["territory"], [{"fips": "26021"}])
            self.assertEqual(body["recipe"]["signal_types"], ["taxsale"])
            return _FakeResponse({"job_id": landos_jid, "status": "queued"}, 202)

        stack = make_stack(self)
        params = {
            "territory": [{"fips": "26021"}],
            "recipe": {"signal_types": ["taxsale"]},
        }
        with mock.patch("urllib.request.urlopen", side_effect=fake_open):
            job_id = stack.queue.enqueue(
                stack.catalog.get(WORKFLOW_NAME), params, requested_by="test"
            )
            state = stack.runner.execute(job_id)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        result = state.step_results["create_hunt"]
        self.assertEqual(result["job_id"], landos_jid)
        self.assertEqual(result[LANDOS_HUNT_JOB_ID_FIELD], landos_jid)
        self.assertEqual(
            landos_hunt_job_id_from_step_results(state.step_results), landos_jid
        )
        succeeded = [
            r for r in stack.ledger.receipts(job_id)
            if r.receipt_type == ReceiptType.STEP_SUCCEEDED
        ]
        # Existing FR/JobQueue outcome shape: job_id in STEP_SUCCEEDED result.
        self.assertEqual(succeeded[-1].payload["result"]["job_id"], landos_jid)
        self.assertEqual(
            succeeded[-1].payload["result"][LANDOS_HUNT_JOB_ID_FIELD], landos_jid
        )
        # Workers execution_id remains the JobQueue job id (distinct from LandOS job_id).
        self.assertNotEqual(job_id, landos_jid)

    def test_create_hunt_fail_closed_without_token(self):
        ctx = StepContext(
            job_id="j1", workflow=WORKFLOW_NAME, step_id="create_hunt",
            attempt=1, dry_run=False,
        )
        env = {k: v for k, v in os.environ.items()
               if k not in ("NVE_LAND_TOKEN", "LANDOS_ENGINE_TOKEN")}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ValueError) as caught:
                create_hunt(ctx, {"territory": [{"fips": "26021"}]})
        self.assertIn("token", str(caught.exception).lower())

    def test_post_hunt_rejects_non_loopback(self):
        with self.assertRaises(ValueError):
            post_hunt(
                territory=[{"fips": "26021"}],
                recipe={"signal_types": ["taxsale"]},
                base_url="http://example.com:8410",
                token="x",
            )


if __name__ == "__main__":
    unittest.main()
