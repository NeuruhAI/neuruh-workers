"""Unit tests for landos.hunt.create WRAP — no live LandOS calls."""
from __future__ import annotations

import unittest
from typing import Any, Mapping
from unittest import mock

from neuruh_workers.definitions import WorkflowCatalog
from neuruh_workers.landos_hunt import (
    CAPABILITY_ID,
    WORKFLOW_NAME,
    bind_landos_hunt_workers,
    create_hunt,
)
from neuruh_workers.registry import StepContext, WorkerRegistry
from neuruh_workers.workflows import build_catalog, build_registry


def _ctx() -> StepContext:
    return StepContext(
        job_id="job-test-hunt-1",
        workflow=WORKFLOW_NAME,
        step_id="create_hunt",
        attempt=1,
        dry_run=False,
    )


class LandosHuntRegistrationTests(unittest.TestCase):
    def test_bind_registers_capability_and_workflow(self):
        registry = WorkerRegistry()
        catalog = WorkflowCatalog()
        bind_landos_hunt_workers(registry, catalog)
        cap = registry.resolve(CAPABILITY_ID)
        self.assertEqual(cap.name, "landos.hunt.create")
        # POST /hunt creates a hunt job: mutating, never auto-executed.
        self.assertFalse(cap.read_only)
        self.assertFalse(cap.external)
        wf = catalog.get(WORKFLOW_NAME)
        self.assertEqual(wf.name, "landos-hunt")
        self.assertFalse(wf.read_only)
        self.assertEqual(wf.steps[0].capability, CAPABILITY_ID)

    def test_default_stack_resolves_capability(self):
        registry = build_registry()
        catalog = build_catalog()
        self.assertEqual(registry.resolve(CAPABILITY_ID).name, CAPABILITY_ID)
        self.assertEqual(catalog.get(WORKFLOW_NAME).name, WORKFLOW_NAME)


class LandosHuntExecuteTests(unittest.TestCase):
    def test_execute_mocked_http_returns_hunt_job_id(self):
        calls: list[tuple[str, dict[str, Any], Mapping[str, str]]] = []

        def fake_post(url, payload, *, timeout_s=0.25, opener=None, headers=None):
            calls.append((url, dict(payload), dict(headers or {})))
            return {
                "attempted": True,
                "url": url,
                "ok": True,
                "status": 202,
                "error": None,
                "decision": "queued",
                "data": {"job_id": "hunt-abc-123", "status": "queued"},
            }

        with mock.patch.dict(
            "os.environ",
            {
                "LANDOS_ENGINE_URL": "http://127.0.0.1:8410",
                "LANDOS_HUNT_TOKEN": "test-token",
            },
            clear=False,
        ):
            out = create_hunt(
                _ctx(),
                {"territory": [{"fips": "26021"}], "signal_types": ["taxsale"]},
                http_post=fake_post,
            )
        self.assertEqual(out["hunt_job_id"], "hunt-abc-123")
        self.assertEqual(out["status"], "queued")
        self.assertEqual(len(calls), 1)
        url, payload, headers = calls[0]
        self.assertEqual(url, "http://127.0.0.1:8410/hunt")
        self.assertEqual(payload["territory"], [{"fips": "26021"}])
        self.assertEqual(payload["recipe"]["signal_types"], ["taxsale"])
        self.assertEqual(headers.get("Authorization"), "Bearer test-token")

    def test_empty_territory_raises(self):
        with self.assertRaises(ValueError) as caught:
            create_hunt(_ctx(), {"territory": []}, http_post=lambda *a, **k: {})
        self.assertIn("territory", str(caught.exception))

    def test_no_live_landos_call_when_http_mocked(self):
        with mock.patch(
            "neuruh_workers.landos_hunt.http_post_json",
            side_effect=AssertionError("live http_post_json must not run in unit tests"),
        ):
            out = create_hunt(
                _ctx(),
                {"territory": [{"fips": "18141", "state": "IN", "county": "St. Joseph"}]},
                http_post=lambda *a, **k: {
                    "ok": True,
                    "status": 202,
                    "error": None,
                    "decision": "queued",
                    "data": {"job_id": "hunt-no-live", "status": "queued"},
                },
            )
        self.assertEqual(out["hunt_job_id"], "hunt-no-live")


class SamplesImportSmokeTests(unittest.TestCase):
    def test_samples_still_import_and_build(self):
        registry = build_registry()
        catalog = build_catalog()
        self.assertIn("property-research", catalog.names())
        self.assertTrue(registry.resolve("research.normalize_subject").read_only)


if __name__ == "__main__":
    unittest.main()
