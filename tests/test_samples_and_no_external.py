"""Sample workflow end-to-end runs, the external-action lockout, and the
Mother/AXON registration seam."""
import unittest

from neuruh_workers import (
    Capability, DefinitionError, ExternalActionDenied, JobStatus,
    OrganRegistrationSeam, OrganSeamDeclaration, ReceiptType, SeamNotAuthorized,
    StepDefinition, WorkerDeclaration, WorkerRegistry, WorkflowCatalog,
    WorkflowDefinition, WorkflowRunner,
)
from neuruh_workers.registry import EXTERNAL_AUTHORIZATION_SENTINEL
from tests.helpers import make_stack, pending_approval, run_brief_to_gate


class SampleWorkflowTests(unittest.TestCase):
    def _run(self, name, params):
        stack = make_stack(self)
        job = stack.queue.enqueue(stack.catalog.get(name), params,
                                  requested_by="op")
        return stack, stack.runner.execute(job)

    def test_property_research(self):
        _, state = self._run("property-research",
                             {"address": "428 Bluff Rd, Decatur, TX",
                              "county": "Wise"})
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        dossier = state.step_results["compile_research_dossier"]["dossier"]
        self.assertEqual(dossier["query_count"], 5)
        self.assertEqual(dossier["subject"]["county"], "WISE")
        self.assertEqual(dossier["external_execution"], "NOT_AUTHORIZED_IN_V1")

    def test_field_pin_enrichment(self):
        _, state = self._run("field-pin-enrichment", {
            "pins": [{"pin_id": "b", "lat": 33.4, "lon": -97.7},
                     {"pin_id": "a", "lat": -33.4, "lon": 97.7}]})
        enriched = state.step_results["enrich_pins"]["enriched_pins"]
        self.assertEqual([p["pin_id"] for p in enriched], ["a", "b"])
        self.assertEqual(enriched[0]["hemisphere_ns"], "S")
        self.assertEqual(enriched[1]["grid_cell"], "33:-98")

    def test_parcel_intelligence_assembly(self):
        _, state = self._run("parcel-intelligence-assembly", {
            "parcel": {"apn": "R-1441", "county": "wise", "acreage": 12.5},
            "signals": [{"apn": "R-1441", "kind": "tax_delinquent"}]})
        report = state.step_results["assemble_parcel_report"]["parcel_report"]
        self.assertEqual(report["signal_match_ratio"], 1.0)
        self.assertEqual(report["flags"], [])

    def test_daily_opportunity_brief_full_path(self):
        stack = make_stack(self)
        job = run_brief_to_gate(stack)
        pending = pending_approval(stack, job)
        stack.runner.grant_approval(
            job, "finalize_brief", approver="founder", nonce="n1",
            challenge=pending["challenge"], params_sha=pending["params_sha"])
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        draft = state.step_results["draft_brief"]["draft"]
        self.assertEqual(draft["item_count"], 2)  # duplicate opp-1 deduped
        self.assertIn("Tax lien parcel", draft["body"].split("\n")[2])  # top rank

    def test_world_emitter_refresh_preview_never_writes(self):
        _, state = self._run("world-emitter-refresh-preview", {
            "current_worlds": ["defensenet", "aegis"],
            "proposed_worlds": ["defensenet", "meshgrid"]})
        preview = state.step_results["render_preview"]["preview"]
        self.assertTrue(preview["no_write"])
        self.assertEqual(preview["plan"]["add"], ["meshgrid"])
        self.assertEqual(preview["plan"]["remove"], ["aegis"])

    def test_cockpit_health_snapshot(self):
        _, state = self._run("cockpit-health-snapshot", {
            "components": [{"name": "qdrant", "status": "healthy"},
                           {"name": "bonsai", "status": "down"},
                           {"name": "iar", "status": "degraded"}]})
        snapshot = state.step_results["render_snapshot"]["snapshot"]
        self.assertEqual(snapshot["overall"], "down")
        self.assertEqual(snapshot["worst_component"], "bonsai")
        self.assertEqual(snapshot["counts"],
                         {"healthy": 1, "down": 1, "degraded": 1})

    def test_invalid_inputs_fail_closed_not_defaulted(self):
        stack = make_stack(self)
        job = stack.queue.enqueue(
            stack.catalog.get("cockpit-health-snapshot"),
            {"components": [{"name": "x", "status": "sorta-ok"}]},
            requested_by="op")
        state = stack.runner.execute(job)
        self.assertEqual(state.status, JobStatus.FAILED)
        failed = [r for r in stack.ledger.receipts(job)
                  if r.receipt_type == ReceiptType.STEP_FAILED]
        self.assertIn("refusing to guess", failed[0].payload["error"])


class NoExternalActionTests(unittest.TestCase):
    def _external_cap(self):
        return Capability(
            name="comms.send_email", description="sends email",
            read_only=False, external=True,
            execute=lambda ctx, params: {"sent": True})

    def test_external_capability_cannot_even_register(self):
        registry = WorkerRegistry()
        with self.assertRaises(ExternalActionDenied):
            registry.register_worker(WorkerDeclaration(
                worker_id="mailer", description="d",
                capabilities=(self._external_cap(),)))
        self.assertEqual(registry.declarations(), {})

    def test_runner_denies_external_execution_even_if_registered(self):
        # Defense in depth: even a registry constructed with the authorization
        # sentinel cannot get an external step EXECUTED by the v1 runner.
        registry = WorkerRegistry(
            external_authorization=EXTERNAL_AUTHORIZATION_SENTINEL)
        registry.register_worker(WorkerDeclaration(
            worker_id="mailer", description="d",
            capabilities=(self._external_cap(),)))
        catalog = WorkflowCatalog()
        catalog.register(WorkflowDefinition(
            name="mail-flow", version="1", description="d", read_only=False,
            steps=(StepDefinition(step_id="send", capability="comms.send_email"),)))
        stack = make_stack(self, registry=registry, catalog=catalog)
        job = stack.queue.enqueue(catalog.get("mail-flow"), {}, requested_by="op")
        with self.assertRaises(ExternalActionDenied):
            stack.runner.execute(job)
        types = [r.receipt_type for r in stack.ledger.receipts(job)]
        self.assertIn(ReceiptType.EXTERNAL_ACTION_DENIED, types)
        self.assertEqual(stack.queue.load(job).status, JobStatus.FAILED)

    def test_read_only_workflow_cannot_bind_mutating_capability(self):
        registry = WorkerRegistry()
        registry.register_worker(WorkerDeclaration(
            worker_id="writer", description="d",
            capabilities=(Capability(
                name="disk.write", description="", read_only=False,
                external=False, execute=lambda c, p: {}),)))
        catalog = WorkflowCatalog()
        catalog.register(WorkflowDefinition(
            name="sneaky", version="1", description="d", read_only=True,
            steps=(StepDefinition(step_id="w", capability="disk.write"),)))
        stack = make_stack(self, registry=registry, catalog=catalog)
        with self.assertRaises(Exception) as caught:
            stack.queue.enqueue(catalog.get("sneaky"), {}, requested_by="op")
        self.assertIn("CAPABILITY_VIOLATION", str(caught.exception))

    def test_sample_worker_census_is_fully_readonly_and_internal(self):
        stack = make_stack(self)
        census = stack.registry.declarations()
        caps = census["local-readonly-worker-v1"]["capabilities"]
        self.assertGreaterEqual(len(caps), 19)
        self.assertTrue(all(c["read_only"] for c in caps))
        self.assertTrue(all(not c["external"] for c in caps))


class OrganSeamTests(unittest.TestCase):
    def test_declaration_is_receipted_and_typed(self):
        stack = make_stack(self)
        seam = OrganRegistrationSeam(stack.ledger)
        seam.declare(OrganSeamDeclaration(
            organ="mother", endpoint="http://127.0.0.1:8077",
            source_sha="f29851c548f96a65a38c2753733f4b37365643ce"))
        declared = [r for r in stack.ledger.receipts("seam-registry")
                    if r.receipt_type == ReceiptType.SEAM_DECLARED]
        self.assertEqual(declared[0].payload["status"], "PENDING_AUTHORIZATION")

    def test_activation_always_refuses_in_v1(self):
        stack = make_stack(self)
        seam = OrganRegistrationSeam(stack.ledger)
        seam.declare(OrganSeamDeclaration(
            organ="axon", endpoint="http://127.0.0.1:8300",
            source_sha="f3a94d1a68ac6b07525e962692901411bd43e6bf"))
        with self.assertRaises(SeamNotAuthorized):
            seam.activate("axon")
        refused = [r for r in stack.ledger.receipts("seam-registry")
                   if r.receipt_type == ReceiptType.SEAM_ACTIVATION_REFUSED]
        self.assertEqual(len(refused), 1)

    def test_seam_validation_fails_closed(self):
        with self.assertRaises(DefinitionError):
            OrganSeamDeclaration(organ="governance",  # not a seam organ in v1
                                 endpoint="http://127.0.0.1:1",
                                 source_sha="a" * 40)
        with self.assertRaises(DefinitionError):
            OrganSeamDeclaration(organ="mother",
                                 endpoint="http://10.0.0.5:8077",  # not loopback
                                 source_sha="a" * 40)
        with self.assertRaises(DefinitionError):
            OrganSeamDeclaration(organ="mother",
                                 endpoint="http://127.0.0.1:8077",
                                 source_sha="short")


if __name__ == "__main__":
    unittest.main()
