"""Legion court: 10,000 agents, supervision, crash-resume, determinism, governance."""
from __future__ import annotations

import http.server
import json
import os
import shutil
import tempfile
import threading
import unittest
from collections import Counter

from neuruh_workers import ExecutionLedger, JobQueue, WorkflowRunner
from neuruh_workers.contracts import JobStatus, LedgerCorrupt
from neuruh_workers.definitions import WorkflowCatalog
from neuruh_workers.legion import (
    LEGION_MAX_AGENTS_ABSOLUTE, PROFILES, Legion, LegionBusy, LegionJournal,
    LegionRefused, LegionSpec, LegionSpecDrift, register_legion, stage_units,
    unit_manifest_sha, verify_journal,
)
from neuruh_workers.legion.profiles import OPPORTUNITY_ANALYST, SCALE_PROBE
from neuruh_workers.legion.runtime import AgentProfile, LegionCrashInjected
from neuruh_workers.registry import WorkerRegistry


def synthetic(n, *, fault_every=0, work=20):
    units = []
    for i in range(n):
        unit = {"unit_id": f"u{i:06d}", "work": work}
        if fault_every and i % fault_every == fault_every - 1:
            unit["fault"] = ("raise_once", "raise_always", "hang_once")[(i // fault_every) % 3]
            unit["hang_s"] = 0.6
        units.append(unit)
    return units


def spec_for(units, profile=SCALE_PROBE, **kw):
    return LegionSpec(mission=kw.pop("mission", "court"), profile=profile.name,
                      profile_version=profile.version,
                      unit_manifest_sha=unit_manifest_sha(units), unit_count=len(units), **kw)


class LegionCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="legion-court-")
        self.addCleanup(shutil.rmtree, self.root, True)

    def run_legion(self, units, profile=SCALE_PROBE, root=None, **kw):
        legion_kw = {k: kw.pop(k) for k in ("crash_after_terminal", "halt_path") if k in kw}
        spec = spec_for(units, profile, **kw)
        return Legion(spec, profile, units, root or self.root, **legion_kw).run()

    def journal_kinds(self, legion_id, root=None):
        path = os.path.join(root or self.root, legion_id, "journal.jsonl")
        with open(path) as fh:
            return [json.loads(line) for line in fh]


class TenThousandAgents(LegionCase):
    def test_10000_agents_one_final_output(self):
        units = synthetic(10_000)
        result = self.run_legion(units, max_parallel=64)
        self.assertEqual(result.state, "SEALED")
        self.assertEqual(result.counts["agents_spawned"], 10_000)
        self.assertEqual(result.counts["agents_succeeded"], 10_000)
        self.assertEqual(result.counts["squads_sealed"], 200)
        self.assertEqual(result.counts["cohorts_sealed"], 10)
        self.assertEqual(result.final_output["output"]["agents_reduced"], 10_000)
        records = self.journal_kinds(result.legion_id)
        kinds = Counter(r["kind"] for r in records)
        self.assertEqual(kinds["legion.sealed"], 1)
        self.assertEqual(kinds["agent.succeeded"], 10_000)
        self.assertEqual(len({r["body"]["agent_id"] for r in records
                              if r["kind"] == "agent.succeeded"}), 10_000)
        self.assertEqual(verify_journal(os.path.join(self.root, result.legion_id,
                                                     "journal.jsonl"), result.legion_id)
                         ["receipts"], len(records))
        self.assertTrue(os.path.exists(result.final_path))

    def test_output_is_independent_of_parallelism(self):
        units = synthetic(1_500)
        a = self.run_legion(units, max_parallel=1, root=os.path.join(self.root, "a"))
        b = self.run_legion(units, max_parallel=96, root=os.path.join(self.root, "b"))
        self.assertEqual(a.legion_id, b.legion_id)
        self.assertEqual(a.final_sha, b.final_sha)
        self.assertEqual(a.merkle_root, b.merkle_root)

    def test_unit_order_does_not_change_identity(self):
        units = synthetic(300)
        a = self.run_legion(units, root=os.path.join(self.root, "a"))
        b = self.run_legion(list(reversed(units)), root=os.path.join(self.root, "b"))
        self.assertEqual((a.legion_id, a.final_sha), (b.legion_id, b.final_sha))


class Supervision(LegionCase):
    def test_retry_timeout_and_final_failure_are_receipted(self):
        units = synthetic(600, fault_every=50)          # 12 faults: 4 of each kind
        result = self.run_legion(units, agent_timeout_s=0.3, max_attempts=2, max_parallel=16)
        self.assertEqual(result.state, "SEALED")
        self.assertEqual(result.counts["agents_failed"], 4)        # raise_always
        self.assertEqual(result.counts["agents_succeeded"], 596)
        self.assertEqual(result.counts["attempts_total"], 612)
        kinds = Counter(r["kind"] for r in self.journal_kinds(result.legion_id))
        self.assertEqual(kinds["agent.timed_out"], 4)
        failures = result.final_output["failures"]
        self.assertEqual(len(failures), 4)
        self.assertTrue(all("raise_always" in f["error"] for f in failures))

    def test_timed_out_result_is_discarded(self):
        units = [{"unit_id": "slow", "fault": "hang_once", "hang_s": 0.5}]
        result = self.run_legion(units, agent_timeout_s=0.1, max_attempts=2)
        records = self.journal_kinds(result.legion_id)
        succeeded = [r for r in records if r["kind"] == "agent.succeeded"]
        self.assertEqual(len(succeeded), 1)
        self.assertEqual(succeeded[0]["body"]["attempt"], 2)

    def test_final_timeout_fails_agent(self):
        units = [{"unit_id": "slow", "fault": "hang_once", "hang_s": 0.4}]
        result = self.run_legion(units, agent_timeout_s=0.1, max_attempts=1)
        self.assertEqual(result.counts["agents_failed"], 1)
        self.assertIn("TIMEOUT", result.final_output["failures"][0]["error"])


class CrashResume(LegionCase):
    def test_crash_then_resume_matches_uninterrupted_run(self):
        units = synthetic(2_000, fault_every=97)
        clean = self.run_legion(units, root=os.path.join(self.root, "clean"),
                                agent_timeout_s=0.3)
        crashed_root = os.path.join(self.root, "crash")
        with self.assertRaises(LegionCrashInjected):
            self.run_legion(units, root=crashed_root, agent_timeout_s=0.3,
                            crash_after_terminal=700)
        resumed = self.run_legion(units, root=crashed_root, agent_timeout_s=0.3)
        self.assertEqual(resumed.final_sha, clean.final_sha)
        self.assertEqual(resumed.merkle_root, clean.merkle_root)
        records = self.journal_kinds(resumed.legion_id, crashed_root)
        self.assertEqual(sum(r["kind"] == "legion.resumed" for r in records), 1)
        per_agent = Counter(r["body"]["agent_id"] for r in records
                            if r["kind"] == "agent.succeeded")
        self.assertEqual(max(per_agent.values()), 1, "a terminal agent was re-run")

    def test_sealed_legion_is_idempotent(self):
        units = synthetic(200)
        first = self.run_legion(units)
        before = len(self.journal_kinds(first.legion_id))
        again = self.run_legion(units)
        self.assertEqual(again.final_sha, first.final_sha)
        self.assertEqual(len(self.journal_kinds(first.legion_id)), before)

    def test_torn_tail_is_discarded_and_receipted(self):
        units = synthetic(150)
        with self.assertRaises(LegionCrashInjected):
            self.run_legion(units, crash_after_terminal=60)
        legion_id = spec_for(units).legion_id
        path = os.path.join(self.root, legion_id, "journal.jsonl")
        with open(path, "a") as fh:
            fh.write('{"seq": 99999, "kind": "agent.succ')       # torn write
        result = self.run_legion(units)
        self.assertEqual(result.state, "SEALED")
        kinds = [r["kind"] for r in self.journal_kinds(legion_id)]
        self.assertIn("journal.torn_tail_discarded", kinds)

    def test_tampered_journal_is_a_hard_stop(self):
        units = synthetic(100)
        result = self.run_legion(units)
        path = os.path.join(self.root, result.legion_id, "journal.jsonl")
        with open(path) as fh:
            lines = fh.readlines()
        lines[5] = lines[5].replace('"attempt":1', '"attempt":2')
        with open(path, "w") as fh:
            fh.writelines(lines)
        with self.assertRaises(LedgerCorrupt):
            LegionJournal(path, result.legion_id)


class Governance(LegionCase):
    def test_absolute_ceiling(self):
        units = [{"unit_id": f"u{i}"} for i in range(LEGION_MAX_AGENTS_ABSOLUTE + 1)]
        with self.assertRaises(LegionRefused):
            spec_for(units, max_agents=LEGION_MAX_AGENTS_ABSOLUTE + 1).validate()
        with self.assertRaises(LegionRefused):
            spec_for(units[:11], max_agents=10).validate()

    def test_unit_drift_is_refused(self):
        units = synthetic(20)
        spec = spec_for(units)
        tampered = [dict(u) for u in units]
        tampered[3]["work"] = 999
        with self.assertRaises(LegionSpecDrift):
            Legion(spec, SCALE_PROBE, tampered, self.root)

    def test_duplicate_unit_ids_refused(self):
        units = synthetic(5) + [{"unit_id": "u000001"}]
        with self.assertRaises(LegionRefused):
            Legion(spec_for(units), SCALE_PROBE, units, self.root)

    def test_non_read_only_profile_refused(self):
        writer = AgentProfile(name="writer", version="1", description="x",
                              run=lambda c, u: {}, reduce_squad=lambda r: {},
                              reduce_cohort=lambda s: {}, finalize=lambda s, m: {},
                              read_only=False)
        units = synthetic(3)
        with self.assertRaises(LegionRefused):
            Legion(spec_for(units, writer), writer, units, self.root)

    def test_second_live_supervisor_refused(self):
        units = synthetic(10)
        spec = spec_for(units)
        legion_dir = os.path.join(self.root, spec.legion_id)
        os.makedirs(legion_dir)
        with open(os.path.join(legion_dir, "supervisor.lock"), "w") as fh:
            fh.write(str(os.getppid()))        # a live process that is not us
        with self.assertRaises(LegionBusy):
            Legion(spec, SCALE_PROBE, units, self.root).run()

    def test_kill_switch_halts_then_resumes(self):
        units = synthetic(400)
        spec = spec_for(units)
        halt = os.path.join(self.root, "HALT")
        open(halt, "w").close()
        halted = Legion(spec, SCALE_PROBE, units, self.root, halt_path=halt).run()
        self.assertEqual(halted.state, "HALTED")
        self.assertGreater(halted.counts["agents_pending"], 0)
        os.unlink(halt)
        sealed = Legion(spec, SCALE_PROBE, units, self.root, halt_path=halt).run()
        self.assertEqual(sealed.state, "SEALED")
        self.assertEqual(sealed.counts["agents_succeeded"], 400)


def lro_opportunity(oid, fips, signals, recipes=()):
    return {"unit_id": oid, "run_id": "run-x", "subject": f"parcel:{fips}:{oid}",
            "identity": {"address": f"{oid} MAIN ST", "owner_pattern": "PERSON"},
            "eligibility": "MATCH", "signals": signals, "recipes": list(recipes)}


def sig(value=True, support=0.9, state="PRESENT"):
    return {"state": state, "support": support, "contradiction": 0.0, "value": value,
            "confidence": "HIGH", "basis": "VERIFIED"}


class OpportunityAnalyst(LegionCase):
    def units(self):
        return [
            lro_opportunity("opp_a", "37183", {
                "tax.delinquent": sig(), "tax.delinquent_amount": sig(1693.91),
                "heirship.unresolved": sig(), "tax.foreclosed": sig(),
                "transfer.tenure_years": sig(22.0), "valuation.assessed_value": sig(412000.0)},
                [{"recipe_id": "estate_death_signal", "decision": "UNKNOWN",
                  "qualification": "NEEDS_EVIDENCE",
                  "nbe_top": {"signal_id": "probate.opened",
                              "source_id": "court.nc.ecourts_portal", "blocks": "MEMBERSHIP"}}]),
            lro_opportunity("opp_b", "37063", {"tax.delinquent": sig(),
                                               "tax.delinquent_amount": sig(90.0)}),
            lro_opportunity("opp_gov", "37183", {"tax.delinquent": sig(),
                                                 "ownership.government": sig()}),
            lro_opportunity("opp_none", "37179", {"acreage.acres": sig(2.0)}),
        ]

    def test_brief_laws(self):
        result = self.run_legion(self.units(), OPPORTUNITY_ANALYST)
        out = result.final_output["output"]
        self.assertEqual(out["kind"], "OPPORTUNITY_BRIEF")
        top = out["top_opportunities"]
        self.assertEqual(top[0]["opportunity_id"], "opp_a")
        self.assertEqual(top[0]["tier"], "A")
        self.assertNotIn("opp_gov", [r["opportunity_id"] for r in top])
        self.assertEqual(out["tier_distribution"]["EXCLUDED_GOVERNMENT"], 1)
        self.assertEqual(out["tier_distribution"]["D"], 1)
        for row in top:
            self.assertEqual(row["contact"], "NOT_AUTHORIZED")
            self.assertTrue(row["equity"].startswith("UNKNOWN"))
        self.assertEqual(out["evidence_unlock_map"][0],
                         {"source_id": "court.nc.ecourts_portal", "opportunities_blocked": 1})
        self.assertEqual(sorted(out["top_by_county"]), ["Durham", "Union", "Wake"])


class FakeIAR(http.server.BaseHTTPRequestHandler):
    reply = "Tax foreclosure plus unresolved heirship suggests a motivated estate. Verify probate."

    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert body["privacy"] is True
        out = json.dumps({"output": FakeIAR.reply, "model": "llama3.2:3b",
                          "backend": "ollama-llama3.2"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def log_message(self, *a):
        pass


class WorkersIntegration(LegionCase):
    def stack(self):
        registry, catalog = WorkerRegistry(), WorkflowCatalog()
        register_legion(registry, catalog)
        ledger = ExecutionLedger(os.path.join(self.root, "ledger.jsonl"))
        queue = JobQueue(ledger, registry)
        return ledger, catalog, queue, WorkflowRunner(queue, registry, catalog)

    @staticmethod
    def step_errors(ledger, job):
        return [r.payload for r in ledger.receipts(job) if r.receipt_type == "step.failed"]

    def params(self, staged, profile="scale-probe", **extra):
        return {"mission": "court", "profile": profile,
                "unit_manifest_sha": staged["unit_manifest_sha"],
                "unit_count": staged["unit_count"], "legion_root": self.root, **extra}

    def test_legion_mission_job_seals_one_output_with_few_ledger_receipts(self):
        staged = stage_units(synthetic(3_000), self.root)
        ledger, catalog, queue, runner = self.stack()
        job = queue.enqueue(catalog.get("legion-mission"), self.params(staged),
                            requested_by="founder")
        state = runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        sealed = state.step_results["seal_final_output"]
        self.assertEqual(sealed["counts"]["agents_succeeded"], 3_000)
        self.assertFalse(sealed["published"])
        self.assertFalse(sealed["contact_authorized"])
        self.assertEqual(sealed["cognition"], "SKIPPED")
        self.assertTrue(os.path.exists(sealed["final_output"]["md"]))
        self.assertLess(ledger.verify()["receipts"], 20)
        self.assertEqual(queue.enqueue(catalog.get("legion-mission"), self.params(staged),
                                       requested_by="founder"), job)

    def test_all_legion_capabilities_are_read_only_and_internal(self):
        registry, catalog = WorkerRegistry(), WorkflowCatalog()
        register_legion(registry, catalog)
        caps = registry.declarations()["legion-supervisor-v1"]["capabilities"]
        self.assertTrue(all(c["read_only"] and not c["external"] for c in caps))
        self.assertFalse(registry.external_authorized)

    def test_staged_manifest_tamper_is_refused(self):
        staged = stage_units(synthetic(50), self.root)
        with open(staged["path"], "a") as fh:
            fh.write(json.dumps({"unit_id": "intruder"}) + "\n")
        ledger, catalog, queue, runner = self.stack()
        job = queue.enqueue(catalog.get("legion-mission"), self.params(staged),
                            requested_by="founder")
        state = runner.execute(job)
        self.assertEqual(state.status, JobStatus.FAILED)
        self.assertEqual(self.step_errors(ledger, job)[-1]["error_code"], "LEGION_SPEC_DRIFT")

    def _serve(self):
        server = http.server.HTTPServer(("127.0.0.1", 0), FakeIAR)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}/route"

    def test_cognition_pass_is_loopback_and_validated(self):
        staged = stage_units(OpportunityAnalyst.units(None), self.root)
        ledger, catalog, queue, runner = self.stack()
        job = queue.enqueue(catalog.get("legion-mission"), self.params(
            staged, "opportunity-analyst", cognition_top_k=2, iar_url=self._serve()),
            requested_by="founder")
        state = runner.execute(job)
        self.assertEqual(state.status, JobStatus.SUCCEEDED)
        cognition = state.step_results["cognition_pass"]
        self.assertEqual(cognition["advisory"], 2)
        with open(state.step_results["seal_final_output"]["final_output"]["md"]) as fh:
            self.assertIn("Advisory notes", fh.read())

    def test_cognition_withholds_banned_output(self):
        self.addCleanup(setattr, FakeIAR, "reply", FakeIAR.reply)
        FakeIAR.reply = "Call them at 555-0100 about their equity of $200,000."
        staged = stage_units(OpportunityAnalyst.units(None), self.root)
        ledger, catalog, queue, runner = self.stack()
        job = queue.enqueue(catalog.get("legion-mission"), self.params(
            staged, "opportunity-analyst", cognition_top_k=1, iar_url=self._serve()),
            requested_by="founder")
        state = runner.execute(job)
        self.assertEqual(state.step_results["cognition_pass"]["notes"][0]["status"], "WITHHELD")

    def test_cognition_refuses_non_loopback(self):
        staged = stage_units(synthetic(5), self.root)
        ledger, catalog, queue, runner = self.stack()
        job = queue.enqueue(catalog.get("legion-mission"), self.params(
            staged, cognition_top_k=1, iar_url="https://example.com/route"),
            requested_by="founder")
        state = runner.execute(job)
        self.assertEqual(state.status, JobStatus.FAILED)
        error = self.step_errors(ledger, job)[-1]
        self.assertEqual(error["error_code"], "LEGION_REFUSED")
        self.assertIn("loopback", error["error"])


if __name__ == "__main__":
    unittest.main()
