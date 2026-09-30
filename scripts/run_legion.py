#!/usr/bin/env python3
"""Legion CLI: stage work units, launch a governed legion, watch it, verify it.

  # stage live Lead Recipe OS opportunities (token from env, never printed)
  LRO_CORE_TOKEN=... python3 scripts/run_legion.py stage-lro --runs RUN_A,RUN_B
  # stage synthetic units for a scale court
  python3 scripts/run_legion.py stage-synthetic --agents 10000 --fault-every 997
  # launch as a legion-mission@1.0.0 job on the legion Workers ledger
  python3 scripts/run_legion.py launch --units <sha> --profile opportunity-analyst \
      --mission lro-nc5 --requested-by founder [--max-parallel 32] [--cognition-top-k 5]
  python3 scripts/run_legion.py status <legion_id>
  python3 scripts/run_legion.py verify <legion_id>
  touch ~/.neuruh/legion/<legion_id>/HALT      # kill switch: drain + stop leasing

Everything is read-only.  No contact, no spend, no writes outside the legion root.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neuruh_workers import ExecutionLedger, JobQueue, WorkflowRunner  # noqa: E402
from neuruh_workers.definitions import WorkflowCatalog  # noqa: E402
from neuruh_workers.legion import (  # noqa: E402
    DEFAULT_LEGION_ROOT, PROFILES, compact_lro_opportunity, register_legion,
    stage_units, verify_journal,
)
from neuruh_workers.registry import WorkerRegistry  # noqa: E402

DEFAULT_LEDGER = os.path.expanduser("~/.neuruh/workers/legion/execution-ledger.jsonl")


def _get(url: str, token: str, timeout: float = 300.0) -> dict:
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 loopback
        return json.loads(resp.read())


def cmd_stage_lro(a) -> int:
    token = os.environ.get("LRO_CORE_TOKEN")
    if not token:
        print("LRO_CORE_TOKEN is not set", file=sys.stderr)
        return 2
    if not a.base.startswith(("http://127.0.0.1:", "http://localhost:")):
        print("stage-lro reads the loopback core only", file=sys.stderr)
        return 2
    units, per_run = {}, {}
    for run_id in [r for r in a.runs.split(",") if r]:
        offset, got = 0, 0
        while True:
            page = _get(f"{a.base}/v1/core/runs/{run_id}/opportunities"
                        f"?limit={a.page}&offset={offset}", token)["rows"]
            for opp in page:
                unit = compact_lro_opportunity({**opp, "run_id": run_id})
                units.setdefault(unit["unit_id"], unit)
            got += len(page)
            offset += a.page
            if len(page) < a.page or (a.limit and len(units) >= a.limit):
                break
        per_run[run_id] = got
        if a.limit and len(units) >= a.limit:
            break
    staged = stage_units(list(units.values())[: a.limit or None], a.root)
    print(json.dumps({**staged, "per_run": per_run,
                      "duplicates_collapsed": sum(per_run.values()) - len(units)}, indent=1))
    return 0


def cmd_stage_synthetic(a) -> int:
    units = []
    for i in range(a.agents):
        unit = {"unit_id": f"u{i:06d}", "work": a.work}
        if a.fault_every and i % a.fault_every == a.fault_every - 1:
            unit["fault"] = ("raise_once", "raise_always", "hang_once")[(i // a.fault_every) % 3]
            unit["hang_s"] = a.hang_s
        units.append(unit)
    print(json.dumps(stage_units(units, a.root), indent=1))
    return 0


def _stack(ledger_path: str):
    registry, catalog = WorkerRegistry(), WorkflowCatalog()
    register_legion(registry, catalog)
    ledger = ExecutionLedger(ledger_path)
    queue = JobQueue(ledger, registry)
    return catalog, queue, WorkflowRunner(queue, registry, catalog, sleep_fn=__import__("time").sleep)


def cmd_launch(a) -> int:
    if a.profile not in PROFILES:
        print(f"unknown profile {a.profile}; have {sorted(PROFILES)}", file=sys.stderr)
        return 2
    count_path = os.path.join(a.root, "units", f"{a.units}.jsonl")
    with open(count_path, encoding="utf-8") as fh:
        count = sum(1 for line in fh if line.strip())
    params = {"mission": a.mission, "profile": a.profile,
              "profile_version": PROFILES[a.profile].version, "unit_manifest_sha": a.units,
              "unit_count": count, "max_agents": a.max_agents, "max_parallel": a.max_parallel,
              "squad_size": a.squad_size, "cohort_size": a.cohort_size,
              "agent_timeout_s": a.agent_timeout_s, "max_attempts": a.max_attempts,
              "cognition_top_k": a.cognition_top_k, "legion_root": a.root}
    catalog, queue, runner = _stack(a.ledger)
    job_id = queue.enqueue(catalog.get("legion-mission"), params,
                           requested_by=a.requested_by, dedupe_key=a.dedupe_key)
    state = queue.load(job_id)
    if not state.terminal:
        state = runner.execute(job_id)
    out = {"job_id": job_id, "status": state.status.value, "ledger": a.ledger,
           "failure_reason": state.failure_reason}
    if "seal_final_output" in state.step_results:
        out.update(state.step_results["seal_final_output"])
    if "plan_legion" in state.step_results:
        out["legion_id"] = state.step_results["plan_legion"]["legion_id"]
    print(json.dumps(out, indent=1, default=str))
    return 0 if state.status.value == "SUCCEEDED" else 1


def cmd_status(a) -> int:
    with open(os.path.join(a.root, a.legion_id, "status.json"), encoding="utf-8") as fh:
        print(json.dumps(json.load(fh), indent=1))
    return 0


def cmd_verify(a) -> int:
    path = os.path.join(a.root, a.legion_id, "journal.jsonl")
    print(json.dumps(verify_journal(path, a.legion_id), indent=1))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", default=DEFAULT_LEGION_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("stage-lro")
    p.add_argument("--runs", required=True)
    p.add_argument("--base", default="http://127.0.0.1:8480")
    p.add_argument("--page", type=int, default=200)
    p.add_argument("--limit", type=int, default=0)
    p.set_defaults(fn=cmd_stage_lro)
    p = sub.add_parser("stage-synthetic")
    p.add_argument("--agents", type=int, required=True)
    p.add_argument("--work", type=int, default=200)
    p.add_argument("--fault-every", type=int, default=0)
    p.add_argument("--hang-s", type=float, default=2.0)
    p.set_defaults(fn=cmd_stage_synthetic)
    p = sub.add_parser("launch")
    p.add_argument("--units", required=True)
    p.add_argument("--profile", required=True)
    p.add_argument("--mission", required=True)
    p.add_argument("--requested-by", required=True)
    p.add_argument("--ledger", default=DEFAULT_LEDGER)
    p.add_argument("--dedupe-key", default=None)
    p.add_argument("--max-agents", type=int, default=10_000)
    p.add_argument("--max-parallel", type=int, default=32)
    p.add_argument("--squad-size", type=int, default=50)
    p.add_argument("--cohort-size", type=int, default=20)
    p.add_argument("--agent-timeout-s", type=float, default=30.0)
    p.add_argument("--max-attempts", type=int, default=2)
    p.add_argument("--cognition-top-k", type=int, default=0)
    p.set_defaults(fn=cmd_launch)
    for name, fn in (("status", cmd_status), ("verify", cmd_verify)):
        p = sub.add_parser(name)
        p.add_argument("legion_id")
        p.set_defaults(fn=fn)
    a = parser.parse_args()
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
