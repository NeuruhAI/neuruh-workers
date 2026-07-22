#!/usr/bin/env python3
"""CLI for the Neuruh workflow layer.

Examples:
  python3 scripts/run_workflow.py list
  python3 scripts/run_workflow.py enqueue cockpit-health-snapshot \
      --params '{"components":[{"name":"qdrant","status":"healthy"}]}' \
      --requested-by operator [--dry-run]
  python3 scripts/run_workflow.py run <job-id>
  python3 scripts/run_workflow.py status <job-id>
  python3 scripts/run_workflow.py pause <job-id> / resume <job-id>
  python3 scripts/run_workflow.py approve <job-id> <step-id> \
      --approver founder --nonce <unique> [--deny --reason "..."]
  python3 scripts/run_workflow.py verify-ledger

The ledger lives at .neuruh/ledger.jsonl under the repo (git-ignored).
External actions are structurally disabled; every sample workflow is read-only.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neuruh_workers import ExecutionLedger, JobQueue, WorkflowRunner  # noqa: E402
from neuruh_workers.workflows import build_catalog, build_registry  # noqa: E402

LEDGER_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".neuruh", "ledger.jsonl")


def _stack():
    ledger = ExecutionLedger(LEDGER_PATH)
    registry = build_registry()
    catalog = build_catalog()
    queue = JobQueue(ledger, registry)
    runner = WorkflowRunner(queue, registry, catalog)
    return ledger, registry, catalog, queue, runner


def _print_state(state):
    print(json.dumps({
        "job_id": state.job_id,
        "workflow": state.workflow,
        "status": state.status.value,
        "dry_run": state.dry_run,
        "steps_completed": sorted(state.step_results),
        "pending_approval": state.pending_approval,
        "failure_reason": state.failure_reason,
    }, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list")
    sub.add_parser("verify-ledger")
    sub.add_parser("jobs")

    p = sub.add_parser("enqueue")
    p.add_argument("workflow")
    p.add_argument("--params", default="{}")
    p.add_argument("--requested-by", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--dedupe-key", default=None)

    for name in ("run", "status", "pause", "resume"):
        p = sub.add_parser(name)
        p.add_argument("job_id")

    p = sub.add_parser("approve")
    p.add_argument("job_id")
    p.add_argument("step_id")
    p.add_argument("--approver", required=True)
    p.add_argument("--nonce", required=True)
    p.add_argument("--deny", action="store_true")
    p.add_argument("--reason", default="")

    args = parser.parse_args()
    ledger, registry, catalog, queue, runner = _stack()

    if args.command == "list":
        for name in catalog.names():
            wf = catalog.get(name)
            gates = [s.step_id for s in wf.steps if s.requires_approval]
            print(f"{name} v{wf.version} — {wf.description}"
                  + (f" [approval: {', '.join(gates)}]" if gates else ""))
    elif args.command == "verify-ledger":
        print(json.dumps(ledger.verify(), indent=2))
    elif args.command == "jobs":
        for job_id in queue.job_ids():
            if job_id == "seam-registry":
                continue
            state = queue.load(job_id)
            print(f"{job_id}  {state.status.value:>20}  {state.workflow}")
    elif args.command == "enqueue":
        job_id = queue.enqueue(
            catalog.get(args.workflow), json.loads(args.params),
            requested_by=args.requested_by, dry_run=args.dry_run,
            dedupe_key=args.dedupe_key)
        print(job_id)
    elif args.command == "run":
        _print_state(runner.execute(args.job_id))
    elif args.command == "status":
        _print_state(queue.load(args.job_id))
    elif args.command == "pause":
        _print_state(queue.pause(args.job_id))
    elif args.command == "resume":
        _print_state(queue.resume(args.job_id))
    elif args.command == "approve":
        state = queue.load(args.job_id)
        pending = state.pending_approval or {}
        if args.deny:
            _print_state(runner.deny_approval(
                args.job_id, args.step_id, approver=args.approver,
                nonce=args.nonce, reason=args.reason or "denied via CLI"))
        else:
            _print_state(runner.grant_approval(
                args.job_id, args.step_id, approver=args.approver,
                nonce=args.nonce,
                challenge=pending.get("challenge", ""),
                params_sha=pending.get("params_sha", "")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
