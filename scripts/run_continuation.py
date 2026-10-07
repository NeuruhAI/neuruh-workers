#!/usr/bin/env python3
"""Mission continuation CLI — one durable ledger, no daemon.

Every invocation is a fresh process over the ledger file, which is exactly
the restart the O4 court demands: nothing survives between calls except
the hash-chained receipts.

  python3 scripts/run_continuation.py open   --mission M --params-file p.json
  python3 scripts/run_continuation.py tick   --mission M [--no-run-children]
  python3 scripts/run_continuation.py run-child --mission M --role specialist
  python3 scripts/run_continuation.py dispatch  --mission M --role specialist
  python3 scripts/run_continuation.py export-receipt --mission M --role R --out f.json
  python3 scripts/run_continuation.py ingest-receipt --file f.json
  python3 scripts/run_continuation.py status --mission M
  python3 scripts/run_continuation.py verify-ledger

Default ledger: ~/.neuruh/workers/continuation/execution-ledger.jsonl
(override with --ledger).  Read-only workflows only; nothing is published.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neuruh_workers.continuation import ContinuationRefused, MissionContinuation  # noqa: E402
from neuruh_workers.contracts import WorkflowError  # noqa: E402
from neuruh_workers.ledger import ExecutionLedger  # noqa: E402

DEFAULT_LEDGER = os.path.join(
    os.path.expanduser("~"), ".neuruh", "workers", "continuation",
    "execution-ledger.jsonl")


def _emit(payload) -> None:
    print(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("open")
    p.add_argument("--mission", required=True)
    p.add_argument("--params-file", required=True)
    p.add_argument("--workflow", default="mission-continuation-parent")

    p = sub.add_parser("tick")
    p.add_argument("--mission", required=True)
    p.add_argument("--no-run-children", action="store_true")

    p = sub.add_parser("run-child")
    p.add_argument("--mission", required=True)
    p.add_argument("--role", required=True)

    p = sub.add_parser("dispatch")
    p.add_argument("--mission", required=True)
    p.add_argument("--role", required=True)

    p = sub.add_parser("export-receipt")
    p.add_argument("--mission", required=True)
    p.add_argument("--role", required=True)
    p.add_argument("--out", required=True)

    p = sub.add_parser("ingest-receipt")
    p.add_argument("--file", required=True)

    p = sub.add_parser("status")
    p.add_argument("--mission", required=True)

    sub.add_parser("verify-ledger")

    args = parser.parse_args(argv)
    mc = MissionContinuation(ExecutionLedger(args.ledger))
    try:
        if args.command == "open":
            with open(args.params_file, encoding="utf-8") as fh:
                params = json.load(fh)
            _emit(mc.open_mission(args.mission, params, workflow=args.workflow))
        elif args.command == "tick":
            _emit(mc.tick(args.mission, run_children=not args.no_run_children))
        elif args.command == "run-child":
            child = mc.status(args.mission)["children"].get(args.role)
            if child is None:
                raise ContinuationRefused(f"child {args.role!r} is not dispatched")
            _emit(mc._run_child(child["child_job_id"]))
        elif args.command == "dispatch":
            _emit(mc.dispatch_child(args.mission, args.role))
        elif args.command == "export-receipt":
            receipt = mc.export_child_receipt(args.mission, args.role)
            with open(args.out, "w", encoding="utf-8") as fh:
                json.dump(receipt, fh, sort_keys=True, indent=2, ensure_ascii=False)
            _emit({"written": args.out, "receipt_sha": receipt["receipt_sha"]})
        elif args.command == "ingest-receipt":
            with open(args.file, encoding="utf-8") as fh:
                _emit(mc.ingest_sealed_receipt(json.load(fh)))
        elif args.command == "status":
            _emit(mc.status(args.mission))
        elif args.command == "verify-ledger":
            _emit({"ledger": args.ledger, **mc.ledger.verify(),
                   "jobs": len(mc.queue.job_ids())})
    except WorkflowError as exc:
        _emit({"refused": str(exc), "code": exc.code, "detail": exc.detail})
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
