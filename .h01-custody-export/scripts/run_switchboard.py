#!/usr/bin/env python3
"""Observe local PRIME receipts and tick the Switchboard first seam.

Loopback/file observation only.  No public listener.  Does not execute a
Sentinel model and does not git push.  The one remote read is the canonical
base: a read-only ``git ls-remote`` against the executed worktree's own remote,
so the court request names a base this board observed rather than a gap.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neuruh_workers.switchboard import (  # noqa: E402
    Switchboard, observe_canonical_base_via_git,
)

ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".neuruh", "switchboard")
RECEIPTS = os.path.join(ROOT, "receipts")


def _board(args) -> Switchboard:
    # The production board observes the canonical base itself. Without an
    # observer the request names the canonical_base gap and no PASS can ever be
    # certified against it (switchboard._require_observed_relation).
    return Switchboard(
        args.root, receipts_dir=args.receipts,
        canonical_observer=observe_canonical_base_via_git)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=ROOT)
    parser.add_argument("--receipts", default=RECEIPTS)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("tick")
    sub.add_parser("status")
    sub.add_parser("events")
    p = sub.add_parser("record-court")
    p.add_argument("--passed", action="store_true")
    p.add_argument("--failed", action="store_true")
    p.add_argument("--court-job-id", required=True)
    p.add_argument("--mission-id", required=True)
    p.add_argument("--plan-id", required=True)
    p.add_argument("--task-id", required=True)
    p.add_argument("--correlation-id", required=True)
    p.add_argument("--causation-id", required=True)
    p.add_argument("--source-head", required=True)
    p.add_argument("--authority-ref", required=True)
    p.add_argument("--source-repo", required=True)
    p.add_argument("--receipt-ref", required=True)
    p.add_argument("--evidence-kind", required=True,
                   help="independent_sentinel_court; reconstruction/injected kinds are refused")
    p.add_argument("--evidence-ref", required=True,
                   help="pointer to independent Sentinel court evidence (not the PRIME receipt)")
    p.add_argument("--evidence-source-role", default="SENTINEL")
    p = sub.add_parser("listen")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    board = _board(args)
    if args.command == "tick":
        report = board.tick()
        print(json.dumps({
            "observed": report.observed,
            "events_emitted": report.events_emitted,
            "jobs_created": report.jobs_created,
            "acked": report.acked,
            "deadlettered": report.deadlettered,
            "recovered": report.recovered,
        }, indent=2))
    elif args.command == "status":
        print(json.dumps({
            "events": len(board.events()),
            "deadletters": len(board.deadletters()),
            "sentinel_jobs": board.jobs_for("sentinel-court-artifact"),
            "honey_pot_jobs": board.jobs_for("honey-pot-candidate-push"),
            "remediation_jobs": board.jobs_for("court-fail-remediation"),
            "bind_host": board.bind_host,
            "sentinel_model_execution": "BLOCKED",
            "git_push": "BLOCKED",
        }, indent=2))
    elif args.command == "events":
        print(json.dumps(board.events(), indent=2))
    elif args.command == "record-court":
        if args.passed == args.failed:
            print("exactly one of --passed or --failed is required", file=sys.stderr)
            return 2
        envelope = board.record_court_outcome(
            passed=bool(args.passed),
            court_job_id=args.court_job_id,
            mission_id=args.mission_id,
            plan_id=args.plan_id,
            task_id=args.task_id,
            correlation_id=args.correlation_id,
            causation_id=args.causation_id,
            source_head=args.source_head,
            authority_ref=args.authority_ref,
            source_repo=args.source_repo,
            receipt_ref=args.receipt_ref,
            evidence={
                "kind": args.evidence_kind,
                "ref": args.evidence_ref,
                "source_role": args.evidence_source_role,
                "independent": True,
            },
        )
        report = board.tick()
        print(json.dumps({"ingested": envelope["event_id"],
                          "event_type": envelope["event_type"],
                          "jobs_created": report.jobs_created}, indent=2))
    elif args.command == "listen":
        board.listen(args.host, args.port)
    return 0


if __name__ == "__main__":
    sys.exit(main())
