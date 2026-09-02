#!/usr/bin/env python3
"""Run one safe dispatch-bridge demo against the existing workflow layer.

Consumes an FR APPROVED/QUEUED ticket shape and dispatches it onto JobQueue.
No network, no external action, no production mutation. The demo creates a
local ledger, autonomously executes the existing property-research workflow,
hands receipt.sealed to live SWITCHAROO, then prints the structured result.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from neuruh_workers.founder_runtime import (
    DispatchBridge,
    DispatchTicket,
    IntelligenceLayer,
    WorkCandidate,
)
from neuruh_workers.ledger import ExecutionLedger
from neuruh_workers.organ_consumers import FR_API_VERSION
from neuruh_workers.queue import JobQueue
from neuruh_workers.runner import WorkflowRunner
from neuruh_workers.workflows import build_catalog, build_registry


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", default="jgi-demo-ledger.jsonl")
    parser.add_argument("--address", default="123 Main St, Benton Harbor, MI")
    parser.add_argument("--county", default="Berrien")
    args = parser.parse_args()

    ledger = ExecutionLedger(args.ledger)
    registry = build_registry()
    catalog = build_catalog()
    queue = JobQueue(ledger, registry)
    runner = WorkflowRunner(queue, registry, catalog)
    scratch = tempfile.TemporaryDirectory()
    runtime = DispatchBridge(
        queue,
        catalog,
        runner,
        receipts_dir=str(Path(scratch.name) / "receipts"),
        switchboard_root=str(Path(scratch.name) / "switchboard"),
    )

    ticket = DispatchTicket(
        mission_id="JGI-DEMO-PROPERTY-RESEARCH-001",
        objective="turn a property question into a receipted research plan without a manual handoff",
        fr_state="QUEUED",
        fr_api_version=FR_API_VERSION,
        expected_value_usd=500.0,
        uncertainty=0.2,
        relevance=1.0,
        model_budget_usd=0.0,
        canonical_refs=("workflow:property-research@1.0.0",),
        do_not_rebuild=("WorkflowRunner", "JobQueue", "ExecutionLedger"),
    )
    candidate = WorkCandidate(
        candidate_id="existing-property-research-v1",
        workflow_name="property-research",
        params={"address": args.address, "county": args.county},
        layer=IntelligenceLayer.L0,
        estimated_latency_minutes=1.0,
        evidence_refs=("neuruh-workers:property-research",),
    )
    result = runtime.dispatch(ticket, [candidate], auto_execute_read_only=True)
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True, default=str))
    print(json.dumps({"ledger": ledger.verify()}, indent=2, sort_keys=True))
    scratch.cleanup()
    return 0 if result.status.value in {"SUCCEEDED", "ALREADY_SUCCEEDED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
