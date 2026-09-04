#!/usr/bin/env python3
"""Dispatch one real authority-bound FR mission through the Workers seam."""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from neuruh_workers.founder_runtime import (
    DispatchBridge,
    map_founder_runtime_dispatch,
)
from neuruh_workers.ledger import ExecutionLedger
from neuruh_workers.organ_consumers import pull_authoritative_mission
from neuruh_workers.queue import JobQueue
from neuruh_workers.runner import WorkflowRunner
from neuruh_workers.workflows import build_catalog, build_registry


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", default="jgi-demo-ledger.jsonl")
    parser.add_argument("--mission-id", required=True)
    args = parser.parse_args()

    ledger = ExecutionLedger(args.ledger)
    registry = build_registry()
    catalog = build_catalog()
    queue = JobQueue(ledger, registry)
    runner = WorkflowRunner(queue, registry, catalog)
    token = os.environ.get("FR_API_OPERATOR_TOKEN")
    if not token:
        parser.error("FR_API_OPERATOR_TOKEN is required")
    pulled = pull_authoritative_mission(args.mission_id, token=token)
    if not pulled.get("ok"):
        print(json.dumps(pulled, indent=2, sort_keys=True), file=sys.stderr)
        return 2
    ticket, candidate = map_founder_runtime_dispatch(
        pulled["envelope"], catalog=catalog)
    scratch = tempfile.TemporaryDirectory()
    runtime = DispatchBridge(
        queue,
        catalog,
        runner,
        receipts_dir=str(Path(scratch.name) / "receipts"),
        switchboard_root=str(Path(scratch.name) / "switchboard"),
        source_root=str(REPO_ROOT),
        fr_token=token,
    )
    result = runtime.dispatch(ticket, [candidate], auto_execute_read_only=True)
    print(json.dumps(result.as_dict(), indent=2, sort_keys=True, default=str))
    print(json.dumps({"ledger": ledger.verify()}, indent=2, sort_keys=True))
    scratch.cleanup()
    return 0 if result.status.value in {"SUCCEEDED", "ALREADY_SUCCEEDED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
