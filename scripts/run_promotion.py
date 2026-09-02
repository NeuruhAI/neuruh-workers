#!/usr/bin/env python3
"""Run the PROMOTION EXECUTOR against one nrh.git.promotion.v1 packet.

    python3 scripts/run_promotion.py --promotion packet.json \
        --sentinel-record sealed.json --custody-record custody.json \
        [--governance-url http://127.0.0.1:8848/v2/authorize] [--source-sha <40hex>] \
        [--ledger ./promotion-ledger.jsonl] [--dry-run]

--dry-run validates the packet, the Sentinel record, the custody record and the
authority policy, prints the exact Governance facts that WOULD be presented, and
performs no git act and no Governance call. Without it the executor asks the
live loopback Governance and merges only on a fresh, bound ALLOW.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neuruh_workers.git_custodian import (  # noqa: E402
    FOUNDER_PROMOTION_V1, GitCustodianRefused, LoopbackGovernanceAuthorize,
    MemoryAuthorityStore, MemoryCourtStore, PromotionExecutor, RealGitBackend,
    RecordingSwitchboard, parse_promotion, promotion_facts,
)
from neuruh_workers.git_custodian.gh_backend import GhCIAdapter, GhPRAdapter  # noqa: E402
from neuruh_workers.git_custodian.promotion import (  # noqa: E402
    promotion_nonce, verify_custody_record, verify_promotion_authority,
    verify_sentinel_record,
)
from neuruh_workers.ledger import ExecutionLedger  # noqa: E402


def _load(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _source_sha(explicit: str | None) -> str:
    if explicit:
        return explicit.lower()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = subprocess.run(["git", "-C", root, "rev-parse", "HEAD"], capture_output=True, text=True)
    return (out.stdout or "").strip().lower() if out.returncode == 0 else ""


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--promotion", required=True)
    p.add_argument("--sentinel-record", required=True)
    p.add_argument("--custody-record", required=True)
    p.add_argument("--governance-url", default="http://127.0.0.1:8848/v2/authorize")
    p.add_argument("--source-sha", default=None)
    p.add_argument("--ledger", default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    promotion = _load(args.promotion)
    sentinel = _load(args.sentinel_record)
    custody = _load(args.custody_record)
    source_sha = _source_sha(args.source_sha)
    try:
        parsed = parse_promotion(promotion)
        verify_promotion_authority(FOUNDER_PROMOTION_V1, candidate_branch=parsed["branch"],
                                   base_branch=parsed["base_branch"])
        verify_sentinel_record(sentinel, parsed)
        verify_custody_record(custody, parsed)
    except GitCustodianRefused as exc:
        print(json.dumps({"REFUSED": exc.reason, "detail": exc.detail}, indent=2))
        return 2
    if args.dry_run:
        issued_at = "1970-01-01T00:00:00.000Z"
        facts = promotion_facts(parsed, source_sha=source_sha or "0" * 40,
                                nonce=promotion_nonce(parsed, issued_at), issued_at=issued_at)
        print(json.dumps({"DRY_RUN": True, "gates_0_to_2": "PASS",
                          "would_present": facts, "merge": "NOT_PERFORMED"}, indent=2))
        return 0
    ledger = ExecutionLedger(args.ledger) if args.ledger else None
    executor = PromotionExecutor(
        git=RealGitBackend(), pr=GhPRAdapter(), ci=GhCIAdapter(),
        court_store=MemoryCourtStore({parsed["sentinel_receipt_ref"]: sentinel}),
        custody_store=MemoryCourtStore({parsed["custody_receipt_ref"]: custody}),
        authority_store=MemoryAuthorityStore({parsed["authority_ref"]: FOUNDER_PROMOTION_V1}),
        governance=LoopbackGovernanceAuthorize(args.governance_url),
        switchboard=RecordingSwitchboard(), ledger=ledger, source_sha=source_sha,
    )
    try:
        receipt = executor.run(promotion)
    except GitCustodianRefused as exc:
        print(json.dumps({"REFUSED": exc.reason, "detail": exc.detail,
                          "events": executor.switchboard.events}, indent=2, default=str))
        return 3
    print(json.dumps({"receipt": receipt, "events": executor.switchboard.events}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
