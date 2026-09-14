#!/usr/bin/env python3
"""Observe local PRIME receipts and tick the Switchboard first seam.

Loopback/file observation only.  No public listener.  Does not execute a
Sentinel model.  The one remote read is the canonical base: a read-only
``git ls-remote`` against the executed worktree's own remote, so the court
request names a base this board observed rather than a gap.

Composition root (W1): the EXISTING Honey Pot organs are bound here and only
here, each behind an explicit env switch that defaults OFF, so a tick at a pin
without the switches behaves exactly as before (the board itself never pushes
or merges):

  NEURUH_SWITCHBOARD_GIT_CUSTODIAN=1
      bind ``neuruh_workers.git_custodian.GitCustodian`` as the
      ``candidate.push.requested`` consumer (feature-branch push + draft PR
      after a structured court; direct-main push / force / auto-merge stay
      structurally absent in the worker itself).
  NEURUH_SWITCHBOARD_PROMOTION_EXECUTOR=1
      bind ``neuruh_workers.git_custodian.PromotionExecutor`` as the
      ``promotion.requested`` consumer (merge commit only on a fresh, bound
      Governance ALLOW over a sealed Sentinel PASS and a custody record).
  NEURUH_GOVERNANCE_AUTHORIZE_URL   (default http://127.0.0.1:8848/v2/authorize)
      loopback Governance the executor asks; never a public host.
  NEURUH_SWITCHBOARD_RECORD_DIRS    (default: <root>/courts)
      colon-separated local directories in which bare record refs resolve.

Record refs (court / sentinel / custody) resolve to JSON on local disk only
(absolute path, ``file://`` path, or a name under the record dirs).  The
authority store presents the one founder policy each organ may act under
(``FOUNDER_CANDIDATE_V1`` / ``FOUNDER_PROMOTION_V1``) -- the same binding
``scripts/run_promotion.py`` already makes -- and nothing else.  No secret is
read here.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neuruh_workers.switchboard import (  # noqa: E402
    missions_on_ledger,
    AdjudicatorUnavailable, Switchboard, observe_canonical_base_via_git,
)
from neuruh_workers.adjudicator_http import (  # noqa: E402
    court_url, sentinel_court_adjudicator,
)
from neuruh_workers.git_custodian import (  # noqa: E402
    FOUNDER_CANDIDATE_V1, FOUNDER_PROMOTION_V1, GitCustodian,
    LoopbackGovernanceAuthorize, PromotionExecutor, RealGitBackend,
    RecordingSwitchboard,
)
from neuruh_workers.git_custodian.promotion import GOVERNANCE_AUTHORIZE_URL  # noqa: E402
from neuruh_workers.ledger import ExecutionLedger  # noqa: E402

ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".neuruh", "switchboard")
RECEIPTS = os.path.join(ROOT, "receipts")

GIT_CUSTODIAN_ENV = "NEURUH_SWITCHBOARD_GIT_CUSTODIAN"
PROMOTION_EXECUTOR_ENV = "NEURUH_SWITCHBOARD_PROMOTION_EXECUTOR"
GOVERNANCE_URL_ENV = "NEURUH_GOVERNANCE_AUTHORIZE_URL"
RECORD_DIRS_ENV = "NEURUH_SWITCHBOARD_RECORD_DIRS"
HONEY_POT_LEDGER = "honey-pot-ledger.jsonl"
_ON = {"1", "true", "yes", "on"}


def _enabled(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in _ON


class FileRecordStore:
    """CourtStore over local JSON files; never a network read.

    A ref is an absolute path, a ``file://`` absolute path, or a bare name that
    is looked up under each record dir (``<name>`` / ``<name>.json``).  Anything
    that does not resolve returns None and the organ refuses on the evidence
    (COURT_MISSING) -- the store never invents a record.  Hash binding
    (``court_receipt_hash == content_sha(record)``) stays with the verifier.
    """

    def __init__(self, dirs: list[str]):
        self.dirs = [os.path.abspath(d) for d in dirs if d]

    def _path(self, ref: str) -> str | None:
        if not isinstance(ref, str) or not ref.strip():
            return None
        if ref.startswith("file://"):
            ref = ref[len("file://"):]
        if os.path.isabs(ref):
            return ref if os.path.isfile(ref) else None
        if "/" in ref or ref.startswith("."):
            return None
        for d in self.dirs:
            for cand in (os.path.join(d, ref), os.path.join(d, ref + ".json")):
                if os.path.isfile(cand):
                    return cand
        return None

    def get(self, ref: str):
        path = self._path(ref)
        if path is None:
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None


class FounderPolicyStore:
    """AuthorityStore that presents exactly one founder policy for any ref.

    This is the binding ``scripts/run_promotion.py`` already makes
    (``MemoryAuthorityStore({authority_ref: FOUNDER_PROMOTION_V1})``): the
    organ's own verifier still checks the policy's allowed/forbidden set, and a
    blank ref resolves to nothing.
    """

    def __init__(self, policy: dict):
        self.policy = dict(policy)

    def get(self, ref: str):
        if not isinstance(ref, str) or not ref.strip():
            return None
        return dict(self.policy)


def record_dirs(root: str) -> list[str]:
    extra = [d for d in (os.environ.get(RECORD_DIRS_ENV) or "").split(":") if d]
    return [os.path.join(os.path.abspath(root), "courts"), *extra]


def _source_sha(root: str) -> str:
    """The workers head this composition executes from: the supervision
    attestation when invoked under sops, else the checkout's own HEAD."""
    attested = (os.environ.get("NSV_INVOKED_SHA") or "").strip().lower()
    if len(attested) == 40:
        return attested
    import subprocess
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"],
                         capture_output=True, text=True)
    return (out.stdout or "").strip().lower() if out.returncode == 0 else ""


def _gh_adapters():
    from neuruh_workers.git_custodian.gh_backend import GhCIAdapter, GhPRAdapter
    return GhPRAdapter(), GhCIAdapter()


def compose_git_custodian(root: str, *, pr=None, ci=None, ledger=None) -> GitCustodian:
    """The existing Git Custodian, bound to local records and the founder
    candidate policy.  ``pr``/``ci`` default to the real gh adapters."""
    if pr is None or ci is None:
        gh_pr, gh_ci = _gh_adapters()
        pr = pr or gh_pr
        ci = ci or gh_ci
    if ledger is None:
        ledger = ExecutionLedger(os.path.join(os.path.abspath(root), HONEY_POT_LEDGER))
    return GitCustodian(
        git=RealGitBackend(), pr=pr, ci=ci,
        court_store=FileRecordStore(record_dirs(root)),
        authority_store=FounderPolicyStore(FOUNDER_CANDIDATE_V1),
        switchboard=RecordingSwitchboard(), ledger=ledger)


def compose_promotion_executor(root: str, *, pr=None, ci=None, governance=None,
                               ledger=None, source_sha: str | None = None) -> PromotionExecutor:
    """The existing Promotion Executor, bound to local records, the founder
    promotion policy and the loopback Governance authorize route."""
    if pr is None or ci is None:
        gh_pr, gh_ci = _gh_adapters()
        pr = pr or gh_pr
        ci = ci or gh_ci
    if ledger is None:
        ledger = ExecutionLedger(os.path.join(os.path.abspath(root), HONEY_POT_LEDGER))
    if governance is None:
        governance = LoopbackGovernanceAuthorize(
            os.environ.get(GOVERNANCE_URL_ENV) or GOVERNANCE_AUTHORIZE_URL)
    store = FileRecordStore(record_dirs(root))
    return PromotionExecutor(
        git=RealGitBackend(), pr=pr, ci=ci,
        court_store=store, custody_store=store,
        authority_store=FounderPolicyStore(FOUNDER_PROMOTION_V1),
        governance=governance, switchboard=RecordingSwitchboard(), ledger=ledger,
        source_sha=_source_sha(root) if source_sha is None else source_sha)


def composition(root: str) -> dict:
    """Which optional organs the env binds into this board (default: none)."""
    return {
        "git_custodian": compose_git_custodian(root) if _enabled(GIT_CUSTODIAN_ENV) else None,
        "promotion_executor": (compose_promotion_executor(root)
                               if _enabled(PROMOTION_EXECUTOR_ENV) else None),
    }


def _board(args) -> Switchboard:
    # The production board observes the canonical base itself. Without an
    # observer the request names the canonical_base gap and no PASS can ever be
    # certified against it (switchboard._require_observed_relation).
    #
    # It also carries an adjudicator: a loopback HTTP client to the independent
    # Sentinel court organ, running in its own process. Without one
    # court.requested is unroutable and the board deadletters the very court it
    # asked for -- the T02 seam. Workers never imports the organ; the process
    # boundary is what keeps "independent" true at runtime.
    #
    # W1: the Honey Pot custodian and the promotion executor are bound only
    # when the env says so (module docstring).  Without them the board keeps
    # its first-seam shape and candidate.push.requested / promotion.requested
    # stay unroutable -- the board never pushes or merges anything itself.
    organs = composition(args.root)
    # Scoped to the missions this root's own ledger already carries: the bridge
    # seals receipts and asks for courts here for whatever the Founder Runtime
    # dispatched. Left at DEFAULT_MISSIONS, this board deadlettered every such
    # court as "event mission outside consumer scope" (2026-09-04, 2026-09-14).
    return Switchboard(
        args.root, receipts_dir=args.receipts,
        known_missions=missions_on_ledger(args.root),
        canonical_observer=observe_canonical_base_via_git,
        adjudicator=sentinel_court_adjudicator(),
        git_custodian=organs["git_custodian"],
        promotion_executor=organs["promotion_executor"])


def _court_health(board: Switchboard) -> dict:
    """Is the organ this board depends on actually reachable right now?

    An operational dependency that can only be discovered by watching courts
    stall is not an operational dependency anyone can run. `status` answers it
    directly, and answers UNREACHABLE loudly rather than raising.
    """
    adjudicator = getattr(board, "adjudicator", None)
    probe = getattr(adjudicator, "health", None)
    if probe is None:
        return {"status": "NO_ADJUDICATOR",
                "detail": "court.requested is unroutable; the board will deadletter its own court"}
    try:
        return {"status": "UP", "organ": probe()}
    except AdjudicatorUnavailable as exc:
        return {"status": "UNREACHABLE", "detail": str(exc)}


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
            # An unreachable Sentinel is a blocked court, not a quiet tick.
            "blocked": report.blocked,
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
            "sentinel_court_url": court_url(),
            "sentinel_court": _court_health(board),
            "git_custodian": "BOUND" if board.git_custodian is not None else "NOT_BOUND",
            "promotion_executor": (
                "BOUND" if board.promotion_executor is not None else "NOT_BOUND"),
            "governance_authorize_url": (
                os.environ.get(GOVERNANCE_URL_ENV) or GOVERNANCE_AUTHORIZE_URL),
            "record_dirs": record_dirs(args.root),
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
