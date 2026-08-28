"""Shared golden facts and local git helpers for Git Custodian courts.

NeuruhAI/neuruh-axon is OBSERVE-ONLY. Tests never talk to it for writes;
T07 uses a recorded/mock adapter seeded with live-read facts.
"""
from __future__ import annotations

import os
import subprocess
from typing import Any

from neuruh_workers.contracts import content_sha

# --- Golden T07 fixture (live-read 2026-08-28, observe only) -----------------

T07_REPO = "NeuruhAI/neuruh-axon"
T07_MISSION = "NEURUH_GROK_SOVEREIGN_BINDING_V4"
T07_STAGE = "T07_LOCAL_GROK_OBSERVER_BIND"
T07_HEAD = "7461eb835b3f979a8243f9f70da634a4e5ab62fc"
T07_BASE_SHA = "0b131308ebfdbd47d013ea2ff600a8e2f001f5f9"
T07_BRANCH = "feat/grok-sovereign-mcp-v4-20260827"
T07_PR = 4
T07_PR_TITLE = (
    "[DO NOT MERGE] feat(axon): governed Streamable HTTP MCP floor + "
    "observer membrane"
)
T07_EXISTING_COMMENTS = 1  # SENTINEL T07 CLOSURE already on the PR

T07_COURT_REF = "court://sentinel/T07_LOCAL_GROK_OBSERVER_BIND"
T07_AUTHORITY_REF = "authority://founder.candidate.v1"

T07_COURT = {
    "schema_version": "nrh.court.receipt.v1",
    "independent_court": "PASS",
    "mission_id": T07_MISSION,
    "stage": T07_STAGE,
    "source_head": T07_HEAD,
    "candidate": {
        "repo": T07_REPO,
        "branch": T07_BRANCH,
        "head": T07_HEAD,
        "pr": T07_PR,
        "base": "main",
        "base_head": T07_BASE_SHA,
    },
}

T07_COURT_HASH = content_sha(T07_COURT)

# A credential-shaped value that must NEVER appear in receipts/logs/ledger.
# Deliberately avoids secret_scan patterns (no gh[pousr]_, no sk-, etc.).
PLANTED_CREDENTIAL = "TESTCRED_abc123xyz789notreal"


def t07_candidate(**overrides: Any) -> dict[str, Any]:
    body = {
        "schema_version": "nrh.git.candidate.v1",
        "mission_id": T07_MISSION,
        "task_id": T07_STAGE,
        "repo": T07_REPO,
        "worktree": "/recorded/neuruh-axon",
        "branch": T07_BRANCH,
        "expected_local_head": T07_HEAD,
        "remote_name": "origin",
        "remote_url_or_identity": "github.com/NeuruhAI/neuruh-axon",
        "expected_remote_base": f"main@{T07_BASE_SHA}",
        "court_receipt_ref": T07_COURT_REF,
        "court_receipt_hash": T07_COURT_HASH,
        "authority_ref": T07_AUTHORITY_REF,
        "commit_scope": "T07 local grok observer bind (3 commits on main)",
        "pr_title": T07_PR_TITLE,
        "pr_body_metadata": {
            "TEST_SUMMARY": "T07 local observer bind proven; CI not configured",
            "KNOWN_RESIDUAL_RISKS": "DNS rebind residual P1 OPEN; no merge authority",
            "RECEIPT_REFERENCE": T07_COURT_REF,
        },
        "idempotency_key": "t07-grok-sovereign-mcp-v4-20260827",
    }
    body.update(overrides)
    return body


def make_court(*, verdict: str = "PASS", **overrides: Any) -> dict[str, Any]:
    evidence = dict(T07_COURT)
    evidence["independent_court"] = verdict
    evidence.update(overrides)
    return evidence


# --- Local git helpers for the bare-remote court -----------------------------

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "Custodian Test",
    "GIT_AUTHOR_EMAIL": "custodian-test@neuruh.local",
    "GIT_COMMITTER_NAME": "Custodian Test",
    "GIT_COMMITTER_EMAIL": "custodian-test@neuruh.local",
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_CONFIG_NOSYSTEM": "1",
}


def git(cwd: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.update(_GIT_ENV)
    result = subprocess.run(
        ["git", "-c", "commit.gpgsign=false",
         "-c", "init.defaultBranch=main", *args],
        cwd=cwd, env=env, capture_output=True, text=True, check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"git {args} failed ({result.returncode}): "
            f"{result.stderr or result.stdout}")
    return result


def write_file(root: str, rel: str, content: str) -> None:
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path) or root, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)


def sha(cwd: str, ref: str = "HEAD") -> str:
    return git(cwd, "rev-parse", ref).stdout.strip()


def init_worktree(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    git(path, "init", "-b", "main")
    git(path, "config", "user.name", "Custodian Test")
    git(path, "config", "user.email", "custodian-test@neuruh.local")
    git(path, "config", "commit.gpgsign", "false")


def make_seeded_pair(tmpdir: str, *, feature_branch: str = "feat/custodian-v1"
                     ) -> dict[str, str]:
    """A local worktree + bare remote, with main pushed and a feature commit."""
    remote = os.path.join(tmpdir, "remote.git")
    work = os.path.join(tmpdir, "work")
    os.makedirs(remote, exist_ok=True)
    git(remote, "init", "--bare", "-b", "main")
    init_worktree(work)
    write_file(work, "README.md", "seed\n")
    git(work, "add", "README.md")
    git(work, "commit", "-m", "seed")
    git(work, "remote", "add", "origin", remote)
    git(work, "push", "-u", "origin", "main")
    base_sha = sha(work)
    git(work, "checkout", "-b", feature_branch)
    write_file(work, "candidate.txt", "payload\n")
    git(work, "add", "candidate.txt")
    git(work, "commit", "-m", "candidate work")
    feature_sha = sha(work)
    return {
        "work": work,
        "remote": remote,
        "base_sha": base_sha,
        "feature_sha": feature_sha,
        "feature_branch": feature_branch,
    }


def local_candidate(pair: dict[str, str], *, court: dict[str, Any],
                    authority_ref: str = T07_AUTHORITY_REF,
                    **overrides: Any) -> dict[str, Any]:
    repo = overrides.pop("repo", "local/custodian-fixture")
    body = {
        "schema_version": "nrh.git.candidate.v1",
        "mission_id": "LOCAL_CUSTODIAN_COURT",
        "task_id": "BARE_REMOTE_PUSH",
        "repo": repo,
        "worktree": pair["work"],
        "branch": pair["feature_branch"],
        "expected_local_head": pair["feature_sha"],
        "remote_name": "origin",
        "remote_url_or_identity": pair["remote"],
        "expected_remote_base": f"main@{pair['base_sha']}",
        "court_receipt_ref": "court://local/bare-remote",
        "court_receipt_hash": content_sha(court),
        "authority_ref": authority_ref,
        "commit_scope": "one feature commit on seeded main",
        "pr_title": "[DO NOT MERGE] local custodian candidate",
        "pr_body_metadata": {
            "TEST_SUMMARY": "bare-remote court",
            "KNOWN_RESIDUAL_RISKS": "none in fixture",
            "RECEIPT_REFERENCE": "court://local/bare-remote",
        },
        "idempotency_key": "local-bare-remote-v1",
    }
    body.update(overrides)
    return body


def local_court(pair: dict[str, str], *, repo: str = "local/custodian-fixture",
                mission_id: str = "LOCAL_CUSTODIAN_COURT",
                stage: str = "BARE_REMOTE_PUSH") -> dict[str, Any]:
    return {
        "schema_version": "nrh.court.receipt.v1",
        "independent_court": "PASS",
        "mission_id": mission_id,
        "stage": stage,
        "source_head": pair["feature_sha"],
        "candidate": {
            "repo": repo,
            "branch": pair["feature_branch"],
            "head": pair["feature_sha"],
            "base": "main",
            "base_head": pair["base_sha"],
        },
    }
