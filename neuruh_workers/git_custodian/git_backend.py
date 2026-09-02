"""Git backends: recorded (tests) and real subprocess (no force)."""
from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field, replace
from typing import Any, Sequence

from .authority import is_protected_branch
from .errors import GitCustodianRefused
from .redact import identity_key, redact_text


@dataclass(frozen=True)
class GitIdentity:
    worktree: str
    git_dir: str
    head: str
    branch: str | None
    detached: bool
    tracked_dirty: tuple[str, ...]
    untracked: tuple[str, ...]
    remote_identity: str
    remote_branch_head: str | None
    remote_base_head: str | None
    ahead: int
    behind: int
    commits_to_push: int


def _force_args(args: Sequence[str]) -> list[str]:
    bad = []
    for arg in args:
        if arg in ("--force", "--force-with-lease", "-f"):
            bad.append(arg)
        elif arg.startswith("--force") or arg.startswith("--force-with-lease"):
            bad.append(arg)
    return bad


class RecordedGitBackend:
    """In-memory git identity. Never talks to the network or axon."""
    backend_name = "recorded.git"

    def __init__(self, identity: GitIdentity):
        self.identity = identity
        self.push_calls: list[dict[str, str]] = []
        self.command_log: list[list[str]] = []
        self.force_used = False
        self.auto_advance_remote = False

    def inspect(self, worktree: str, remote_name: str, branch: str,
                base_branch: str) -> GitIdentity:
        return self.identity

    def fetch(self, worktree: str, remote_name: str) -> None:
        return None

    def remote_head(self, worktree: str, remote_name: str, branch: str) -> str | None:
        return self.identity.remote_branch_head

    def push_feature_branch(self, worktree: str, remote: str, branch: str,
                            expected_sha: str) -> dict[str, Any]:
        if is_protected_branch(branch):
            raise GitCustodianRefused(
                "refusing protected branch push", {"branch": branch},
                reason="PROTECTED_BRANCH")
        before = self.identity.remote_branch_head
        self.push_calls.append({
            "remote": remote, "branch": branch, "sha": expected_sha,
        })
        if before and before != expected_sha and not self.auto_advance_remote:
            raise GitCustodianRefused(
                "remote branch has diverged; force is impossible",
                {"remote_before": before, "expected": expected_sha},
                reason="STOP_REMOTE_DIVERGENCE")
        if self.auto_advance_remote or before in (None, expected_sha):
            self.identity = replace(
                self.identity,
                remote_branch_head=expected_sha,
                ahead=0, behind=0, commits_to_push=0,
            )
        after = self.identity.remote_branch_head
        if after != expected_sha:
            raise GitCustodianRefused(
                "push proof failed: remote HEAD does not match expected SHA",
                {"remote_after": after, "expected": expected_sha},
                reason="PUSH_PROOF_FAILED")
        return {
            "already_present": before == expected_sha,
            "remote_before": before,
            "remote_after": after,
        }

    def create_tag(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused(
            "tag writes are structurally forbidden", reason="TAG_WRITE")

    def deploy(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused(
            "production deployment is structurally forbidden",
            reason="PRODUCTION_DEPLOY")


class RealGitBackend:
    """Local git via argv. Force flags are a hard refusal."""
    backend_name = "subprocess.git"

    def __init__(self) -> None:
        self.command_log: list[list[str]] = []
        self.push_calls: list[dict[str, str]] = []
        self.force_used = False

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        return env

    def _run(self, worktree: str, args: Sequence[str], *,
             timeout: int = 60) -> subprocess.CompletedProcess:
        forbidden = _force_args(args)
        if forbidden:
            self.force_used = True
            raise GitCustodianRefused(
                "force flags are structurally impossible",
                {"args": list(forbidden)}, reason="FORCE_PUSH")
        argv = ["git", *args]
        self.command_log.append(list(argv))
        return subprocess.run(
            argv, cwd=worktree, env=self._env(),
            capture_output=True, text=True, timeout=timeout, check=False,
        )

    def inspect(self, worktree: str, remote_name: str, branch: str,
                base_branch: str) -> GitIdentity:
        if not os.path.isdir(worktree):
            raise GitCustodianRefused(
                "worktree does not exist", {"worktree": worktree},
                reason="WORKTREE_MISSING")
        real = os.path.realpath(worktree)
        top = self._run(real, ["rev-parse", "--show-toplevel"])
        if top.returncode != 0:
            raise GitCustodianRefused(
                "path is not a git worktree",
                {"stderr": redact_text(top.stderr)}, reason="GIT_IDENTITY")
        toplevel = os.path.realpath(top.stdout.strip())
        if toplevel != real:
            raise GitCustodianRefused(
                "worktree realpath does not match git toplevel",
                reason="GIT_IDENTITY")
        git_dir = self._require(real, ["rev-parse", "--git-dir"]).strip()
        if not os.path.isabs(git_dir):
            git_dir = os.path.realpath(os.path.join(real, git_dir))
        head = self._require(real, ["rev-parse", "HEAD"]).strip().lower()
        sym = self._run(real, ["symbolic-ref", "-q", "HEAD"])
        detached = sym.returncode != 0
        current = None if detached else sym.stdout.strip()
        if current and current.startswith("refs/heads/"):
            current = current[len("refs/heads/"):]
        porcelain = self._require(real, ["status", "--porcelain=v1", "-uall"])
        tracked: list[str] = []
        untracked: list[str] = []
        for line in porcelain.splitlines():
            if not line:
                continue
            code, path = line[:2], line[3:]
            if code == "??":
                untracked.append(path)
            elif code != "  ":
                tracked.append(path)
        url = self._run(real, ["remote", "get-url", remote_name])
        if url.returncode != 0:
            raise GitCustodianRefused(
                "remote name is not configured in this worktree",
                {"remote": remote_name}, reason="GIT_IDENTITY")
        remote_identity = identity_key(url.stdout.strip())
        self.fetch(real, remote_name)
        remote_branch_head = self.remote_head(real, remote_name, branch)
        remote_base_head = self.remote_head(real, remote_name, base_branch)
        ahead, behind = 0, 0
        commits_to_push = 0
        if remote_branch_head == head:
            ahead, behind, commits_to_push = 0, 0, 0
        elif remote_branch_head is None:
            if remote_base_head:
                commits_to_push = self._count(
                    real, f"{remote_base_head}..{head}")
            else:
                commits_to_push = 1
            ahead = commits_to_push
        else:
            if self._is_ancestor(real, remote_branch_head, head):
                commits_to_push = self._count(
                    real, f"{remote_branch_head}..{head}")
                ahead = commits_to_push
            elif self._is_ancestor(real, head, remote_branch_head):
                behind = self._count(real, f"{head}..{remote_branch_head}")
            else:
                # True divergence: still report counts if possible.
                behind = 1
                ahead = 1
                commits_to_push = 1
        return GitIdentity(
            worktree=real, git_dir=git_dir, head=head, branch=current,
            detached=detached,
            tracked_dirty=tuple(tracked), untracked=tuple(untracked),
            remote_identity=remote_identity,
            remote_branch_head=remote_branch_head,
            remote_base_head=remote_base_head,
            ahead=ahead, behind=behind, commits_to_push=commits_to_push,
        )

    def fetch(self, worktree: str, remote_name: str) -> None:
        result = self._run(worktree, ["fetch", "--prune", remote_name],
                           timeout=120)
        if result.returncode != 0:
            raise GitCustodianRefused(
                "git fetch failed",
                {"remote": remote_name, "stderr": redact_text(result.stderr)},
                reason="FETCH_FAILED")

    def remote_head(self, worktree: str, remote_name: str,
                    branch: str) -> str | None:
        ref = f"refs/remotes/{remote_name}/{branch}"
        result = self._run(worktree, ["rev-parse", "--verify", ref])
        if result.returncode == 0:
            return result.stdout.strip().lower()
        # Bare remotes / just-pushed refs: ls-remote without printing URL creds
        ls = self._run(worktree, [
            "ls-remote", "--heads", remote_name, f"refs/heads/{branch}"])
        if ls.returncode != 0 or not ls.stdout.strip():
            return None
        return ls.stdout.split()[0].strip().lower()

    def push_feature_branch(self, worktree: str, remote: str, branch: str,
                            expected_sha: str) -> dict[str, Any]:
        if is_protected_branch(branch):
            raise GitCustodianRefused(
                "refusing protected branch push", {"branch": branch},
                reason="PROTECTED_BRANCH")
        before = self.remote_head(worktree, remote, branch)
        if before == expected_sha:
            return {"already_present": True, "remote_before": before,
                    "remote_after": before}
        if before and not self._is_ancestor(worktree, before, expected_sha):
            raise GitCustodianRefused(
                "remote branch is not a fast-forward of local HEAD; force is impossible",
                {"remote_before": before, "expected": expected_sha},
                reason="STOP_REMOTE_DIVERGENCE")
        self.push_calls.append({
            "remote": remote, "branch": branch, "sha": expected_sha,
        })
        result = self._run(
            worktree, ["push", remote, f"{expected_sha}:refs/heads/{branch}"],
            timeout=120)
        if result.returncode != 0:
            err = redact_text(result.stderr or result.stdout)
            reason = "STOP_REMOTE_DIVERGENCE" if (
                "non-fast-forward" in err.lower()
                or "fetch first" in err.lower()
            ) else "PUSH_FAILED"
            raise GitCustodianRefused(
                "git push refused", {"stderr": err}, reason=reason)
        self.fetch(worktree, remote)
        after = self.remote_head(worktree, remote, branch)
        if after != expected_sha:
            raise GitCustodianRefused(
                "push proof failed: REMOTE_BRANCH_HEAD != EXPECTED_LOCAL_HEAD",
                {"remote_after": after, "expected": expected_sha},
                reason="PUSH_PROOF_FAILED")
        return {"already_present": False, "remote_before": before,
                "remote_after": after}

    def create_tag(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused(
            "tag writes are structurally forbidden", reason="TAG_WRITE")

    def deploy(self, *args: Any, **kwargs: Any) -> None:
        raise GitCustodianRefused(
            "production deployment is structurally forbidden",
            reason="PRODUCTION_DEPLOY")

    def _require(self, worktree: str, args: Sequence[str]) -> str:
        result = self._run(worktree, args)
        if result.returncode != 0:
            raise GitCustodianRefused(
                f"git {' '.join(args)} failed",
                {"stderr": redact_text(result.stderr)}, reason="GIT_IDENTITY")
        return result.stdout

    def _is_ancestor(self, worktree: str, maybe_ancestor: str,
                     maybe_descendant: str) -> bool:
        result = self._run(worktree, [
            "merge-base", "--is-ancestor", maybe_ancestor, maybe_descendant])
        return result.returncode == 0

    def _count(self, worktree: str, rev_range: str) -> int:
        result = self._run(worktree, ["rev-list", "--count", rev_range])
        if result.returncode != 0:
            return 0
        try:
            return int(result.stdout.strip() or "0")
        except ValueError:
            return 0
