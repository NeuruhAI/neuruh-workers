"""An offline canonical remote for the legacy Switchboard suites.

A board that observed nothing can no longer certify a PASS
(``switchboard._require_observed_relation``), so every suite that ingests one
needs a base its board can actually read. ``OfflineCanonical`` gives a worktree a
bare repository on local disk as its canonical remote, and the observer a test
board may carry. That observer reads the bare repository through the same
read-only git the production observer uses (``observe_canonical_base_via_git``)
-- it is an observation, not an echo of the receipt -- and it refuses every
other worktree, so a tick inside a test can never resolve a URL a receipt names
and never reaches a network host. The live T07 axon checkout on the founder's
machine, whose ``origin`` is GitHub, is the case this guards against.
"""
from __future__ import annotations

import os
import subprocess

from neuruh_workers.switchboard import observe_canonical_base_via_git

GIT = os.environ.get("NEURUH_GIT", "/opt/homebrew/bin/git")
# The host's own git identity, signing and hooks must not decide these courts.
_GIT_ENV = {
    "GIT_AUTHOR_NAME": "Court",
    "GIT_AUTHOR_EMAIL": "court@local",
    "GIT_COMMITTER_NAME": "Court",
    "GIT_COMMITTER_EMAIL": "court@local",
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_CONFIG_NOSYSTEM": "1",
}


def _git(cwd: str, *args: str) -> str:
    env = {**os.environ, **_GIT_ENV}
    return subprocess.run(
        [GIT, "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main",
         "-c", "core.hooksPath=", "-C", cwd, *args],
        capture_output=True, text=True, check=True, env=env).stdout.strip()


class OfflineCanonical:
    """A worktree, the bare repository its board calls canonical, and the observer."""

    def __init__(self, worktree: str, bare: str, *, remote_name: str = "origin") -> None:
        self.worktree = os.path.realpath(worktree)
        self.bare = bare
        self.remote_name = remote_name

    @classmethod
    def seed(cls, tmp: str, *, branch: str = "main") -> "OfflineCanonical":
        """A fresh worktree with one commit, pushed to a bare canonical as main.

        ``branch`` is checked out at that same commit, so the candidate IS the
        canonical base: the identity relation the legacy fixtures assert is true
        of the repository, not merely claimed by the record."""
        worktree = os.path.join(tmp, "worktree")
        bare = os.path.join(tmp, "canonical.git")
        os.makedirs(worktree)
        _git(worktree, "init", "-q", "-b", "main")
        _git(worktree, "commit", "-q", "--allow-empty", "-m", "base")
        _git(tmp, "init", "-q", "--bare", "-b", "main", bare)
        _git(worktree, "remote", "add", "origin", bare)
        _git(worktree, "push", "-q", "origin", "main")
        if branch != "main":
            _git(worktree, "checkout", "-q", "-b", branch)
        return cls(worktree, bare)

    @classmethod
    def mirror(cls, tmp: str, worktree: str, *, remote_name: str) -> "OfflineCanonical":
        """A bare canonical for an EXISTING worktree whose ``origin`` must stay as
        it is -- the dispatch bridge's identity check pins origin to GitHub. The
        worktree's current HEAD becomes the bare's main, reachable under
        ``remote_name``; the observer maps the receipt's ``origin`` onto it."""
        bare = os.path.join(tmp, "canonical.git")
        _git(tmp, "init", "-q", "--bare", "-b", "main", bare)
        _git(worktree, "remote", "add", remote_name, bare)
        _git(worktree, "push", "-q", remote_name, "HEAD:refs/heads/main")
        return cls(worktree, bare, remote_name=remote_name)

    @property
    def head(self) -> str:
        return _git(self.worktree, "rev-parse", "HEAD").lower()

    @property
    def base(self) -> str:
        """What the bare canonical's main points at RIGHT NOW: read, not remembered."""
        return _git(self.bare, "rev-parse", "refs/heads/main").lower()

    def advance_main(self) -> str:
        """Move the canonical's main past the candidate without moving the candidate.

        Proves an observer reads the remote rather than echoing the worktree: after
        this, a truthful request names a base that is not the head."""
        current = _git(self.worktree, "rev-parse", "--abbrev-ref", "HEAD")
        _git(self.worktree, "checkout", "-q", "--detach")
        _git(self.worktree, "commit", "-q", "--allow-empty", "-m", "main moved on")
        moved = _git(self.worktree, "rev-parse", "HEAD").lower()
        _git(self.worktree, "push", "-q", self.remote_name, f"{moved}:refs/heads/main")
        _git(self.worktree, "checkout", "-q", current)
        return moved

    def observe(self, worktree: str, remote: str, base_ref: str) -> tuple[str, str]:
        """The observer a test board carries.

        Only the fixture worktree, only its offline canonical, only the remote
        NAME ``origin`` the receipts name: anything else is a gap, never a remote
        read. What it returns comes from ``observe_canonical_base_via_git`` --
        ``git remote get-url`` then ``git ls-remote`` on the bare path -- so a
        request built from it names what the canonical says, not what the
        receipt says."""
        if os.path.realpath(worktree) != self.worktree or remote.split() != ["origin"]:
            return "", ""
        return observe_canonical_base_via_git(worktree, self.remote_name, base_ref)
