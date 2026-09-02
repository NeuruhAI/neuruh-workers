"""Typed refusals for Git Custodian. Fail closed; no silent fallback."""
from __future__ import annotations

from typing import Any, Mapping

from ..contracts import WorkflowError


class GitCustodianRefused(WorkflowError):
    """A custody step refused to proceed."""
    code = "GIT_CUSTODIAN_REFUSED"

    def __init__(self, message: str, detail: Mapping[str, Any] | None = None,
                 *, reason: str = "REFUSED"):
        payload = dict(detail or {})
        payload.setdefault("reason", reason)
        super().__init__(f"{reason}: {message}", payload)
        self.reason = reason

    def code_detail(self) -> str:
        return str(self.detail.get("reason", self.reason))
