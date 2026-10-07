"""Declarative workflow definitions.

A workflow is data, not code: an ordered list of steps, each bound to a named
worker capability, with explicit retry, timeout, idempotency and approval
semantics.  Validation is strict — an invalid definition is refused at
construction time, never patched over at run time.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .contracts import DefinitionError

_NAME_RULES = "lowercase letters, digits, hyphens and underscores"


def _valid_name(name: str) -> bool:
    return (
        isinstance(name, str) and 0 < len(name) <= 80
        and all(c.islower() or c.isdigit() or c in "-_." for c in name)
    )


@dataclass(frozen=True)
class RetryPolicy:
    """Deterministic exponential backoff.  Delays are computed, never random."""
    max_attempts: int = 1
    base_delay_s: float = 0.0
    multiplier: float = 2.0
    max_delay_s: float = 60.0

    def __post_init__(self):
        if not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise DefinitionError("retry max_attempts must be an integer >= 1")
        if self.base_delay_s < 0 or self.max_delay_s < 0 or self.multiplier < 1.0:
            raise DefinitionError("retry delays must be >= 0 and multiplier >= 1.0")

    def delay_for_attempt(self, failed_attempt: int) -> float:
        """Backoff before attempt N+1, after attempt N failed (1-indexed)."""
        if failed_attempt < 1:
            raise DefinitionError("failed_attempt must be >= 1")
        delay = self.base_delay_s * (self.multiplier ** (failed_attempt - 1))
        return min(delay, self.max_delay_s)


@dataclass(frozen=True)
class StepDefinition:
    step_id: str
    capability: str
    params: Mapping[str, Any] = field(default_factory=dict)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeout_s: float = 30.0
    requires_approval: bool = False
    approvers: tuple[str, ...] = ()

    def __post_init__(self):
        if not _valid_name(self.step_id):
            raise DefinitionError(
                f"step_id {self.step_id!r} is invalid ({_NAME_RULES})")
        if not _valid_name(self.capability):
            raise DefinitionError(
                f"capability {self.capability!r} is invalid ({_NAME_RULES})")
        if not isinstance(self.timeout_s, (int, float)) or self.timeout_s <= 0:
            raise DefinitionError(f"step {self.step_id}: timeout_s must be > 0")
        if self.requires_approval and not self.approvers:
            raise DefinitionError(
                f"step {self.step_id}: an approval gate must name its approvers")
        if not self.requires_approval and self.approvers:
            raise DefinitionError(
                f"step {self.step_id}: approvers listed without requires_approval")


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    version: str
    description: str
    steps: tuple[StepDefinition, ...]
    read_only: bool = True

    def __post_init__(self):
        if not _valid_name(self.name):
            raise DefinitionError(f"workflow name {self.name!r} is invalid ({_NAME_RULES})")
        if not isinstance(self.version, str) or not self.version:
            raise DefinitionError(f"workflow {self.name}: version is required")
        if not self.steps:
            raise DefinitionError(f"workflow {self.name}: at least one step is required")
        seen: set[str] = set()
        for step in self.steps:
            if not isinstance(step, StepDefinition):
                raise DefinitionError(f"workflow {self.name}: steps must be StepDefinition")
            if step.step_id in seen:
                raise DefinitionError(
                    f"workflow {self.name}: duplicate step_id {step.step_id!r}")
            seen.add(step.step_id)

    def step(self, step_id: str) -> StepDefinition:
        for candidate in self.steps:
            if candidate.step_id == step_id:
                return candidate
        raise DefinitionError(f"workflow {self.name}: unknown step {step_id!r}")

    def spec(self) -> dict[str, Any]:
        """The declarative form recorded into the enqueue receipt."""
        return {
            "name": self.name,
            "version": self.version,
            "read_only": self.read_only,
            "steps": [
                {
                    "step_id": s.step_id,
                    "capability": s.capability,
                    "params": dict(s.params),
                    "retry": {
                        "max_attempts": s.retry.max_attempts,
                        "base_delay_s": s.retry.base_delay_s,
                        "multiplier": s.retry.multiplier,
                        "max_delay_s": s.retry.max_delay_s,
                    },
                    "timeout_s": s.timeout_s,
                    "requires_approval": s.requires_approval,
                    "approvers": list(s.approvers),
                }
                for s in self.steps
            ],
        }


class WorkflowCatalog:
    """Named, versioned registry of definitions.  Unknown lookups fail closed."""

    def __init__(self):
        self._defs: dict[str, WorkflowDefinition] = {}

    def register(self, definition: WorkflowDefinition) -> None:
        if not isinstance(definition, WorkflowDefinition):
            raise DefinitionError("only WorkflowDefinition can be registered")
        if definition.name in self._defs:
            raise DefinitionError(f"workflow {definition.name!r} is already registered")
        self._defs[definition.name] = definition

    def get(self, name: str) -> WorkflowDefinition:
        if name not in self._defs:
            raise DefinitionError(f"workflow {name!r} is not registered")
        return self._defs[name]

    def names(self) -> list[str]:
        return sorted(self._defs)
