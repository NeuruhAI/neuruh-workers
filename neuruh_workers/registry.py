"""Worker capability declarations and the external-action policy.

Workers declare what they can do; workflows may only bind declared
capabilities.  Two hard gates live here:

1. A capability declared ``external`` cannot even be REGISTERED unless the
   registry was constructed with an explicit authorization token.  This lane
   ships no such token, so the external surface does not exist at runtime.
2. A workflow declared ``read_only`` may only bind ``read_only`` capabilities.

Both gates fail closed with typed errors — there is no downgrade path.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from .contracts import (
    CapabilityUnknown,
    CapabilityViolation,
    DefinitionError,
    ExternalActionDenied,
)
from .definitions import WorkflowDefinition

# The literal a future operator lane would have to supply, sourced from a
# reviewed authorization audit.  Its VALUE is not secret; its PRESENCE is the
# switch.  v1 never constructs a registry with it.
EXTERNAL_AUTHORIZATION_SENTINEL = "EXTERNAL-ACTIONS-AUTHORIZED-BY-OPERATOR-AUDIT"


@dataclass(frozen=True)
class Capability:
    name: str
    description: str
    read_only: bool
    external: bool
    execute: Callable[["StepContext", Mapping[str, Any]], Mapping[str, Any]]
    plan: Callable[["StepContext", Mapping[str, Any]], Mapping[str, Any]] | None = None


@dataclass(frozen=True)
class WorkerDeclaration:
    worker_id: str
    description: str
    capabilities: tuple[Capability, ...]


@dataclass
class StepContext:
    """What a capability handler is allowed to see."""
    job_id: str
    workflow: str
    step_id: str
    attempt: int
    dry_run: bool
    params: Mapping[str, Any] = field(default_factory=dict)
    results: Mapping[str, Any] = field(default_factory=dict)  # prior step outputs


class WorkerRegistry:
    def __init__(self, *, external_authorization: str | None = None):
        self._caps: dict[str, Capability] = {}
        self._workers: dict[str, WorkerDeclaration] = {}
        self._external_authorized = (
            external_authorization == EXTERNAL_AUTHORIZATION_SENTINEL
        )

    @property
    def external_authorized(self) -> bool:
        return self._external_authorized

    def register_worker(self, worker: WorkerDeclaration) -> None:
        if worker.worker_id in self._workers:
            raise DefinitionError(f"worker {worker.worker_id!r} already registered")
        for cap in worker.capabilities:
            if cap.external and not self._external_authorized:
                raise ExternalActionDenied(
                    f"capability {cap.name!r} is external and external actions "
                    "are not authorized in this runtime",
                    {"capability": cap.name, "worker": worker.worker_id})
            if cap.external and cap.read_only:
                raise DefinitionError(
                    f"capability {cap.name!r} cannot be both external and read_only")
            if cap.name in self._caps:
                raise DefinitionError(f"capability {cap.name!r} already registered")
        for cap in worker.capabilities:
            self._caps[cap.name] = cap
        self._workers[worker.worker_id] = worker

    def resolve(self, capability_name: str) -> Capability:
        if capability_name not in self._caps:
            raise CapabilityUnknown(
                f"capability {capability_name!r} is not registered",
                {"capability": capability_name})
        return self._caps[capability_name]

    def validate_binding(self, definition: WorkflowDefinition) -> None:
        """A workflow may only be enqueued if every binding is legal."""
        for step in definition.steps:
            cap = self.resolve(step.capability)
            if definition.read_only and not cap.read_only:
                raise CapabilityViolation(
                    f"read-only workflow {definition.name!r} binds non-read-only "
                    f"capability {cap.name!r} at step {step.step_id!r}",
                    {"workflow": definition.name, "step": step.step_id,
                     "capability": cap.name})
            if cap.external and not self._external_authorized:
                raise ExternalActionDenied(
                    f"workflow {definition.name!r} binds external capability "
                    f"{cap.name!r} and external actions are not authorized",
                    {"workflow": definition.name, "capability": cap.name})

    def declarations(self) -> dict[str, Any]:
        """Capability census for audits and receipts."""
        return {
            worker_id: {
                "description": w.description,
                "capabilities": [
                    {"name": c.name, "read_only": c.read_only, "external": c.external}
                    for c in w.capabilities
                ],
            }
            for worker_id, w in sorted(self._workers.items())
        }
