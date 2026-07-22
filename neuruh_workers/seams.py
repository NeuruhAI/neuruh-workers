"""Registration seam for future organ workers (Mother, AXON).

This is a SEAM, not an integration: the typed contract a later authorized
lane will fill.  In v1 a seam can be DECLARED (receipted intent, loopback
endpoint, exact source SHA) but can never be ACTIVATED — activation refuses
unconditionally with a receipt.  No network client exists behind it.
"""
from __future__ import annotations

from dataclasses import dataclass

from .contracts import DefinitionError, ReceiptType, SeamNotAuthorized
from .ledger import ExecutionLedger

KNOWN_ORGANS = ("mother", "axon")
_SEAM_JOB = "seam-registry"


@dataclass(frozen=True)
class OrganSeamDeclaration:
    organ: str            # "mother" | "axon"
    endpoint: str         # loopback only
    source_sha: str       # exact 40-hex commit the future worker must run

    def __post_init__(self):
        if self.organ not in KNOWN_ORGANS:
            raise DefinitionError(
                f"unknown organ {self.organ!r}; seam accepts {KNOWN_ORGANS}")
        if not (self.endpoint.startswith("http://127.0.0.1:")
                or self.endpoint.startswith("http://localhost:")):
            raise DefinitionError(
                f"organ seam endpoint must be loopback, got {self.endpoint!r}")
        if len(self.source_sha) != 40 or any(
                c not in "0123456789abcdef" for c in self.source_sha):
            raise DefinitionError("organ seam source_sha must be exact 40-hex")


class OrganRegistrationSeam:
    def __init__(self, ledger: ExecutionLedger):
        self._ledger = ledger
        self._declared: dict[str, OrganSeamDeclaration] = {}

    def declare(self, declaration: OrganSeamDeclaration) -> None:
        if declaration.organ in self._declared:
            raise DefinitionError(
                f"organ {declaration.organ!r} seam already declared")
        self._declared[declaration.organ] = declaration
        self._ledger.append(ReceiptType.SEAM_DECLARED, _SEAM_JOB, {
            "organ": declaration.organ,
            "endpoint": declaration.endpoint,
            "source_sha": declaration.source_sha,
            "status": "PENDING_AUTHORIZATION",
        })

    def declared(self) -> dict[str, OrganSeamDeclaration]:
        return dict(self._declared)

    def activate(self, organ: str) -> None:
        """v1: always refuses.  A future lane replaces this body under its own
        audit; nothing else in the layer assumes activation can succeed."""
        self._ledger.append(ReceiptType.SEAM_ACTIVATION_REFUSED, _SEAM_JOB, {
            "organ": organ,
            "reason": "organ worker activation is not authorized in "
                      "autonomous-workflows v1",
        })
        raise SeamNotAuthorized(
            f"organ {organ!r} registration seam is declared-only in v1; "
            "activation requires a reviewed authorization lane",
            {"organ": organ})
