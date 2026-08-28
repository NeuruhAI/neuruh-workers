"""Typed switchboard port. V1 records events; it is not live-wired."""
from __future__ import annotations

from typing import Any, Mapping

from .constants import SWITCHBOARD_EVENTS
from .errors import GitCustodianRefused


class RecordingSwitchboard:
    """In-process event log. SWITCHAROO_INTEGRATED stays false."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    @property
    def integrated(self) -> bool:
        return False

    def emit(self, event_type: str, payload: Mapping[str, Any] | None = None) -> None:
        if event_type not in SWITCHBOARD_EVENTS:
            raise GitCustodianRefused(
                "unknown switchboard event type",
                {"event_type": event_type}, reason="SWITCHBOARD_UNKNOWN")
        self.events.append({"type": event_type, "payload": dict(payload or {})})
