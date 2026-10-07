"""Default workflow stack builders (samples + LandOS Hunt WRAP)."""
from __future__ import annotations

from ..definitions import WorkflowCatalog
from ..landos_hunt import bind_landos_hunt_workers
from ..registry import WorkerRegistry
from .samples import build_catalog as _samples_catalog
from .samples import build_registry as _samples_registry

__all__ = ["build_catalog", "build_registry"]


def build_registry() -> WorkerRegistry:
    """Samples registry + landos.hunt.create (FR default stack)."""
    registry = _samples_registry()
    bind_landos_hunt_workers(registry, WorkflowCatalog())
    return registry


def build_catalog() -> WorkflowCatalog:
    """Samples catalog + landos-hunt workflow (FR default stack)."""
    catalog = _samples_catalog()
    bind_landos_hunt_workers(WorkerRegistry(), catalog)
    return catalog
