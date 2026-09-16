from .samples import build_catalog as _build_catalog, build_registry as _build_registry
from ..landos_hunt import bind_landos_hunt


def build_registry():
    """Sample registry plus LandOS Hunt WRAP (JobQueue consume-only)."""
    registry = _build_registry()
    catalog = _build_catalog()
    bind_landos_hunt(registry, catalog)
    return registry


def build_catalog():
    """Sample catalog plus landos-hunt workflow."""
    registry = _build_registry()
    catalog = _build_catalog()
    bind_landos_hunt(registry, catalog)
    return catalog


__all__ = ["build_catalog", "build_registry"]
