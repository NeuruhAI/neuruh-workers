"""Legion: governed mass fan-out of read-only agents to ONE sealed output.

See LEGION.md.  Entry points: ``Legion`` (runtime), ``PROFILES`` (what an
agent does), ``register_legion`` (the ``legion-mission@1.0.0`` Workers job).
"""
from .journal import LegionJournal, verify_journal
from .profiles import PROFILES, compact_lro_opportunity
from .runtime import (
    LEGION_MAX_AGENTS_ABSOLUTE,
    LEGION_MAX_AGENTS_DEFAULT,
    AgentContext,
    AgentProfile,
    Legion,
    LegionBusy,
    LegionRefused,
    LegionResult,
    LegionSpec,
    LegionSpecDrift,
    merkle_root,
    unit_manifest_sha,
)
from .workflow import (
    DEFAULT_LEGION_ROOT,
    load_units,
    register_legion,
    stage_units,
)

__all__ = [
    "AgentContext", "AgentProfile", "DEFAULT_LEGION_ROOT", "LEGION_MAX_AGENTS_ABSOLUTE",
    "LEGION_MAX_AGENTS_DEFAULT", "Legion", "LegionBusy", "LegionJournal",
    "LegionRefused", "LegionResult", "LegionSpec", "LegionSpecDrift", "PROFILES",
    "compact_lro_opportunity", "load_units", "merkle_root", "register_legion",
    "stage_units", "unit_manifest_sha", "verify_journal",
]
