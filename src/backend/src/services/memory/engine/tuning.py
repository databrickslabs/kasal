"""Read a Memory Tuning value off whatever memory object a caller holds.

The maintenance, recall and write-screening passes are handed "a memory" —
normally ``engine.Memory``, whose declared fields carry the teamspace's Memory
Tuning (Configuration → Memory; they replaced the KASAL_MEMORY_* environment
variables). Tests and older call sites pass doubles, and ``getattr`` on a
``MagicMock`` returns a truthy mock, which would silently switch an opt-in pass
such as forgetting ON. So a value is used only when it has the default's type.
"""

from typing import Any, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T", bool, int, float, str)


def tuned(memory: Any, name: str, default: T) -> T:
    """``memory.<name>`` when it has the type of ``default``, else ``default``."""
    value = getattr(memory, name, None)
    if isinstance(default, bool):
        return value if isinstance(value, bool) else default  # type: ignore[return-value]
    if isinstance(value, bool):
        return default
    if isinstance(default, float) and isinstance(value, (int, float)):
        return float(value)  # type: ignore[return-value]
    return value if isinstance(value, type(default)) else default


class MemoryHygiene(BaseModel):
    """Write screening, recall cut-off and retention — the Memory Tuning knobs
    that replaced the KASAL_MEMORY_* environment variables. ``Memory`` inherits
    them, so each is a declared field pydantic cannot drop; readers go through
    :func:`tuned` because they may be handed a double instead of a ``Memory``.
    """

    recall_max_drop: float = Field(
        default=0.12,
        description="Recall drops candidates scoring this far below the best one.",
    )
    write_screening: str = Field(
        default="quarantine",
        description="Prompt-injection screening of writes: quarantine, annotate, off.",
    )
    forgetting_enabled: bool = Field(
        default=False, description="Delete records past their retention rule."
    )
    superseded_retention_days: float = Field(
        default=90.0, description="Days a superseded fact is kept once replaced."
    )
    episodic_ttl_days: float = Field(
        default=180.0, description="Days an unimportant episodic record is kept."
    )
    importance_floor: float = Field(
        default=0.4, description="Records at/above this importance are never forgotten."
    )
    supersession_enabled: bool = Field(
        default=True, description="Retire facts contradicted by a newer one."
    )
    llm_consolidation_enabled: bool = Field(
        default=True, description="Merge near-duplicate clusters between runs."
    )
