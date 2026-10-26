"""Freeze a run's config before its history row is written and its worker starts.

Two steps, in this order, both in place on the config:

1. ``auto_model``: every "auto" model becomes one concrete, enabled model key.
2. ``agent_settings_snapshot``: saved agent settings are copied into the specs.

``ExecutionService.create_execution`` calls ``freeze`` first and ``record`` once
the run's history row exists, so the run's trace shows what Auto picked.

Paths that write their own history row (the scheduler, queue triggers, the MCP
``ask`` tool, ``/flow-execution``) call ``resolve`` (step 1 only; they never
snapshotted agent settings) or ``resolve_mapping`` for a dict config, and
``record`` the same way.
"""

from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.execution import CrewConfig
from src.services.decisions.model_selection import ModelSelection
from src.services.execution.config.agent_settings_snapshot import (
    snapshot_agent_settings,
)
from src.services.execution.config.auto_model import (
    record_selection,
    resolve_mapping_models,
    resolve_run_models,
    selection_for,
)
from src.utils.user_context import GroupContext


async def freeze(
    config: CrewConfig,
    session: Optional[AsyncSession],
    group_context: Optional[GroupContext],
) -> Optional[ModelSelection]:
    """Resolve Auto, then snapshot agent settings. Returns the run's Auto pick."""
    resolved = await resolve_run_models(config, session, group_context)
    await snapshot_agent_settings(config, session, group_context)
    return selection_for(config, resolved)


async def resolve(
    config: CrewConfig,
    session: Optional[AsyncSession],
    group_context: Optional[GroupContext],
) -> Optional[ModelSelection]:
    """Step 1 only: replace every "auto" in ``config``. Returns the Auto pick."""
    return await resolve_run_models(config, session, group_context)


async def resolve_mapping(
    config: dict,
    session: Optional[AsyncSession],
    group_context: Optional[GroupContext],
) -> Optional[ModelSelection]:
    """``resolve`` for a plain-dict run config."""
    return await resolve_mapping_models(config, session, group_context)


def record(
    selection: Optional[ModelSelection],
    execution_id: str,
    group_context: Optional[GroupContext],
) -> None:
    """Add the Auto pick to the run's trace (no-op when the run did not use Auto)."""
    record_selection(selection, execution_id, group_context)
