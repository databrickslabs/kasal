"""Freeze saved agent settings before the execution record and worker diverge."""

from typing import Any, Dict, Optional, cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.llm.effort import apply_effort_to_config
from src.schemas.execution import CrewConfig
from src.services.catalog.agents import AgentService
from src.utils.user_context import GroupContext

AGENT_SETTING_FIELDS = (
    "execution_effort",
    "max_iter",
    "max_execution_time",
    "max_retry_limit",
    "temperature",
    "thinking_budget_tokens",
    "reasoning_effort",
    "max_tokens",
)


async def snapshot_agent_settings(
    config: CrewConfig,
    session: Optional[AsyncSession],
    group_context: Optional[GroupContext],
) -> None:
    if not group_context or not group_context.primary_group_id:
        return
    # Only reached from ExecutionService.create_execution, whose session is set.
    service = AgentService(cast(AsyncSession, session))
    agents: Dict[str, Any] = {}
    for key, original in config.agents_yaml.items():
        spec = dict(original)
        raw_id = str(spec.get("id") or spec.get("db_id") or key)
        raw_id = raw_id.removeprefix("agent_").removeprefix("agent-")
        try:
            agent_id = str(UUID(raw_id))
        except ValueError:
            agents[key] = spec
            continue
        agent = await service.get_with_group_check(agent_id, group_context)
        if agent is not None and agent.group_id == group_context.primary_group_id:
            for field in AGENT_SETTING_FIELDS:
                if field not in spec:
                    spec[field] = getattr(agent, field, None)
            # Explicit NULL means inherit; do not resurrect a later row value
            # during worker enrichment, after the history snapshot was taken.
            spec["agent_settings_snapshot"] = True
        agents[key] = spec
    config.agents_yaml = agents
    apply_effort_to_config(config)
