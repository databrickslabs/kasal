"""The chat "Workspace memory" switch must actually reach the backend.

    ON  (default) → filter on group_id
    OFF           → filter on group_id + session_id

``memory_workspace_scope`` and ``session_id`` were carried all the way from the
request through ``config_adapter`` into ``CrewMemoryService.config`` — and then
``create_unified_storage`` passed ``workspace_wide=True, session_scope_id=None``
to the factory regardless. The switch changed a log line and nothing else.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.services.memory.crew_memory import CrewMemoryService

_LAKEBASE_CONFIG = {
    "backend_type": "lakebase",
    "lakebase_config": {"memory_table": "crew_memory"},
}


async def _factory_kwargs(config: dict) -> dict:
    """Build storage for ``config`` and return what reached the factory."""
    service = CrewMemoryService(config)
    with patch(
        "src.services.memory.crew_memory.MemoryBackendFactory.create_unified_storage",
        new_callable=AsyncMock,
    ) as factory:
        await service.create_unified_storage(
            dict(_LAKEBASE_CONFIG), crew_id="grp_crew_abc", embedder=None
        )
    return factory.call_args.kwargs


class TestWorkspaceScopeReachesTheBackend:
    @pytest.mark.asyncio
    async def test_enabled_is_workspace_wide(self):
        kwargs = await _factory_kwargs(
            {
                "group_id": "grp",
                "session_id": "chat-sess-1",
                "memory_workspace_scope": True,
            }
        )
        assert kwargs["workspace_wide"] is True

    @pytest.mark.asyncio
    async def test_disabled_scopes_to_the_session(self):
        kwargs = await _factory_kwargs(
            {
                "group_id": "grp",
                "session_id": "chat-sess-1",
                "memory_workspace_scope": False,
            }
        )
        assert kwargs["workspace_wide"] is False
        assert kwargs["session_scope_id"] == "chat-sess-1"

    @pytest.mark.asyncio
    async def test_unset_defaults_to_workspace_wide(self):
        """Every non-chat execution arrives without the flag and must stay
        workspace-wide — a crew narrowing itself to one run would be a
        regression, not a default."""
        kwargs = await _factory_kwargs({"group_id": "grp", "execution_id": "job-1"})
        assert kwargs["workspace_wide"] is True

    @pytest.mark.asyncio
    async def test_session_id_is_the_chat_session_not_the_run(self):
        """The partition key must be stable across the turns of one conversation;
        job_id changes every run and is only the fallback."""
        kwargs = await _factory_kwargs(
            {
                "group_id": "grp",
                "session_id": "chat-sess-1",
                "execution_id": "job-1",
                "memory_workspace_scope": False,
            }
        )
        assert kwargs["session_scope_id"] == "chat-sess-1"
        assert kwargs["job_id"] == "job-1"

    @pytest.mark.asyncio
    async def test_group_id_is_always_forwarded(self):
        for scope in (True, False, None):
            kwargs = await _factory_kwargs(
                {
                    "group_id": "grp",
                    "session_id": "s1",
                    "memory_workspace_scope": scope,
                }
            )
            assert kwargs["group_id"] == "grp"
