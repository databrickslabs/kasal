"""Tests for flow_methods.configure_flow_crew_memory.

Crews built inside a Flow must wire the unified Databricks/Lakebase memory
backend the same way the regular crew path does — otherwise they fall back to
CrewAI's default LanceDB + OpenAI embedder ("CHROMA_OPENAI_API_KEY is not set").
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.paths.flow.modules.flow_methods import configure_flow_crew_memory


def _agent(role="Researcher", model="databricks-gpt-5"):
    a = MagicMock()
    a.role = role
    a.llm = MagicMock()
    a.llm.model = model
    return a


@pytest.mark.asyncio
async def test_no_active_config_falls_back_to_default_and_wires_memory():
    """No ACTIVE backend config (e.g. the "Disabled Configuration" row →
    get_active_config returns None) must FALL BACK to the DEFAULT local backend
    and STILL wire unified Memory — same as the crew path. Bailing here would
    leave crew_kwargs["memory"]=True, so CrewAI builds its own ChromaDB+OpenAI
    Memory and the memory tools fail with "CHROMA_OPENAI_API_KEY is not set"."""
    configured = {"memory": MagicMock(name="Memory")}
    with patch(
        "src.engines.crewai.memory.crew_memory_service.CrewMemoryService"
    ) as MockSvc, patch(
        "src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder"
    ) as MockEmb:
        svc = MockSvc.return_value
        svc.fetch_memory_backend_config = AsyncMock(return_value=None)
        svc.generate_crew_id = MagicMock(return_value="g_crew_x")
        svc.setup_storage_directory = MagicMock()
        svc.create_unified_storage = AsyncMock(return_value=None)  # DEFAULT → no custom storage
        svc.resolve_memory_llm_override = AsyncMock(return_value=None)
        svc.configure_crew_memory_components = MagicMock(return_value=configured)
        emb = MockEmb.return_value
        emb.configure_embedder = AsyncMock(
            return_value=({"memory": True}, MagicMock(name="embedder"), None)
        )

        result = await configure_flow_crew_memory(
            {"memory": True}, [_agent()], [MagicMock(description="t")], "crew", "g", None
        )

        # Memory is wired (not left as bare True) via the DEFAULT fallback.
        svc.setup_storage_directory.assert_called_once()
        svc.configure_crew_memory_components.assert_called_once()
        assert result is configured
        # The fallback config is the DEFAULT backend.
        memcfg = svc.configure_crew_memory_components.call_args.args[1]
        assert str(getattr(memcfg.backend_type, "value", memcfg.backend_type)) == "default"


@pytest.mark.asyncio
async def test_backend_config_wires_unified_memory():
    """With a Lakebase backend configured, the unified storage is built and the
    Memory is wired into crew_kwargs (and agents) via the memory service."""
    backend_cfg = {"backend_type": "lakebase", "lakebase_config": {"memory_table": "crew_memory"}}
    callable_embedder = lambda texts: [[0.1] for _ in texts]
    configured_kwargs = {"memory": MagicMock(name="Memory")}

    with patch(
        "src.engines.crewai.memory.crew_memory_service.CrewMemoryService"
    ) as MockSvc, patch(
        "src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder"
    ) as MockEmb:
        svc = MockSvc.return_value
        svc.fetch_memory_backend_config = AsyncMock(return_value=backend_cfg)
        svc.generate_crew_id = MagicMock(return_value="g_crew_abc")
        storage = MagicMock(name="LakebaseStorageBackend")
        svc.create_unified_storage = AsyncMock(return_value=storage)
        svc.resolve_memory_llm_override = AsyncMock(return_value=None)
        svc.configure_crew_memory_components = MagicMock(return_value=configured_kwargs)
        emb = MockEmb.return_value
        emb.configure_embedder = AsyncMock(
            return_value=({"memory": True}, callable_embedder, None)
        )

        result = await configure_flow_crew_memory(
            {"memory": True}, [_agent()], [MagicMock(description="t")], "crew", "g", "tok"
        )

        # Flow points CREWAI_STORAGE_DIR at the deterministic store, same as the
        # crew path — so flow DEFAULT memory writes/reads where crews do.
        svc.setup_storage_directory.assert_called_once_with("g_crew_abc", backend_cfg)
        # Unified storage built with the callable embedder for lakebase backend.
        svc.create_unified_storage.assert_awaited_once()
        _, kwargs = svc.create_unified_storage.await_args
        args = svc.create_unified_storage.await_args.args
        assert "g_crew_abc" in args  # crew_id passed
        assert callable_embedder in args  # callable embedder used for lakebase
        # Memory components configured and returned.
        svc.configure_crew_memory_components.assert_called_once()
        assert result is configured_kwargs


@pytest.mark.asyncio
async def test_backend_config_error_falls_back_gracefully():
    """If unified storage creation raises, we don't blow up the flow crew."""
    backend_cfg = {"backend_type": "lakebase", "lakebase_config": {"memory_table": "crew_memory"}}
    with patch(
        "src.engines.crewai.memory.crew_memory_service.CrewMemoryService"
    ) as MockSvc, patch(
        "src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder"
    ) as MockEmb:
        svc = MockSvc.return_value
        svc.fetch_memory_backend_config = AsyncMock(return_value=backend_cfg)
        svc.generate_crew_id = MagicMock(return_value="g_crew_abc")
        svc.create_unified_storage = AsyncMock(side_effect=RuntimeError("index missing"))
        emb = MockEmb.return_value
        emb.configure_embedder = AsyncMock(return_value=({"memory": True}, lambda t: [[0.1]], None))

        # Should not raise, and must DISABLE memory (not leave the bare True that
        # makes CrewAI build its own ChromaDB+OpenAI Memory and fail).
        result = await configure_flow_crew_memory(
            {"memory": True}, [_agent()], [MagicMock(description="t")], "crew", "g", None
        )
        assert result["memory"] is False
