"""Tests for flow_methods.configure_flow_crew_memory.

Crews built inside a Flow must wire the unified Databricks/Lakebase memory
backend the same way the regular crew path does — otherwise they fall back to
CrewAI's default LanceDB + OpenAI embedder ("CHROMA_OPENAI_API_KEY is not set").
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.flow.modules.flow_methods import configure_flow_crew_memory


def _agent(role="Researcher", model="databricks-gpt-5"):
    a = MagicMock()
    a.role = role
    a.llm = MagicMock()
    a.llm.model = model
    return a


@pytest.mark.asyncio
async def test_no_backend_config_skips_unified_wiring():
    """With no memory backend configured, only the embedder is set (CrewAI
    default storage) — no unified storage / Memory wiring."""
    with patch(
        "src.engines.crewai.services.crew_memory_service.CrewMemoryService"
    ) as MockSvc, patch(
        "src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder"
    ) as MockEmb:
        svc = MockSvc.return_value
        svc.fetch_memory_backend_config = AsyncMock(return_value=None)
        svc.create_unified_storage = AsyncMock()
        svc.configure_crew_memory_components = MagicMock()
        emb = MockEmb.return_value
        emb.configure_embedder = AsyncMock(return_value=({"memory": True}, None, None))

        result = await configure_flow_crew_memory(
            {"memory": True}, [_agent()], [MagicMock(description="t")], "crew", "g", None
        )

        assert result == {"memory": True}
        svc.create_unified_storage.assert_not_called()
        svc.configure_crew_memory_components.assert_not_called()


@pytest.mark.asyncio
async def test_backend_config_wires_unified_memory():
    """With a Lakebase backend configured, the unified storage is built and the
    Memory is wired into crew_kwargs (and agents) via the memory service."""
    backend_cfg = {"backend_type": "lakebase", "lakebase_config": {"memory_table": "crew_memory"}}
    callable_embedder = lambda texts: [[0.1] for _ in texts]
    configured_kwargs = {"memory": MagicMock(name="Memory")}

    with patch(
        "src.engines.crewai.services.crew_memory_service.CrewMemoryService"
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
        "src.engines.crewai.services.crew_memory_service.CrewMemoryService"
    ) as MockSvc, patch(
        "src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder"
    ) as MockEmb:
        svc = MockSvc.return_value
        svc.fetch_memory_backend_config = AsyncMock(return_value=backend_cfg)
        svc.generate_crew_id = MagicMock(return_value="g_crew_abc")
        svc.create_unified_storage = AsyncMock(side_effect=RuntimeError("index missing"))
        emb = MockEmb.return_value
        emb.configure_embedder = AsyncMock(return_value=({"memory": True}, lambda t: [[0.1]], None))

        # Should not raise.
        result = await configure_flow_crew_memory(
            {"memory": True}, [_agent()], [MagicMock(description="t")], "crew", "g", None
        )
        assert "memory" in result
