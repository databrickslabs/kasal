"""
Unit tests for ``LightAgentService._attach_memory`` — the chat/light agent's
cognitive-memory wiring.

The light path composes the EXISTING public ``CrewMemoryService`` building blocks
(no changes to the crew path) to build + attach a unified ``Memory`` to the single
agent, so ``Agent.kickoff_async`` auto-recalls and persists. Memory is ON by
default; the chat "No memory" toggle arrives as ``agent_spec['memory'] is False``.
"""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.paths.light_agent.light_agent_service import LightAgentService


def _config(session_id="sess-1", workspace_scope=True, model="m"):
    return SimpleNamespace(
        session_id=session_id,
        memory_workspace_scope=workspace_scope,
        model=model,
    )


def _patches(*, disabled_config=False, storage=MagicMock(), sets_memory=True):
    """Patch the CrewMemoryService building blocks the light path composes.

    ``configure_crew_memory_components`` is given a side effect that mirrors the
    real one: it sets ``agent.memory`` on each agent in ``crew_kwargs['agents']``.
    """
    mem_service = MagicMock()
    mem_service.fetch_memory_backend_config = AsyncMock(return_value={"backend_type": "databricks"})
    mem_service.generate_crew_id = MagicMock(return_value="g1_crew_abcd1234")
    mem_service.setup_storage_directory = MagicMock()
    mem_service.create_unified_storage = AsyncMock(return_value=storage)
    mem_service.resolve_memory_llm_override = AsyncMock(return_value=None)

    def _configure(crew_kwargs, *a, **k):
        if sets_memory:
            for ag in crew_kwargs.get("agents", []):
                ag.memory = MagicMock(name="UnifiedMemory")
        return crew_kwargs
    mem_service.configure_crew_memory_components = MagicMock(side_effect=_configure)

    cfg_builder = MagicMock()
    cfg_builder.check_memory_disabled_by_backend_config = MagicMock(return_value=disabled_config)

    embedder_builder = MagicMock()
    embedder_builder.configure_embedder = AsyncMock(
        side_effect=lambda ck: (ck, MagicMock(name="embedder"), None)
    )

    return patch.multiple(
        "src.engines.crewai.memory.crew_memory_service",
        CrewMemoryService=MagicMock(return_value=mem_service),
    ), patch(
        "src.engines.crewai.config.crew_config_builder.CrewConfigBuilder",
        MagicMock(return_value=cfg_builder),
    ), patch(
        "src.engines.crewai.config.embedder_config_builder.EmbedderConfigBuilder",
        MagicMock(return_value=embedder_builder),
    ), patch(
        "src.schemas.memory_backend.MemoryBackendConfig", MagicMock()
    ), mem_service


@pytest.mark.asyncio
async def test_attach_memory_sets_agent_memory_when_backend_configured():
    """With a configured backend, a unified Memory is built and attached to the
    single agent (so kickoff_async will recall + persist)."""
    agent = SimpleNamespace(memory=None, id="aid-1")
    p_service, p_cfg, p_emb, p_mbc, mem_service = _patches()
    logs = []
    with p_service, p_cfg, p_emb, p_mbc:
        await LightAgentService()._attach_memory(
            agent, {"role": "Assistant"}, _config(), None, "g1", "hi", "exec-1", logs.append
        )
    assert agent.memory is not None and agent.memory not in (True, False)
    mem_service.configure_crew_memory_components.assert_called_once()
    assert any("Memory enabled" in m for m in logs)


@pytest.mark.asyncio
async def test_attach_memory_skipped_when_agent_memory_disabled():
    """The chat 'No memory' toggle (agent_spec['memory'] is False) skips setup
    entirely — no CrewMemoryService work, agent stays memory-less."""
    agent = SimpleNamespace(memory=None, id="aid-1")
    p_service, p_cfg, p_emb, p_mbc, mem_service = _patches()
    logs = []
    with p_service, p_cfg, p_emb, p_mbc:
        await LightAgentService()._attach_memory(
            agent, {"role": "Assistant", "memory": False}, _config(), None, "g1", "hi", "exec-1", logs.append
        )
    assert agent.memory is None
    mem_service.fetch_memory_backend_config.assert_not_called()
    assert any("disabled" in m.lower() for m in logs)


@pytest.mark.asyncio
async def test_attach_memory_noop_for_disabled_configuration():
    """The 'Disabled Configuration' backend → no memory attached (returns before
    building storage)."""
    agent = SimpleNamespace(memory=None, id="aid-1")
    p_service, p_cfg, p_emb, p_mbc, mem_service = _patches(disabled_config=True)
    logs = []
    with p_service, p_cfg, p_emb, p_mbc:
        await LightAgentService()._attach_memory(
            agent, {"role": "Assistant"}, _config(), None, "g1", "hi", "exec-1", logs.append
        )
    assert agent.memory is None
    mem_service.create_unified_storage.assert_not_called()
    mem_service.configure_crew_memory_components.assert_not_called()


@pytest.mark.asyncio
async def test_attach_memory_best_effort_on_failure():
    """Any failure during memory setup is swallowed — the agent is left
    memory-less and the chat still answers."""
    agent = SimpleNamespace(memory=None, id="aid-1")
    p_service, p_cfg, p_emb, p_mbc, mem_service = _patches()
    mem_service.fetch_memory_backend_config = AsyncMock(side_effect=RuntimeError("db down"))
    logs = []
    with p_service, p_cfg, p_emb, p_mbc:
        # Must not raise.
        await LightAgentService()._attach_memory(
            agent, {"role": "Assistant"}, _config(), None, "g1", "hi", "exec-1", logs.append
        )
    assert agent.memory is None


# ── Conversation history (cross-turn recall) ─────────────────────────────────
from contextlib import asynccontextmanager


def _msg(mtype, content):
    return SimpleNamespace(message_type=mtype, content=content)


def _history_patches(messages):
    """Patch request_scoped_session + ChatHistoryRepository to return ``messages``."""
    @asynccontextmanager
    async def _fake_session():
        yield MagicMock(name="db_session")

    repo = MagicMock()
    repo.get_by_session_and_group = AsyncMock(return_value=messages)
    return patch("src.db.session.request_scoped_session", _fake_session), patch(
        "src.repositories.chat_history_repository.ChatHistoryRepository",
        MagicMock(return_value=repo),
    )


def _cfg_ctx(session_id="sess-1"):
    config = SimpleNamespace(session_id=session_id)
    ctx = SimpleNamespace(group_ids=["g1"])
    return config, ctx


@pytest.mark.asyncio
async def test_preamble_builds_transcript_excluding_current_turn():
    """Prior turns become a transcript; the current turn (last user row + its
    Thinking.../[ui-card] placeholders) and placeholder rows are excluded."""
    messages = [
        _msg("user", "my name is nehme tohme"),
        _msg("assistant", "Thinking..."),
        _msg("assistant", "[ui-card]"),
        _msg("assistant", "Hello Nehme Tohme! Nice to meet you."),
        _msg("user", "who am i"),            # <- current turn (last user)
        _msg("assistant", "Thinking..."),
    ]
    p_sess, p_repo = _history_patches(messages)
    config, ctx = _cfg_ctx()
    with p_sess, p_repo:
        out = await LightAgentService()._conversation_preamble(
            config, ctx, "g1", lambda *_: None
        )
    assert "User: my name is nehme tohme" in out
    assert "Assistant: Hello Nehme Tohme! Nice to meet you." in out
    assert "who am i" not in out          # current turn excluded
    assert "Thinking..." not in out and "[ui-card]" not in out


@pytest.mark.asyncio
async def test_preamble_empty_without_session_id():
    config, ctx = _cfg_ctx(session_id=None)
    out = await LightAgentService()._conversation_preamble(
        config, ctx, "g1", lambda *_: None
    )
    assert out == ""


@pytest.mark.asyncio
async def test_preamble_best_effort_on_repo_failure():
    """A history-fetch failure never breaks the chat — returns ''."""
    @asynccontextmanager
    async def _fake_session():
        yield MagicMock()

    repo = MagicMock()
    repo.get_by_session_and_group = AsyncMock(side_effect=RuntimeError("db down"))
    config, ctx = _cfg_ctx()
    with patch("src.db.session.request_scoped_session", _fake_session), patch(
        "src.repositories.chat_history_repository.ChatHistoryRepository",
        MagicMock(return_value=repo),
    ):
        out = await LightAgentService()._conversation_preamble(
            config, ctx, "g1", lambda *_: None
        )
    assert out == ""


@pytest.mark.asyncio
async def test_preamble_empty_when_no_prior_turns():
    """First message of a session → only the current user row exists → no preamble."""
    messages = [_msg("user", "my name is nehme tohme"), _msg("assistant", "Thinking...")]
    p_sess, p_repo = _history_patches(messages)
    config, ctx = _cfg_ctx()
    with p_sess, p_repo:
        out = await LightAgentService()._conversation_preamble(
            config, ctx, "g1", lambda *_: None
        )
    assert out == ""
