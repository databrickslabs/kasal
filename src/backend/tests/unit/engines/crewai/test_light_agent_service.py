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
    # The preamble fetches the most-recent window via get_recent_by_session_and_group.
    repo.get_recent_by_session_and_group = AsyncMock(return_value=messages)
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
        _msg("user", "my name is ada lovelace"),
        _msg("assistant", "Thinking..."),
        _msg("assistant", "[ui-card]"),
        _msg("assistant", "Hello Ada Lovelace! Nice to meet you."),
        _msg("user", "who am i"),            # <- current turn (last user)
        _msg("assistant", "Thinking..."),
    ]
    p_sess, p_repo = _history_patches(messages)
    config, ctx = _cfg_ctx()
    with p_sess, p_repo:
        out = await LightAgentService()._conversation_preamble(
            config, ctx, "g1", lambda *_: None
        )
    assert "User: my name is ada lovelace" in out
    assert "Assistant: Hello Ada Lovelace! Nice to meet you." in out
    assert "who am i" not in out          # current turn excluded
    assert "Thinking..." not in out and "[ui-card]" not in out


@pytest.mark.asyncio
async def test_preamble_keeps_user_facts_when_bloated_by_assistant_output():
    """A user fact stated early must survive even when many large assistant turns
    follow — user turns are never dropped, old assistant turns are."""
    big = "X" * 5000  # bloated assistant output
    messages = [_msg("user", "my name is ada lovelace")]
    for i in range(12):
        messages.append(_msg("assistant", f"{big} deck {i}"))
        messages.append(_msg("user", f"make slide {i}"))
    messages.append(_msg("user", "what is my name"))   # current turn
    messages.append(_msg("assistant", "Thinking..."))

    p_sess, p_repo = _history_patches(messages)
    config, ctx = _cfg_ctx()
    with p_sess, p_repo:
        out = await LightAgentService()._conversation_preamble(
            config, ctx, "g1", lambda *_: None
        )
    # The early name fact is retained despite the bloat...
    assert "User: my name is ada lovelace" in out
    # ...all the intermediate user instructions are retained too...
    assert "User: make slide 0" in out
    assert "User: make slide 11" in out
    # ...the current turn is excluded...
    assert "what is my name" not in out
    # ...and the output stays within the character budget (assistant bloat trimmed).
    assert len(out) <= 6000 + 400  # budget + header allowance


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
    messages = [_msg("user", "my name is ada lovelace"), _msg("assistant", "Thinking...")]
    p_sess, p_repo = _history_patches(messages)
    config, ctx = _cfg_ctx()
    with p_sess, p_repo:
        out = await LightAgentService()._conversation_preamble(
            config, ctx, "g1", lambda *_: None
        )
    assert out == ""


# ── Genie MCP fixups (parity with crew/flow build_task_args) ──────────────────


def _genie_mcp_tool(space_id="01ef"):
    """A managed-Genie MCP tool whose adapter URL carries the space id."""
    adapter = SimpleNamespace(
        server_url=f"https://w.databricks.com/api/2.0/mcp/genie/{space_id}"
    )
    return SimpleNamespace(_mcp_tool_wrapper=SimpleNamespace(adapter=adapter))


def _genie_tool():
    """A custom GenieTool starting with no configured space id."""
    tool = MagicMock()
    tool.name = "GenieTool"
    tool._space_id = None
    return tool


def _agent_with_tools(tools):
    return SimpleNamespace(tools=tools)


def test_genie_fixups_bridge_space_id_to_genie_tool():
    """A picked Genie MCP server hands its space id to a co-assigned GenieTool. No
    output formatting is injected — the answer renders via the shared A2UI composer."""
    genie_tool = _genie_tool()
    agent = _agent_with_tools([_genie_mcp_tool("01efspace"), genie_tool])

    LightAgentService._apply_genie_mcp_fixups(agent)

    assert genie_tool._space_id == "01efspace"  # bridged from the MCP server URL


def test_genie_fixups_noop_without_genie_mcp():
    """No Genie MCP server attached → no space id set."""
    genie_tool = _genie_tool()
    agent = _agent_with_tools([genie_tool])

    LightAgentService._apply_genie_mcp_fixups(agent)

    assert genie_tool._space_id is None


def test_genie_fixups_handle_no_tools():
    """An agent with no tools is safe (catalog agents)."""
    agent = _agent_with_tools([])
    assert LightAgentService._apply_genie_mcp_fixups(agent) is None


# ── _build_mcp_configs — OBO token threading for chat MCP ─────────────────────


def test_build_mcp_configs_threads_obo_token():
    """With a user OBO token, both configs carry it so managed MCP (Genie) auths
    on behalf of the user, not the app SPN."""
    spec = {"role": "Assistant", "tool_configs": {"MCP_SERVERS": {"servers": ["g"]}}}
    mcp_config, call_config = LightAgentService._build_mcp_configs(
        spec, "grp-1", "USER_OBO_TOKEN"
    )
    assert mcp_config["group_id"] == "grp-1"
    assert mcp_config["user_token"] == "USER_OBO_TOKEN"
    assert call_config == {"group_id": "grp-1", "user_token": "USER_OBO_TOKEN"}
    # agent_spec is copied, not mutated.
    assert "user_token" not in spec


def test_build_mcp_configs_omits_token_when_absent():
    """No OBO token → user_token is omitted so PAT/SPN service auth still applies."""
    mcp_config, call_config = LightAgentService._build_mcp_configs(
        {"role": "Assistant"}, "grp-1", None
    )
    assert "user_token" not in mcp_config
    assert call_config == {"group_id": "grp-1"}


# ── _resolve_group_id — workspace scoping for chat (regression: MCP overrides) ─


def test_resolve_group_id_prefers_config_group_id_over_default_context():
    """Regression: when the runtime group_context lacks primary_group_id/group_ids
    (would yield "default"), the execution's authoritative config.group_id must win
    — otherwise workspace-scoped MCP servers resolve to 0 in chat "answer mode"."""
    config = SimpleNamespace(group_id="user_ws")
    ctx = SimpleNamespace(primary_group_id=None, group_ids=[])
    assert LightAgentService._resolve_group_id(config, ctx) == "user_ws"


def test_resolve_group_id_falls_back_to_primary_group_id():
    config = SimpleNamespace(group_id=None)
    ctx = SimpleNamespace(primary_group_id="pg", group_ids=["x"])
    assert LightAgentService._resolve_group_id(config, ctx) == "pg"


def test_resolve_group_id_falls_back_to_first_group_id():
    config = SimpleNamespace(group_id=None)
    ctx = SimpleNamespace(primary_group_id=None, group_ids=["g1", "g2"])
    assert LightAgentService._resolve_group_id(config, ctx) == "g1"


def test_resolve_group_id_derives_personal_group_from_email():
    """Regression (chat answer mode): with no selected workspace — no config.group_id,
    no primary_group_id, no group_ids — the personal workspace must be derived from
    the user's email (the id MCP workspace overrides are stored under), NOT "default"
    (which matches no MCP rows and silently drops workspace-enabled servers)."""
    config = SimpleNamespace(group_id=None)
    ctx = SimpleNamespace(
        primary_group_id=None, group_ids=[], group_email="nehme.tohme@databricks.com"
    )
    assert (
        LightAgentService._resolve_group_id(config, ctx)
        == "user_nehme_tohme_databricks_com"
    )


def test_resolve_group_id_defaults_when_no_group_or_email():
    config = SimpleNamespace(group_id=None)
    ctx = SimpleNamespace(primary_group_id=None, group_ids=[], group_email=None)
    assert LightAgentService._resolve_group_id(config, ctx) == "default"


def test_resolve_group_id_handles_missing_group_context():
    config = SimpleNamespace(group_id=None)
    assert LightAgentService._resolve_group_id(config, None) == "default"


# ── _event_matches_run — bus event → this run attribution ─────────────────────


def _evt(**kw):
    """A bus event with the given identity fields (others default to None)."""
    defaults = dict(agent_id=None, agent=None, from_agent=None, agent_role=None,
                    tool_name="t")
    defaults.update(kw)
    return SimpleNamespace(**defaults)


_SENTINEL_AGENT = SimpleNamespace(id="aid-1", role="Assistant")


def _match(event, source, *, agent=_SENTINEL_AGENT, agent_id="aid-1",
           role_lower="assistant", agent_llm=None):
    return LightAgentService._event_matches_run(
        event, source, agent=agent, agent_id=agent_id,
        role_lower=role_lower, agent_llm=agent_llm,
    )


def test_match_by_llm_source_when_no_agent_attribution():
    """Native MCP/function tool events arrive with from_agent nulled and a blank
    agent_id; the LLM-instance source is what attributes them to this run."""
    llm = MagicMock(name="agent.llm")
    # Event carries NO agent identity (the regression case).
    assert _match(_evt(), source=llm, agent_llm=llm) is True


def test_no_match_for_other_runs_llm_source():
    """A concurrent chat run's LLM instance must NOT match this run."""
    our_llm, other_llm = MagicMock(), MagicMock()
    assert _match(_evt(), source=other_llm, agent_llm=our_llm) is False


def test_match_by_agent_id():
    assert _match(_evt(agent_id="aid-1"), source=None, agent_id="aid-1") is True
    assert _match(_evt(agent_id="aid-2"), source=None, agent_id="aid-1") is False


def test_match_by_agent_identity_and_role():
    agent = SimpleNamespace(id="aid-1", role="Assistant")
    assert _match(_evt(agent=agent), source=None, agent=agent) is True
    assert _match(_evt(agent_role="Assistant"), source=None) is True


def test_no_match_when_nothing_identifies_the_run():
    """No source, no agent_id, no agent, no role → not ours (gets dropped+logged)."""
    assert _match(_evt(), source=None, agent_llm=MagicMock()) is False
