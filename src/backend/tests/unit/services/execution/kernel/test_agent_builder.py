"""Shared agent-build logic (build_agent_llm + build_agent_kwargs) used by BOTH
the crew path (agent_helpers.create_agent) and the flow path (agent_config).
These pin the canonical behavior so the two paths can never diverge."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.execution.kernel.agent_builder import (
    build_agent,
    build_agent_kwargs,
    build_agent_llm,
    DEFAULT_REASONING_EFFORT,
)


class _FakeLLM:
    """Stand-in for a built engine LLM (src.core.llm.transport.LLM subclass), which
    carries the resolved provider model and an optional reasoning_effort field."""

    def __init__(self, model):
        self.model = model


# ─────────────────────────────────────────────────────────────────────────────
# build_agent_kwargs
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildAgentKwargs:
    def _spec(self, **over):
        s = {"role": "R", "goal": "G", "backstory": "B"}
        s.update(over)
        return s

    def test_defaults_match_crew(self):
        kw = build_agent_kwargs(self._spec(), [], "llm-obj")
        assert kw["role"] == "R" and kw["goal"] == "G" and kw["backstory"] == "B"
        assert kw["llm"] == "llm-obj"
        assert kw["tools"] == []
        assert kw["verbose"] is True
        assert kw["allow_delegation"] is False
        assert kw["cache"] is False
        assert kw["max_retry_limit"] == 3
        assert kw["use_system_prompt"] is True
        assert kw["respect_context_window"] is True
        # SECURITY: always hardcoded False regardless of spec
        assert kw["allow_code_execution"] is False

    def test_allow_code_execution_forced_false(self):
        kw = build_agent_kwargs(self._spec(allow_code_execution=True), [], None)
        assert kw["allow_code_execution"] is False

    def test_overrides_and_tools(self):
        kw = build_agent_kwargs(
            self._spec(verbose=False, cache=True, max_retry_limit=7, allow_delegation=True),
            ["t1"],
            None,
        )
        assert kw["verbose"] is False
        assert kw["cache"] is True
        assert kw["max_retry_limit"] == 7
        assert kw["allow_delegation"] is True
        assert kw["tools"] == ["t1"]

    def test_additional_params_only_when_set(self):
        kw = build_agent_kwargs(self._spec(max_iter=5, max_execution_time=120, max_rpm=None), [], None)
        assert kw["max_iter"] == 5
        assert kw["max_execution_time"] == 120
        # None values are not propagated
        assert "max_rpm" not in kw

    # ── Reasoning is NOT an Agent kwarg anymore ───────────────────────────────
    # The CrewAI-style planner/replan loop was removed; reasoning is the MODEL's
    # native reasoning budget and lands on the agent's LLM in build_agent_llm.
    def test_reasoning_never_reaches_the_agent(self):
        """Neither the deprecated reasoning flags nor the (inert) planning fields
        are ever passed to the Agent, with or without reasoning enabled."""
        for spec in (
            self._spec(),
            self._spec(reasoning=True),
            self._spec(reasoning=True, reasoning_config={"reasoning_effort": "high"}),
            self._spec(reasoning=True, max_reasoning_attempts=5),
        ):
            kw = build_agent_kwargs(spec, [], None)
            assert "planning_config" not in kw
            assert "planning" not in kw
            assert "reasoning" not in kw
            assert "max_reasoning_attempts" not in kw

    def test_default_reasoning_effort_is_low(self):
        """Pin the shipped default thinking budget."""
        assert DEFAULT_REASONING_EFFORT == "low"

    def test_memory_never_propagated(self):
        kw = build_agent_kwargs(self._spec(memory=True), [], None)
        assert "memory" not in kw

    def test_templates_and_passthrough_default(self):
        kw = build_agent_kwargs(self._spec(system_template="SYS"), [], None)
        assert kw["system_template"] == "SYS"
        # passthrough user template supplied when only system_template configured
        assert kw["prompt_template"] == "{{ .Prompt }}"

    def test_explicit_prompt_template_kept(self):
        kw = build_agent_kwargs(
            self._spec(system_template="SYS", prompt_template="P", response_template="RESP"),
            [],
            None,
        )
        assert kw["prompt_template"] == "P"
        assert kw["response_template"] == "RESP"


# ─────────────────────────────────────────────────────────────────────────────
# build_agent_llm
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildAgentLlm:
    @pytest.mark.asyncio
    async def test_string_llm_uses_configure_kasal_llm(self):
        with patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(return_value="LLM")
            out = await build_agent_llm({"llm": "my-model"}, group_id="grp")
            assert out == "LLM"
            MockLM.configure_kasal_llm.assert_awaited_once_with("my-model", "grp", None)

    @pytest.mark.asyncio
    async def test_temperature_converted_0_100_to_0_1(self):
        with patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(return_value="LLM")
            await build_agent_llm({"llm": "m", "temperature": 50}, group_id="grp")
            MockLM.configure_kasal_llm.assert_awaited_once_with("m", "grp", 0.5)

    @pytest.mark.asyncio
    async def test_dict_llm_uses_model_and_applies_overrides(self):
        class FakeLLM:
            pass

        fake = FakeLLM()
        with patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(return_value=fake)
            out = await build_agent_llm(
                {"llm": {"model": "m2", "top_p": 0.9, "stop": None}}, group_id="grp"
            )
            assert out is fake
            MockLM.configure_kasal_llm.assert_awaited_once_with("m2", "grp", None)
            assert fake.top_p == 0.9  # override applied
            assert not hasattr(fake, "stop")  # None override skipped

    @pytest.mark.asyncio
    async def test_no_llm_uses_default_model_without_temperature(self):
        with patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(return_value="LLM")
            await build_agent_llm({}, group_id="grp", default_model="databricks-llama-4-maverick")
            MockLM.configure_kasal_llm.assert_awaited_once_with(
                "databricks-llama-4-maverick", "grp"
            )

    @pytest.mark.asyncio
    async def test_missing_group_id_raises(self):
        with pytest.raises(ValueError):
            await build_agent_llm({"llm": "m"}, group_id=None)

    @pytest.mark.asyncio
    async def test_dict_llm_missing_group_id_raises(self):
        with pytest.raises(ValueError):
            await build_agent_llm({"llm": {"model": "m"}}, group_id=None)

    @pytest.mark.asyncio
    async def test_no_llm_missing_group_id_raises(self):
        with pytest.raises(ValueError):
            await build_agent_llm({}, group_id=None)

    @pytest.mark.asyncio
    async def test_configure_failure_falls_back_to_model_string(self):
        with patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(side_effect=RuntimeError("boom"))
            out = await build_agent_llm({"llm": "fallback-model"}, group_id="grp")
            assert out == "fallback-model"


# ─────────────────────────────────────────────────────────────────────────────
# Reasoning = the MODEL's native reasoning budget on the agent's own LLM.
# The effort must reach the provider for models that support the parameter and
# must be DROPPED SILENTLY for models that do not (sending it would 400 on a
# strict gateway and break previously-working runs).
# ─────────────────────────────────────────────────────────────────────────────


class TestReasoningEffortReachesTheLLM:
    async def _build(self, model, **spec_over):
        spec = {"llm": model}
        spec.update(spec_over)
        fake = _FakeLLM(f"databricks/{model}")
        with patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(return_value=fake)
            out = await build_agent_llm(spec, group_id="grp", label="A")
        return out

    @pytest.mark.asyncio
    async def test_effort_lands_on_supported_model(self):
        """gpt-5 accepts a native reasoning budget → the param is set on the LLM,
        which kasal_engine emits as reasoning_effort (chat) / reasoning.effort
        (Responses API)."""
        llm = await self._build(
            "databricks-gpt-5-2",
            reasoning=True,
            reasoning_config={"reasoning_effort": "high"},
        )
        assert llm.reasoning_effort == "high"

    @pytest.mark.asyncio
    async def test_effort_defaults_to_low_when_unspecified(self):
        llm = await self._build("databricks-gpt-5-2", reasoning=True)
        assert llm.reasoning_effort == DEFAULT_REASONING_EFFORT

    @pytest.mark.asyncio
    async def test_effort_absent_for_unsupported_model(self):
        """Claude uses `thinking: {budget_tokens}`, not reasoning_effort — the
        preference must be dropped, never sent."""
        llm = await self._build(
            "databricks-claude-sonnet-4-5",
            reasoning=True,
            reasoning_config={"reasoning_effort": "high"},
        )
        assert not hasattr(llm, "reasoning_effort")

    @pytest.mark.asyncio
    async def test_effort_absent_when_reasoning_toggle_off(self):
        llm = await self._build(
            "databricks-gpt-5-2", reasoning_config={"reasoning_effort": "high"}
        )
        assert not hasattr(llm, "reasoning_effort")

    @pytest.mark.asyncio
    async def test_unknown_effort_value_is_ignored(self):
        llm = await self._build(
            "databricks-gpt-5-2", reasoning=True, reasoning_config={"reasoning_effort": "turbo"}
        )
        assert not hasattr(llm, "reasoning_effort")

    @pytest.mark.asyncio
    async def test_string_fallback_llm_is_never_mutated(self):
        """A configuration failure yields a bare model-name string — applying the
        budget must not raise."""
        with patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(side_effect=RuntimeError("boom"))
            out = await build_agent_llm(
                {"llm": "databricks-gpt-5-2", "reasoning": True}, group_id="grp"
            )
        assert out == "databricks-gpt-5-2"

    @pytest.mark.asyncio
    async def test_kill_switch_env_disables_effort(self, monkeypatch):
        monkeypatch.setenv("KASAL_REASONING_EFFORT_DISABLED", "true")
        llm = await self._build("databricks-gpt-5-2", reasoning=True)
        assert not hasattr(llm, "reasoning_effort")

    @pytest.mark.asyncio
    async def test_env_allow_list_extends_supported_models(self, monkeypatch):
        monkeypatch.setenv("KASAL_REASONING_EFFORT_MODELS", "my-thinking-endpoint")
        llm = await self._build("my-thinking-endpoint", reasoning=True)
        assert llm.reasoning_effort == "low"


# ─────────────────────────────────────────────────────────────────────────────
# build_agent — the SINGLE builder both crew and flow call
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildAgent:
    @pytest.mark.asyncio
    async def test_builds_llm_kwargs_preamble_construction_and_custom_attrs(self):
        with patch("src.services.execution.kernel.agent_builder.Agent") as MockAgent, \
             patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(return_value="LLM-OBJ")
            MockAgent.return_value = MagicMock()
            spec = {"role": "R", "goal": "G", "backstory": "B", "llm": "m", "temperature": 50}
            agent = await build_agent(
                spec,
                ["t"],
                group_id="g1",
                default_model="databricks-llama-4-maverick",
                label="A",
                extra_kwargs={"config": {"x": 1}},
                custom_attrs={"_kasal_memory_disabled": True},
            )
        # LLM built the crew way: explicit group + converted temperature
        MockLM.configure_kasal_llm.assert_awaited_once_with("m", "g1", 0.5)
        kwargs = MockAgent.call_args[1]
        assert kwargs["llm"] == "LLM-OBJ"
        assert kwargs["tools"] == ["t"]
        assert kwargs["config"] == {"x": 1}  # extra_kwargs merged before construction
        assert "SECURITY INSTRUCTION" in kwargs["backstory"]  # preamble injected
        assert agent._kasal_memory_disabled is True  # custom attr set

    @pytest.mark.asyncio
    async def test_no_extra_kwargs_or_custom_attrs(self):
        with patch("src.services.execution.kernel.agent_builder.Agent") as MockAgent, \
             patch("src.services.llm.manager.LLMManager") as MockLM:
            MockLM.configure_kasal_llm = AsyncMock(return_value="LLM")
            MockAgent.return_value = MagicMock()
            await build_agent(
                {"role": "R", "goal": "G", "backstory": "B"},
                [],
                group_id="g",
                default_model="gpt-4o",
                label="A",
            )
        # No llm in spec → default_model, no temperature
        MockLM.configure_kasal_llm.assert_awaited_once_with("gpt-4o", "g")
