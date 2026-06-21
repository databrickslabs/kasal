"""Shared agent-build logic (build_agent_llm + build_agent_kwargs) used by BOTH
the crew path (agent_helpers.create_agent) and the flow path (agent_config).
These pin the canonical behavior so the two paths can never diverge."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.common.agent_builder import (
    build_agent,
    build_agent_kwargs,
    build_agent_llm,
    DEFAULT_REASONING_CONFIG,
)


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

    # ── Reasoning → PlanningConfig (CrewAI 1.14.x migration) ──────────────────
    def test_reasoning_off_sets_no_planning_config(self):
        """No reasoning => no planning_config, and the deprecated reasoning flags
        are never passed to the Agent."""
        kw = build_agent_kwargs(self._spec(), [], None)
        assert "planning_config" not in kw
        assert "reasoning" not in kw
        assert "max_reasoning_attempts" not in kw

    def test_reasoning_on_builds_bounded_planning_config(self):
        """reasoning=True => a bounded PlanningConfig (NOT CrewAI's expansive
        defaults), and the deprecated reasoning flags are dropped to avoid the
        'pass both → cap ignored' footgun."""
        kw = build_agent_kwargs(self._spec(reasoning=True), [], None)
        assert "reasoning" not in kw and "max_reasoning_attempts" not in kw
        pc = kw["planning_config"]
        assert pc.reasoning_effort == DEFAULT_REASONING_CONFIG["reasoning_effort"]
        assert pc.max_attempts == DEFAULT_REASONING_CONFIG["max_attempts"]
        assert pc.max_steps == DEFAULT_REASONING_CONFIG["max_steps"]
        assert pc.max_step_iterations == DEFAULT_REASONING_CONFIG["max_step_iterations"]
        assert pc.step_timeout == DEFAULT_REASONING_CONFIG["step_timeout"]
        assert pc.max_replans == DEFAULT_REASONING_CONFIG["max_replans"]
        # The bound that matters most: max_attempts must NOT be None (CrewAI's default),
        # else the plan-refine loop runs `while not ready` forever.
        assert pc.max_attempts is not None

    def test_reasoning_config_overrides_apply(self):
        kw = build_agent_kwargs(
            self._spec(reasoning=True, reasoning_config={
                "reasoning_effort": "low", "max_steps": 4,
                "max_step_iterations": 3, "step_timeout": 30, "max_replans": 0,
            }),
            [], None,
        )
        pc = kw["planning_config"]
        assert pc.reasoning_effort == "low"
        assert pc.max_steps == 4
        assert pc.max_step_iterations == 3
        assert pc.step_timeout == 30
        assert pc.max_replans == 0  # 0 is valid (no replanning) and must be honored

    def test_legacy_max_reasoning_attempts_maps_to_max_attempts(self):
        """The legacy/flow field still feeds PlanningConfig.max_attempts."""
        kw = build_agent_kwargs(self._spec(reasoning=True, max_reasoning_attempts=5), [], None)
        assert kw["planning_config"].max_attempts == 5

    def test_default_profile_is_low_and_small(self):
        """Pin the shipped default to the validated low/small profile (self-terminates
        ~15s on the real model). A regression to medium/large reintroduces the
        rate-limit retry-loop runaway."""
        assert DEFAULT_REASONING_CONFIG == {
            "reasoning_effort": "low",
            "max_attempts": 1,
            "max_steps": 3,
            "max_step_iterations": 3,
            "step_timeout": 20,
            "max_replans": 0,
        }

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
    async def test_string_llm_uses_configure_crewai_llm(self):
        with patch("src.core.llm_manager.LLMManager") as MockLM:
            MockLM.configure_crewai_llm = AsyncMock(return_value="LLM")
            out = await build_agent_llm({"llm": "my-model"}, group_id="grp")
            assert out == "LLM"
            MockLM.configure_crewai_llm.assert_awaited_once_with("my-model", "grp", None)

    @pytest.mark.asyncio
    async def test_temperature_converted_0_100_to_0_1(self):
        with patch("src.core.llm_manager.LLMManager") as MockLM:
            MockLM.configure_crewai_llm = AsyncMock(return_value="LLM")
            await build_agent_llm({"llm": "m", "temperature": 50}, group_id="grp")
            MockLM.configure_crewai_llm.assert_awaited_once_with("m", "grp", 0.5)

    @pytest.mark.asyncio
    async def test_dict_llm_uses_model_and_applies_overrides(self):
        class FakeLLM:
            pass

        fake = FakeLLM()
        with patch("src.core.llm_manager.LLMManager") as MockLM:
            MockLM.configure_crewai_llm = AsyncMock(return_value=fake)
            out = await build_agent_llm(
                {"llm": {"model": "m2", "top_p": 0.9, "stop": None}}, group_id="grp"
            )
            assert out is fake
            MockLM.configure_crewai_llm.assert_awaited_once_with("m2", "grp", None)
            assert fake.top_p == 0.9  # override applied
            assert not hasattr(fake, "stop")  # None override skipped

    @pytest.mark.asyncio
    async def test_no_llm_uses_default_model_without_temperature(self):
        with patch("src.core.llm_manager.LLMManager") as MockLM:
            MockLM.configure_crewai_llm = AsyncMock(return_value="LLM")
            await build_agent_llm({}, group_id="grp", default_model="databricks-llama-4-maverick")
            MockLM.configure_crewai_llm.assert_awaited_once_with(
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
        with patch("src.core.llm_manager.LLMManager") as MockLM:
            MockLM.configure_crewai_llm = AsyncMock(side_effect=RuntimeError("boom"))
            out = await build_agent_llm({"llm": "fallback-model"}, group_id="grp")
            assert out == "fallback-model"


# ─────────────────────────────────────────────────────────────────────────────
# build_agent — the SINGLE builder both crew and flow call
# ─────────────────────────────────────────────────────────────────────────────


class TestBuildAgent:
    @pytest.mark.asyncio
    async def test_builds_llm_kwargs_preamble_construction_and_custom_attrs(self):
        with patch("src.engines.crewai.common.agent_builder.Agent") as MockAgent, \
             patch("src.core.llm_manager.LLMManager") as MockLM:
            MockLM.configure_crewai_llm = AsyncMock(return_value="LLM-OBJ")
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
        MockLM.configure_crewai_llm.assert_awaited_once_with("m", "g1", 0.5)
        kwargs = MockAgent.call_args[1]
        assert kwargs["llm"] == "LLM-OBJ"
        assert kwargs["tools"] == ["t"]
        assert kwargs["config"] == {"x": 1}  # extra_kwargs merged before construction
        assert "SECURITY INSTRUCTION" in kwargs["backstory"]  # preamble injected
        assert agent._kasal_memory_disabled is True  # custom attr set

    @pytest.mark.asyncio
    async def test_no_extra_kwargs_or_custom_attrs(self):
        with patch("src.engines.crewai.common.agent_builder.Agent") as MockAgent, \
             patch("src.core.llm_manager.LLMManager") as MockLM:
            MockLM.configure_crewai_llm = AsyncMock(return_value="LLM")
            MockAgent.return_value = MagicMock()
            await build_agent(
                {"role": "R", "goal": "G", "backstory": "B"},
                [],
                group_id="g",
                default_model="gpt-4o",
                label="A",
            )
        # No llm in spec → default_model, no temperature
        MockLM.configure_crewai_llm.assert_awaited_once_with("gpt-4o", "g")
