"""Budget profiles: the caps a mode actually buys."""

import pytest

from src.services.execution.config.budget_profile import resolve_budget_profile


class TestProfiles:
    def test_modes_are_ordered_by_how_much_work_they_buy(self):
        chat = resolve_budget_profile("chat")
        research = resolve_budget_profile("research")
        deep = resolve_budget_profile("deep")
        assert chat.max_iter < research.max_iter < deep.max_iter
        assert chat.run_wall_clock < research.run_wall_clock < deep.run_wall_clock
        assert (
            chat.guardrail_max_retries
            < research.guardrail_max_retries
            < deep.guardrail_max_retries
        )

    def test_chat_gets_no_guardrail_retries(self):
        """The light path must stay sub-second; none of the verification
        machinery applies to it."""
        assert resolve_budget_profile("chat").guardrail_max_retries == 0

    def test_run_clock_exceeds_the_per_call_clock(self):
        """A run cap tighter than one call's cap would make the per-call value
        unreachable and the profile self-contradictory."""
        for mode in ("chat", "research", "deep"):
            profile = resolve_budget_profile(mode)
            assert profile.run_wall_clock > profile.max_execution_time

    def test_deep_is_not_starved_relative_to_an_unprofiled_mode(self):
        """The bug this guards: deep shipped at 600s while research — which has
        NO profile and so falls to the engine default of 900s — effectively got
        more. Deep was simultaneously swapped onto `sonar-deep-research`, which
        answers in minutes rather than seconds, so the mode meant to think
        longest had the least time and the slowest tool. Any applied profile
        must be at least the default it displaces."""
        from src.services.execution.config.budget_profile import (
            _ENGINE_DEFAULT_MAX_EXECUTION_TIME,
        )
        from src.services.execution.kernel.agent_builder import (
            DEFAULT_AGENT_MAX_EXECUTION_TIME,
        )
        from src.services.generation.crew.answer_mode import GATED_MODES

        assert _ENGINE_DEFAULT_MAX_EXECUTION_TIME == DEFAULT_AGENT_MAX_EXECUTION_TIME
        for mode in GATED_MODES:
            profile = resolve_budget_profile(mode)
            assert profile.max_execution_time >= DEFAULT_AGENT_MAX_EXECUTION_TIME

    @pytest.mark.parametrize("mode", [None, "", "nonsense", "DEEP RESEARCH"])
    def test_unknown_modes_fall_back_to_the_tightest_profile(self, mode):
        """A typo must not accidentally buy an hour of runtime."""
        assert resolve_budget_profile(mode) == resolve_budget_profile("chat")

    def test_case_and_whitespace_are_tolerated(self):
        assert resolve_budget_profile("  DEEP  ") == resolve_budget_profile("deep")


class TestEnvOverrides:
    def test_override_applies(self, monkeypatch):
        # Deliberately not the shipped default, or the test would pass with the
        # override wired to nothing.
        monkeypatch.setenv("KASAL_BUDGET_DEEP_RUN_WALL_CLOCK", "5400")
        assert resolve_budget_profile("deep").run_wall_clock == 5400

    def test_override_is_scoped_to_its_mode(self, monkeypatch):
        monkeypatch.setenv("KASAL_BUDGET_DEEP_MAX_ITER", "99")
        assert resolve_budget_profile("deep").max_iter == 99
        assert resolve_budget_profile("research").max_iter == 15

    @pytest.mark.parametrize("bad", ["bogus", "0", "-5", ""])
    def test_unusable_override_falls_back_to_the_default(self, monkeypatch, bad):
        """Zero would mean 'no rounds at all' rather than 'unlimited' — a
        footgun disguised as a kill switch."""
        monkeypatch.setenv("KASAL_BUDGET_DEEP_MAX_ITER", bad)
        assert resolve_budget_profile("deep").max_iter == 30


class TestNoInertKnobs:
    def test_every_field_is_enforced_somewhere(self):
        """A knob that looks like a cap and is not one is worse than no knob —
        this codebase already carries max_rpm, declared everywhere and enforced
        nowhere. If you add a field here, wire it before you land it."""
        from dataclasses import fields

        from src.services.execution.config.budget_profile import BudgetProfile

        assert {f.name for f in fields(BudgetProfile)} == {
            "max_iter",
            "max_execution_time",
            "run_wall_clock",
            "guardrail_max_retries",
        }


class TestExecutionEffort:
    def test_crew_caps_are_snapshotted_and_survive_serialization(self):
        from src.schemas.execution import CrewConfig
        from src.services.execution.config.crew_config_builder import CrewConfigBuilder
        from src.services.execution.config_adapter import adapt_config

        agents = {
            "a": {"role": "Researcher", "max_iter": 25, "max_execution_time": 900}
        }
        config = CrewConfig(
            agents_yaml=agents,
            tasks_yaml={"t": {"description": "Research"}},
            inputs={"execution_effort": {"tier": "low"}},
        )
        assert agents["a"]["max_iter"] == 25  # no mutation of the source catalog
        assert config.agents_yaml["a"]["max_iter"] == 8
        assert config.tasks_yaml["t"] == {"description": "Research"}
        replay = CrewConfig.model_validate(config.model_dump())
        assert replay.inputs["resolved_effort"]["run_max_seconds"] == 180
        assert CrewConfigBuilder(adapt_config(replay))._resolve_run_wall_clock() == 180
        # The primary crew path assembles a fresh dict and carries inputs.
        assert (
            CrewConfigBuilder({"inputs": replay.inputs})._resolve_run_wall_clock()
            == 180
        )

    def test_custom_limit_binds_the_agent_and_catalog_settings_are_accepted(self):
        from src.schemas.execution import CrewConfig

        cfg = CrewConfig(
            agents_yaml={"a": {}},
            inputs={
                "reasoning_config": {
                    "execution_effort": {
                        "tier": "high",
                        "run_max_seconds": 60,
                        "max_iter": 12,
                    }
                }
            },
        )
        assert cfg.agents_yaml["a"]["max_execution_time"] == 60
        assert cfg.agents_yaml["a"]["max_iter"] == 12
        assert cfg.inputs["execution_effort"]["tier"] == "high"

    @pytest.mark.parametrize(
        "bad",
        [
            {"tier": "ultra"},
            {"tier": "low", "max_iter": 0},
            {"tier": "high", "run_max_seconds": -1},
            {"tier": "high", "run_max_seconds": 14401},
        ],
    )
    def test_invalid_settings_fail_before_execution(self, bad):
        from pydantic import ValidationError

        from src.schemas.execution import CrewConfig

        with pytest.raises(ValidationError):
            CrewConfig(inputs={"execution_effort": bad})

    def test_existing_api_requests_keep_their_agent_settings(self):
        from src.schemas.execution import CrewConfig

        cfg = CrewConfig(agents_yaml={"a": {"max_iter": 42}})
        assert cfg.agents_yaml["a"] == {"max_iter": 42}
        assert "resolved_effort" not in cfg.inputs

    @pytest.mark.parametrize(
        "model,tier,native",
        [
            ("databricks-gpt-5", "max", "high"),
            ("databricks-gpt-5-2", "xhigh", "xhigh"),
            ("databricks-gemini-3-flash", "max", "high"),
        ],
    )
    def test_each_model_gets_only_a_supported_native_effort(self, model, tier, native):
        from types import SimpleNamespace

        from src.core.llm.model_capabilities import allowed_efforts
        from src.services.execution.kernel.agent_effort import apply_execution_effort

        llm = SimpleNamespace(model=model, max_tokens=24000, max_completion_tokens=None)
        apply_execution_effort(llm, {"execution_effort": {"tier": tier}})
        assert llm.reasoning_effort == native
        assert llm.reasoning_effort in allowed_efforts(model)
        assert llm.max_tokens <= 24000

    def test_manual_and_adaptive_thinking_use_distinct_fields(self):
        from types import SimpleNamespace

        from src.services.execution.kernel.agent_effort import apply_execution_effort

        manual = SimpleNamespace(model="databricks-claude-sonnet-4-5", max_tokens=8192)
        apply_execution_effort(manual, {"execution_effort": {"tier": "high"}})
        assert 1024 <= manual.thinking_budget_tokens < manual.max_tokens
        adaptive = SimpleNamespace(model="databricks-claude-opus-5", max_tokens=25000)
        apply_execution_effort(adaptive, {"execution_effort": {"tier": "max"}})
        assert adaptive.thinking_effort == "max"
        assert adaptive.reasoning_effort is None

    def test_non_reasoning_model_still_gets_output_allowance(self):
        from types import SimpleNamespace

        from src.services.execution.kernel.agent_effort import apply_execution_effort

        llm = SimpleNamespace(model="custom-model", max_tokens=25000)
        apply_execution_effort(llm, {"execution_effort": {"tier": "low"}})
        assert llm.max_tokens == 8192
        assert not hasattr(llm, "reasoning_effort")

    def test_dispatch_settings_reach_streaming_generation(self):
        from src.schemas.dispatcher import DispatcherRequest
        from src.services.chat.streaming_request import streaming_request_for

        req = DispatcherRequest(
            message="Gather news", execution_effort={"tier": "high"}
        )
        assert streaming_request_for(req, None, []).execution_effort.tier == "high"


class TestEffortDeadlines:
    def test_wrapup_inherits_deadline_and_unrelated_calls_do_not(self, monkeypatch):
        from types import SimpleNamespace

        from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
        from src.core.llm.transport.request_deadline import (
            bounded_params,
            call_deadline,
        )

        clock = [100.0]
        monkeypatch.setattr("time.monotonic", lambda: clock[0])
        with call_deadline(SimpleNamespace(max_execution_time=10)):
            assert bounded_params({})["timeout"] == 10
            clock[0] = 109
            with call_deadline(None):
                assert bounded_params({})["timeout"] == 1
                clock[0] = 111
                with pytest.raises(ExecutionBudgetExceededError):
                    bounded_params({})
        assert bounded_params({}) == {}

    def test_whole_run_deadline_includes_agentless_memory_calls(self, monkeypatch):
        from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
        from src.core.llm.transport.request_deadline import (
            bounded_params,
            call_deadline,
            run_deadline,
        )

        clock = [100.0]
        monkeypatch.setattr("time.monotonic", lambda: clock[0])
        with run_deadline(5):
            clock[0] = 106
            with call_deadline(None), pytest.raises(ExecutionBudgetExceededError):
                bounded_params({})

    @pytest.mark.asyncio
    @pytest.mark.parametrize("saved_agent", [False, True])
    async def test_light_chat_retains_partial_output_at_its_limit(self, saved_agent):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, Mock

        from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
        from src.services.chat.turn_kickoff import kickoff_chat_turn

        agent = SimpleNamespace()
        service = SimpleNamespace(
            _kickoff_with_mlflow_trace=AsyncMock(
                side_effect=ExecutionBudgetExceededError(
                    "limit reached", partial="Two verified stories"
                )
            )
        )
        config = SimpleNamespace(
            inputs={} if saved_agent else {"execution_effort": {"tier": "low"}}
        )
        result = await kickoff_chat_turn(
            service,
            agent,
            config,
            "run",
            "trace",
            None,
            "group",
            "Gather news",
            {"execution_effort": {"tier": "low"}} if saved_agent else {},
            "",
            None,
            Mock(),
        )
        assert result.budget_exhausted is True
        assert "Two verified stories" in result.raw
        assert "Execution limit reached" in result.raw
        assert agent.run_deadline > 0
        assert agent._kasal_run_deadline == agent.run_deadline


class TestSavedAgentEffort:
    def test_api_create_and_patch_apply_profile_defaults_but_keep_explicit_limits(self):
        from src.schemas.agent import AgentCreate, AgentUpdate
        from src.schemas.execution import CrewConfig

        saved = AgentCreate(
            role="Researcher",
            goal="Research",
            backstory="Expert",
            execution_effort={"tier": "high"},
            max_iter=7,
        )
        assert saved.max_iter == 7
        assert saved.max_execution_time == 900
        snapshot = saved.model_dump(mode="json")
        replay = CrewConfig(agents_yaml={"a": snapshot})
        assert replay.agents_yaml["a"]["max_iter"] == 7
        assert replay.inputs.get("execution_effort") is None
        patch = AgentUpdate(execution_effort={"tier": "low"})
        assert patch.model_dump(exclude_unset=True)["max_iter"] == 8
        assert AgentUpdate(execution_effort=None).model_dump(exclude_unset=True) == {
            "execution_effort": None
        }

    def test_saved_profiles_share_one_crew_clock(self):
        from src.services.execution.config.crew_config_builder import CrewConfigBuilder

        config = {
            "agents": [
                {"execution_effort": {"tier": "low"}},
                {"execution_effort": {"tier": "high"}},
            ]
        }
        assert CrewConfigBuilder(config)._resolve_run_wall_clock() == 1200
        config["inputs"] = {"execution_effort": {"tier": "low", "run_max_seconds": 40}}
        assert CrewConfigBuilder(config)._resolve_run_wall_clock() == 40

    def test_effort_never_weakens_task_guardrails(self):
        from src.schemas.execution import CrewConfig

        tasks = {
            "task": {
                "guardrail": "Required validation",
                "max_retries": 3,
                "guardrail_on_exhausted": "raise",
                "on_budget_exceeded": "raise",
            }
        }
        cfg = CrewConfig(tasks_yaml=tasks, inputs={"execution_effort": {"tier": "low"}})
        assert cfg.tasks_yaml == tasks

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "model,native_field",
        [
            ("databricks-gpt-5", "reasoning_effort"),
            ("databricks-claude-opus-5", "thinking_effort"),
        ],
    )
    async def test_saved_native_overrides_win_until_a_run_explicitly_overrides(
        self, monkeypatch, model, native_field
    ):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from src.schemas.execution import CrewConfig
        from src.services.execution.kernel.agent_builder import build_agent_llm

        def new_llm(*args):
            return SimpleNamespace(
                model=model, max_tokens=64000, max_completion_tokens=None
            )

        engine = SimpleNamespace(name="test", build_llm=AsyncMock(side_effect=new_llm))
        monkeypatch.setattr(
            "src.services.execution.kernel.agent_builder.active_harness", lambda: engine
        )
        spec = {
            "llm": model,
            "execution_effort": {"tier": "high"},
            "reasoning_effort": "low",
            "max_tokens": 2000,
        }
        llm = await build_agent_llm(spec, group_id="workspace")
        assert getattr(llm, native_field) == "low"
        assert llm.max_tokens == 2000
        cfg = CrewConfig(
            agents_yaml={"a": spec}, inputs={"execution_effort": {"tier": "high"}}
        )
        overridden = await build_agent_llm(cfg.agents_yaml["a"], group_id="workspace")
        assert getattr(overridden, native_field) == "high"
        assert overridden.max_tokens == 32768
        assert spec["max_tokens"] == 2000

    def test_flow_projection_preserves_saved_effort_and_native_overrides(self):
        from types import SimpleNamespace

        from src.services.flow_builder.modules.agent_adapter import AgentConfig

        agent = SimpleNamespace(
            role="Researcher",
            goal="Research",
            backstory="Expert",
            execution_effort={"tier": "low"},
            reasoning_effort="high",
            thinking_budget_tokens=2048,
        )
        spec = AgentConfig._agent_data_to_spec(agent)
        assert spec["execution_effort"] == {"tier": "low"}
        assert spec["reasoning_effort"] == "high"
        assert spec["thinking_budget_tokens"] == 2048

    @pytest.mark.asyncio
    async def test_snapshot_freezes_saved_settings_and_respects_workspace(
        self, monkeypatch
    ):
        from types import SimpleNamespace
        from unittest.mock import AsyncMock

        from src.schemas.execution import CrewConfig
        from src.services.catalog.agents import AgentService
        from src.services.execution.config.agent_settings_snapshot import (
            snapshot_agent_settings,
        )

        agent_id = "12345678-1234-1234-1234-123456789012"
        row = SimpleNamespace(
            group_id="space",
            execution_effort={"tier": "high"},
            max_iter=21,
            max_execution_time=180,
            reasoning_effort="low",
        )
        monkeypatch.setattr(
            AgentService, "get_with_group_check", AsyncMock(return_value=row)
        )
        config = CrewConfig(agents_yaml={f"agent_agent-{agent_id}": {"max_iter": 5}})
        await snapshot_agent_settings(
            config, None, SimpleNamespace(primary_group_id="space")
        )
        snapshot = config.model_dump(mode="json")
        agent = next(iter(snapshot["agents_yaml"].values()))
        assert agent["max_iter"] == 5
        assert agent["max_execution_time"] == 180
        assert agent["execution_effort"] == {"tier": "high"}
        assert agent["agent_settings_snapshot"] is True
        row.execution_effort = {"tier": "low"}
        assert agent["execution_effort"] == {"tier": "high"}
        other = CrewConfig(agents_yaml={agent_id: {}})
        await snapshot_agent_settings(
            other, None, SimpleNamespace(primary_group_id="another")
        )
        assert other.agents_yaml[agent_id] == {}
