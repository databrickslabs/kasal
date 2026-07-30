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
