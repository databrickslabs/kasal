"""Deciding a run's harness once, and making that decision travel.

The behaviour these guard is the whole reason the column exists: a run must not
change harness because someone changed a setting while it was queued, running, or
waiting to be resumed.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.execution import harness_choice
from src.services.execution.harnesses import selection
from src.services.execution.harnesses import coerce as engine_choice_coerce
from src.services.execution.harnesses.binding import HarnessName
from src.services.execution.harness_choice import adopt_in_subprocess
from src.services.execution.harnesses import (
    DEFAULT_HARNESS,
    HARNESS_CONFIG_KEY,
    HARNESS_ENV_VAR,
    reset_for_tests,
)


@pytest.fixture(autouse=True)
def _clean_selection():
    selection.reset_for_tests()
    yield
    selection.reset_for_tests()


class TestResolve:
    @pytest.mark.asyncio
    async def test_a_stored_value_wins_without_reading_the_setting(self):
        """A resume must not consult the configuration at all."""
        with patch("src.services.settings.engine.EngineConfigService") as service:
            resolved = await harness_choice.resolve_run_harness(MagicMock(), "crewai")
        assert resolved is HarnessName.CREWAI
        service.assert_not_called()

    @pytest.mark.asyncio
    async def test_reads_the_setting_through_the_settings_service(self):
        service = MagicMock(get_harness=AsyncMock(return_value="crewai"))
        session = MagicMock()
        with patch(
            "src.services.settings.engine.EngineConfigService", return_value=service
        ) as cls:
            resolved = await harness_choice.resolve_run_harness(session)
        assert resolved is HarnessName.CREWAI
        # On the caller's session — never one it acquired itself.
        cls.assert_called_once_with(session)

    @pytest.mark.asyncio
    async def test_a_failed_config_read_degrades_instead_of_failing_the_run(self):
        with patch(
            "src.services.settings.engine.EngineConfigService",
            side_effect=RuntimeError("no row"),
        ):
            resolved = await harness_choice.resolve_run_harness(MagicMock())
        assert resolved is HarnessName.KASAL


class TestTravel:
    def test_stamp_mutates_the_caller_s_config(self):
        """A copy would silently drop the stamp for whoever holds the original."""
        config = {"agents": []}
        returned = harness_choice.stamp_on_config(config, "crewai")
        assert returned is config
        assert config[harness_choice.HARNESS_CONFIG_KEY] == "crewai"

    def test_stamp_round_trips(self):
        config = harness_choice.stamp_on_config({}, HarnessName.CREWAI)
        assert harness_choice.engine_from_config(config) is HarnessName.CREWAI

    @pytest.mark.parametrize("value", [None, {}, {"_harness": "bogus"}, "x"])
    def test_reading_back_nothing_usable_is_none(self, value):
        assert harness_choice.engine_from_config(value) is None

    def test_subprocess_env_carries_the_same_string(self):
        assert harness_choice.subprocess_env("crewai") == {
            selection.HARNESS_ENV_VAR: "crewai"
        }


class TestAdoptInSubprocess:
    def test_the_payload_beats_an_inherited_environment(self, monkeypatch):
        """The env var can come from a parent reconfigured since this run began."""
        monkeypatch.setenv(selection.HARNESS_ENV_VAR, "kasal")
        adopted = harness_choice.adopt_in_subprocess({"_harness": "crewai"})
        assert adopted is HarnessName.CREWAI
        assert selection.active_name() is HarnessName.CREWAI

    def test_falls_back_to_the_environment_when_the_payload_says_nothing(
        self, monkeypatch
    ):
        monkeypatch.setenv(selection.HARNESS_ENV_VAR, "crewai")
        assert harness_choice.adopt_in_subprocess({}) is HarnessName.CREWAI

    def test_falls_back_to_kasal_when_nothing_says_anything(self):
        assert harness_choice.adopt_in_subprocess(None) is HarnessName.KASAL

    def test_pins_the_whole_interpreter_not_a_context(self):
        """A crew runs its tasks on a thread pool, which copies no ContextVar."""
        harness_choice.adopt_in_subprocess({"_harness": "crewai"})

        seen = []
        import threading

        thread = threading.Thread(target=lambda: seen.append(selection.active_name()))
        thread.start()
        thread.join()
        assert seen == [HarnessName.CREWAI]


class TestEngineForExecution:
    @pytest.mark.asyncio
    async def test_reads_the_engine_recorded_on_the_run(self):
        service = MagicMock(
            get_run_by_job_id=AsyncMock(return_value=MagicMock(harness="crewai"))
        )
        session = MagicMock()
        with patch(
            "src.services.execution.service.ExecutionService", return_value=service
        ) as cls:
            resolved = await harness_choice.harness_for_execution(session, "job-1")
        assert resolved is HarnessName.CREWAI
        # Through the execution domain's own SERVICE, on the caller's session.
        cls.assert_called_once_with(session)

    @pytest.mark.asyncio
    async def test_a_row_with_no_engine_is_kasal_not_the_current_setting(self):
        """Every row written before the column existed ran on Kasal.

        Reading the SETTING here would break resume across a harness switch:
        task identity is harness-dependent (CrewAI's Task inherits its agent's
        tools), so an old run resumed under a newly-selected CrewAI would match
        no stored unit and restart from scratch — reporting that task 0 had
        changed when nothing about it had.
        """
        service = MagicMock(
            get_run_by_job_id=AsyncMock(return_value=MagicMock(harness=None))
        )
        setting = AsyncMock(return_value=HarnessName.CREWAI)
        with (
            patch(
                "src.services.execution.service.ExecutionService", return_value=service
            ),
            patch.object(harness_choice, "resolve_run_harness", setting),
        ):
            resolved = await harness_choice.harness_for_execution(MagicMock(), "job-1")
        assert resolved is HarnessName.KASAL
        setting.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_lookup_failure_never_blocks_the_run(self):
        with (
            patch(
                "src.services.execution.service.ExecutionService",
                side_effect=RuntimeError("gone"),
            ),
            patch.object(
                harness_choice,
                "resolve_run_harness",
                AsyncMock(return_value=HarnessName.KASAL),
            ),
        ):
            resolved = await harness_choice.harness_for_execution(MagicMock(), "job-1")
        assert resolved is HarnessName.KASAL


class TestARunMayNameItsOwnHarness:
    """The picker sits beside the model, so a run carries its own choice.

    The configured default still exists and still matters: scheduled runs and
    API-triggered runs have no picker, so "omitted" has to keep meaning "use
    the default" rather than "use kasal".
    """

    @pytest.mark.asyncio
    async def test_a_named_harness_wins_over_the_default(self):
        from src.services.execution.status import ExecutionStatusService

        data = {"job_id": "j", "status": "RUNNING", "harness": "crewai"}
        settings = MagicMock()
        with patch("src.services.settings.engine.EngineConfigService", settings):
            await ExecutionStatusService._fill_harness(MagicMock(), data)

        assert data["harness"] == "crewai"
        settings.assert_not_called()

    @pytest.mark.asyncio
    async def test_naming_none_falls_back_to_the_configured_default(self):
        from src.services.execution.status import ExecutionStatusService

        service = MagicMock(get_harness=AsyncMock(return_value="crewai"))
        data = {"job_id": "j", "status": "RUNNING"}
        with patch(
            "src.services.settings.engine.EngineConfigService", return_value=service
        ):
            await ExecutionStatusService._fill_harness(MagicMock(), data)

        assert data["harness"] == "crewai"

    def test_the_schema_declares_it_on_both_run_shapes(self):
        """Declared, not an extra.

        ``CrewConfig`` allows extra fields, so an undeclared ``harness`` would
        be accepted and validated by nothing — the shape of bug this codebase
        has hit before with a field that looked set and did nothing.
        """
        from src.schemas.execution import CrewConfig, FlowConfig

        assert "harness" in CrewConfig.model_fields
        assert "harness" in FlowConfig.model_fields
        assert CrewConfig(model="m").harness is None

    @pytest.mark.asyncio
    async def test_an_unknown_harness_falls_back_rather_than_failing_the_run(self):
        """A bad value in a payload must not be able to fail an execution."""
        assert engine_choice_coerce("nonsense") is None


class TestDispatchSession:
    """The ONE place a run's harness resolution may acquire a session."""

    @pytest.mark.asyncio
    async def test_the_caller_s_session_is_used_untouched(self):
        session = MagicMock()
        with patch("src.db.session.routed_scoped_session") as opened:
            async with harness_choice.dispatch_session(session) as used:
                assert used is session
        opened.assert_not_called()

    @pytest.mark.asyncio
    async def test_without_one_it_goes_through_the_router(self):
        acquired = MagicMock()
        opened = MagicMock()
        opened.return_value.__aenter__ = AsyncMock(return_value=acquired)
        opened.return_value.__aexit__ = AsyncMock(return_value=None)
        with patch("src.db.session.routed_scoped_session", opened):
            async with harness_choice.dispatch_session() as used:
                assert used is acquired
        opened.assert_called_once_with()


class TestTheChildSaysWhichHarnessItAdopted:
    """A spawned interpreter announces its runtime, and where it read it from.

    Nothing in the child said so, and the parts of a run that are SHARED between
    harnesses — the plan tool, memory, the LLM transport, the tool wrappers —
    all log as "kasal". Reading a flow log, that looks like the wrong runtime
    ran, and there was no line to check it against.

    The module logs through LoggerManager (its own handlers, no propagation), so
    the logger itself is the seam these assert on.
    """

    @staticmethod
    def _adopt(config=None, env=None):
        # reset_for_tests clears the env stamp, so the environment case has to
        # set it AFTER the reset — not before.
        reset_for_tests()
        if env:
            os.environ[HARNESS_ENV_VAR] = env
        with patch.object(harness_choice, "logger") as logger:
            adopted = adopt_in_subprocess(config)
        said = " ".join(str(a) for call in logger.info.call_args_list for a in call.args)
        return adopted, said

    def test_it_names_the_harness_and_the_payload_it_came_from(self):
        adopted, said = self._adopt({HARNESS_CONFIG_KEY: "crewai"})
        assert adopted is HarnessName.CREWAI
        assert "crewai" in said
        assert "payload" in said

    def test_it_says_when_the_environment_supplied_it(self):
        adopted, said = self._adopt(None, env="crewai")
        assert adopted is HarnessName.CREWAI
        assert "environment" in said

    def test_it_says_when_it_fell_back_to_the_default(self):
        adopted, said = self._adopt(None)
        assert adopted is DEFAULT_HARNESS
        assert "default" in said
