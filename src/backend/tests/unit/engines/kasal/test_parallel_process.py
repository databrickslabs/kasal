"""Crew-level "parallel" process.

Parallelism used to be reachable only by ticking async_execution on each task in
the task Advanced form — a per-task switch users have no reason to know about,
and one that silently did nothing unless every independent task got it. The crew
now carries process="parallel" and the engine opts the independent tasks in.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from kasal_engine.core import Process
from src.engines.kasal.config_adapter import adapt_config
from src.engines.kasal.config.crew_config_builder import CrewConfigBuilder
from src.engines.kasal.paths.crew.crew_preparation import CrewPreparation
from src.schemas.execution import CrewConfig


class TestProcessSurvivesTheRunPayload:
    """The UI sends the crew's process inside `inputs`; the engine reads it from
    config["crew"]["process"]. adapt_config is the only thing joining those two,
    so a saved "parallel" crew depends on it copying the value through."""

    def _adapted(self, inputs):
        return adapt_config(
            CrewConfig(
                agents_yaml={"a1": {"role": "r", "goal": "g", "backstory": "b"}},
                tasks_yaml={"t1": {"description": "d", "expected_output": "e", "agent": "a1"}},
                inputs=inputs,
                model="gpt-5-nano",
            )
        )

    def test_parallel_reaches_the_crew_config(self):
        assert self._adapted({"process": "parallel"})["crew"]["process"] == "parallel"

    def test_hierarchical_and_sequential_still_reach_it(self):
        assert self._adapted({"process": "hierarchical"})["crew"]["process"] == "hierarchical"
        assert self._adapted({"process": "sequential"})["crew"]["process"] == "sequential"

    def test_missing_process_defaults_to_sequential(self):
        assert self._adapted({})["crew"]["process"] == "sequential"


class TestDetermineProcessType:
    """The engine has no "parallel" Process — it runs the sequential kernel and
    dispatches the async tasks together."""

    def _builder(self, process):
        return CrewConfigBuilder({"crew": {"process": process}})

    def test_parallel_maps_to_the_sequential_kernel(self):
        assert self._builder("parallel").determine_process_type() == Process.sequential

    def test_hierarchical_is_untouched(self):
        assert self._builder("hierarchical").determine_process_type() == Process.hierarchical

    def test_sequential_and_unknown_stay_sequential(self):
        assert self._builder("sequential").determine_process_type() == Process.sequential
        assert self._builder("nonsense").determine_process_type() == Process.sequential

    def test_case_is_ignored(self):
        assert self._builder("PARALLEL").determine_process_type() == Process.sequential


def _config(process, tasks):
    return {
        "crew": {"process": process},
        "agents": [{"name": "a1", "role": "r"}],
        "tasks": tasks,
    }


def _prep(config):
    prep = CrewPreparation(config, MagicMock(), MagicMock())
    prep.agents = {"a1": MagicMock()}
    return prep


async def _async_flags(config):
    """Run _create_tasks and return the async_execution each task was built with."""
    seen = []

    def _capture(**kwargs):
        seen.append(bool(kwargs["task_config"].get("async_execution")))
        task = MagicMock()
        task.async_execution = seen[-1]
        task.context = None
        return task

    with patch(
        "src.engines.kasal.paths.crew.task_adapter.create_task", side_effect=_capture
    ):
        await _prep(config)._create_tasks()
    return seen


class TestParallelCrewMarksIndependentTasksAsync:
    @pytest.mark.asyncio
    async def test_context_free_tasks_run_async(self):
        """The reported case: three independent analyses, no edges between them,
        that nonetheless ran one after another."""
        flags = await _async_flags(
            _config(
                "parallel",
                [
                    {"name": "t1", "agent": "a1", "description": "d", "expected_output": "e"},
                    {"name": "t2", "agent": "a1", "description": "d", "expected_output": "e"},
                    {"name": "t3", "agent": "a1", "description": "d", "expected_output": "e"},
                ],
            )
        )
        assert flags == [True, True, True], flags

    @pytest.mark.asyncio
    async def test_a_task_that_consumes_another_stays_sync(self):
        """Parallel is a fan-out, not an ordering-free free-for-all."""
        flags = await _async_flags(
            _config(
                "parallel",
                [
                    {"name": "t1", "agent": "a1", "description": "d", "expected_output": "e"},
                    {
                        "name": "t2",
                        "agent": "a1",
                        "description": "d",
                        "expected_output": "e",
                        "context": ["t1"],
                    },
                ],
            )
        )
        assert flags[0] is True
        assert flags[1] is False

    @pytest.mark.asyncio
    async def test_sequential_crew_is_unaffected(self):
        flags = await _async_flags(
            _config(
                "sequential",
                [
                    {"name": "t1", "agent": "a1", "description": "d", "expected_output": "e"},
                    {"name": "t2", "agent": "a1", "description": "d", "expected_output": "e"},
                ],
            )
        )
        assert flags == [False, False], flags

    @pytest.mark.asyncio
    async def test_stored_per_task_flag_is_ignored(self):
        """The task form's Async Execution toggle is gone — the crew process is
        the only input. A value stored from that era must not quietly make a
        sequential crew run tasks concurrently, because nothing in the UI would
        show it or let you turn it off."""
        flags = await _async_flags(
            _config(
                "sequential",
                [
                    {
                        "name": "t1",
                        "agent": "a1",
                        "description": "d",
                        "expected_output": "e",
                        "async_execution": True,
                    },
                ],
            )
        )
        assert flags == [False], flags

    @pytest.mark.asyncio
    async def test_stored_false_does_not_block_a_parallel_crew(self):
        """The mirror case: every task generated before this change carries
        async_execution=False, and a parallel crew must still fan them out."""
        flags = await _async_flags(
            _config(
                "parallel",
                [
                    {"name": "t1", "agent": "a1", "description": "d",
                     "expected_output": "e", "async_execution": False},
                    {"name": "t2", "agent": "a1", "description": "d",
                     "expected_output": "e", "async_execution": False},
                ],
            )
        )
        assert flags == [True, True], flags
