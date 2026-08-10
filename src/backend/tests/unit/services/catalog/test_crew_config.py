"""Projecting a saved crew into the config the engine takes.

Two callers need this: external invocation (a published crew, no browser) and
resume (rebuilding a run from the crew as it is NOW rather than from the inputs
snapshot frozen when it started).

Fidelity is the thing worth testing. A resume that rebuilds a THINNER crew than
the one that ran would quietly drop guardrails, structured output or tool
configuration — the run still succeeds and produces something else. So the
field set is asserted against the model, not just spot-checked, and a column
added later fails this rather than being silently left behind.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.models.agent import Agent
from src.models.task import Task
from src.services.catalog.crew_config import (
    _AGENT_FIELDS,
    _TASK_FIELDS,
    build_crew_execution_config,
)


def make_agent(agent_id="a1", **overrides):
    fields = {
        "id": agent_id,
        "name": "Researcher",
        "role": "researcher",
        "goal": "find things",
        "backstory": "curious",
        "llm": "gpt-x",
        "tools": ["SerperDevTool"],
        "tool_configs": None,
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def make_task(task_id="t1", **overrides):
    fields = {
        "id": task_id,
        "name": "Research",
        "description": "research the topic",
        "expected_output": "notes",
        "agent_id": "a1",
        "tools": [],
        "tool_configs": None,
        "context": [],
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def crew(agent_ids=("a1",), task_ids=("t1",)):
    return SimpleNamespace(
        id="crew-1", agent_ids=list(agent_ids), task_ids=list(task_ids)
    )


def patch_catalog(agents, tasks):
    """Resolve agents/tasks from dicts keyed by id; anything else is missing."""

    def service(rows):
        svc = AsyncMock()
        svc.get = AsyncMock(side_effect=lambda i: rows.get(str(i)))
        svc.get_with_group_check = AsyncMock(side_effect=lambda i, _c: rows.get(str(i)))
        return svc

    return (
        patch("src.services.catalog.agents.AgentService", return_value=service(agents)),
        patch("src.services.catalog.tasks.TaskService", return_value=service(tasks)),
    )


async def project(crew_row, agents, tasks, group_context=None):
    agent_p, task_p = patch_catalog(agents, tasks)
    with agent_p, task_p:
        return await build_crew_execution_config(None, crew_row, group_context)


class TestTheProjection:
    @pytest.mark.asyncio
    async def test_agents_and_tasks_are_keyed_by_name(self):
        agents_yaml, tasks_yaml = await project(
            crew(), {"a1": make_agent()}, {"t1": make_task()}
        )

        assert list(agents_yaml) == ["Researcher"]
        assert list(tasks_yaml) == ["Research"]
        assert agents_yaml["Researcher"]["role"] == "researcher"
        assert tasks_yaml["Research"]["description"] == "research the topic"

    @pytest.mark.asyncio
    async def test_a_task_references_its_agent_by_key(self):
        _, tasks_yaml = await project(
            crew(), {"a1": make_agent()}, {"t1": make_task(agent_id="a1")}
        )
        # CrewPreparation resolves this string against the agents_yaml keys —
        # and falls back to the FIRST agent when it cannot, silently.
        assert tasks_yaml["Research"]["agent"] == "Researcher"

    @pytest.mark.asyncio
    async def test_the_database_id_rides_along(self):
        _, tasks_yaml = await project(crew(), {"a1": make_agent()}, {"t1": make_task()})
        assert tasks_yaml["Research"]["id"] == "t1"

    @pytest.mark.asyncio
    async def test_task_order_follows_task_ids(self):
        tasks = {
            "t1": make_task("t1", name="first"),
            "t2": make_task("t2", name="second"),
        }
        _, tasks_yaml = await project(
            crew(task_ids=("t2", "t1")), {"a1": make_agent()}, tasks
        )
        # Order is the crew's, not the dict's — a sequential crew runs in it.
        assert list(tasks_yaml) == ["second", "first"]


class TestContext:
    @pytest.mark.asyncio
    async def test_context_ids_become_task_keys(self):
        tasks = {
            "t1": make_task("t1", name="first"),
            "t2": make_task("t2", name="second", context=["t1"]),
        }
        _, tasks_yaml = await project(
            crew(task_ids=("t1", "t2")), {"a1": make_agent()}, tasks
        )
        assert tasks_yaml["second"]["context"] == ["first"]

    @pytest.mark.asyncio
    async def test_a_forward_reference_still_resolves(self):
        """A task may depend on one declared after it, so the key mapping has
        to be complete before context is translated."""
        tasks = {
            "t1": make_task("t1", name="first", context=["t2"]),
            "t2": make_task("t2", name="second"),
        }
        _, tasks_yaml = await project(
            crew(task_ids=("t1", "t2")), {"a1": make_agent()}, tasks
        )
        assert tasks_yaml["first"]["context"] == ["second"]

    @pytest.mark.asyncio
    async def test_a_reference_outside_the_crew_is_dropped(self):
        """Passing it through would be worse than dropping it: CrewPreparation
        falls back to the FIRST task on an unresolvable reference, which wires
        the crew wrongly and says nothing."""
        tasks = {"t1": make_task("t1", context=["not-in-this-crew"])}
        _, tasks_yaml = await project(crew(), {"a1": make_agent()}, tasks)
        assert "context" not in tasks_yaml["Research"]


class TestMissingRows:
    @pytest.mark.asyncio
    async def test_a_deleted_task_is_left_out(self):
        _, tasks_yaml = await project(
            crew(task_ids=("t1", "gone")), {"a1": make_agent()}, {"t1": make_task()}
        )
        assert list(tasks_yaml) == ["Research"]

    @pytest.mark.asyncio
    async def test_a_deleted_agent_leaves_its_task_unassigned(self):
        _, tasks_yaml = await project(
            crew(agent_ids=("gone",)), {}, {"t1": make_task(agent_id="gone")}
        )
        # Better unassigned than pointing at an agent that is not in the config.
        assert "agent" not in tasks_yaml["Research"]

    @pytest.mark.asyncio
    async def test_group_context_uses_the_checked_accessors(self):
        agents = {"a1": make_agent()}
        tasks = {"t1": make_task()}
        agent_p, task_p = patch_catalog(agents, tasks)
        with agent_p as agent_service, task_p as task_service:
            await build_crew_execution_config(None, crew(), group_context="ctx")

        agent_service.return_value.get_with_group_check.assert_awaited()
        task_service.return_value.get_with_group_check.assert_awaited()
        agent_service.return_value.get.assert_not_awaited()


class TestFidelity:
    """The field lists must keep up with the models.

    A column added to agents/tasks and not projected here is invisible: the
    resumed run just behaves slightly differently from the one it claims to
    continue.
    """

    # Identity and audit, not behaviour — deliberately not projected.
    AGENT_EXCLUDED = {
        "id",
        "name",
        "group_id",
        "created_by_email",
        "created_at",
        "updated_at",
        # Emitted explicitly rather than via the pass-through list.
        "role",
        "goal",
        "backstory",
        "tools",
        "tool_configs",
        # Carried in the model config, not the agent entry.
        "temperature",
    }

    TASK_EXCLUDED = {
        "id",
        "name",
        "group_id",
        "created_by_email",
        "created_at",
        "updated_at",
        "description",
        "expected_output",
        "tools",
        "tool_configs",
        # Translated from ids to keys rather than copied.
        "context",
        "agent_id",
        # An OUTPUT of a previous run, not part of the definition.
        "output",
        # Paired with `callback`, which is projected; the config carries the
        # callback name and the engine resolves its configuration.
        "callback_config",
        # Deprecated alias the engine no longer reads.
        "retry_on_fail",
        "max_retries",
    }

    def test_every_behavioural_agent_column_is_projected(self):
        columns = {c.name for c in Agent.__table__.columns}
        missing = columns - self.AGENT_EXCLUDED - set(_AGENT_FIELDS)
        assert not missing, (
            f"agents columns not carried into a rebuilt crew: {sorted(missing)}. "
            f"Add them to _AGENT_FIELDS, or to AGENT_EXCLUDED with the reason."
        )

    def test_every_behavioural_task_column_is_projected(self):
        columns = {c.name for c in Task.__table__.columns}
        missing = columns - self.TASK_EXCLUDED - set(_TASK_FIELDS)
        assert not missing, (
            f"tasks columns not carried into a rebuilt crew: {sorted(missing)}. "
            f"Add them to _TASK_FIELDS, or to TASK_EXCLUDED with the reason."
        )

    @pytest.mark.asyncio
    async def test_absent_values_are_omitted_rather_than_nulled(self):
        """The engine's builders read an absent key as "use the default", and
        an explicit null is not always the same thing."""
        agents_yaml, tasks_yaml = await project(
            crew(),
            {"a1": make_agent(max_iter=None, verbose=True)},
            {"t1": make_task(markdown=None, human_input=True)},
        )
        assert "max_iter" not in agents_yaml["Researcher"]
        assert agents_yaml["Researcher"]["verbose"] is True
        assert "markdown" not in tasks_yaml["Research"]
        assert tasks_yaml["Research"]["human_input"] is True
