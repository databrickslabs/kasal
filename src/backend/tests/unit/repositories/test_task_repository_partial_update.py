"""A partial task update leaves what it did not name alone.

The rule is "absent = untouched, present = replace":

- no ``config`` in the payload -> the row's config is kept, and any root field
  that is sent is mirrored into a copy of it;
- a ``config`` in the payload -> it replaces the stored one, so a form that
  clears a field by leaving it out still clears it (a key-by-key merge would
  quietly keep the stale value);
- ``agent_id: ""`` means "not provided", never "unassign".

Run against a real SQLite database rather than a mocked session: the bug this
pins was a JSON column being overwritten with ``{}``, which a mock that only
records ``setattr`` calls cannot see.
"""

from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.models.agent import Agent
from src.models.task import Task
from src.repositories.task_repository import TaskRepository
from src.schemas.task import TaskUpdate
from src.services.catalog.tasks import TaskService

GUARDRAIL = {"description": "Every claim cites a source"}
GOVERNED_CONFIG = {
    "human_input": True,
    "output_pydantic": "Report",
    "llm_guardrail": GUARDRAIL,
    "guardrail": "no_pii",
    "callback": "notify",
    "markdown": True,
}
GROUP = SimpleNamespace(group_ids=["acme"])


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Agent.__table__.create)
        await conn.run_sync(Task.__table__.create)
    maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def task(session):
    session.add(Agent(id="writer", name="Writer", role="writer", goal="write"))
    row = Task(
        id="t1",
        name="Draft",
        description="draft the report",
        expected_output="a report",
        agent_id="writer",
        context=["t0"],
        tools=["SerperDevTool"],
        config=dict(GOVERNED_CONFIG),
        human_input=True,
        output_pydantic="Report",
        llm_guardrail=GUARDRAIL,
        guardrail="no_pii",
        callback="notify",
        markdown=True,
        group_id="acme",
    )
    session.add(row)
    await session.flush()
    return row


async def reread(session, task_id="t1"):
    session.expire_all()
    return await TaskRepository(session).get(task_id)


def assert_governed(row):
    assert row.config["human_input"] is True
    assert row.config["output_pydantic"] == "Report"
    assert row.config["llm_guardrail"] == GUARDRAIL
    assert row.config["guardrail"] == "no_pii"
    assert row.llm_guardrail == GUARDRAIL


class TestAnUpdateWithoutConfigKeepsIt:
    @pytest.mark.asyncio
    async def test_gepa_prompt_rewrite_keeps_the_gate_guardrail_and_schema(
        self, session, task
    ):
        """GEPA Apply/Revert send only the prompt text."""
        ok = await TaskService(session).update_prompt_text_with_group_check(
            "t1",
            {"description": "sharper", "expected_output": "a tighter report"},
            GROUP,
        )

        assert ok
        row = await reread(session)
        assert row.description == "sharper"
        assert_governed(row)

    @pytest.mark.asyncio
    async def test_genie_tool_configs_write_keeps_the_config(self, session, task):
        """The Genie-space prompt PUTs ``{tool_configs}`` and nothing else."""
        payload = TaskUpdate(tool_configs={"GenieTool": {"spaceId": "space-1"}})

        await TaskService(session).update_with_group_check("t1", payload, GROUP)

        row = await reread(session)
        assert row.tool_configs == {"GenieTool": {"spaceId": "space-1"}}
        assert_governed(row)

    @pytest.mark.asyncio
    async def test_tools_chip_payload_keeps_config_agent_and_context(
        self, session, task
    ):
        """The Tools/MCP chip and the knowledge upload send tools + tool_configs."""
        payload = TaskUpdate(
            tools=["SerperDevTool", "ScrapeWebsiteTool"],
            tool_configs={"MCP_SERVERS": {"servers": ["docs"]}},
        )

        await TaskService(session).update_with_group_check("t1", payload, GROUP)

        row = await reread(session)
        assert row.tools == ["SerperDevTool", "ScrapeWebsiteTool"]
        assert_governed(row)
        assert row.agent_id == "writer"
        assert row.context == ["t0"]

    @pytest.mark.asyncio
    async def test_a_root_field_is_mirrored_into_the_kept_config(self, session, task):
        await TaskRepository(session).update("t1", {"output_pydantic": "Summary"})

        row = await reread(session)
        assert row.config["output_pydantic"] == "Summary"
        assert row.config["human_input"] is True
        assert row.config["llm_guardrail"] == GUARDRAIL

    @pytest.mark.asyncio
    async def test_clearing_the_root_llm_guardrail_clears_it_in_the_kept_config(
        self, session, task
    ):
        await TaskRepository(session).update("t1", {"llm_guardrail": None})

        row = await reread(session)
        assert row.config["llm_guardrail"] is None
        assert row.llm_guardrail is None
        assert row.config["human_input"] is True

    @pytest.mark.asyncio
    async def test_the_stored_config_object_is_not_mutated_in_place(
        self, session, task
    ):
        """A JSON column mutated in place is a change SQLAlchemy never writes."""
        before = task.config
        await TaskRepository(session).update("t1", {"output_pydantic": "Summary"})
        assert before["output_pydantic"] == "Report"


class TestAnUpdateWithConfigReplacesIt:
    @pytest.mark.asyncio
    async def test_a_field_left_out_of_the_sent_config_is_cleared(self, session, task):
        """TaskForm clears output_pydantic, condition, callback by omitting them."""
        payload = TaskUpdate(config={"human_input": False, "markdown": True})

        await TaskService(session).update_with_group_check("t1", payload, GROUP)

        row = await reread(session)
        assert row.config["human_input"] is False
        assert "output_pydantic" not in row.config
        assert "guardrail" not in row.config
        assert "callback" not in row.config

    @pytest.mark.asyncio
    async def test_a_field_cleared_in_the_sent_config_is_cleared_at_the_root(
        self, session, task
    ):
        """The root columns mirror the config, and a rebuilt crew (MCP/A2A,
        triggers, resume) projects them, so a stale root value would bring a
        cleared output schema or guardrail back on those runs."""
        task.output_json = "report.json"
        task.output_file = "report.md"
        await session.flush()
        payload = TaskUpdate(config={"human_input": False, "output_pydantic": None})

        await TaskService(session).update_with_group_check("t1", payload, GROUP)

        row = await reread(session)
        for column in (
            "output_pydantic",
            "output_json",
            "output_file",
            "callback",
            "guardrail",
        ):
            assert getattr(row, column) is None, column

    @pytest.mark.asyncio
    async def test_a_value_in_the_sent_config_still_reaches_the_root(
        self, session, task
    ):
        payload = TaskUpdate(config={"output_pydantic": "Summary", "callback": "ping"})

        await TaskService(session).update_with_group_check("t1", payload, GROUP)

        row = await reread(session)
        assert row.output_pydantic == "Summary"
        assert row.callback == "ping"
        assert row.guardrail is None


class TestAnEmptyAgentIdIsNotAnUnassign:
    @pytest.mark.asyncio
    async def test_put_with_empty_agent_id_keeps_the_assignment(self, session, task):
        """The canvas task editor used to send ``agent_id: ""`` on every save."""
        payload = TaskUpdate(description="edited on the canvas", agent_id="")

        await TaskService(session).update_with_group_check("t1", payload, GROUP)

        row = await reread(session)
        assert row.description == "edited on the canvas"
        assert row.agent_id == "writer"

    @pytest.mark.asyncio
    async def test_full_update_with_empty_agent_id_keeps_the_assignment(
        self, session, task
    ):
        await TaskService(session).update_full_with_group_check(
            "t1", {"name": "Draft v2", "agent_id": ""}, GROUP
        )

        row = await reread(session)
        assert row.name == "Draft v2"
        assert row.agent_id == "writer"

    @pytest.mark.asyncio
    async def test_the_repository_drops_an_empty_agent_id(self, session, task):
        await TaskRepository(session).update("t1", {"agent_id": ""})

        row = await reread(session)
        assert row.agent_id == "writer"

    @pytest.mark.asyncio
    async def test_a_real_agent_id_still_reassigns(self, session, task):
        session.add(Agent(id="editor", name="Editor", role="editor", goal="edit"))
        await session.flush()

        payload = TaskUpdate(agent_id="editor")
        await TaskService(session).update_with_group_check("t1", payload, GROUP)

        row = await reread(session)
        assert row.agent_id == "editor"
