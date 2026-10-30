"""A flow started over MCP or A2A receives what the caller sent.

The flow service rebuilds its config from a whitelist and reads the caller's
values from ``inputs``. Arguments passed anywhere else are dropped one hop
before the flow, which then runs on empty state with its placeholders literal.

End to end through the real MCP tool, publication lookup, invocation layer and
KasalFlowService on SQLite; only the engine that would spawn the flow
subprocess is stubbed, so what it was handed is what the flow would get.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

import src.models  # noqa: F401  registers every ORM model on Base.metadata
from src.core.exceptions import UnprocessableEntityError
from src.db.base import Base
from src.models.flow import Flow
from src.schemas.a2a import Message, Part
from src.schemas.crew_publication import CrewPublicationCreate
from src.services.a2a.a2a_server import tasks as a2a_tasks
from src.services.external.identity import ExternalCaller
from src.services.mcp.mcp_server import tools as mcp_tools
from src.services.publications.publication import PublicationService
from src.utils.user_context import GroupContext

GROUP = "team_reports"
ENGINE = "src.services.execution.engine_factory.EngineFactory.get_engine"


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as s:
        yield s
    await engine.dispose()


def _caller(protocol: str) -> ExternalCaller:
    return ExternalCaller(
        group_context=GroupContext(
            group_ids=[GROUP],
            group_email="caller@example.com",
            user_role="operator",
            access_token="caller-token",
        ),
        protocol=protocol,
        identifier="caller@example.com",
    )


async def _published_flow(session, name: str, input_schema=None) -> None:
    flow_id = uuid.uuid4()
    session.add(Flow(id=flow_id, name=name, nodes=[], edges=[], group_id=GROUP))
    await session.commit()
    await PublicationService(session).publish(
        entity_id=str(flow_id),
        data=CrewPublicationCreate(
            external_name=name,
            description=f"Does {name}.",
            protocols=["mcp", "a2a"],
            input_schema=input_schema,
        ),
        group_context=GroupContext(group_ids=[GROUP], group_email="a@example.com"),
        entity_type="flow",
    )
    await session.commit()


def _schema(*names: str) -> dict:
    return {
        "type": "object",
        "properties": {name: {"type": "string"} for name in names},
    }


def _engine() -> MagicMock:
    engine = MagicMock()
    engine.run_flow = AsyncMock(side_effect=lambda execution_id, **_: execution_id)
    return engine


class TestFlowStartedOverMcp:
    @pytest.mark.asyncio
    async def test_the_tool_arguments_reach_the_flow_as_inputs(self, session):
        await _published_flow(session, "policy_brief", _schema("topic", "audience"))
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"),
                "policy_brief",
                {"topic": "EU AI act", "audience": "board"},
                session=session,
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["inputs"] == {"topic": "EU AI act", "audience": "board"}

    @pytest.mark.asyncio
    async def test_a_conversation_keeps_its_session_and_opening_line(self, session):
        """A conversational flow continues a thread by session_id and takes the
        free-text request as the turn's user line."""
        await _published_flow(session, "research_chat")
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"),
                "research_chat",
                {"request": "Which frameworks did you find?", "session_id": "s-1"},
                session=session,
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["session_id"] == "s-1"
        assert flow_config["user_message"] == "Which frameworks did you find?"
        # Not state: a conversational flow's typed state has no channel for
        # either key, and would refuse the run at kickoff.
        assert flow_config["inputs"] == {}

    @pytest.mark.asyncio
    async def test_a_declared_request_is_also_a_state_input(self, session):
        await _published_flow(session, "policy_brief", _schema("request"))
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"),
                "policy_brief",
                {"request": "EU AI act"},
                session=session,
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["inputs"] == {"request": "EU AI act"}
        assert flow_config["user_message"] == "EU AI act"


class TestCallerArgumentsCannotSteerTheRun:
    @pytest.mark.asyncio
    async def test_a_supplied_graph_does_not_replace_the_stored_flow(self, session):
        await _published_flow(session, "policy_brief")
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"),
                "policy_brief",
                {
                    "nodes": [{"id": "injected", "type": "crewNode"}],
                    "edges": [{"source": "a", "target": "b"}],
                    "flow_config": {"startingPoints": [{"crewId": "x"}]},
                },
                session=session,
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        # Empty, so the runner builds the run from the flow as saved.
        assert flow_config["nodes"] == []
        assert flow_config["edges"] == []
        assert flow_config["flow_config"] == {}

    @pytest.mark.asyncio
    async def test_id_and_run_metadata_are_not_caller_inputs(self, session):
        """`id` names the saved state a flow restores."""
        await _published_flow(session, "policy_brief")
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"),
                "policy_brief",
                {
                    "id": "another-teams-thread",
                    "flow_id": "f",
                    "run_name": "r",
                    "job_id": "j",
                    "execution_id": "e",
                    "topic": "EU AI act",
                },
                session=session,
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["inputs"] == {"topic": "EU AI act"}


class TestArgumentsAreCheckedBeforeTheRunStarts:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "arguments,problem",
        [
            ({"topic": "EU AI act", "colour": "red"}, "unknown argument"),
            ({"topic": 7}, "'topic' should be string"),
        ],
    )
    async def test_arguments_the_schema_refuses_are_a_422(
        self, session, arguments, problem
    ):
        schema = {**_schema("topic", "audience"), "required": ["topic"]}
        await _published_flow(session, "policy_brief", schema)
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            with pytest.raises(UnprocessableEntityError, match=problem):
                await mcp_tools.call_crew_tool(
                    _caller("mcp"), "policy_brief", arguments, session=session
                )

        engine.run_flow.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_extra_keys_are_allowed_when_the_schema_allows_them(self, session):
        """`additionalProperties: {}` is a schema that accepts anything."""
        schema = {**_schema("topic"), "additionalProperties": {}}
        await _published_flow(session, "policy_brief", schema)
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"),
                "policy_brief",
                {"topic": "EU AI act", "colour": "red"},
                session=session,
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["inputs"] == {"topic": "EU AI act", "colour": "red"}

    @pytest.mark.asyncio
    async def test_an_integral_number_is_an_integer(self, session):
        schema = {
            "type": "object",
            "properties": {"quarter": {"type": "integer"}},
        }
        await _published_flow(session, "policy_brief", schema)
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"), "policy_brief", {"quarter": 3.0}, session=session
            )
            with pytest.raises(UnprocessableEntityError, match="integer"):
                await mcp_tools.call_crew_tool(
                    _caller("mcp"), "policy_brief", {"quarter": 3.5}, session=session
                )

        assert engine.run_flow.await_count == 1


class TestRequiredInputsDoNotRefuseARun:
    """The publish dialog marks every placeholder required by default, and the
    crew path enforces nothing; refusing here stopped flows that ran."""

    REQUIRED = {**_schema("topic", "audience"), "required": ["topic", "audience"]}

    @pytest.mark.asyncio
    async def test_an_a2a_start_carrying_only_its_text_runs(self, session):
        await _published_flow(session, "policy_brief", self.REQUIRED)
        engine = _engine()
        message = Message(role="user", parts=[Part(kind="text", text="EU AI act")])
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await a2a_tasks.send_message(
                _caller("a2a"), message, skill_id="policy_brief", session=session
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["inputs"] == {}
        assert flow_config["user_message"] == "EU AI act"

    @pytest.mark.asyncio
    async def test_an_mcp_follow_up_turn_with_only_its_session_and_line_runs(
        self, session
    ):
        await _published_flow(session, "research_chat", self.REQUIRED)
        engine = _engine()
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await mcp_tools.call_crew_tool(
                _caller("mcp"),
                "research_chat",
                {"session_id": "s-1", "user_message": "And the second one?"},
                session=session,
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["session_id"] == "s-1"
        assert flow_config["user_message"] == "And the second one?"
        assert flow_config["inputs"] == {}


class TestFlowStartedOverA2a:
    @pytest.mark.asyncio
    async def test_the_message_text_reaches_the_flow(self, session):
        await _published_flow(session, "policy_brief")
        engine = _engine()
        message = Message(role="user", parts=[Part(kind="text", text="EU AI act")])
        with patch(ENGINE, new=AsyncMock(return_value=engine)):
            await a2a_tasks.send_message(
                _caller("a2a"), message, skill_id="policy_brief", session=session
            )

        flow_config = engine.run_flow.await_args.kwargs["flow_config"]
        assert flow_config["user_message"] == "EU AI act"
