"""GET agents, tasks, crews and tools: operators see masked credentials.

Each route is driven over HTTP so the visibility dependency is exercised as
wired. Services are faked; the membership lookup behind a builder override is
patched at GroupService.
"""

import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.agents_router import get_agent_service
from src.api.agents_router import router as agents_routes
from src.api.crews_router import get_crew_service
from src.api.crews_router import router as crews_routes
from src.api.tasks_router import get_task_service
from src.api.tasks_router import router as tasks_routes
from src.api.tools_router import get_tool_service
from src.api.tools_router import router as tools_routes
from src.db.database_router import get_smart_db_session
from src.dependencies.providers import get_group_context
from src.models.agent import Agent
from src.models.crew import Crew
from src.models.task import Task
from src.schemas.tool import ToolListResponse, ToolResponse
from src.utils.sensitive_data_utils import REDACTED_PLACEHOLDER
from src.utils.user_context import GroupContext

SECRET = "sp-secret-value-123"
PAT = "dapi-pat-value-456"
NOW = datetime(2026, 10, 1, 12, 0, 0)
PBI = {"Power BI Tool": {"client_secret": SECRET, "tenant_id": "t", "max_tokens": 9}}
CREW_ID = uuid.uuid4()


def _agent():
    return Agent(
        id="a1",
        name="a",
        role="r",
        goal="g",
        backstory="b",
        tools=[],
        tool_configs=PBI,
        created_at=NOW,
        updated_at=NOW,
    )


def _task():
    return Task(
        id="t1",
        name="t",
        description="d",
        expected_output="o",
        tools=[],
        tool_configs=PBI,
        callback_config={"token": PAT, "volume_path": "/Volumes/a/b/c"},
        # The task form's copy of the callback config, PAT included.
        config={"callback_config": {"token": PAT}, "timeout": 30},
        created_at=NOW,
        updated_at=NOW,
    )


def _crew():
    node = {
        "id": "task-t1",
        "type": "taskNode",
        "position": {"x": 0, "y": 0},
        "data": {"label": "t", "tool_configs": PBI},
    }
    return Crew(
        id=CREW_ID,
        name="c",
        agent_ids=[],
        task_ids=[],
        nodes=[node],
        edges=[],
        tool_configs={"PBI": {"password": SECRET}},
        created_at=NOW,
        updated_at=NOW,
    )


def _tool():
    return ToolResponse(
        id=7,
        title="Power BI Tool",
        description="d",
        icon="i",
        config={"client_secret": SECRET, "workspace_id": "w"},
        enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.fixture
def rows():
    return {"agent": _agent(), "task": _task(), "crew": _crew()}


def _app(rows, role, membership):
    app = FastAPI()
    for routes in (agents_routes, tasks_routes, crews_routes, tools_routes):
        app.include_router(routes)

    context = GroupContext(
        group_ids=["ws"],
        group_email="u@example.com",
        email_domain="example.com",
        user_id="u1",
        user_role=role,
    )

    async def session():
        # SessionDep commits the session before replying, so it is awaitable.
        yield AsyncMock()

    agents = SimpleNamespace(
        find_by_group=AsyncMock(return_value=[rows["agent"]]),
        get_with_group_check=AsyncMock(return_value=rows["agent"]),
    )
    tasks = SimpleNamespace(
        find_by_group=AsyncMock(return_value=[rows["task"]]),
        get_with_group_check=AsyncMock(return_value=rows["task"]),
    )
    crews = SimpleNamespace(
        find_by_group=AsyncMock(return_value=[rows["crew"]]),
        get_by_group=AsyncMock(return_value=rows["crew"]),
    )
    tools = SimpleNamespace(
        get_all_tools_for_group=AsyncMock(
            return_value=ToolListResponse(tools=[_tool()], count=1)
        ),
        get_enabled_tools_for_group=AsyncMock(
            return_value=ToolListResponse(tools=[_tool()], count=1)
        ),
        get_tool_with_group_check=AsyncMock(return_value=_tool()),
        get_all_tool_configurations_for_group=AsyncMock(
            return_value={"Power BI Tool": {"client_secret": SECRET}}
        ),
        get_tool_configuration_with_group_check=AsyncMock(
            return_value={"client_secret": SECRET, "workspace_id": "w"}
        ),
    )
    app.dependency_overrides.update(
        {
            get_group_context: lambda: context,
            get_smart_db_session: session,
            get_agent_service: lambda: agents,
            get_task_service: lambda: tasks,
            get_crew_service: lambda: crews,
            get_tool_service: lambda: tools,
        }
    )
    lookup = patch(
        "src.services.groups.groups.GroupService.get_user_group_membership",
        AsyncMock(return_value=membership),
    )
    return app, lookup


ROUTES = {
    "agents": "/agents",
    "agent": "/agents/a1",
    "tasks": "/tasks",
    "task": "/tasks/t1",
    "crews": "/crews",
    "crew": f"/crews/{CREW_ID}",
    "tools": "/tools",
    "enabled": "/tools/enabled",
    "tool": "/tools/7",
    "configs": "/tools/configurations/all",
    "config": "/tools/configurations/Power%20BI%20Tool",
}


def _fetch_all(rows, role, membership=None):
    app, lookup = _app(rows, role, membership)
    with lookup, TestClient(app) as client:
        responses = {name: client.get(path) for name, path in ROUTES.items()}
    for name, response in responses.items():
        assert response.status_code == 200, (name, response.text)
    return {name: response.text for name, response in responses.items()}


def _override(agent=None, flow=None):
    return SimpleNamespace(allow_agent_builder=agent, allow_flow_builder=flow)


@pytest.mark.parametrize(
    "role, membership",
    [
        ("operator", None),
        ("operator", _override(agent=False, flow=False)),
        (None, None),
    ],
)
def test_members_without_builder_rights_get_masked_values(rows, role, membership):
    for name, body in _fetch_all(rows, role, membership).items():
        assert SECRET not in body and PAT not in body, name
        assert REDACTED_PLACEHOLDER in body, name


@pytest.mark.parametrize(
    "role, membership",
    [
        ("editor", None),
        ("admin", None),
        ("operator", _override(agent=True)),
        ("operator", _override(flow=True)),
    ],
)
def test_builders_keep_todays_view(rows, role, membership):
    for name, body in _fetch_all(rows, role, membership).items():
        assert SECRET in body, name
        assert REDACTED_PLACEHOLDER not in body, name


def test_masking_leaves_non_secret_values_and_the_rows_alone(rows):
    app, lookup = _app(rows, "operator", None)
    with lookup, TestClient(app) as client:
        agent = client.get("/agents/a1").json()
        task = client.get("/tasks/t1").json()
        crew = client.get(f"/crews/{CREW_ID}").json()
    tool_config = agent["tool_configs"]["Power BI Tool"]
    assert tool_config == {
        "client_secret": REDACTED_PLACEHOLDER,
        "tenant_id": "t",
        "max_tokens": 9,
    }
    assert task["callback_config"]["volume_path"] == "/Volumes/a/b/c"
    assert task["config"]["callback_config"]["token"] == REDACTED_PLACEHOLDER
    assert task["config"]["timeout"] == 30
    assert crew["nodes"][0]["data"]["tool_configs"]["Power BI Tool"]["tenant_id"] == (
        "t"
    )
    # The response is a copy: the rows the session would commit are untouched.
    assert rows["agent"].tool_configs == PBI
    assert rows["task"].callback_config["token"] == PAT
    assert rows["task"].config["callback_config"]["token"] == PAT
    assert rows["crew"].nodes[0]["data"]["tool_configs"] == PBI
