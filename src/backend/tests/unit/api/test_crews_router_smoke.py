import pytest
from unittest.mock import AsyncMock
from types import SimpleNamespace
from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.crews_router import (
    router as crews_router,
    get_crew_service,
    list_crews,
    get_crew,
    create_crew,
    debug_crew_data,
    update_crew,
    delete_crew,
    delete_all_crews,
)
from src.core.dependencies import get_group_context
from src.db.database_router import get_smart_db_session
from src.schemas.crew import CrewCreate, CrewUpdate
from src.utils.user_context import GroupContext
from tests.unit.router.conftest import register_exception_handlers


def gc(role="user"):
    return GroupContext(group_ids=["g1"], group_email="u@x", email_domain="x.com", user_role=role)


def make_crew(i=None):
    now = datetime.utcnow()
    return SimpleNamespace(
        id=(i or uuid4()),
        name="C",
        agent_ids=["a1"],
        task_ids=["t1"],
        nodes=[],
        edges=[],
        created_at=now,
        updated_at=now,
    )


def test_crew_schemas_carry_reasoning_config():
    """reasoning_config must round-trip through CrewCreate/Update (create persists
    via model_dump, update via model_dump(exclude_none=True))."""
    rc = {"reasoning_effort": "low", "max_steps": 3, "max_replans": 0}
    created = CrewCreate(name="t", agent_ids=[], task_ids=[], reasoning=True, reasoning_config=rc)
    assert created.model_dump()["reasoning_config"] == rc
    updated = CrewUpdate(reasoning_config=rc)
    assert updated.model_dump(exclude_none=True)["reasoning_config"] == rc


def test_crew_to_response_carries_execution_config():
    """Regression: the response serializer must include reasoning_config / planning /
    reasoning / llms — they were dropped before, so a catalog-saved crew reloaded
    with empty execution config."""
    from src.api.crews_router import _crew_to_response
    crew = make_crew()
    crew.process = "hierarchical"
    crew.planning = True
    crew.planning_llm = "databricks-claude-haiku-4-5"
    crew.reasoning = True
    crew.reasoning_llm = "databricks-claude-haiku-4-5"
    crew.reasoning_config = {"reasoning_effort": "low", "max_steps": 3, "max_replans": 0}
    crew.manager_llm = "m"
    crew.tool_configs = {}
    crew.memory = False
    crew.verbose = True
    crew.max_rpm = 10

    resp = _crew_to_response(crew)
    assert resp.reasoning is True
    assert resp.reasoning_config == {"reasoning_effort": "low", "max_steps": 3, "max_replans": 0}
    assert resp.planning is True
    assert resp.planning_llm == "databricks-claude-haiku-4-5"
    assert resp.process == "hierarchical"

    # defensive default path (stub without config fields) must not raise
    bare = _crew_to_response(make_crew())
    assert bare.reasoning_config is None and bare.reasoning is False


@pytest.mark.asyncio
async def test_list_get_create_paths():
    svc = AsyncMock()

    # list
    svc.find_by_group = AsyncMock(return_value=[make_crew()])
    out = await list_crews(service=svc, group_context=gc("operator"))
    assert isinstance(out, list) and out[0].name == "C"

    # get 404 and success
    svc.get_by_group = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await get_crew(uuid4(), service=svc, group_context=gc("operator"))
    c = make_crew()
    svc.get_by_group = AsyncMock(return_value=c)
    got = await get_crew(c.id, service=svc, group_context=gc("operator"))
    assert got.name == "C"

    # create forbidden and success
    with pytest.raises(Exception):
        await create_crew(CrewCreate(name="C", agent_ids=["a1"], task_ids=["t1"], nodes=[], edges=[]), service=svc, group_context=gc("user"))
    svc.create_with_group = AsyncMock(return_value=make_crew())
    created = await create_crew(CrewCreate(name="C", agent_ids=["a1"], task_ids=["t1"], nodes=[], edges=[]), service=svc, group_context=gc("editor"))
    assert created.name == "C"


@pytest.mark.asyncio
async def test_create_crew_overwrite_param_direct():
    """Calling the handler directly forwards overwrite=True to the service."""
    svc = AsyncMock()
    svc.create_with_group = AsyncMock(return_value=make_crew())
    ctx = gc("editor")
    crew_in = CrewCreate(name="C", agent_ids=["a1"], task_ids=["t1"], nodes=[], edges=[])

    await create_crew(crew_in, service=svc, group_context=ctx, overwrite=True)
    svc.create_with_group.assert_awaited_once_with(crew_in, ctx, overwrite=True)


def _overwrite_client(svc):
    """Build a TestClient for the crews router with deps overridden.

    The HTTP layer is required to exercise the resolved ``Query(False)``
    default for the ``overwrite`` param (a direct call would pass the
    unevaluated Query object instead of the default value).
    """
    app = FastAPI()
    app.include_router(crews_router)
    register_exception_handlers(app)

    async def override_group_context():
        return gc("editor")

    async def override_session():
        return AsyncMock()

    def override_service(session=None):
        return svc

    app.dependency_overrides[get_group_context] = override_group_context
    app.dependency_overrides[get_smart_db_session] = override_session
    app.dependency_overrides[get_crew_service] = override_service
    return TestClient(app, raise_server_exceptions=False)


def _create_payload():
    return {"name": "C", "agent_ids": ["a1"], "task_ids": ["t1"], "nodes": [], "edges": []}


def test_create_crew_overwrite_true_via_http():
    """overwrite=true query param is forwarded to the service as True."""
    svc = AsyncMock()
    svc.create_with_group = AsyncMock(return_value=make_crew())
    client = _overwrite_client(svc)

    resp = client.post("/crews?overwrite=true", json=_create_payload())

    assert resp.status_code == 201
    assert svc.create_with_group.await_args.kwargs["overwrite"] is True


def test_create_crew_overwrite_defaults_to_false_via_http():
    """Omitting the param resolves the Query default and passes overwrite=False."""
    svc = AsyncMock()
    svc.create_with_group = AsyncMock(return_value=make_crew())
    client = _overwrite_client(svc)

    resp = client.post("/crews", json=_create_payload())

    assert resp.status_code == 201
    assert svc.create_with_group.await_args.kwargs["overwrite"] is False


@pytest.mark.asyncio
async def test_debug_and_update_delete_paths():
    svc = AsyncMock()

    # debug returns success structure
    dbg = await debug_crew_data(CrewCreate(name="C", agent_ids=[], task_ids=[], nodes=[], edges=[]), group_context=gc("operator"))
    assert dbg["status"] in ("success", "error")

    # update forbidden, 404, success
    with pytest.raises(Exception):
        await update_crew(uuid4(), CrewUpdate(name="X"), service=svc, group_context=gc("user"))
    svc.update_with_partial_data_by_group = AsyncMock(return_value=None)
    with pytest.raises(Exception):
        await update_crew(uuid4(), CrewUpdate(name="X"), service=svc, group_context=gc("admin"))
    svc.update_with_partial_data_by_group = AsyncMock(return_value=make_crew())
    upd = await update_crew(uuid4(), CrewUpdate(name="X"), service=svc, group_context=gc("editor"))
    assert upd.name in ("C", "X")

    # delete forbidden, 404, success
    with pytest.raises(Exception):
        await delete_crew(uuid4(), service=svc, group_context=gc("user"))
    svc.delete_by_group = AsyncMock(return_value=False)
    with pytest.raises(Exception):
        await delete_crew(uuid4(), service=svc, group_context=gc("editor"))
    svc.delete_by_group = AsyncMock(return_value=True)
    assert await delete_crew(uuid4(), service=svc, group_context=gc("admin")) is None


@pytest.mark.asyncio
async def test_delete_all_crews_admin_only():
    svc = AsyncMock()

    with pytest.raises(Exception):
        await delete_all_crews(service=svc, group_context=gc("operator"))
    svc.delete_all_by_group = AsyncMock(return_value=None)
    assert await delete_all_crews(service=svc, group_context=gc("admin")) is None

