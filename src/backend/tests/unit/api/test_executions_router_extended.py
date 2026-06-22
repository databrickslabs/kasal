"""
Extended tests for executions_router.py to cover missing lines.
Focuses on: get_execution_service factory, flow_id lookup branches,
list_executions result processing (JSON parse, list, bool, other),
stop execution not-running state, and debug-context endpoint.
"""
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from src.api.executions_router import (
    create_execution,
    get_execution_service,
    get_execution_status,
    list_executions,
    stop_execution,
    force_stop_execution,
    debug_context,
)
from src.schemas.execution import (
    CrewConfig,
    StopExecutionRequest,
    StopType,
)
from src.core.exceptions import ForbiddenError, NotFoundError


class Ctx:
    def __init__(self, user_role="admin", group_ids=None, group_email="u@x", access_token="tok"):
        self.user_role = user_role
        self.group_ids = group_ids or ["g1"]
        self.group_email = group_email
        self.access_token = access_token
        self.primary_group_id = "g1"
        self.email_domain = "x"


# ── get_execution_service dependency ─────────────────────────────────────────

def test_get_execution_service_returns_service():
    """get_execution_service creates ExecutionService with session."""
    from src.services.execution_service import ExecutionService

    fake_session = MagicMock()
    with patch("src.api.executions_router.ExecutionService") as MockSvc:
        MockSvc.return_value = MagicMock(spec=ExecutionService)
        svc = get_execution_service(session=fake_session)
        MockSvc.assert_called_once_with(session=fake_session)


# ── create_execution: flow_id found in DB ─────────────────────────────────────

@pytest.mark.asyncio
async def test_create_execution_with_valid_flow_id_in_db():
    """create_execution verifies saved flow exists before executing."""
    import uuid

    svc = AsyncMock()
    svc.session = MagicMock()
    svc.create_execution = AsyncMock(
        return_value={"execution_id": "e1", "status": "created", "run_name": "r"}
    )

    flow_id = str(uuid.uuid4())
    cfg = CrewConfig(agents_yaml={"a": {}}, tasks_yaml={"t": {}}, inputs={}, flow_id=flow_id)
    ctx = Ctx(user_role="admin")

    mock_flow = SimpleNamespace(id=flow_id, name="My Flow")
    mock_flow_svc = AsyncMock()
    mock_flow_svc.get_flow = AsyncMock(return_value=mock_flow)

    with patch("src.api.executions_router.FlowService", return_value=mock_flow_svc):
        out = await create_execution(
            cfg,
            background_tasks=SimpleNamespace(add_task=lambda *a, **k: None),
            service=svc,
            group_context=ctx,
        )
    assert out.execution_id == "e1"


@pytest.mark.asyncio
async def test_create_execution_flow_id_not_found_raises():
    """create_execution raises ValueError when flow not found in DB."""
    import uuid
    from fastapi import HTTPException

    svc = AsyncMock()
    svc.session = MagicMock()

    flow_id = str(uuid.uuid4())
    cfg = CrewConfig(agents_yaml={"a": {}}, tasks_yaml={"t": {}}, inputs={}, flow_id=flow_id)
    ctx = Ctx(user_role="admin")

    mock_flow_svc = AsyncMock()
    mock_flow_svc.get_flow = AsyncMock(
        side_effect=HTTPException(status_code=404, detail="Not found")
    )

    with patch("src.api.executions_router.FlowService", return_value=mock_flow_svc):
        with pytest.raises(ValueError, match="not found"):
            await create_execution(
                cfg,
                background_tasks=SimpleNamespace(add_task=lambda *a, **k: None),
                service=svc,
                group_context=ctx,
            )


@pytest.mark.asyncio
async def test_create_execution_with_nodes_skips_db_lookup():
    """create_execution skips DB flow lookup when nodes provided in config."""
    import uuid

    svc = AsyncMock()
    svc.session = MagicMock()
    svc.create_execution = AsyncMock(
        return_value={"execution_id": "e2", "status": "created", "run_name": "r2"}
    )

    flow_id = str(uuid.uuid4())
    cfg = CrewConfig(
        agents_yaml={"a": {}},
        tasks_yaml={"t": {}},
        inputs={},
        flow_id=flow_id,
        nodes=[{"id": "n1", "type": "crew"}],
    )
    ctx = Ctx(user_role="admin")

    # FlowService should NOT be called when nodes are provided
    with patch("src.api.executions_router.FlowService") as MockFlowSvc:
        out = await create_execution(
            cfg,
            background_tasks=SimpleNamespace(add_task=lambda *a, **k: None),
            service=svc,
            group_context=ctx,
        )
    MockFlowSvc.assert_not_called()
    assert out.execution_id == "e2"


# ── get_execution_status result processing ────────────────────────────────────

def make_exec_data(**kwargs):
    """Build a minimal valid ExecutionResponse data dict."""
    from datetime import datetime
    base = {
        "execution_id": "e1",
        "status": "completed",
        "run_name": "r",
        "created_at": datetime.utcnow().isoformat(),
        "result": None,
    }
    base.update(kwargs)
    return base


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_get_execution_status_json_string_result(MockExecSvc):
    """get_execution_status parses JSON string result into dict."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value=make_exec_data(
        result=json.dumps({"output": "hello"}),
    ))
    MockExecSvc.return_value = svc

    out = await get_execution_status("e1", group_context=Ctx(), db=MagicMock())
    assert isinstance(out.result, dict)
    assert out.result["output"] == "hello"


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_get_execution_status_invalid_json_string_result(MockExecSvc):
    """get_execution_status wraps invalid JSON string in dict."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value=make_exec_data(
        result="not-json-string",
    ))
    MockExecSvc.return_value = svc

    out = await get_execution_status("e1", group_context=Ctx(), db=MagicMock())
    assert isinstance(out.result, dict)
    assert out.result["value"] == "not-json-string"


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_get_execution_status_list_result(MockExecSvc):
    """get_execution_status wraps list result in dict."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value=make_exec_data(
        result=["item1", "item2"],
    ))
    MockExecSvc.return_value = svc

    out = await get_execution_status("e1", group_context=Ctx(), db=MagicMock())
    assert isinstance(out.result, dict)
    assert "items" in out.result


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_get_execution_status_bool_result(MockExecSvc):
    """get_execution_status wraps bool result in dict."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value=make_exec_data(
        result=True,
    ))
    MockExecSvc.return_value = svc

    out = await get_execution_status("e1", group_context=Ctx(), db=MagicMock())
    assert isinstance(out.result, dict)
    assert out.result["success"] is True


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_get_execution_status_unexpected_type_result(MockExecSvc):
    """get_execution_status sets result to empty dict for unexpected types."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value=make_exec_data(
        result=42,  # integer — unexpected type
    ))
    MockExecSvc.return_value = svc

    out = await get_execution_status("e1", group_context=Ctx(), db=MagicMock())
    assert out.result == {}


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_get_execution_status_not_found(MockExecSvc):
    """get_execution_status raises NotFoundError when no data returned."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value=None)
    MockExecSvc.return_value = svc

    with pytest.raises(NotFoundError):
        await get_execution_status("missing", group_context=Ctx(), db=MagicMock())


# ── list_executions result processing ─────────────────────────────────────────

@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_list_executions_various_result_types(MockExecSvc):
    """list_executions processes JSON string, list, bool, and unexpected types."""
    from datetime import datetime

    ts = datetime.utcnow().isoformat()
    svc = AsyncMock()
    svc.list_executions = AsyncMock(return_value=[
        {"execution_id": "e1", "status": "done", "run_name": "r1", "created_at": ts, "result": json.dumps({"k": "v"})},
        {"execution_id": "e2", "status": "done", "run_name": "r2", "created_at": ts, "result": ["a", "b"]},
        {"execution_id": "e3", "status": "done", "run_name": "r3", "created_at": ts, "result": True},
        {"execution_id": "e4", "status": "done", "run_name": "r4", "created_at": ts, "result": 99},
        {"execution_id": "e5", "status": "done", "run_name": "r5", "created_at": ts, "result": "bad-json"},
        {"execution_id": "e6", "status": "done", "run_name": "r6", "created_at": ts, "result": None},
    ])
    MockExecSvc.return_value = svc

    out = await list_executions(group_context=Ctx(), db=MagicMock())
    assert len(out) == 6
    assert out[0].result == {"k": "v"}
    assert out[1].result == {"items": ["a", "b"]}
    assert out[2].result == {"success": True}
    assert out[3].result == {}
    assert out[4].result == {"value": "bad-json"}
    assert out[5].result is None


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_list_executions_scopes_to_selected_workspace(MockExecSvc):
    """Only the explicitly selected workspace is queried, never the UNION of all
    the user's groups. group_context.group_ids can be a union (e.g. when the
    personal workspace is selected it is personal + every group), so the endpoint
    must scope strictly to the selected `group_id` header for tenant isolation."""
    svc = AsyncMock()
    svc.list_executions = AsyncMock(return_value=[])
    MockExecSvc.return_value = svc

    # Union as produced by GroupContext.from_email for the personal workspace case.
    ctx = Ctx(group_ids=["user_alice_x", "g1", "g2"])
    out = await list_executions(group_context=ctx, db=MagicMock(), x_group_id="g1")

    assert isinstance(out, list)
    _, kwargs = svc.list_executions.call_args
    assert kwargs["group_ids"] == ["g1"], "must scope to the selected workspace, not the union"


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_list_executions_no_workspace_selected_fails_closed(MockExecSvc):
    """With no selected workspace (no group_id header) the endpoint fails closed
    (empty group filter) rather than returning the union of the user's groups."""
    svc = AsyncMock()
    svc.list_executions = AsyncMock(return_value=[])
    MockExecSvc.return_value = svc

    ctx = Ctx(group_ids=["g1", "g2"])
    out = await list_executions(group_context=ctx, db=MagicMock(), x_group_id=None)

    assert out == []
    _, kwargs = svc.list_executions.call_args
    assert kwargs["group_ids"] == []


@pytest.mark.asyncio
@patch("src.api.executions_router.ExecutionService")
async def test_list_executions_unauthorized_group_fails_closed(MockExecSvc):
    """Defensive: a selected group_id not present in the authorized context is
    rejected (fail closed) even though get_group_context normally 403s first."""
    svc = AsyncMock()
    svc.list_executions = AsyncMock(return_value=[])
    MockExecSvc.return_value = svc

    ctx = Ctx(group_ids=["g1"])
    out = await list_executions(group_context=ctx, db=MagicMock(), x_group_id="g_other")

    assert out == []
    _, kwargs = svc.list_executions.call_args
    assert kwargs["group_ids"] == []


# ── stop_execution: non-running state ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_stop_execution_not_in_running_state_returns_response():
    """stop_execution returns response with message when execution not running."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value={
        "status": "completed",
        "result": {"output": "done"},
    })
    ctx = Ctx(user_role="admin")

    out = await stop_execution(
        "e1",
        StopExecutionRequest(stop_type=StopType.GRACEFUL),
        service=svc,
        group_context=ctx,
        db=MagicMock(),
    )
    assert "not running" in out.message.lower()


@pytest.mark.asyncio
async def test_stop_execution_preparing_state_is_stoppable():
    """stop_execution proceeds for PREPARING status."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value={
        "status": "PREPARING",
        "result": None,
    })
    svc.stop_execution = AsyncMock(return_value={
        "execution_id": "e1",
        "status": "stopping",
        "message": "Stopping",
        "partial_results": None,
    })
    ctx = Ctx(user_role="admin")

    out = await stop_execution(
        "e1",
        StopExecutionRequest(stop_type=StopType.GRACEFUL),
        service=svc,
        group_context=ctx,
        db=MagicMock(),
    )
    assert out.execution_id == "e1"


@pytest.mark.asyncio
async def test_stop_execution_not_found_raises():
    """stop_execution raises NotFoundError when execution not found."""
    svc = AsyncMock()
    svc.get_execution_status = AsyncMock(return_value=None)
    ctx = Ctx(user_role="admin")

    with pytest.raises(NotFoundError):
        await stop_execution(
            "missing",
            StopExecutionRequest(),
            service=svc,
            group_context=ctx,
            db=MagicMock(),
        )


# ── debug_context endpoint ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_debug_context_returns_404_when_not_debug():
    """debug_context raises HTTPException 404 when DEBUG_MODE is off."""
    from fastapi import HTTPException
    from src.config.settings import settings as app_settings

    orig = app_settings.DEBUG_MODE
    app_settings.DEBUG_MODE = False
    try:
        with pytest.raises(HTTPException) as exc_info:
            await debug_context(group_context=Ctx())
        assert exc_info.value.status_code == 404
    finally:
        app_settings.DEBUG_MODE = orig


@pytest.mark.asyncio
async def test_debug_context_returns_group_info_in_debug_mode():
    """debug_context returns group context data when DEBUG_MODE is on."""
    from src.config.settings import settings as app_settings

    orig = app_settings.DEBUG_MODE
    app_settings.DEBUG_MODE = True
    try:
        ctx = Ctx(group_ids=["g1", "g2"])
        out = await debug_context(group_context=ctx)
        assert "group_ids" in out
        # group_ids contains the list from the context
        assert isinstance(out["group_ids"], list)
    finally:
        app_settings.DEBUG_MODE = orig
