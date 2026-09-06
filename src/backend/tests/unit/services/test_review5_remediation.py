"""Regressions for flow ownership and automatic MCP credential transport."""

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import BackgroundTasks, HTTPException

from src.core.exceptions import BadRequestError, ForbiddenError
from src.schemas.execution import CrewConfig
from src.services.execution.kasal_service import KasalExecutionService
from src.services.execution.service import ExecutionService
from src.services.flow_builder.execution_service import FlowExecutionService
from src.services.flow_builder.flow_runner_service import FlowRunnerService
from src.services.flow_builder.flow_service import FlowService
from src.services.flow_builder.kasal_flow_service import KasalFlowService
from src.services.tools.mcp_integration import MCPIntegration
from src.utils.user_context import GroupContext, UserContext


@pytest.fixture
def caller():
    context = GroupContext(
        group_ids=["tenant-a"],
        group_email="a@example.com",
        user_role="operator",
        highest_role="operator",
        current_user=NS(id="a", personal_group_id="user_a"),
    )
    yield context
    UserContext.clear_context()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner,requested",
    [("tenant-b", "tenant-a"), (None, "tenant-a"), ("tenant-a", None)],
)
async def test_existing_job_cannot_change_owner_or_use_missing_scope(owner, requested):
    record = NS(group_id=owner, inputs={"private": "original"}, flow_id="original")
    service = FlowExecutionService.__new__(FlowExecutionService)
    service.session = NS(commit=AsyncMock())
    service.execution_service = NS(get_run_by_job_id=AsyncMock(return_value=record))
    with pytest.raises(ForbiddenError):
        await service.create_execution(
            uuid.uuid4(), "foreign-job", config={"replace": True}, group_id=requested
        )
    assert vars(record) == {
        "group_id": owner,
        "inputs": {"private": "original"},
        "flow_id": "original",
    }
    service.session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_owned_precreated_job_can_be_completed():
    record = NS(id=1, group_id="tenant-a", inputs={})
    service = FlowExecutionService.__new__(FlowExecutionService)
    service.session = NS(commit=AsyncMock())
    service.execution_service = NS(
        get_run_by_job_id=AsyncMock(return_value=record), reload_run=AsyncMock()
    )
    result = await service.create_execution(
        uuid.uuid4(), "own-job", config={"nodes": [{"id": "own"}]}, group_id="tenant-a"
    )
    assert result is record and record.group_id == "tenant-a"
    assert record.inputs["nodes"] == [{"id": "own"}]
    service.session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_foreign_job_rejected_before_engine_setup(caller):
    with (
        patch.object(
            ExecutionService,
            "get_run_by_job_id",
            new=AsyncMock(return_value=NS(group_id="tenant-b")),
        ),
        patch(
            "src.services.execution.engine_factory.EngineFactory.get_engine",
            new=AsyncMock(),
        ) as engine,
    ):
        with pytest.raises(ForbiddenError):
            await KasalFlowService(NS()).run_flow(
                job_id="foreign-job", group_context=caller
            )
        engine.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["tenant-b", None])
async def test_foreign_or_unowned_saved_flow_rejected_even_with_inline_nodes(
    caller, owner
):
    runner = FlowRunnerService.__new__(FlowRunnerService)
    runner.flow_repo = NS(get=AsyncMock(return_value=NS(group_id=owner)))
    runner.flow_execution_service = NS(create_execution=AsyncMock())
    with pytest.raises(HTTPException) as error:
        await runner.run_flow(
            uuid.uuid4(),
            "new-job",
            config={
                "nodes": [{"id": "injected"}],
                "group_context": caller,
                "group_id": "tenant-a",
            },
        )
    assert error.value.status_code == 403
    runner.flow_execution_service.create_execution.assert_not_awaited()


@pytest.mark.asyncio
async def test_unsaved_canvas_uuid_still_executes(caller):
    runner = FlowRunnerService.__new__(FlowRunnerService)
    runner.flow_repo = NS(get=AsyncMock(return_value=None))
    runner.flow_execution_service = NS(
        create_execution=AsyncMock(return_value=NS(id=1))
    )
    runner._run_flow_execution = AsyncMock(return_value={"success": True})
    await runner.run_flow(
        uuid.uuid4(),
        "new-job",
        config={
            "nodes": [{"id": "own"}],
            "group_context": caller,
            "group_id": "tenant-a",
        },
    )
    runner._run_flow_execution.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner,allowed", [("tenant-a", True), ("tenant-b", False), (None, False)]
)
async def test_saved_flow_load_authorizes_before_content_is_forwarded(
    caller, owner, allowed
):
    flow = NS(
        group_id=owner,
        nodes=[{"id": "private"}],
        edges=[{"source": "private", "target": "end"}],
        flow_config={"startingPoints": []},
    )

    @asynccontextmanager
    async def session_scope():
        yield NS()

    with (
        patch("src.db.session.routed_scoped_session", session_scope),
        patch("src.services.flow_builder.flow_service.FlowRepository") as repo,
        patch.object(
            KasalFlowService, "run_flow", new=AsyncMock(return_value={"success": True})
        ) as run,
    ):
        repo.return_value.get = AsyncMock(return_value=flow)
        result = await KasalExecutionService().run_flow_execution(
            flow_id=str(uuid.uuid4()), job_id="new-job", group_context=caller
        )
        assert result["success"] is allowed
        if allowed:
            assert run.await_args.kwargs["config"]["nodes"] == flow.nodes
        else:
            run.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_flow_id_never_selects_global_latest(caller):
    service = ExecutionService.__new__(ExecutionService)
    service.session = NS()
    with (
        patch.object(FlowService, "get_most_recent_flow", new=AsyncMock()) as latest,
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new=AsyncMock(),
        ) as write,
    ):
        with pytest.raises(BadRequestError, match="Either flow_id or nodes"):
            await service.create_execution(
                CrewConfig(execution_type="flow"), BackgroundTasks(), caller
            )
        latest.assert_not_awaited()
        write.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("from_inputs", [False, True])
async def test_foreign_saved_flow_rejected_before_initial_execution_write(
    caller, from_inputs
):
    fid = str(uuid.uuid4())
    config = CrewConfig(
        execution_type="flow",
        **({"inputs": {"flow_id": fid}} if from_inputs else {"flow_id": fid}),
    )
    service = ExecutionService.__new__(ExecutionService)
    service.session = NS()
    with (
        patch("src.services.flow_builder.flow_service.FlowRepository") as repo,
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new=AsyncMock(),
        ) as write,
    ):
        repo.return_value.get = AsyncMock(return_value=NS(group_id="tenant-b"))
        with pytest.raises(ForbiddenError):
            await service.create_execution(config, BackgroundTasks(), caller)
        write.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth_type,path",
    [
        ("databricks_obo", "/mcp"),
        ("databricks_spn", "/mcp"),
        ("api_key", "/api/2.0/mcp/sql"),
    ],
)
async def test_http_mcp_rejected_before_databricks_credentials_are_resolved(
    auth_type, path
):
    server = {
        "name": "insecure",
        "server_url": "http://adb-fixture.azuredatabricks.net" + path,
        "auth_type": auth_type,
    }
    with (
        patch("src.utils.databricks_auth.get_auth_context", new=AsyncMock()) as auth,
        patch(
            "src.services.tools.mcp_handler.get_or_create_mcp_adapter", new=AsyncMock()
        ) as adapter,
    ):
        assert (
            await MCPIntegration._create_tools_for_server(server, "review", None) == []
        )
        auth.assert_not_awaited()
        adapter.assert_not_awaited()


@pytest.mark.asyncio
async def test_https_mcp_keeps_databricks_authentication():
    server = {
        "name": "secure",
        "server_url": "https://adb-fixture.azuredatabricks.net/api/2.0/mcp/sql",
        "auth_type": "databricks_obo",
    }
    auth = NS(
        token="synthetic-token",
        workspace_url="https://adb-fixture.azuredatabricks.net",
        auth_method="obo",
    )
    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            new=AsyncMock(return_value=auth),
        ),
        patch(
            "src.services.tools.mcp_handler.get_or_create_mcp_adapter",
            new=AsyncMock(return_value=None),
        ) as adapter,
    ):
        await MCPIntegration._create_tools_for_server(server, "review", None)
        assert (
            adapter.await_args.args[0]["headers"]["Authorization"]
            == "Bearer synthetic-token"
        )


@pytest.mark.asyncio
async def test_local_http_without_automatic_credentials_still_connects():
    server = {
        "name": "local",
        "server_url": "http://localhost:8000/mcp",
        "auth_type": "api_key",
    }
    with (
        patch("src.utils.databricks_auth.get_auth_context", new=AsyncMock()) as auth,
        patch(
            "src.services.tools.mcp_handler.get_or_create_mcp_adapter",
            new=AsyncMock(return_value=None),
        ) as adapter,
    ):
        await MCPIntegration._create_tools_for_server(server, "review", None)
        auth.assert_not_awaited()
        assert adapter.await_args.args[0]["headers"] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["sse", "streamable"])
async def test_connection_probe_does_not_send_automatic_credentials_over_http(
    transport,
):
    from src.schemas.mcp import MCPTestConnectionRequest
    from src.services.mcp.mcp_client.service import MCPService

    service = MCPService.__new__(MCPService)
    request = MCPTestConnectionRequest(
        server_url="http://adb-fixture.azuredatabricks.net/mcp",
        server_type=transport,
        auth_type="databricks_obo",
        api_key="",
    )
    with patch("src.utils.databricks_auth.get_auth_context", new=AsyncMock()) as auth:
        response = await service.test_connection(request)
        assert response.success is False and "HTTPS" in response.message
        auth.assert_not_awaited()


@pytest.mark.asyncio
async def test_adapter_rejects_http_automatic_and_cached_fallback_credentials():
    from src.services.tools.mcp_adapter import MCPAdapter

    adapter = MCPAdapter(
        {
            "url": "http://adb-fixture.azuredatabricks.net/mcp",
            "auth_type": "databricks_obo",
            "headers": {"Authorization": "Bearer synthetic"},
        }
    )
    with patch("src.utils.databricks_auth.get_auth_context", new=AsyncMock()) as auth:
        assert await adapter._get_spn_fallback_headers() is None
        assert await adapter._get_authentication_headers() is None
        adapter._spn_fallback_headers = {"Authorization": "Bearer synthetic-spn"}
        assert await adapter._get_authentication_headers() is None
        auth.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("user_token", [None, "synthetic-obo"])
async def test_auth_fallback_never_obtains_obo_or_cli_credentials_for_http(user_token):
    from src.utils.databricks_auth import DatabricksAuth, get_mcp_auth_headers

    with (
        patch.object(DatabricksAuth, "_load_config", new=AsyncMock()) as load,
        patch("src.utils.databricks_auth.get_mcp_access_token", new=AsyncMock()) as cli,
    ):
        headers, error = await get_mcp_auth_headers(
            "http://localhost/mcp", user_token=user_token
        )
        assert headers is None and "HTTPS" in error
        headers, error = await DatabricksAuth().get_auth_headers(
            "http://localhost/mcp", user_token=user_token
        )
        assert headers is None and "HTTPS" in error
        load.assert_not_awaited()
        cli.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_local_api_key_does_not_trigger_automatic_obo_fallback():
    from src.utils.databricks_auth import DatabricksAuth, get_mcp_auth_headers

    with patch.object(DatabricksAuth, "get_auth_headers", new=AsyncMock()) as obo:
        headers, error = await get_mcp_auth_headers(
            "http://localhost/mcp", user_token="synthetic-obo", api_key="local-key"
        )
        assert headers == {"Authorization": "Bearer local-key"} and error is None
        obo.assert_not_awaited()


@pytest.mark.asyncio
async def test_owned_saved_flow_preserves_workspace_through_api_and_runner(caller):
    from src.api.executions_router import create_execution

    fid = uuid.uuid4()
    flow = NS(
        id=fid,
        name="Own flow",
        group_id="tenant-a",
        nodes=[{"id": "own-node"}],
        edges=[{"source": "own-node", "target": "end"}],
        flow_config={"startingPoints": []},
    )
    session = NS(commit=AsyncMock())
    record = NS(id=1, group_id="tenant-a", inputs={})

    @asynccontextmanager
    async def session_scope():
        yield session

    runner = FlowRunnerService.__new__(FlowRunnerService)
    runner.flow_repo = NS(get=AsyncMock(return_value=flow))
    runner.flow_execution_service = FlowExecutionService(session)
    runner._run_flow_execution = AsyncMock(return_value={"success": True})

    async def engine_bridge(execution_id, flow_config, group_context, user_token):
        assert group_context.primary_group_id == "tenant-a"
        await runner.run_flow(flow_config["flow_id"], execution_id, config=flow_config)
        return execution_id

    service = ExecutionService.__new__(ExecutionService)
    service.session = session
    with (
        patch("src.services.flow_builder.flow_service.FlowRepository") as repo,
        patch("src.db.session.routed_scoped_session", session_scope),
        patch.object(
            ExecutionService, "get_run_by_job_id", new=AsyncMock(return_value=record)
        ),
        patch.object(ExecutionService, "reload_run", new=AsyncMock()),
        patch.object(ExecutionService, "_generate_run_name_async", new=AsyncMock()),
        patch(
            "src.services.execution.status.ExecutionStatusService.create_execution",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "src.services.execution.engine_factory.EngineFactory.get_engine",
            new=AsyncMock(return_value=NS(run_flow=engine_bridge)),
        ),
    ):
        repo.return_value.get = AsyncMock(return_value=flow)
        background = BackgroundTasks()
        result = await create_execution(
            CrewConfig(execution_type="flow", flow_id=str(fid)),
            background,
            service,
            caller,
        )
        await background()
        assert result.status == "RUNNING"
        runner._run_flow_execution.assert_awaited_once()
        assert record.group_id == "tenant-a" and record.inputs["nodes"] == flow.nodes


@pytest.mark.asyncio
@pytest.mark.parametrize("as_string", [False, True])
async def test_saved_flow_authorization_with_real_sqlite_uuid_column(caller, as_string):
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from src.models.flow import Flow

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Flow.__table__.create)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            flow = Flow(
                name="Own saved flow", group_id="tenant-a", nodes=[{"id": "own"}]
            )
            session.add(flow)
            await session.commit()
            flow_id = str(flow.id) if as_string else flow.id
            service = FlowService(session)
            loaded = await service.get_flow_for_execution(flow_id, caller)
            assert loaded.id == flow.id and loaded.nodes == [{"id": "own"}]
            with pytest.raises(ForbiddenError):
                await service.get_flow_for_execution(
                    flow_id, NS(group_ids=["tenant-b"])
                )
            assert (
                await service.get_flow_for_execution(
                    str(uuid.uuid4()), caller, allow_unsaved=True
                )
                is None
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_invalid_flow_id_rejected_before_database_lookup(caller):
    with patch("src.services.flow_builder.flow_service.FlowRepository") as repo:
        with pytest.raises(BadRequestError):
            await FlowService(NS()).get_flow_for_execution("invalid", caller)
        repo.assert_not_called()
