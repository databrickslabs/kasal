"""Adversarial regressions for the third/fourth security reviews."""

import importlib
from datetime import datetime
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.utils.user_context import GroupContext


def caller():
    user = NS(
        id="attacker",
        email="alice_smith@example.com",
        personal_group_id="user_allocated_a",
        is_system_admin=False,
    )
    return GroupContext(
        group_ids=[user.personal_group_id],
        group_email=user.email,
        email_domain="example.com",
        current_user=user,
        user_role="editor",
        highest_role="editor",
    )


@pytest.mark.asyncio
async def test_deployment_never_queries_colliding_legacy_pat():
    from src.services.deployment.app import CrewAppDeploymentService

    service = CrewAppDeploymentService.__new__(CrewAppDeploymentService)
    service.session = NS()
    queries = []

    async def lookup(name, group_id):
        queries.append(group_id)
        return (
            NS(encrypted_value="victim-secret")
            if group_id == "user_alice_smith_example_com"
            else None
        )

    with patch("src.services.settings.api_keys.ApiKeyRepository") as Repo:
        Repo.return_value.find_by_name = AsyncMock(side_effect=lookup)
        with pytest.raises(PermissionError, match="PAT"):
            await service._get_deploy_client(caller(), "create apps")
    assert set(queries) == {"user_allocated_a"}


def test_deployment_status_requires_owner_and_matching_crew():
    from src.services.deployment.app import CrewAppDeploymentService

    service = CrewAppDeploymentService.__new__(CrewAppDeploymentService)
    service._deployments = {
        "d": dict(
            deployment_id="d",
            crew_id="crew",
            group_id="tenant-b",
            app_name="private",
            status="FAILED",
        )
    }
    assert service.get_owned_status("d", "crew", caller()) is None
    service._deployments["d"]["group_id"] = "user_allocated_a"
    assert service.get_owned_status("d", "other-crew", caller()) is None
    assert service.get_owned_status("d", "crew", caller()).app_name == "private"
    del service._deployments["d"]["group_id"]
    assert service.get_owned_status("d", "crew", caller()) is None


@pytest.mark.asyncio
async def test_history_uses_allocated_scope_and_returns_nonempty_result():
    router = importlib.import_module("src.api.execution_history_router")
    from src.services.execution.history import ExecutionHistoryService

    context = caller()
    row = NS(id=7, job_id="own", created_at=datetime.now(), result={"content": "own"})
    service = ExecutionHistoryService.__new__(ExecutionHistoryService)
    service.history_repo = NS(get_execution_history=AsyncMock(return_value=([row], 1)))
    with (
        patch.object(router, "UserService") as Users,
        patch.object(router, "GroupService") as Groups,
    ):
        Users.return_value.get_or_create_user_by_email = AsyncMock(
            return_value=context.current_user
        )
        Groups.return_value.get_user_groups = AsyncMock(return_value=[])
        result = await router.get_all_groups_execution_history(
            None, 50, 0, context.group_email, None, service
        )
    assert result.executions[0].result == {"content": "own"}
    service.history_repo.get_execution_history.assert_awaited_once_with(
        limit=50, offset=0, group_ids=["user_allocated_a"]
    )


@pytest.mark.asyncio
async def test_real_allocation_is_atomic_and_preserves_other_users():
    from src.models.user import User
    from src.repositories.user_repository import UserRepository
    from src.services.groups.users import UserService

    engine = create_async_engine("sqlite+aiosqlite://")
    try:
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                "CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT, personal_group_id TEXT UNIQUE, updated_at TEXT)"
            )
            await conn.exec_driver_sql(
                "INSERT INTO users (id,email,personal_group_id) VALUES ('a','alice.smith@example.com','user_alice_smith_example_com'),('b','alice_smith@example.com',NULL)"
            )
        async with async_sessionmaker(engine)() as session:
            repo = UserRepository(User, session)
            service = UserService.__new__(UserService)
            service.session, service.user_repo = session, repo
            user = NS(id="b", email="alice_smith@example.com", personal_group_id=None)
            assigned = await service.ensure_personal_workspace_id(user)
            assert (
                assigned.startswith("user_")
                and assigned != "user_alice_smith_example_com"
            )
            assert await repo.allocate_personal_group_id("b", "replacement") == assigned
            await session.commit()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_failed_allocation_never_sets_or_returns_legacy_scope():
    from src.services.groups.users import UserService

    service = UserService.__new__(UserService)
    transaction = AsyncMock()
    service.session = NS(begin_nested=lambda: transaction)
    service.user_repo = NS(
        allocate_personal_group_id=AsyncMock(side_effect=RuntimeError("unavailable"))
    )
    user = NS(id="b", email="alice_smith@example.com", personal_group_id=None)
    with pytest.raises(RuntimeError):
        await service.ensure_personal_workspace_id(user)
    with pytest.raises(ValueError, match="Access denied"):
        GroupContext.personal_workspace_id_of(user, user.email)


@pytest.mark.asyncio
async def test_legacy_collision_denied_if_startup_migration_failed():
    from src.services.groups.users import UserService

    service = UserService.__new__(UserService)
    service.user_repo = NS(has_legacy_personal_collision=AsyncMock(return_value=True))
    user = NS(
        id="a",
        email="alice.smith@example.com",
        personal_group_id="user_alice_smith_example_com",
    )
    with pytest.raises(ValueError, match="ownership review"):
        await service.ensure_personal_workspace_id(user)


@pytest.mark.asyncio
@pytest.mark.parametrize("source_id", [71, "victim-job"])
async def test_resume_rejects_foreign_source_before_commit(source_id):
    from src.services.flow_builder.flow_runner_service import FlowRunnerService

    victim = NS(id=71, job_id="victim-job", group_id="tenant-b", status="completed")
    runner = FlowRunnerService.__new__(FlowRunnerService)
    runner.db = NS(commit=AsyncMock())
    runner._run_dynamic_flow = AsyncMock()
    with patch("src.services.execution.service.ExecutionService") as Service:
        Service.return_value.get_run_by_job_id = AsyncMock(return_value=victim)
        with pytest.raises(HTTPException) as denied:
            await runner.run_flow(
                None,
                "new-job",
                config={
                    "nodes": [{"id": "a"}],
                    "group_id": "tenant-a",
                    "resume_from_execution_id": source_id,
                },
            )
    assert denied.value.status_code == 404
    assert victim.status == "completed"
    runner.db.commit.assert_not_awaited()
    runner._run_dynamic_flow.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_rejects_foreign_requested_job_even_with_owned_source():
    from src.services.flow_builder.flow_runner_service import FlowRunnerService

    own = NS(id=1, group_id="tenant-a")
    foreign = NS(id=2, group_id="tenant-b", status="completed")
    runner = FlowRunnerService.__new__(FlowRunnerService)
    runner.db = NS(commit=AsyncMock())
    with patch("src.services.execution.service.ExecutionService") as Service:
        Service.return_value.get_run_by_job_id = AsyncMock(side_effect=[own, foreign])
        with pytest.raises(HTTPException):
            await runner.run_flow(
                None,
                "foreign-job",
                config={
                    "nodes": [{"id": "a"}],
                    "group_id": "tenant-a",
                    "resume_from_execution_id": "own-source",
                },
            )
    runner.db.commit.assert_not_awaited()
    assert foreign.status == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner,groups", [("tenant-b", ["tenant-a"]), ("tenant-a", []), (None, ["tenant-a"])]
)
async def test_checkpoint_loader_denies_foreign_or_unresolved_scope(owner, groups):
    from src.services.flow_builder.checkpoint_resume import load_resume_outputs

    service = NS(get_run_by_job_id=AsyncMock(return_value=NS(group_id=owner)))
    with pytest.raises(HTTPException):
        await load_resume_outputs(
            "source", {"execution_history": service}, group_ids=groups
        )


@pytest.mark.asyncio
async def test_owned_checkpoint_still_restores():
    from src.services.flow_builder.checkpoint_resume import load_resume_outputs

    row = NS(
        job_id="source",
        group_id="tenant-a",
        checkpoint_data={
            "checkpoint": {
                "version": 1,
                "kind": "flow",
                "units": {
                    "0": {"key": "0", "index": 0, "name": "crew", "output_raw": "own"}
                },
            }
        },
    )
    service = NS(get_run_by_job_id=AsyncMock(return_value=row))
    outputs, _ = await load_resume_outputs(
        "source", {"execution_history": service}, group_ids=["tenant-a"]
    )
    assert outputs == {"crew": "own"}
    service.get_run_by_job_id.assert_awaited_once_with("source", group_ids=["tenant-a"])


@pytest.mark.asyncio
@pytest.mark.parametrize("source_id", [71, "victim-job"])
async def test_public_flow_service_denies_foreign_source_before_engine(source_id):
    from src.services.flow_builder.kasal_flow_service import KasalFlowService

    service = KasalFlowService.__new__(KasalFlowService)
    service.session = NS()
    foreign = NS(id=71, group_id="tenant-b", job_id="victim-job")
    with (
        patch("src.services.execution.service.ExecutionService") as Runs,
        patch(
            "src.services.execution.engine_factory.EngineFactory.get_engine",
            new_callable=AsyncMock,
        ) as engine,
    ):
        Runs.return_value.get_run_by_job_id = AsyncMock(return_value=foreign)
        with pytest.raises(HTTPException) as denied:
            await service.run_flow(
                run_name="resume",
                group_context=caller(),
                resume_from_execution_id=source_id,
            )
    assert denied.value.status_code == 404
    engine.assert_not_awaited()


def test_opaque_personal_identity_allows_gmail_only_in_allocated_workspace():
    from src.services.tools.gmail_tool import GmailTool
    from src.services.tools.tool_service import _is_personal_workspace

    context = caller()
    assert _is_personal_workspace(context)
    tool = GmailTool(
        tool_config={},
        group_id=context.primary_group_id,
        user_email=context.group_email,
        personal_group_id=context.current_user.personal_group_id,
    )
    assert tool._is_personal_workspace()
    context.group_ids = [GroupContext.generate_individual_group_id(context.group_email)]
    assert not _is_personal_workspace(context)
    tool._group_id = context.primary_group_id
    assert not tool._is_personal_workspace()


@pytest.mark.parametrize("configured_personal_id", ["user_allocated_a", "user_foreign"])
def test_gmail_factory_uses_authenticated_allocation_not_tool_override(
    configured_personal_id,
):
    from unittest.mock import MagicMock
    from src.services.tools.tool_factory import ToolFactory
    from src.services.tools.gmail_tool import GmailTool

    context = caller()
    factory = ToolFactory(
        config={
            "group_id": context.primary_group_id,
            "user_email": context.group_email,
        },
        user_token="obo",
    )
    info = MagicMock(id=96, title="Gmail", config={})
    factory._available_tools["Gmail"] = info
    factory._tool_implementations["Gmail"] = GmailTool
    with patch(
        "src.utils.user_context.UserContext.get_group_context", return_value=context
    ):
        tool = factory.create_tool(
            "Gmail", tool_config_override={"personal_group_id": configured_personal_id}
        )
    assert isinstance(tool, GmailTool)
    assert tool._personal_group_id == context.current_user.personal_group_id
    assert tool._is_personal_workspace()
