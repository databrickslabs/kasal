"""
Unit tests for execution_history_router module.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.api.execution_history_router import (
    check_execution_exists,
    debug_execution_groups,
    delete_all_executions,
    delete_execution,
    delete_execution_by_job_id,
    get_all_groups_execution_history,
    get_execution_by_id,
    get_execution_debug_outputs,
    get_execution_history,
    get_execution_outputs,
    update_execution_result,
)
from src.core.exceptions import NotFoundError
from src.schemas.execution_history import (
    DeleteResponse,
    ExecutionHistoryList,
    ExecutionOutputDebugList,
    ExecutionOutputList,
    UpdateExecutionResultRequest,
)
from src.utils.user_context import GroupContext


def make_gc(role="admin"):
    return GroupContext(
        group_ids=["g1", "g2"],
        group_email="user@test.com",
        email_domain="test.com",
        user_role=role,
    )


# The caller's email arrives through ``RequestEmailDep`` (the shared identity
# resolver, which 401s when there is none and owns the header precedence; see
# tests/unit/api/test_external_identity_precedence.py). These handlers are
# therefore always called with a resolved ``user_email``.


def _user_and_group_services(user, groups):
    mock_user_svc = AsyncMock()
    mock_user_svc.get_or_create_user_by_email = AsyncMock(return_value=user)
    mock_group_svc = AsyncMock()
    mock_group_svc.get_user_groups = AsyncMock(return_value=groups)
    return mock_user_svc, mock_group_svc


class TestDebugExecutionGroups:
    @pytest.mark.asyncio
    async def test_returns_404_when_debug_mode_off(self):
        """debug_execution_groups raises 404 when DEBUG_MODE is False."""
        session = MagicMock()
        with patch("src.api.execution_history_router.settings") as mock_settings:
            mock_settings.DEBUG_MODE = False
            from fastapi import HTTPException

            with pytest.raises(HTTPException) as exc_info:
                await debug_execution_groups(
                    session=session, user_email="user@test.com"
                )
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_groups_info_when_user_not_found(self):
        """With DEBUG_MODE on, execution groups are listed even if the user is unknown."""
        session = MagicMock()
        mock_svc = AsyncMock()
        mock_svc.get_execution_groups_with_counts = AsyncMock(return_value=[("g1", 5)])
        mock_user_svc, mock_group_svc = _user_and_group_services(None, [])

        with (
            patch("src.api.execution_history_router.settings") as mock_settings,
            patch(
                "src.api.execution_history_router.ExecutionHistoryService",
                return_value=mock_svc,
            ),
            patch(
                "src.api.execution_history_router.UserService",
                return_value=mock_user_svc,
            ),
            patch(
                "src.api.execution_history_router.GroupService",
                return_value=mock_group_svc,
            ),
        ):
            mock_settings.DEBUG_MODE = True
            result = await debug_execution_groups(
                session=session, user_email="ghost@test.com"
            )
        assert result["total_unique_groups"] == 1
        assert result["user_email"] == "ghost@test.com"
        assert result["user_groups"] == []
        assert result["all_execution_groups"] == [
            {"group_id": "g1", "execution_count": 5}
        ]
        mock_group_svc.get_user_groups.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_user_groups_for_resolved_email(self):
        """debug_execution_groups fetches the resolved caller's groups."""
        session = MagicMock()
        mock_svc = AsyncMock()
        mock_svc.get_execution_groups_with_counts = AsyncMock(return_value=[("g1", 3)])

        mock_user = SimpleNamespace(id="u1", personal_group_id="user_allocated")
        mock_group = SimpleNamespace(id="g1", name="Group 1")
        mock_user_svc, mock_group_svc = _user_and_group_services(
            mock_user, [mock_group]
        )

        with (
            patch("src.api.execution_history_router.settings") as mock_settings,
            patch(
                "src.api.execution_history_router.ExecutionHistoryService",
                return_value=mock_svc,
            ),
            patch(
                "src.api.execution_history_router.UserService",
                return_value=mock_user_svc,
            ),
            patch(
                "src.api.execution_history_router.GroupService",
                return_value=mock_group_svc,
            ),
        ):
            mock_settings.DEBUG_MODE = True
            result = await debug_execution_groups(
                session=session, user_email="user@test.com"
            )
        assert result["user_email"] == "user@test.com"
        assert result["user_groups"] == [{"id": "g1", "name": "Group 1"}]
        mock_user_svc.get_or_create_user_by_email.assert_awaited_once_with(
            "user@test.com"
        )
        mock_group_svc.get_user_groups.assert_awaited_once_with("u1")


class TestGetAllGroupsExecutionHistory:
    @pytest.mark.asyncio
    async def test_returns_empty_when_user_not_found(self):
        """Returns empty list when user cannot be found."""
        session = MagicMock()
        service = AsyncMock()
        mock_user_svc, _ = _user_and_group_services(None, [])

        with patch(
            "src.api.execution_history_router.UserService", return_value=mock_user_svc
        ):
            result = await get_all_groups_execution_history(
                session=session,
                user_email="user@test.com",
                limit=50,
                offset=0,
                include_payload=False,
                service=service,
            )
        assert result.total == 0
        assert result.executions == []
        mock_user_svc.get_or_create_user_by_email.assert_awaited_once_with(
            "user@test.com"
        )
        service.get_execution_history.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetches_executions_for_all_groups(self):
        """Returns executions from all user groups plus the personal workspace."""
        session = MagicMock()
        mock_user = SimpleNamespace(id="u1", personal_group_id="user_allocated")
        mock_group = SimpleNamespace(id="g1", name="Group 1")
        mock_user_svc, mock_group_svc = _user_and_group_services(
            mock_user, [mock_group]
        )

        expected_list = ExecutionHistoryList(executions=[], total=3, offset=0, limit=50)
        service = AsyncMock()
        service.get_execution_history = AsyncMock(return_value=expected_list)

        with (
            patch(
                "src.api.execution_history_router.UserService",
                return_value=mock_user_svc,
            ),
            patch(
                "src.api.execution_history_router.GroupService",
                return_value=mock_group_svc,
            ),
        ):
            result = await get_all_groups_execution_history(
                session=session,
                user_email="user@test.com",
                limit=50,
                offset=0,
                include_payload=False,
                service=service,
            )
        assert result.total == 3
        service.get_execution_history.assert_awaited_once_with(
            50, 0, group_ids=["g1", "user_allocated"], include_payload=False
        )

    @pytest.mark.asyncio
    async def test_adds_personal_workspace_to_groups(self):
        """Adds the allocated personal workspace ID to the group list."""
        session = MagicMock()
        mock_user = SimpleNamespace(id="u1", personal_group_id="user_allocated")
        mock_user_svc, mock_group_svc = _user_and_group_services(mock_user, [])

        expected_list = ExecutionHistoryList(executions=[], total=0, offset=0, limit=50)
        service = AsyncMock()
        service.get_execution_history = AsyncMock(return_value=expected_list)

        with (
            patch(
                "src.api.execution_history_router.UserService",
                return_value=mock_user_svc,
            ),
            patch(
                "src.api.execution_history_router.GroupService",
                return_value=mock_group_svc,
            ),
        ):
            await get_all_groups_execution_history(
                session=session,
                user_email="alice@example.com",
                limit=50,
                offset=0,
                include_payload=False,
                service=service,
            )
        call_kwargs = service.get_execution_history.call_args
        assert call_kwargs.kwargs["group_ids"] == ["user_allocated"]

    @pytest.mark.asyncio
    async def test_include_payload_is_forwarded(self):
        """include_payload=True is passed through to the service."""
        session = MagicMock()
        mock_user = SimpleNamespace(id="u1", personal_group_id="user_allocated")
        mock_user_svc, mock_group_svc = _user_and_group_services(mock_user, [])
        service = AsyncMock()
        service.get_execution_history = AsyncMock(
            return_value=ExecutionHistoryList(
                executions=[], total=0, offset=10, limit=20
            )
        )

        with (
            patch(
                "src.api.execution_history_router.UserService",
                return_value=mock_user_svc,
            ),
            patch(
                "src.api.execution_history_router.GroupService",
                return_value=mock_group_svc,
            ),
        ):
            await get_all_groups_execution_history(
                session=session,
                user_email="alice@example.com",
                limit=20,
                offset=10,
                include_payload=True,
                service=service,
            )
        service.get_execution_history.assert_awaited_once_with(
            20, 10, group_ids=["user_allocated"], include_payload=True
        )


class TestGetExecutionHistory:
    @pytest.mark.asyncio
    async def test_calls_service_with_group_ids(self):
        """get_execution_history passes group_ids from context."""
        service = AsyncMock()
        expected = ExecutionHistoryList(executions=[], total=0, offset=0, limit=50)
        service.get_execution_history = AsyncMock(return_value=expected)
        ctx = make_gc()

        result = await get_execution_history(
            group_context=ctx, limit=50, offset=0, service=service
        )
        service.get_execution_history.assert_called_once_with(
            50, 0, group_ids=ctx.group_ids, include_payload=False
        )
        assert result == expected


class TestCheckExecutionExists:
    @pytest.mark.asyncio
    async def test_raises_not_found_when_missing(self):
        """Raises NotFoundError for non-existent execution."""
        service = AsyncMock()
        service.check_execution_exists = AsyncMock(return_value=False)
        ctx = make_gc()

        with pytest.raises(NotFoundError):
            await check_execution_exists(
                execution_id=999, group_context=ctx, service=service
            )

    @pytest.mark.asyncio
    async def test_returns_200_when_found(self):
        """Returns 200 response when execution exists."""
        service = AsyncMock()
        service.check_execution_exists = AsyncMock(return_value=True)
        ctx = make_gc()

        response = await check_execution_exists(
            execution_id=1, group_context=ctx, service=service
        )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_forwards_group_ids_to_service(self):
        """Existence check is scoped to the caller's groups (tenant isolation)."""
        service = AsyncMock()
        service.check_execution_exists = AsyncMock(return_value=True)
        ctx = make_gc()

        await check_execution_exists(execution_id=7, group_context=ctx, service=service)
        service.check_execution_exists.assert_called_once_with(
            7, group_ids=ctx.group_ids
        )


class TestGetExecutionById:
    @pytest.mark.asyncio
    async def test_raises_not_found_when_execution_missing(self):
        """Raises NotFoundError when execution not in DB."""
        service = AsyncMock()
        service.get_execution_by_id = AsyncMock(return_value=None)
        ctx = make_gc()

        with pytest.raises(NotFoundError):
            await get_execution_by_id(
                execution_id=42, group_context=ctx, service=service
            )

    @pytest.mark.asyncio
    async def test_returns_execution_when_found(self):
        """Returns execution item when found."""
        mock_exec = MagicMock()
        service = AsyncMock()
        service.get_execution_by_id = AsyncMock(return_value=mock_exec)
        ctx = make_gc()

        result = await get_execution_by_id(
            execution_id=1, group_context=ctx, service=service
        )
        assert result == mock_exec


class TestGetExecutionOutputs:
    @pytest.mark.asyncio
    async def test_returns_outputs_for_execution(self):
        """Returns outputs list for valid execution."""
        expected = ExecutionOutputList(
            execution_id="exec-1", outputs=[], total=0, limit=1000, offset=0
        )
        service = AsyncMock()
        service.get_execution_outputs = AsyncMock(return_value=expected)
        ctx = make_gc()

        result = await get_execution_outputs(
            execution_id="exec-1", group_context=ctx, service=service
        )
        assert result == expected

    @pytest.mark.asyncio
    async def test_passes_group_ids_to_service(self):
        """get_execution_outputs passes group_ids from context."""
        expected = ExecutionOutputList(
            execution_id="exec-1", outputs=[], total=0, limit=1000, offset=0
        )
        service = AsyncMock()
        service.get_execution_outputs = AsyncMock(return_value=expected)
        ctx = make_gc()

        await get_execution_outputs(
            execution_id="exec-1",
            group_context=ctx,
            service=service,
            limit=100,
            offset=5,
        )
        service.get_execution_outputs.assert_called_once_with(
            "exec-1", 100, 5, group_ids=ctx.group_ids
        )


class TestGetExecutionDebugOutputs:
    @pytest.mark.asyncio
    async def test_raises_not_found_when_no_debug_info(self):
        """Raises NotFoundError when debug info is absent."""
        service = AsyncMock()
        service.get_debug_outputs = AsyncMock(return_value=None)
        ctx = make_gc()

        with pytest.raises(NotFoundError):
            await get_execution_debug_outputs(
                execution_id="missing", group_context=ctx, service=service
            )

    @pytest.mark.asyncio
    async def test_returns_debug_info_when_found(self):
        """Returns debug info when present."""
        expected = ExecutionOutputDebugList(
            run_id=1, execution_id="e1", total_outputs=0, outputs=[]
        )
        service = AsyncMock()
        service.get_debug_outputs = AsyncMock(return_value=expected)
        ctx = make_gc()

        result = await get_execution_debug_outputs(
            execution_id="e1", group_context=ctx, service=service
        )
        assert result == expected


class TestUpdateExecutionResult:
    @pytest.mark.asyncio
    async def test_raises_not_found_when_update_fails(self):
        """Raises NotFoundError when update returns not found."""
        service = AsyncMock()
        service.update_result = AsyncMock(return_value={"success": False})
        ctx = make_gc()
        request = UpdateExecutionResultRequest(result={"key": "value"})

        with pytest.raises(NotFoundError):
            await update_execution_result(
                job_id="missing-job",
                request=request,
                group_context=ctx,
                service=service,
            )

    @pytest.mark.asyncio
    async def test_returns_update_response_on_success(self):
        """Returns update response when update succeeds."""
        expected = {"success": True, "job_id": "job-1", "updated_at": None}
        service = AsyncMock()
        service.update_result = AsyncMock(return_value=expected)
        ctx = make_gc()
        request = UpdateExecutionResultRequest(result={"data": "new"})

        result = await update_execution_result(
            job_id="job-1", request=request, group_context=ctx, service=service
        )
        assert result["success"] is True


class TestDeleteAllExecutions:
    @pytest.mark.asyncio
    async def test_calls_service_delete_all(self):
        """Delegates to service.delete_all_executions."""
        expected = DeleteResponse(success=True, message="ok")
        service = AsyncMock()
        service.delete_all_executions = AsyncMock(return_value=expected)
        ctx = make_gc()

        result = await delete_all_executions(group_context=ctx, service=service)
        assert result == expected
        service.delete_all_executions.assert_called_once_with(group_ids=ctx.group_ids)


class TestDeleteExecution:
    @pytest.mark.asyncio
    async def test_raises_not_found_when_deletion_fails(self):
        """Raises NotFoundError when execution does not exist."""
        service = AsyncMock()
        service.delete_execution = AsyncMock(return_value=None)
        ctx = make_gc()

        with pytest.raises(NotFoundError):
            await delete_execution(execution_id=99, group_context=ctx, service=service)

    @pytest.mark.asyncio
    async def test_returns_delete_response_on_success(self):
        """Returns delete response when execution is deleted."""
        expected = DeleteResponse(success=True, message="deleted")
        service = AsyncMock()
        service.delete_execution = AsyncMock(return_value=expected)
        ctx = make_gc()

        result = await delete_execution(
            execution_id=1, group_context=ctx, service=service
        )
        assert result == expected

    @pytest.mark.asyncio
    async def test_forwards_group_ids_to_service(self):
        """Delete-by-id is scoped to the caller's groups (no cross-tenant delete)."""
        expected = DeleteResponse(success=True, message="deleted")
        service = AsyncMock()
        service.delete_execution = AsyncMock(return_value=expected)
        ctx = make_gc()

        await delete_execution(execution_id=5, group_context=ctx, service=service)
        service.delete_execution.assert_called_once_with(5, group_ids=ctx.group_ids)


class TestDeleteExecutionByJobId:
    @pytest.mark.asyncio
    async def test_raises_not_found_when_job_missing(self):
        """Raises NotFoundError when job_id not found."""
        service = AsyncMock()
        service.delete_execution_by_job_id = AsyncMock(return_value=None)
        ctx = make_gc()

        with pytest.raises(NotFoundError):
            await delete_execution_by_job_id(
                job_id="missing-uuid", group_context=ctx, service=service
            )

    @pytest.mark.asyncio
    async def test_returns_delete_response_on_success(self):
        """Returns delete response on success."""
        expected = DeleteResponse(success=True, message="deleted")
        service = AsyncMock()
        service.delete_execution_by_job_id = AsyncMock(return_value=expected)
        ctx = make_gc()

        result = await delete_execution_by_job_id(
            job_id="job-uuid-123", group_context=ctx, service=service
        )
        assert result == expected

    @pytest.mark.asyncio
    async def test_forwards_group_ids_to_service(self):
        """Delete-by-job_id is scoped to the caller's groups (no cross-tenant delete)."""
        expected = DeleteResponse(success=True, message="deleted")
        service = AsyncMock()
        service.delete_execution_by_job_id = AsyncMock(return_value=expected)
        ctx = make_gc()

        await delete_execution_by_job_id(
            job_id="job-9", group_context=ctx, service=service
        )
        service.delete_execution_by_job_id.assert_called_once_with(
            "job-9", group_ids=ctx.group_ids
        )
