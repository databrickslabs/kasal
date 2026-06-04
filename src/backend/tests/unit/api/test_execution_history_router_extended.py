"""
Unit tests for execution_history_router module.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
from datetime import datetime

from src.api.execution_history_router import (
    debug_execution_groups,
    get_all_groups_execution_history,
    get_execution_history,
    check_execution_exists,
    get_execution_by_id,
    get_execution_outputs,
    get_execution_debug_outputs,
    update_execution_result,
    delete_all_executions,
    delete_execution,
    delete_execution_by_job_id,
)
from src.schemas.execution_history import (
    ExecutionHistoryList,
    ExecutionOutputList,
    ExecutionOutputDebugList,
    DeleteResponse,
    UpdateExecutionResultRequest,
)
from src.utils.user_context import GroupContext
from src.core.exceptions import NotFoundError


def make_gc(role="admin"):
    return GroupContext(
        group_ids=["g1", "g2"],
        group_email="user@test.com",
        email_domain="test.com",
        user_role=role,
    )


class TestDebugExecutionGroups:
    @pytest.mark.asyncio
    async def test_returns_404_when_debug_mode_off(self):
        """debug_execution_groups raises 404 when DEBUG_MODE is False."""
        session = MagicMock()
        with patch("src.api.execution_history_router.settings") as mock_settings:
            mock_settings.DEBUG_MODE = False
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                await debug_execution_groups(session=session)
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_groups_info_when_debug_mode_on_no_email(self):
        """debug_execution_groups returns groups data when DEBUG_MODE is True, no email."""
        session = MagicMock()
        mock_svc = AsyncMock()
        mock_svc.get_execution_groups_with_counts = AsyncMock(return_value=[("g1", 5)])

        with patch("src.api.execution_history_router.settings") as mock_settings, \
             patch("src.api.execution_history_router.ExecutionHistoryService", return_value=mock_svc):
            mock_settings.DEBUG_MODE = True
            result = await debug_execution_groups(
                session=session,
                x_forwarded_email=None,
                x_auth_request_email=None,
            )
            assert result["total_unique_groups"] == 1
            assert result["user_email"] is None
            assert len(result["all_execution_groups"]) == 1

    @pytest.mark.asyncio
    async def test_returns_user_groups_when_email_header_provided(self):
        """debug_execution_groups fetches user groups when email is in headers."""
        session = MagicMock()
        mock_svc = AsyncMock()
        mock_svc.get_execution_groups_with_counts = AsyncMock(return_value=[("g1", 3)])

        mock_user = SimpleNamespace(id="u1")
        mock_user_svc = AsyncMock()
        mock_user_svc.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group = SimpleNamespace(id="g1", name="Group 1")
        mock_group_svc = AsyncMock()
        mock_group_svc.get_user_groups = AsyncMock(return_value=[mock_group])

        with patch("src.api.execution_history_router.settings") as mock_settings, \
             patch("src.api.execution_history_router.ExecutionHistoryService", return_value=mock_svc), \
             patch("src.api.execution_history_router.UserService", return_value=mock_user_svc), \
             patch("src.api.execution_history_router.GroupService", return_value=mock_group_svc):
            mock_settings.DEBUG_MODE = True
            result = await debug_execution_groups(
                session=session,
                x_auth_request_email="user@test.com",
                x_forwarded_email=None,
            )
            assert result["user_email"] == "user@test.com"
            assert len(result["user_groups"]) == 1


class TestGetAllGroupsExecutionHistory:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_email(self):
        """Returns empty list when no email header present."""
        session = MagicMock()
        service = AsyncMock()
        result = await get_all_groups_execution_history(
            session=session,
            service=service,
            limit=50,
            offset=0,
            x_forwarded_email=None,
            x_auth_request_email=None,
        )
        assert result.total == 0
        assert result.executions == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_user_not_found(self):
        """Returns empty list when user cannot be found."""
        session = MagicMock()
        service = AsyncMock()
        mock_user_svc = AsyncMock()
        mock_user_svc.get_or_create_user_by_email = AsyncMock(return_value=None)

        with patch("src.api.execution_history_router.UserService", return_value=mock_user_svc):
            result = await get_all_groups_execution_history(
                session=session,
                service=service,
                limit=50,
                offset=0,
                x_auth_request_email="user@test.com",
                x_forwarded_email=None,
            )
        assert result.total == 0

    @pytest.mark.asyncio
    async def test_fetches_executions_for_all_groups(self):
        """Returns executions from all user groups."""
        session = MagicMock()

        mock_user = SimpleNamespace(id="u1")
        mock_user_svc = AsyncMock()
        mock_user_svc.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group = SimpleNamespace(id="g1", name="Group 1")
        mock_group_svc = AsyncMock()
        mock_group_svc.get_user_groups = AsyncMock(return_value=[mock_group])

        expected_list = ExecutionHistoryList(executions=[], total=3, offset=0, limit=50)
        service = AsyncMock()
        service.get_execution_history = AsyncMock(return_value=expected_list)

        with patch("src.api.execution_history_router.UserService", return_value=mock_user_svc), \
             patch("src.api.execution_history_router.GroupService", return_value=mock_group_svc):
            result = await get_all_groups_execution_history(
                session=session,
                service=service,
                limit=50,
                offset=0,
                x_auth_request_email="user@test.com",
                x_forwarded_email=None,
            )
        assert result.total == 3

    @pytest.mark.asyncio
    async def test_adds_personal_workspace_to_groups(self):
        """Adds personal workspace ID to group list for data access."""
        session = MagicMock()
        mock_user = SimpleNamespace(id="u1")
        mock_user_svc = AsyncMock()
        mock_user_svc.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_svc = AsyncMock()
        mock_group_svc.get_user_groups = AsyncMock(return_value=[])

        expected_list = ExecutionHistoryList(executions=[], total=0, offset=0, limit=50)
        service = AsyncMock()
        service.get_execution_history = AsyncMock(return_value=expected_list)

        with patch("src.api.execution_history_router.UserService", return_value=mock_user_svc), \
             patch("src.api.execution_history_router.GroupService", return_value=mock_group_svc):
            await get_all_groups_execution_history(
                session=session,
                service=service,
                limit=50,
                offset=0,
                x_auth_request_email="alice@example.com",
                x_forwarded_email=None,
            )
        # Verify get_execution_history was called with group_ids containing personal workspace
        call_kwargs = service.get_execution_history.call_args
        group_ids = call_kwargs[1].get("group_ids") or []
        assert any("user_" in gid for gid in group_ids)

    @pytest.mark.asyncio
    async def test_uses_forwarded_email_fallback(self):
        """Falls back to x_forwarded_email when auth email not provided."""
        session = MagicMock()
        mock_user_svc = AsyncMock()
        mock_user_svc.get_or_create_user_by_email = AsyncMock(return_value=None)

        service = AsyncMock()

        with patch("src.api.execution_history_router.UserService", return_value=mock_user_svc):
            result = await get_all_groups_execution_history(
                session=session,
                service=service,
                limit=50,
                offset=0,
                x_auth_request_email=None,
                x_forwarded_email="fwd@example.com",
            )
        assert result.total == 0
        mock_user_svc.get_or_create_user_by_email.assert_called_once_with("fwd@example.com")


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
            50, 0, group_ids=ctx.group_ids
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


class TestGetExecutionById:
    @pytest.mark.asyncio
    async def test_raises_not_found_when_execution_missing(self):
        """Raises NotFoundError when execution not in DB."""
        service = AsyncMock()
        service.get_execution_by_id = AsyncMock(return_value=None)
        ctx = make_gc()

        with pytest.raises(NotFoundError):
            await get_execution_by_id(execution_id=42, group_context=ctx, service=service)

    @pytest.mark.asyncio
    async def test_returns_execution_when_found(self):
        """Returns execution item when found."""
        mock_exec = MagicMock()
        service = AsyncMock()
        service.get_execution_by_id = AsyncMock(return_value=mock_exec)
        ctx = make_gc()

        result = await get_execution_by_id(execution_id=1, group_context=ctx, service=service)
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
            execution_id="exec-1", group_context=ctx, service=service,
            limit=100, offset=5,
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
                job_id="missing-job", request=request, group_context=ctx, service=service
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

        result = await delete_execution(execution_id=1, group_context=ctx, service=service)
        assert result == expected


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
