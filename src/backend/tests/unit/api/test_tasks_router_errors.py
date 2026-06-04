"""
Tests for error handling paths in tasks_router.
Covers lines 36, 70-72, 92-94, 129, 134-136, 180-182, 226-228, 266-268, 293-295
"""
import pytest
from unittest.mock import AsyncMock
from fastapi import HTTPException

from src.api.tasks_router import (
    create_task,
    list_tasks,
    get_task,
    update_task_full,
    update_task,
    delete_task,
    delete_all_tasks,
)
from src.schemas.task import TaskCreate, TaskUpdate
from src.utils.user_context import GroupContext


def gc(role="admin"):
    return GroupContext(
        group_ids=["g1"], group_email="u@x.com", email_domain="x.com", user_role=role
    )


class TestCreateTaskErrorPath:
    @pytest.mark.asyncio
    async def test_create_task_service_exception_raises_500(self):
        """create_task wraps service exception in 500 HTTPException."""
        svc = AsyncMock()
        svc.create_with_group = AsyncMock(side_effect=Exception("DB connection failed"))
        from src.schemas.task import TaskCreate
        task_in = TaskCreate(name="T", description="d", expected_output="o", agent_id=None)
        with pytest.raises(HTTPException) as exc_info:
            await create_task(task_in, service=svc, group_context=gc("admin"))
        assert exc_info.value.status_code == 500


class TestListTasksErrorPath:
    @pytest.mark.asyncio
    async def test_list_tasks_service_exception_raises_500(self):
        """list_tasks wraps service exception in 500 HTTPException."""
        svc = AsyncMock()
        svc.find_by_group = AsyncMock(side_effect=Exception("query failed"))
        with pytest.raises(HTTPException) as exc_info:
            await list_tasks(service=svc, group_context=gc())
        assert exc_info.value.status_code == 500


class TestGetTaskErrorPath:
    @pytest.mark.asyncio
    async def test_get_task_service_exception_raises_500(self):
        """get_task wraps service exception in 500 HTTPException."""
        svc = AsyncMock()
        svc.get_with_group_check = AsyncMock(side_effect=Exception("Unexpected error"))
        with pytest.raises(HTTPException) as exc_info:
            await get_task("task-1", service=svc, group_context=gc())
        assert exc_info.value.status_code == 500


class TestUpdateTaskFullErrorPath:
    @pytest.mark.asyncio
    async def test_update_full_service_exception_raises_500(self):
        """update_task_full wraps service exception in 500 HTTPException."""
        svc = AsyncMock()
        svc.update_full_with_group_check = AsyncMock(side_effect=Exception("DB error"))
        with pytest.raises(HTTPException) as exc_info:
            await update_task_full("task-1", {"name": "X"}, service=svc, group_context=gc("admin"))
        assert exc_info.value.status_code == 500


class TestUpdateTaskErrorPath:
    @pytest.mark.asyncio
    async def test_update_task_service_exception_raises_500(self):
        """update_task wraps service exception in 500 HTTPException."""
        svc = AsyncMock()
        svc.update_with_group_check = AsyncMock(side_effect=Exception("Update error"))
        with pytest.raises(HTTPException) as exc_info:
            await update_task("task-1", TaskUpdate(name="Y"), service=svc, group_context=gc("admin"))
        assert exc_info.value.status_code == 500


class TestDeleteTaskErrorPath:
    @pytest.mark.asyncio
    async def test_delete_task_service_exception_raises_500(self):
        """delete_task wraps service exception in 500 HTTPException."""
        svc = AsyncMock()
        svc.delete_with_group_check = AsyncMock(side_effect=Exception("Delete failed"))
        with pytest.raises(HTTPException) as exc_info:
            await delete_task("task-1", service=svc, group_context=gc("admin"))
        assert exc_info.value.status_code == 500


class TestDeleteAllTasksErrorPath:
    @pytest.mark.asyncio
    async def test_delete_all_tasks_service_exception_raises_500(self):
        """delete_all_tasks wraps service exception in 500 HTTPException."""
        svc = AsyncMock()
        svc.delete_all_for_group = AsyncMock(side_effect=Exception("Mass delete error"))
        with pytest.raises(HTTPException) as exc_info:
            await delete_all_tasks(service=svc, group_context=gc("admin"))
        assert exc_info.value.status_code == 500
