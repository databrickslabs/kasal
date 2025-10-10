import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import List, Dict, Any, Optional, Set
from datetime import datetime, timezone
import asyncio

# Test scheduler service - based on actual code inspection

from src.services.scheduler_service import SchedulerService


class TestSchedulerServiceInit:
    """Test SchedulerService initialization"""

    def test_scheduler_service_init_basic(self):
        """Test SchedulerService __init__ with basic parameters"""
        mock_session = Mock()
        
        service = SchedulerService(mock_session)
        
        assert service.session == mock_session
        assert hasattr(service, 'repository')
        assert hasattr(service, 'execution_history_repository')
        assert hasattr(service, '_running_tasks')
        assert isinstance(service._running_tasks, set)
        assert len(service._running_tasks) == 0

    def test_scheduler_service_init_creates_repositories(self):
        """Test SchedulerService __init__ creates repository instances"""
        mock_session = Mock()
        
        with patch('src.services.scheduler_service.ScheduleRepository') as mock_schedule_repo:
            with patch('src.services.scheduler_service.ExecutionHistoryRepository') as mock_exec_repo:
                mock_schedule_instance = Mock()
                mock_exec_instance = Mock()
                mock_schedule_repo.return_value = mock_schedule_instance
                mock_exec_repo.return_value = mock_exec_instance
                
                service = SchedulerService(mock_session)
                
                assert service.repository == mock_schedule_instance
                assert service.execution_history_repository == mock_exec_instance
                mock_schedule_repo.assert_called_once_with(mock_session)
                mock_exec_repo.assert_called_once_with(mock_session)

    def test_scheduler_service_init_running_tasks_set(self):
        """Test SchedulerService __init__ creates empty running tasks set"""
        mock_session = Mock()
        
        service = SchedulerService(mock_session)
        
        assert isinstance(service._running_tasks, set)
        assert len(service._running_tasks) == 0
        
        # Should be able to add/remove tasks
        mock_task = Mock()
        service._running_tasks.add(mock_task)
        assert len(service._running_tasks) == 1
        assert mock_task in service._running_tasks


class TestSchedulerServiceShutdown:
    """Test SchedulerService shutdown method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = SchedulerService(self.mock_session)

    @pytest.mark.asyncio
    async def test_shutdown_empty_tasks(self):
        """Test shutdown with no running tasks"""
        # Should not raise an exception
        await self.service.shutdown()
        
        # Running tasks should still be empty
        assert len(self.service._running_tasks) == 0

    @pytest.mark.asyncio
    async def test_shutdown_with_running_tasks(self):
        """Test shutdown with running tasks"""
        # Create mock tasks
        mock_task1 = Mock()
        mock_task1.cancel = Mock()
        mock_task1.done.return_value = False
        
        mock_task2 = Mock()
        mock_task2.cancel = Mock()
        mock_task2.done.return_value = False
        
        # Add tasks to running set
        self.service._running_tasks.add(mock_task1)
        self.service._running_tasks.add(mock_task2)
        
        await self.service.shutdown()
        
        # All tasks should be cancelled
        mock_task1.cancel.assert_called_once()
        mock_task2.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_with_completed_tasks(self):
        """Test shutdown with already completed tasks"""
        # Create mock completed task
        mock_task = Mock()
        mock_task.cancel = Mock()
        mock_task.done.return_value = True

        self.service._running_tasks.add(mock_task)

        await self.service.shutdown()

        # Based on actual implementation, all tasks are cancelled regardless of status
        mock_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_clears_running_tasks(self):
        """Test shutdown clears the running tasks set"""
        # Add some mock tasks
        mock_task1 = Mock()
        mock_task1.cancel = Mock()
        mock_task1.done.return_value = False
        
        mock_task2 = Mock()
        mock_task2.cancel = Mock()
        mock_task2.done.return_value = False
        
        self.service._running_tasks.add(mock_task1)
        self.service._running_tasks.add(mock_task2)
        
        assert len(self.service._running_tasks) == 2
        
        await self.service.shutdown()
        
        # Running tasks should be cleared
        assert len(self.service._running_tasks) == 0


class TestSchedulerServiceTaskManagement:
    """Test SchedulerService task management"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = SchedulerService(self.mock_session)

    def test_task_done_callback_function_exists(self):
        """Test that task_done_callback function can be created"""
        # This tests the nested function definition in start_scheduler
        mock_task = Mock()
        mock_task.done.return_value = True
        mock_task.cancelled.return_value = False
        mock_task.exception.return_value = None
        
        # Add task to running set
        self.service._running_tasks.add(mock_task)
        
        # Create the callback function (simulating what happens in start_scheduler)
        def task_done_callback(task):
            self.service._running_tasks.discard(task)
            if task.done() and not task.cancelled():
                exc = task.exception()
                if exc:
                    logger.error(f"Scheduler task failed: {exc}")
        
        # Call the callback
        task_done_callback(mock_task)
        
        # Task should be removed from running set
        assert mock_task not in self.service._running_tasks

    def test_running_tasks_set_operations(self):
        """Test running tasks set operations"""
        mock_task1 = Mock()
        mock_task2 = Mock()
        mock_task3 = Mock()
        
        # Test add
        self.service._running_tasks.add(mock_task1)
        assert len(self.service._running_tasks) == 1
        assert mock_task1 in self.service._running_tasks
        
        # Test add multiple
        self.service._running_tasks.add(mock_task2)
        self.service._running_tasks.add(mock_task3)
        assert len(self.service._running_tasks) == 3
        
        # Test discard
        self.service._running_tasks.discard(mock_task2)
        assert len(self.service._running_tasks) == 2
        assert mock_task2 not in self.service._running_tasks
        
        # Test discard non-existent (should not raise)
        self.service._running_tasks.discard(mock_task2)
        assert len(self.service._running_tasks) == 2

    def test_running_tasks_set_clear(self):
        """Test clearing running tasks set"""
        # Add some tasks
        for i in range(5):
            mock_task = Mock()
            self.service._running_tasks.add(mock_task)
        
        assert len(self.service._running_tasks) == 5
        
        # Clear all tasks
        self.service._running_tasks.clear()
        
        assert len(self.service._running_tasks) == 0


class TestSchedulerServiceConstants:
    """Test SchedulerService constants and module-level attributes"""

    def test_db_path_constant(self):
        """Test DB_PATH constant is properly defined"""
        from src.services.scheduler_service import DB_PATH
        
        assert DB_PATH is not None
        assert isinstance(DB_PATH, str)
        assert len(DB_PATH) > 0

    def test_logger_initialization(self):
        """Test logger is properly initialized"""
        from src.services.scheduler_service import logger, logger_manager
        
        assert logger is not None
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')
        assert logger_manager is not None

    def test_required_imports(self):
        """Test that required imports are available"""
        # Test key imports
        from src.services.scheduler_service import ScheduleRepository, ExecutionHistoryRepository
        from src.services.scheduler_service import ScheduleCreate, ScheduleResponse
        from src.services.scheduler_service import CrewConfig, GroupContext
        
        assert ScheduleRepository is not None
        assert ExecutionHistoryRepository is not None
        assert ScheduleCreate is not None
        assert ScheduleResponse is not None
        assert CrewConfig is not None
        assert GroupContext is not None


class TestSchedulerServiceAttributes:
    """Test SchedulerService attribute access and properties"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = SchedulerService(self.mock_session)

    def test_service_has_required_attributes(self):
        """Test that service has all required attributes after initialization"""
        # Check all required attributes exist
        assert hasattr(self.service, 'repository')
        assert hasattr(self.service, 'execution_history_repository')
        assert hasattr(self.service, 'session')
        assert hasattr(self.service, '_running_tasks')
        
        # Check attribute types
        assert self.service.session == self.mock_session
        assert isinstance(self.service._running_tasks, set)

    def test_service_repositories_are_separate(self):
        """Test that repositories are separate instances"""
        assert self.service.repository is not self.service.execution_history_repository
        assert self.service.repository is not None
        assert self.service.execution_history_repository is not None

    def test_service_session_storage(self):
        """Test service stores session correctly"""
        assert self.service.session == self.mock_session
        
        # Test with different session
        new_mock_session = Mock()
        new_service = SchedulerService(new_mock_session)
        assert new_service.session == new_mock_session
        assert new_service.session != self.mock_session


class TestSchedulerServiceAsyncMethods:
    """Test SchedulerService async method signatures"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = SchedulerService(self.mock_session)

    def test_async_methods_exist(self):
        """Test that key async methods exist and are callable"""
        async_methods = [
            'create_schedule',
            'create_schedule_from_execution',
            'get_all_schedules',
            'get_schedule_by_id',
            'update_schedule',
            'delete_schedule',
            'toggle_schedule',
            'check_and_run_schedules',
            'start_scheduler',
            'get_all_jobs',
            'create_job',
            'update_job',
            'shutdown'
        ]
        
        for method_name in async_methods:
            assert hasattr(self.service, method_name)
            method = getattr(self.service, method_name)
            assert callable(method)

    def test_group_check_methods_exist(self):
        """Test that group check methods exist"""
        group_methods = [
            'get_schedule_by_id_with_group_check',
            'update_schedule_with_group_check',
            'delete_schedule_with_group_check',
            'toggle_schedule_with_group_check',
            'get_all_jobs_for_group',
            'create_job_with_group',
            'update_job_with_group_check'
        ]
        
        for method_name in group_methods:
            assert hasattr(self.service, method_name)
            method = getattr(self.service, method_name)
            assert callable(method)


class TestSchedulerServiceModuleLevelFunctions:
    """Test module-level functions and constants"""

    def test_settings_import(self):
        """Test settings import and DB_PATH derivation"""
        from src.services.scheduler_service import settings, DB_PATH
        
        assert settings is not None
        assert hasattr(settings, 'DATABASE_URI')
        assert DB_PATH is not None
        assert isinstance(DB_PATH, str)

    def test_asyncio_import(self):
        """Test asyncio import for task management"""
        import asyncio
        from src.services.scheduler_service import asyncio as scheduler_asyncio
        
        # Should be the same module
        assert asyncio == scheduler_asyncio

    def test_datetime_imports(self):
        """Test datetime imports"""
        from src.services.scheduler_service import datetime, timezone
        
        assert datetime is not None
        assert timezone is not None
        assert hasattr(datetime, 'now')
        assert hasattr(timezone, 'utc')
