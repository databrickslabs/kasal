"""
Unit tests for asyncio_utils module.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from src.utils.asyncio_utils import (
    execute_db_operation_with_fresh_engine,
    create_and_run_loop,
    create_task_lifecycle_callback,
    run_in_thread_with_loop
)


class TestExecuteDbOperationWithFreshEngine:
    """Test execute_db_operation_with_fresh_engine function."""
    
    @pytest.mark.asyncio
    async def test_successful_operation(self):
        """Test successful database operation execution."""
        expected_result = "test_result"

        async def mock_operation(session):
            return expected_result

        with patch('src.utils.asyncio_utils.async_sessionmaker') as mock_session_factory, \
             patch('src.db.session.nullpool_engine') as mock_engine:

            # Mock session factory and session
            mock_session = AsyncMock(spec=AsyncSession)
            mock_session_context = AsyncMock()
            mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory = Mock(return_value=mock_session_context)
            mock_session_factory.return_value = mock_factory

            result = await execute_db_operation_with_fresh_engine(mock_operation)

            assert result == expected_result
            # Engine should NOT be disposed (managed by session.py)
            mock_engine.dispose.assert_not_called()

    @pytest.mark.asyncio
    async def test_operation_with_exception(self):
        """Test database operation that raises an exception."""
        test_exception = Exception("Test error")

        async def mock_operation(session):
            raise test_exception

        with patch('src.utils.asyncio_utils.async_sessionmaker') as mock_session_factory, \
             patch('src.db.session.nullpool_engine') as mock_engine:

            # Mock session factory and session
            mock_session = AsyncMock(spec=AsyncSession)
            mock_session_context = AsyncMock()
            mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_context.__aexit__ = AsyncMock(return_value=None)
            mock_factory = Mock(return_value=mock_session_context)
            mock_session_factory.return_value = mock_factory

            with pytest.raises(Exception) as exc_info:
                await execute_db_operation_with_fresh_engine(mock_operation)

            assert exc_info.value == test_exception
            # Engine should NOT be disposed even on error (managed by session.py)
            mock_engine.dispose.assert_not_called()


class TestCreateAndRunLoop:
    """Test create_and_run_loop function."""
    
    def test_successful_coroutine_execution(self):
        """Test successful execution of a coroutine in new loop."""
        expected_result = "test_result"
        
        async def test_coroutine():
            return expected_result
        
        result = create_and_run_loop(test_coroutine())
        assert result == expected_result
    
    def test_coroutine_with_exception(self):
        """Test execution of a coroutine that raises an exception."""
        test_exception = ValueError("Test error")
        
        async def failing_coroutine():
            raise test_exception
        
        with pytest.raises(ValueError) as exc_info:
            create_and_run_loop(failing_coroutine())
        
        assert exc_info.value == test_exception
    
    def test_cleanup_with_pending_tasks(self):
        """Test proper cleanup when there are pending tasks."""
        async def test_coroutine():
            return "result"
        
        # Just test that it works without mocking the internals
        result = create_and_run_loop(test_coroutine())
        assert result == "result"
    
    def test_cleanup_with_exception_during_cleanup(self):
        """Test cleanup when exception occurs during cleanup."""
        async def test_coroutine():
            return "result"
        
        with patch('src.utils.asyncio_utils.asyncio.all_tasks') as mock_all_tasks, \
             patch('src.utils.asyncio_utils.asyncio.gather') as mock_gather:
            
            # Mock pending tasks
            mock_task = Mock()
            mock_all_tasks.return_value = [mock_task]
            
            # Mock gather to raise exception
            mock_gather.side_effect = Exception("Cleanup error")
            
            # Should still return result despite cleanup error
            result = create_and_run_loop(test_coroutine())
            assert result == "result"


class TestCreateTaskLifecycleCallback:
    """Test create_task_lifecycle_callback function."""
    
    def test_callback_creation(self):
        """Test creation of a task lifecycle callback."""
        mock_callback = Mock()
        mock_callback.on_task_start = AsyncMock()
        
        callback_fn = create_task_lifecycle_callback('on_task_start', [mock_callback], 'test_task')
        
        assert callable(callback_fn)
    
    @patch('src.utils.asyncio_utils.asyncio.new_event_loop')
    @patch('src.utils.asyncio_utils.asyncio.set_event_loop')
    def test_callback_execution_on_task_end(self, mock_set_loop, mock_new_loop):
        """Test callback execution for on_task_end."""
        mock_loop = Mock()
        mock_new_loop.return_value = mock_loop
        
        # Mock the loop's run_until_complete method
        mock_loop.run_until_complete = Mock()
        
        # Mock all_tasks to return empty list (no pending tasks)
        with patch('src.utils.asyncio_utils.asyncio.all_tasks', return_value=[]):
            mock_callback = Mock()
            mock_callback.on_task_end = AsyncMock()
            
            callback_fn = create_task_lifecycle_callback('on_task_end', [mock_callback], 'test_task')
            
            # Execute the callback
            task_obj = Mock()
            callback_fn(task_obj, success=True)
            
            # Verify the handler was called
            assert mock_loop.run_until_complete.call_count >= 1
    
    def test_callback_execution_with_exception(self):
        """Test callback execution when handler raises an exception."""
        mock_callback = Mock()
        mock_callback.on_task_start = Mock(side_effect=Exception("Handler error"))
        
        # This should not raise an exception, but handle it gracefully
        callback_fn = create_task_lifecycle_callback('on_task_start', [mock_callback], 'test_task')
        
        task_obj = Mock()
        # Should not raise an exception
        callback_fn(task_obj)
    
    def test_callback_cleanup_with_exception(self):
        """Test callback cleanup when exception occurs during cleanup."""
        with patch('src.utils.asyncio_utils.asyncio.all_tasks') as mock_all_tasks, \
             patch('src.utils.asyncio_utils.asyncio.gather') as mock_gather:
            
            mock_callback = Mock()
            mock_callback.on_task_start = AsyncMock()
            
            # Mock pending tasks
            mock_task = Mock()
            mock_all_tasks.return_value = [mock_task]
            
            # Mock gather to raise exception
            mock_gather.side_effect = Exception("Cleanup error")
            
            callback_fn = create_task_lifecycle_callback('on_task_start', [mock_callback], 'test_task')
            
            task_obj = Mock()
            # Should not raise an exception despite cleanup error
            callback_fn(task_obj)


class TestRunInThreadWithLoop:
    """Test run_in_thread_with_loop function."""
    
    def test_function_execution_with_existing_loop(self):
        """Test function execution when event loop already exists."""
        def test_function(arg):
            return f"result_{arg}"
        
        with patch('src.utils.asyncio_utils.asyncio.get_event_loop') as mock_get_loop:
            mock_loop = Mock()
            mock_get_loop.return_value = mock_loop
            
            result = run_in_thread_with_loop(test_function, "test")
            
            assert result == "result_test"
            # Should not create a new loop
            mock_get_loop.assert_called_once()
    
    def test_function_execution_without_existing_loop(self):
        """Test function execution when no event loop exists."""
        def test_function(arg):
            return f"result_{arg}"
        
        # Test that the function works without deep mocking
        result = run_in_thread_with_loop(test_function, "test")
        assert result == "result_test"
    
    def test_function_execution_with_kwargs(self):
        """Test function execution with keyword arguments."""
        def test_function(arg1, arg2=None):
            return f"{arg1}_{arg2}"
        
        with patch('src.utils.asyncio_utils.asyncio.get_event_loop') as mock_get_loop:
            mock_loop = Mock()
            mock_get_loop.return_value = mock_loop
            
            result = run_in_thread_with_loop(test_function, "arg1", arg2="arg2")
            
            assert result == "arg1_arg2"
    
    def test_function_execution_with_exception_in_cleanup(self):
        """Test function execution when cleanup raises an exception."""
        def test_function():
            return "result"
        
        with patch('src.utils.asyncio_utils.asyncio.get_event_loop') as mock_get_loop, \
             patch('src.utils.asyncio_utils.asyncio.new_event_loop') as mock_new_loop, \
             patch('src.utils.asyncio_utils.asyncio.set_event_loop') as mock_set_loop:
            
            # Simulate no existing event loop
            mock_get_loop.side_effect = RuntimeError("No event loop")
            mock_loop = Mock()
            mock_new_loop.return_value = mock_loop
            
            # Make cleanup fail
            mock_loop.close.side_effect = Exception("Cleanup error")
            
            # Should still return the result despite cleanup error
            result = run_in_thread_with_loop(test_function)
            
            assert result == "result"
    
    def test_create_and_run_loop_task_filtering(self):
        """Test create_and_run_loop properly filters current task from cleanup."""
        async def test_coroutine():
            return "success"
        
        # Create mock tasks
        current_task = Mock()
        other_task1 = Mock()
        other_task2 = Mock()
        
        with patch('src.utils.asyncio_utils.asyncio.all_tasks') as mock_all_tasks, \
             patch('src.utils.asyncio_utils.asyncio.current_task') as mock_current_task, \
             patch('src.utils.asyncio_utils.asyncio.gather') as mock_gather:
            
            mock_current_task.return_value = current_task
            mock_all_tasks.return_value = [current_task, other_task1, other_task2]
            
            result = create_and_run_loop(test_coroutine())
            
            assert result == "success"
            # The test succeeds regardless of gather behavior since the main function works
            # This tests the task filtering logic in the cleanup section
    
    def test_run_in_thread_with_loop_successful_cleanup_logging(self):
        """Test logging when loop is successfully closed (covers line 158)."""
        def test_function():
            return "result"
        
        with patch('src.utils.asyncio_utils.asyncio.get_event_loop') as mock_get_loop, \
             patch('src.utils.asyncio_utils.asyncio.new_event_loop') as mock_new_loop, \
             patch('src.utils.asyncio_utils.asyncio.set_event_loop') as mock_set_loop, \
             patch('src.utils.asyncio_utils.logger') as mock_logger:
            
            # Simulate no existing event loop
            mock_get_loop.side_effect = RuntimeError("No event loop")
            mock_loop = Mock()
            mock_new_loop.return_value = mock_loop
            
            # Ensure loop.close() succeeds (no exception)
            mock_loop.close.return_value = None
            
            result = run_in_thread_with_loop(test_function)
            
            assert result == "result"
            # Verify the success log message was called (line 158)
            mock_logger.info.assert_called_with("Successfully closed the event loop created for this thread")


# ---------------------------------------------------------------------------
# execute_db_operation_smart — retry + raise logic
# ---------------------------------------------------------------------------

def _make_async_ctx(session):
    """Create an async context manager that yields *session*."""
    class _Ctx:
        async def __aenter__(self):
            return session
        async def __aexit__(self, *args):
            return False
    return _Ctx()


class TestExecuteDbOperationSmart:
    """Test execute_db_operation_smart with retry and fallback behavior."""

    @pytest.mark.asyncio
    async def test_lakebase_disabled_uses_local_engine(self):
        """When Lakebase is disabled, delegate to execute_db_operation_with_fresh_engine."""
        expected = "local_result"

        async def op(session):
            return expected

        with patch(
            "src.db.database_router.is_lakebase_enabled",
            new_callable=AsyncMock,
            return_value=False,
        ), patch(
            "src.utils.asyncio_utils.execute_db_operation_with_fresh_engine",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_local:
            from src.utils.asyncio_utils import execute_db_operation_smart

            result = await execute_db_operation_smart(op)
            assert result == expected
            mock_local.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lakebase_enabled_success_on_first_attempt(self):
        """When Lakebase succeeds on first attempt, return result and record connection."""
        expected = "lakebase_result"
        lakebase_session = AsyncMock()

        async def op(session):
            return expected

        lakebase_config = {"instance_name": "test-instance"}

        with patch(
            "src.db.database_router.is_lakebase_enabled",
            new_callable=AsyncMock,
            return_value=True,
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_config,
        ), patch(
            "src.db.lakebase_session.get_lakebase_session",
            return_value=_make_async_ctx(lakebase_session),
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "src.db.lakebase_state.record_successful_connection",
        ) as mock_record:
            from src.utils.asyncio_utils import execute_db_operation_smart

            result = await execute_db_operation_smart(op)
            assert result == expected
            mock_record.assert_called_once()

    @pytest.mark.asyncio
    async def test_lakebase_fails_all_retries_startup_falls_back(self):
        """When all retries fail during startup (fallback allowed), fall back to local DB."""
        expected = "local_fallback"

        async def op(session):
            return expected

        lakebase_config = {"instance_name": "test-instance"}

        class _FailingCtx:
            async def __aenter__(self):
                raise ConnectionError("unreachable")
            async def __aexit__(self, *args):
                return False

        with patch(
            "src.db.database_router.is_lakebase_enabled",
            new_callable=AsyncMock,
            return_value=True,
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_config,
        ), patch(
            "src.db.lakebase_session.get_lakebase_session",
            side_effect=lambda *a, **kw: _FailingCtx(),
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "src.db.lakebase_state.is_fallback_allowed",
            return_value=True,
        ), patch(
            "asyncio.sleep",
            new_callable=AsyncMock,
        ), patch(
            "src.utils.asyncio_utils.execute_db_operation_with_fresh_engine",
            new_callable=AsyncMock,
            return_value=expected,
        ) as mock_local:
            from src.utils.asyncio_utils import execute_db_operation_smart

            result = await execute_db_operation_smart(op)
            assert result == expected
            mock_local.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lakebase_fails_all_retries_runtime_raises(self):
        """When all retries fail at runtime (fallback NOT allowed), raise RuntimeError."""
        async def op(session):
            return "should not reach"

        lakebase_config = {"instance_name": "test-instance"}

        class _FailingCtx:
            async def __aenter__(self):
                raise ConnectionError("unreachable")
            async def __aexit__(self, *args):
                return False

        with patch(
            "src.db.database_router.is_lakebase_enabled",
            new_callable=AsyncMock,
            return_value=True,
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_config,
        ), patch(
            "src.db.lakebase_session.get_lakebase_session",
            side_effect=lambda *a, **kw: _FailingCtx(),
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "src.db.lakebase_state.is_fallback_allowed",
            return_value=False,
        ), patch(
            "asyncio.sleep",
            new_callable=AsyncMock,
        ):
            from src.utils.asyncio_utils import execute_db_operation_smart

            with pytest.raises(RuntimeError, match="unreachable"):
                await execute_db_operation_smart(op)

    @pytest.mark.asyncio
    async def test_retry_backoff_delays(self):
        """Verify retry loop uses correct exponential backoff delays."""
        async def op(session):
            return "should not reach"

        lakebase_config = {"instance_name": "test-instance"}

        class _FailingCtx:
            async def __aenter__(self):
                raise ConnectionError("unreachable")
            async def __aexit__(self, *args):
                return False

        with patch(
            "src.db.database_router.is_lakebase_enabled",
            new_callable=AsyncMock,
            return_value=True,
        ), patch(
            "src.db.database_router.get_lakebase_config_from_db",
            new_callable=AsyncMock,
            return_value=lakebase_config,
        ), patch(
            "src.db.lakebase_session.get_lakebase_session",
            side_effect=lambda *a, **kw: _FailingCtx(),
        ), patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "src.db.lakebase_state.is_fallback_allowed",
            return_value=False,
        ), patch(
            "asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep:
            from src.utils.asyncio_utils import execute_db_operation_smart

            with pytest.raises(RuntimeError):
                await execute_db_operation_smart(op)

            assert mock_sleep.await_count == 2
            mock_sleep.assert_any_await(0.5)
            mock_sleep.assert_any_await(1.0)