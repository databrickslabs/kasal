"""
Ultimate test to hit the final 8 lines and achieve 100% coverage.
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, Mock

# Test by directly invoking the timeout path


@pytest.mark.asyncio 
async def test_timeout_lines_466_473_ultimate():
    """Final attempt to hit lines 466-473 with precise control"""
    
    from src.services.execution_logs_service import stop_logs_writer
    import src.services.execution_logs_service as service_module
    
    # Create a real asyncio task that will be cancelled
    async def dummy_coro():
        await asyncio.sleep(10)  # Long-running task
    
    real_task = asyncio.create_task(dummy_coro())
    
    # Set the global task
    original_task = service_module._logs_writer_task
    service_module._logs_writer_task = real_task
    
    try:
        with patch('src.services.execution_logs_service.get_job_output_queue') as mock_queue, \
             patch('src.services.execution_logs_service.logger') as mock_logger:
            
            # Setup queue
            queue_mock = Mock()
            queue_mock.put_nowait = Mock()
            mock_queue.return_value = queue_mock
            
            # Monkey patch asyncio.wait_for to always raise TimeoutError
            original_wait_for = asyncio.wait_for
            
            def timeout_wait_for(*args, **kwargs):
                # This will be called and raise TimeoutError (line 465 -> 466)
                raise asyncio.TimeoutError()
            
            asyncio.wait_for = timeout_wait_for
            
            try:
                # Call stop_logs_writer - this will hit the timeout path
                result = await stop_logs_writer(timeout=1.0)
                
                # Verify all the lines were hit:
                
                # Line 466: timeout warning
                warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
                timeout_warning = any("did not stop in time, cancelling" in w for w in warning_calls)
                assert timeout_warning, f"Line 466 timeout warning not found: {warning_calls}"
                
                # Line 467: task.cancel() called - check that task is cancelled
                assert real_task.cancelled(), "Task should be cancelled"
                
                # Line 473: returns True  
                assert result is True
                
                print("SUCCESS: Lines 466-473 hit successfully!")
                
            finally:
                asyncio.wait_for = original_wait_for
                # Clean up the task
                if not real_task.done():
                    real_task.cancel()
                    try:
                        await real_task
                    except asyncio.CancelledError:
                        pass
                
    finally:
        service_module._logs_writer_task = original_task


if __name__ == "__main__":
    asyncio.run(test_timeout_lines_466_473_ultimate())