import pytest
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from src.engines.crewai.callbacks.base import CrewAICallback, CallbackFailedError


class TestCrewAICallback:
    """Test suite for CrewAICallback base class."""
    
    def test_callback_initialization_default(self):
        """Test callback initialization with default parameters."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        assert callback.max_retries == 3
        assert callback.task_key is None
        assert callback.retry_count == 0
        assert callback.metadata == {}
    
    def test_callback_initialization_custom(self):
        """Test callback initialization with custom parameters."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback(max_retries=5, task_key="test_task")
        
        assert callback.max_retries == 5
        assert callback.task_key == "test_task"
        assert callback.retry_count == 0
        assert callback.metadata == {}
    
    @pytest.mark.asyncio
    async def test_callback_successful_execution(self):
        """Test successful callback execution."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return f"processed_{output}"
        
        callback = TestCallback(task_key="test_task")
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            result = await callback("test_input")
            
            assert result == "processed_test_input"
            mock_logger.info.assert_any_call("=== Starting TestCallback ===")
            mock_logger.info.assert_any_call("Task: test_task")
            mock_logger.info.assert_any_call("Attempt: 1/3")
            mock_logger.info.assert_any_call("=== Successfully completed TestCallback ===")
    
    @pytest.mark.asyncio
    async def test_callback_with_unknown_task(self):
        """Test callback execution without task key."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return f"processed_{output}"
        
        callback = TestCallback()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            result = await callback("test_input")
            
            assert result == "processed_test_input"
            mock_logger.info.assert_any_call("Task: Unknown")
    
    @pytest.mark.asyncio
    async def test_callback_retry_logic_success_on_second_attempt(self):
        """Test callback retry logic that succeeds on second attempt."""
        class TestCallback(CrewAICallback):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.attempt_count = 0
            
            async def execute(self, output):
                self.attempt_count += 1
                if self.attempt_count == 1:
                    raise Exception("First attempt fails")
                return f"processed_{output}"
        
        callback = TestCallback(task_key="test_task")
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger, \
             patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            
            result = await callback("test_input")
            
            assert result == "processed_test_input"
            assert callback.retry_count == 1
            mock_sleep.assert_called_once_with(1)
            mock_logger.info.assert_any_call("Retrying TestCallback (Attempt 2/3)")
    
    @pytest.mark.asyncio
    async def test_callback_retry_exhaustion(self):
        """Test callback that fails after all retries are exhausted."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                raise Exception("Always fails")
        
        callback = TestCallback(max_retries=2, task_key="test_task")
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger, \
             patch('asyncio.sleep', new_callable=AsyncMock):
            
            with pytest.raises(CallbackFailedError) as exc_info:
                await callback("test_input")
            
            error = exc_info.value
            assert error.callback_name == "TestCallback"
            assert error.task_key == "test_task"
            assert error.error == "Always fails"
            assert isinstance(error.timestamp, datetime)
            
            mock_logger.error.assert_any_call("TestCallback failed after 2 attempts")
    
    @pytest.mark.asyncio
    async def test_callback_retry_with_metadata(self):
        """Test callback retry with custom metadata."""
        class TestCallback(CrewAICallback):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.metadata = {"custom_data": "test_value"}
            
            async def execute(self, output):
                raise Exception("Test failure")
        
        callback = TestCallback(max_retries=1, task_key="test_task")
        
        with patch('src.engines.crewai.callbacks.base.logger'), \
             patch('asyncio.sleep', new_callable=AsyncMock):
            
            with pytest.raises(CallbackFailedError) as exc_info:
                await callback("test_input")
            
            error = exc_info.value
            assert error.metadata == {"custom_data": "test_value"}
    
    def test_log_output_info_with_raw_attribute(self):
        """Test _log_output_info with object that has raw attribute."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        class MockOutput:
            def __init__(self):
                self.raw = "This is raw content for testing"
        
        mock_output = MockOutput()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(mock_output)
            
            mock_logger.info.assert_any_call(f"Output Type: {type(mock_output)}")
            mock_logger.info.assert_any_call("Output Content: This is raw content for testing...")
    
    def test_log_output_info_with_string(self):
        """Test _log_output_info with string output."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info("test string")
            
            mock_logger.info.assert_any_call("Output Type: <class 'str'>")
            mock_logger.info.assert_any_call("Output Content: test string...")
    
    def test_log_output_info_with_number(self):
        """Test _log_output_info with numeric output."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(42)
            
            mock_logger.info.assert_any_call("Output Type: <class 'int'>")
            mock_logger.info.assert_any_call("Output Content: 42...")
    
    def test_log_output_info_with_boolean(self):
        """Test _log_output_info with boolean output."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(True)
            
            mock_logger.info.assert_any_call("Output Type: <class 'bool'>")
            mock_logger.info.assert_any_call("Output Content: True...")
    
    def test_log_output_info_with_dict(self):
        """Test _log_output_info with dictionary output."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        test_dict = {"key": "value", "number": 123}
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(test_dict)
            
            mock_logger.info.assert_any_call("Output Type: <class 'dict'>")
            # The exact string representation might vary, so we check it contains key info
            args, _ = mock_logger.info.call_args_list[-1]
            assert "Output Content:" in args[0]
            assert "key" in args[0] or "value" in args[0]
    
    def test_log_output_info_with_custom_object(self):
        """Test _log_output_info with custom object output."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        class CustomObject:
            def __str__(self):
                return "Custom object string representation"
        
        custom_obj = CustomObject()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(custom_obj)
            
            mock_logger.info.assert_any_call(f"Output Type: {type(custom_obj)}")
            mock_logger.info.assert_any_call("Output Content: Custom object string representation...")
    
    def test_log_output_info_with_long_content(self):
        """Test _log_output_info truncates long content."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        long_string = "x" * 600  # Longer than 500 character limit
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(long_string)
            
            args, _ = mock_logger.info.call_args_list[-1]
            # Should be truncated to 500 chars plus "..."
            assert len(args[0]) <= len("Output Content: ") + 500 + 3
            assert args[0].endswith("...")
    
    def test_log_output_info_with_float(self):
        """Test _log_output_info with float output."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(3.14)
            
            mock_logger.info.assert_any_call("Output Type: <class 'float'>")
            mock_logger.info.assert_any_call("Output Content: 3.14...")
    
    def test_log_output_info_with_long_raw_content(self):
        """Test _log_output_info with long raw content that gets truncated."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                return output
        
        callback = TestCallback()
        
        class MockOutput:
            def __init__(self):
                self.raw = "x" * 600  # Longer than 500 character limit
        
        mock_output = MockOutput()
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            callback._log_output_info(mock_output)
            
            mock_logger.info.assert_any_call(f"Output Type: {type(mock_output)}")
            args, _ = mock_logger.info.call_args_list[-1]
            # Should be truncated to 500 chars plus "..."
            assert len(args[0]) <= len("Output Content: ") + 500 + 3
            assert args[0].endswith("...")
    
    @pytest.mark.asyncio
    async def test_callback_error_logging_with_stack_trace(self):
        """Test callback error logging includes stack trace."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                raise ValueError("Test error with stack trace")
        
        callback = TestCallback(max_retries=1, task_key="test_task")
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger, \
             patch('asyncio.sleep', new_callable=AsyncMock):
            
            with pytest.raises(CallbackFailedError):
                await callback("test_input")
            
            # Check that error and stack trace logging was called
            mock_logger.error.assert_any_call("Error in TestCallback: Test error with stack trace")
            mock_logger.error.assert_any_call("Stack trace:", exc_info=True)
    
    @pytest.mark.asyncio
    async def test_callback_max_retries_zero(self):
        """Test callback with max_retries set to 0."""
        class TestCallback(CrewAICallback):
            async def execute(self, output):
                raise Exception("Always fails")
        
        callback = TestCallback(max_retries=0, task_key="test_task")
        
        with patch('src.engines.crewai.callbacks.base.logger') as mock_logger:
            with pytest.raises(CallbackFailedError) as exc_info:
                await callback("test_input")
            
            error = exc_info.value
            assert error.callback_name == "TestCallback"
            # Should fail immediately without retries
            mock_logger.error.assert_any_call("TestCallback failed after 0 attempts")
    
    @pytest.mark.asyncio 
    async def test_callback_retry_count_increments_correctly(self):
        """Test that retry_count increments correctly during retries."""
        class TestCallback(CrewAICallback):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.attempt_count = 0
            
            async def execute(self, output):
                self.attempt_count += 1
                if self.attempt_count <= 2:  # Fail first two attempts
                    raise Exception(f"Attempt {self.attempt_count} fails")
                return f"success_on_attempt_{self.attempt_count}"
        
        callback = TestCallback(max_retries=3, task_key="test_task")
        
        with patch('src.engines.crewai.callbacks.base.logger'), \
             patch('asyncio.sleep', new_callable=AsyncMock):
            
            result = await callback("test_input")
            
            assert result == "success_on_attempt_3"
            assert callback.retry_count == 2  # Should have retried twice


class TestCallbackFailedError:
    """Test suite for CallbackFailedError class."""
    
    def test_callback_failed_error_initialization(self):
        """Test CallbackFailedError initialization."""
        metadata = {"test": "data"}
        error = CallbackFailedError(
            callback_name="TestCallback",
            task_key="test_task",
            error="Test error message",
            metadata=metadata
        )
        
        assert error.callback_name == "TestCallback"
        assert error.task_key == "test_task"
        assert error.error == "Test error message"
        assert error.metadata == metadata
        assert isinstance(error.timestamp, datetime)
    
    def test_callback_failed_error_message_format(self):
        """Test CallbackFailedError message formatting."""
        metadata = {"test": "data"}
        error = CallbackFailedError(
            callback_name="TestCallback",
            task_key="test_task",
            error="Test error message",
            metadata=metadata
        )
        
        message = str(error)
        assert "Callback 'TestCallback' failed for task 'test_task'" in message
        assert "Error: Test error message" in message
        assert "Metadata: {'test': 'data'}" in message
        assert "Timestamp:" in message
    
    def test_callback_failed_error_with_none_task_key(self):
        """Test CallbackFailedError with None task key."""
        error = CallbackFailedError(
            callback_name="TestCallback",
            task_key=None,
            error="Test error message",
            metadata={}
        )
        
        message = str(error)
        assert "Callback 'TestCallback' failed for task 'Unknown'" in message
    
    def test_callback_failed_error_inheritance(self):
        """Test CallbackFailedError inherits from Exception."""
        error = CallbackFailedError(
            callback_name="TestCallback",
            task_key="test_task",
            error="Test error message",
            metadata={}
        )
        
        assert isinstance(error, Exception)
    
    def test_callback_failed_error_with_empty_metadata(self):
        """Test CallbackFailedError with empty metadata."""
        error = CallbackFailedError(
            callback_name="TestCallback",
            task_key="test_task",
            error="Test error message",
            metadata={}
        )
        
        assert error.metadata == {}
        message = str(error)
        assert "Metadata: {}" in message