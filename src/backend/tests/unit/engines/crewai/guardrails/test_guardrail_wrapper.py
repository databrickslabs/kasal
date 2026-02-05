"""
Unit tests for GuardrailWrapper class.

Tests the callable wrapper that enables inspect.getsource() compatibility
for function-based guardrails in CrewAI's guardrail event system.
"""

import pytest
import os
import inspect
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime

from src.engines.crewai.guardrails.guardrail_wrapper import GuardrailWrapper
from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.core.logger import LoggerManager


class MockGuardrail(BaseGuardrail):
    """Mock guardrail for testing."""

    def __init__(self, config=None, return_valid=True, return_feedback=""):
        super().__init__(config or {})
        self.return_valid = return_valid
        self.return_feedback = return_feedback
        self.validate_called = False
        self.last_output = None

    def validate(self, output: str):
        self.validate_called = True
        self.last_output = output
        return {"valid": self.return_valid, "feedback": self.return_feedback}


class MockGuardrailWithException(BaseGuardrail):
    """Mock guardrail that raises an exception."""

    def __init__(self, config=None, exception_msg="Test exception"):
        super().__init__(config or {})
        self.exception_msg = exception_msg

    def validate(self, output: str):
        raise ValueError(self.exception_msg)


class TestGuardrailWrapperInit:
    """Test suite for GuardrailWrapper initialization."""

    def test_init_stores_guardrail(self):
        """Test that initialization stores the guardrail instance."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "test_task")

        assert wrapper.guardrail is guardrail

    def test_init_stores_task_key(self):
        """Test that initialization stores the task key."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "my_task_key")

        assert wrapper.task_key == "my_task_key"

    def test_init_creates_logger(self):
        """Test that initialization creates a logger."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "test_task")

        assert wrapper.logger is not None

    def test_init_sets_log_dir(self):
        """Test that initialization sets log directory."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "test_task")

        assert wrapper.log_dir is not None
        # log_dir can be a Path or str depending on LoggerManager implementation
        assert str(wrapper.log_dir)  # Should be convertible to string

    def test_init_with_different_task_keys(self):
        """Test initialization with various task key formats."""
        guardrail = MockGuardrail()

        # Test with simple string
        wrapper1 = GuardrailWrapper(guardrail, "task1")
        assert wrapper1.task_key == "task1"

        # Test with complex string
        wrapper2 = GuardrailWrapper(guardrail, "task_with_underscore")
        assert wrapper2.task_key == "task_with_underscore"

        # Test with numbers
        wrapper3 = GuardrailWrapper(guardrail, "task_123")
        assert wrapper3.task_key == "task_123"


class TestGuardrailWrapperCall:
    """Test suite for GuardrailWrapper __call__ method."""

    @patch('builtins.open', mock_open())
    def test_call_invokes_guardrail_validate(self):
        """Test that __call__ invokes the guardrail's validate method."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        wrapper("test output")

        assert guardrail.validate_called is True
        assert guardrail.last_output == "test output"

    @patch('builtins.open', mock_open())
    def test_call_returns_tuple_on_success(self):
        """Test that __call__ returns (True, output) on successful validation."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        result = wrapper("test output")

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] is True
        assert result[1] == "test output"

    @patch('builtins.open', mock_open())
    def test_call_returns_tuple_on_failure(self):
        """Test that __call__ returns (False, feedback) on validation failure."""
        guardrail = MockGuardrail(return_valid=False, return_feedback="Validation failed: missing data")
        wrapper = GuardrailWrapper(guardrail, "test_task")

        result = wrapper("test output")

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] is False
        assert result[1] == "Validation failed: missing data"

    @patch('builtins.open', mock_open())
    def test_call_returns_empty_feedback_when_empty(self):
        """Test that __call__ returns empty feedback when guardrail provides empty feedback."""
        guardrail = MockGuardrail(return_valid=False, return_feedback="")
        wrapper = GuardrailWrapper(guardrail, "test_task")

        result = wrapper("test output")

        assert result[0] is False
        # Empty string is returned as-is (not replaced with default)
        assert result[1] == ""

    @patch('builtins.open', mock_open())
    def test_call_returns_default_feedback_when_key_missing(self):
        """Test that __call__ returns default feedback when guardrail doesn't provide feedback key."""
        class NoFeedbackGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": False}  # No feedback key

        guardrail = NoFeedbackGuardrail({})
        wrapper = GuardrailWrapper(guardrail, "test_task")

        result = wrapper("test output")

        assert result[0] is False
        assert result[1] == "Output does not meet requirements. Please try again."

    @patch('builtins.open', mock_open())
    def test_call_handles_exception(self):
        """Test that __call__ handles exceptions from guardrail validation."""
        guardrail = MockGuardrailWithException(exception_msg="Validation error occurred")
        wrapper = GuardrailWrapper(guardrail, "test_task")

        result = wrapper("test output")

        assert result[0] is False
        assert "Validation error:" in result[1]
        assert "Validation error occurred" in result[1]

    @patch('builtins.open', mock_open())
    def test_call_with_various_output_types(self):
        """Test that __call__ handles various output types."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        # Test with string
        result1 = wrapper("string output")
        assert result1[0] is True

        # Test with dict-like string (JSON)
        result2 = wrapper('{"key": "value"}')
        assert result2[0] is True

        # Test with multiline string
        result3 = wrapper("line1\nline2\nline3")
        assert result3[0] is True

    @patch('builtins.open', mock_open())
    def test_call_writes_to_debug_log(self):
        """Test that __call__ writes to debug log file."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        m = mock_open()
        with patch('builtins.open', m):
            wrapper("test output")

        # Verify file was opened for writing
        assert m.called

    @patch('builtins.open', mock_open())
    def test_call_logs_validation_success(self):
        """Test that successful validation is logged."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        with patch.object(wrapper.logger, 'info') as mock_logger:
            wrapper("test output")

            # Check that success was logged
            log_messages = [str(call) for call in mock_logger.call_args_list]
            assert any("passed guardrail validation" in msg for msg in log_messages)

    @patch('builtins.open', mock_open())
    def test_call_logs_validation_failure(self):
        """Test that validation failure is logged."""
        guardrail = MockGuardrail(return_valid=False, return_feedback="Failed validation")
        wrapper = GuardrailWrapper(guardrail, "test_task")

        with patch.object(wrapper.logger, 'warning') as mock_logger:
            wrapper("test output")

            # Check that failure was logged
            mock_logger.assert_called()


class TestGuardrailWrapperGetsource:
    """Test suite for inspect.getsource() compatibility."""

    def test_getsource_on_call_method(self):
        """Test that inspect.getsource() works on the __call__ method."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "test_task")

        # This is the key test - getsource() should NOT raise OSError
        source = inspect.getsource(wrapper.__call__)

        assert source is not None
        assert "def __call__" in source
        assert "Validate task output" in source

    def test_getsource_on_wrapper_class(self):
        """Test that inspect.getsource() works on the wrapper class."""
        source = inspect.getsource(GuardrailWrapper)

        assert source is not None
        assert "class GuardrailWrapper" in source

    def test_callable_wrapper_is_inspectable(self):
        """Test that the wrapper instance can be inspected."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "test_task")

        # Should not raise any exceptions
        assert callable(wrapper)
        assert hasattr(wrapper, '__call__')

        # getsource on the callable should work
        try:
            source = inspect.getsource(type(wrapper).__call__)
            assert "def __call__" in source
        except OSError:
            pytest.fail("getsource() raised OSError - wrapper is not inspectable")


class TestGuardrailWrapperRepr:
    """Test suite for GuardrailWrapper __repr__ method."""

    def test_repr_includes_task_key(self):
        """Test that __repr__ includes the task key."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "my_task")

        repr_str = repr(wrapper)

        assert "my_task" in repr_str

    def test_repr_includes_guardrail_type(self):
        """Test that __repr__ includes the guardrail type name."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "test_task")

        repr_str = repr(wrapper)

        assert "MockGuardrail" in repr_str

    def test_repr_format(self):
        """Test the format of __repr__ output."""
        guardrail = MockGuardrail()
        wrapper = GuardrailWrapper(guardrail, "test_task")

        repr_str = repr(wrapper)

        assert repr_str.startswith("GuardrailWrapper(")
        assert repr_str.endswith(")")


class TestGuardrailWrapperIntegration:
    """Integration tests for GuardrailWrapper with real guardrail classes."""

    @patch('builtins.open', mock_open())
    def test_wrapper_with_company_count_guardrail_config(self):
        """Test wrapper with a config similar to CompanyCountGuardrail."""
        class CompanyCountLikeGuardrail(BaseGuardrail):
            def validate(self, output: str):
                # Simulate counting companies
                count = output.count("Company")
                min_count = self.config.get("min_count", 5)
                if count >= min_count:
                    return {"valid": True, "feedback": ""}
                return {
                    "valid": False,
                    "feedback": f"Found {count} companies, need at least {min_count}"
                }

        config = {"min_count": 3}
        guardrail = CompanyCountLikeGuardrail(config)
        wrapper = GuardrailWrapper(guardrail, "company_task")

        # Test passing validation
        result1 = wrapper("Company A, Company B, Company C, Company D")
        assert result1[0] is True

        # Test failing validation
        result2 = wrapper("Company A, Company B")
        assert result2[0] is False
        assert "need at least 3" in result2[1]

    @patch('builtins.open', mock_open())
    def test_wrapper_preserves_guardrail_state(self):
        """Test that wrapper preserves guardrail state between calls."""
        class StatefulGuardrail(BaseGuardrail):
            def __init__(self, config):
                super().__init__(config)
                self.call_count = 0

            def validate(self, output: str):
                self.call_count += 1
                return {"valid": True, "feedback": f"Call #{self.call_count}"}

        guardrail = StatefulGuardrail({})
        wrapper = GuardrailWrapper(guardrail, "stateful_task")

        wrapper("output 1")
        wrapper("output 2")
        wrapper("output 3")

        assert guardrail.call_count == 3

    @patch('builtins.open', mock_open())
    def test_wrapper_handles_none_feedback(self):
        """Test wrapper handles None feedback from guardrail."""
        class NullFeedbackGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"valid": False, "feedback": None}

        guardrail = NullFeedbackGuardrail({})
        wrapper = GuardrailWrapper(guardrail, "null_feedback_task")

        result = wrapper("test")

        assert result[0] is False
        # None is returned as-is (dict.get returns the value even if None)
        assert result[1] is None

    @patch('builtins.open', mock_open())
    def test_wrapper_handles_missing_valid_key(self):
        """Test wrapper handles missing 'valid' key from guardrail."""
        class MissingValidGuardrail(BaseGuardrail):
            def validate(self, output: str):
                return {"feedback": "Some feedback"}

        guardrail = MissingValidGuardrail({})
        wrapper = GuardrailWrapper(guardrail, "missing_valid_task")

        result = wrapper("test")

        # Missing 'valid' key should default to False
        assert result[0] is False


class TestGuardrailWrapperEdgeCases:
    """Test edge cases for GuardrailWrapper."""

    @patch('builtins.open', mock_open())
    def test_call_with_empty_output(self):
        """Test __call__ with empty string output."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        result = wrapper("")

        assert result[0] is True
        assert result[1] == ""

    @patch('builtins.open', mock_open())
    def test_call_with_very_long_output(self):
        """Test __call__ with very long output (truncation in logs)."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        long_output = "x" * 10000
        result = wrapper(long_output)

        assert result[0] is True
        assert result[1] == long_output

    @patch('builtins.open', mock_open())
    def test_call_with_special_characters(self):
        """Test __call__ with special characters in output."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        special_output = "Test with special chars: \n\t\r\x00 and unicode: \u2603 \u2764"
        result = wrapper(special_output)

        assert result[0] is True

    @patch('builtins.open', mock_open())
    def test_call_with_unicode_task_key(self):
        """Test wrapper with unicode characters in task key."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "task_\u2603_unicode")

        assert wrapper.task_key == "task_\u2603_unicode"

        result = wrapper("test")
        assert result[0] is True


class TestGuardrailWrapperFileOperations:
    """Test file operations in GuardrailWrapper."""

    def test_log_dir_is_created(self):
        """Test that log directory creation is attempted."""
        guardrail = MockGuardrail()

        with patch('os.makedirs') as mock_makedirs:
            wrapper = GuardrailWrapper(guardrail, "test_task")
            mock_makedirs.assert_called()

    @patch('builtins.open', mock_open())
    def test_debug_log_file_path(self):
        """Test that debug log is written to correct path."""
        guardrail = MockGuardrail(return_valid=True)
        wrapper = GuardrailWrapper(guardrail, "test_task")

        m = mock_open()
        with patch('builtins.open', m) as mock_file:
            wrapper("test output")

            # Check the file was opened with the correct path pattern
            calls = mock_file.call_args_list
            assert any("guardrail_debug.log" in str(call) for call in calls)
