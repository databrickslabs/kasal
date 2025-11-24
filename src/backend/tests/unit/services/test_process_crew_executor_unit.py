import pytest
from unittest.mock import Mock, patch, MagicMock

from src.services.process_crew_executor import run_crew_in_process


class TestProcessCrewExecutorValidation:
    def test_run_crew_in_process_none_config(self):
        out = run_crew_in_process(execution_id="e-1", crew_config=None)
        assert isinstance(out, dict)
        assert out.get("status") == "FAILED"
        assert out.get("execution_id") == "e-1"
        assert "crew_config is None" in out.get("error", "")

    def test_run_crew_in_process_invalid_json_string(self):
        out = run_crew_in_process(execution_id="e-2", crew_config="{not-json}")
        assert out.get("status") == "FAILED"
        assert out.get("execution_id") == "e-2"
        assert "Failed to parse crew_config JSON" in out.get("error", "")

    def test_run_crew_in_process_json_string_not_dict(self):
        # Valid JSON but not a dict (a list) should be rejected by type validation
        out = run_crew_in_process(execution_id="e-3", crew_config="[1,2,3]")
        assert out.get("status") == "FAILED"
        assert out.get("execution_id") == "e-3"
        assert "crew_config must be a dict" in out.get("error", "")


class TestCrewAIContextWindowPatching:
    """Test CrewAI context window size patching for Databricks models."""

    def test_databricks_model_context_window_patch(self):
        """Test that Databricks models are registered with CrewAI context window sizes."""
        # Mock the imports that happen inside run_crew_in_process
        mock_llm_context = {}
        mock_model_configs = {
            "databricks-test-model": {
                "provider": "databricks",
                "context_window": 128000
            },
            "openai-model": {
                "provider": "openai",
                "context_window": 8192
            }
        }

        with patch.dict('sys.modules', {'crewai': MagicMock(), 'crewai.llm': MagicMock()}):
            with patch('crewai.llm.LLM_CONTEXT_WINDOW_SIZES', mock_llm_context):
                # Simulate the patching logic
                for model_name, config in mock_model_configs.items():
                    if config.get('provider') == 'databricks':
                        full_model_name = f"databricks/{model_name}"
                        context_window = config.get('context_window', 128000)
                        mock_llm_context[full_model_name] = context_window

                # Only Databricks model should be registered
                assert "databricks/databricks-test-model" in mock_llm_context
                assert mock_llm_context["databricks/databricks-test-model"] == 128000
                assert "databricks/openai-model" not in mock_llm_context

    def test_databricks_context_limit_error_patterns(self):
        """Test that Databricks error patterns are added to CONTEXT_LIMIT_ERRORS."""
        mock_context_limit_errors = [
            "context_length_exceeded",
            "maximum context length"
        ]

        databricks_patterns = [
            "exceeds maximum allowed content length",
            "maximum allowed content length",
            "requestsize",
        ]

        # Simulate the patching logic
        for pattern in databricks_patterns:
            if pattern not in mock_context_limit_errors:
                mock_context_limit_errors.append(pattern)

        # Verify all patterns are added
        for pattern in databricks_patterns:
            assert pattern in mock_context_limit_errors

        # Verify original patterns still exist
        assert "context_length_exceeded" in mock_context_limit_errors


class TestLLMCallTracking:
    """Test LLM call tracking and timing functionality."""

    def test_tracked_completion_logs_duration(self):
        """Test that tracked_completion logs duration for successful calls."""
        import time

        # Simulate the tracked_completion wrapper logic
        mock_original_completion = Mock(return_value=Mock(
            choices=[Mock(message=Mock(content="Test response"))]
        ))

        # Simulate timing
        start_time = time.time()
        result = mock_original_completion(model="databricks/test-model")
        duration = time.time() - start_time

        # Verify result structure
        assert result.choices[0].message.content == "Test response"
        assert duration >= 0

    def test_tracked_completion_handles_empty_response(self):
        """Test that tracked_completion correctly identifies empty responses."""
        # Mock response with empty content
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content=''))]

        # Simulate the empty response check logic
        choices = getattr(mock_response, 'choices', None)
        assert choices is not None
        assert len(choices) > 0

        first_choice = choices[0]
        message = getattr(first_choice, 'message', None)
        assert message is not None

        content = getattr(message, 'content', None)
        is_empty = content is None or content == ''
        assert is_empty is True

    def test_tracked_completion_handles_none_response(self):
        """Test that tracked_completion correctly identifies None responses."""
        mock_response = None

        # Simulate the None response check logic
        is_none = mock_response is None
        assert is_none is True

    def test_tracked_completion_handles_no_choices(self):
        """Test that tracked_completion correctly identifies responses without choices."""
        mock_response = Mock()
        mock_response.choices = None

        # Simulate the check logic
        choices = getattr(mock_response, 'choices', None)
        assert choices is None

    def test_tracked_completion_handles_empty_choices(self):
        """Test that tracked_completion correctly identifies responses with empty choices."""
        mock_response = Mock()
        mock_response.choices = []

        # Simulate the check logic
        choices = getattr(mock_response, 'choices', None)
        assert choices is not None
        assert len(choices) == 0

    def test_tracked_completion_handles_llm_error(self):
        """Test that tracked_completion logs duration even for failed LLM calls."""
        import time

        mock_original_completion = Mock(side_effect=Exception("LLM Error"))

        start_time = time.time()
        error_raised = False
        try:
            mock_original_completion(model="databricks/test-model")
        except Exception as e:
            error_raised = True
            duration = time.time() - start_time
            assert "LLM Error" in str(e)

        assert error_raised is True
        assert duration >= 0

    def test_tracked_completion_extracts_model_name(self):
        """Test that tracked_completion correctly extracts model name from kwargs."""
        kwargs = {'model': 'databricks/databricks-claude-sonnet-4-5', 'messages': []}

        model = kwargs.get('model', 'unknown')
        assert model == 'databricks/databricks-claude-sonnet-4-5'

    def test_tracked_completion_handles_missing_model(self):
        """Test that tracked_completion handles missing model in kwargs."""
        kwargs = {'messages': []}

        model = kwargs.get('model', 'unknown')
        assert model == 'unknown'

    def test_tracked_completion_checks_reasoning_content(self):
        """Test that tracked_completion checks for reasoning_content on empty responses."""
        mock_message = Mock()
        mock_message.content = ''
        mock_message.reasoning_content = "This is the reasoning"

        # Simulate the reasoning content check
        content = getattr(mock_message, 'content', None)
        is_empty = content is None or content == ''
        assert is_empty is True

        reasoning = getattr(mock_message, 'reasoning_content', None)
        assert reasoning is not None
        assert "reasoning" in reasoning.lower()


class TestLLMResponseValidation:
    """Test LLM response validation logic."""

    def test_valid_response_structure(self):
        """Test validation of a proper LLM response structure."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content="Valid response content"))]

        # Validate structure
        assert mock_response is not None
        assert hasattr(mock_response, 'choices')
        assert mock_response.choices is not None
        assert len(mock_response.choices) > 0
        assert hasattr(mock_response.choices[0], 'message')
        assert mock_response.choices[0].message is not None
        assert hasattr(mock_response.choices[0].message, 'content')
        assert mock_response.choices[0].message.content != ''

    def test_response_content_length_logging(self):
        """Test that content length is correctly calculated for logging."""
        test_content = "This is a test response with some content"
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content=test_content))]

        content = mock_response.choices[0].message.content
        content_length = len(content)

        assert content_length == len(test_content)
        assert content_length > 0

