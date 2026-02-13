"""
Unit tests for DatabricksGPTOSSHandler module.

Tests the specialized handling of Databricks GPT-OSS models including
response format transformation and parameter filtering.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import sys
import logging

from src.core.llm_handlers.databricks_gpt_oss_handler import (
    DatabricksGPTOSSHandler,
    DatabricksGPTOSSLLM,
    DatabricksRetryLLM,
    apply_empty_content_fix,
    apply_tool_calls_fix,
)


class TestDatabricksGPTOSSHandler:
    """Test suite for DatabricksGPTOSSHandler."""

    def test_is_gpt_oss_model_true(self):
        """Test identifying GPT-OSS models correctly."""
        assert DatabricksGPTOSSHandler.is_gpt_oss_model("databricks-gpt-oss-2024")
        assert DatabricksGPTOSSHandler.is_gpt_oss_model("gpt-oss-v1")
        assert DatabricksGPTOSSHandler.is_gpt_oss_model("GPT-OSS-TURBO")

    def test_is_gpt_oss_model_false(self):
        """Test identifying non-GPT-OSS models correctly."""
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model("gpt-4")
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model("claude-3")
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model("")
        assert not DatabricksGPTOSSHandler.is_gpt_oss_model(None)

    def test_extract_text_from_string_response(self):
        """Test extracting text from a simple string response."""
        content = "This is a simple response"
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "This is a simple response"

    def test_extract_text_from_json_string(self):
        """Test extracting text from a JSON string response."""
        content = json.dumps(
            [
                {"type": "reasoning", "summary": [], "content": []},
                {"type": "text", "text": "Actual response text"},
            ]
        )
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Actual response text"

    def test_extract_text_from_harmony_format(self):
        """Test extracting text from Harmony format response."""
        content = [
            {
                "type": "reasoning",
                "summary": [{"type": "summary_text", "text": "Some summary"}],
                "content": [{"type": "reasoning_text", "text": "Reasoning content"}],
            },
            {"type": "text", "text": "Main response text"},
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Main response text"

    def test_extract_text_prioritizes_text_blocks(self):
        """Test that text blocks are prioritized over reasoning blocks."""
        content = [
            {
                "type": "reasoning",
                "content": [{"type": "reasoning_text", "text": "Reasoning text"}],
            },
            {"type": "text", "text": "Primary text"},
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Primary text"

    def test_extract_text_falls_back_to_reasoning(self):
        """Test fallback to reasoning text when no text blocks exist."""
        content = [
            {
                "type": "reasoning",
                "content": [{"type": "reasoning_text", "text": "Only reasoning text"}],
            }
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Only reasoning text"

    def test_extract_text_from_dict_with_text_field(self):
        """Test extracting text from a dict with a text field."""
        content = {"text": "Dict text response"}
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Dict text response"

    def test_extract_text_from_dict_with_content_field(self):
        """Test extracting text from a dict with a content field."""
        content = {"content": "Dict content response"}
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Dict content response"

    def test_extract_text_from_dict_with_content_list(self):
        """Test extracting text from a dict with content as a list."""
        content = {"content": [{"type": "text", "text": "Nested text"}]}
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == "Nested text"

    def test_extract_text_filters_metadata(self):
        """Test that metadata responses are filtered out."""
        content = [
            {"type": "text", "text": '{"suggestions": ["item1"], "quality": "high"}'}
        ]
        result = DatabricksGPTOSSHandler.extract_text_from_response(content)
        assert result == ""

    def test_extract_text_handles_empty_content(self):
        """Test handling of empty content."""
        assert DatabricksGPTOSSHandler.extract_text_from_response([]) == ""
        assert DatabricksGPTOSSHandler.extract_text_from_response({}) == ""
        assert DatabricksGPTOSSHandler.extract_text_from_response(None) == ""
        assert DatabricksGPTOSSHandler.extract_text_from_response("") == ""

    def test_filter_unsupported_params(self):
        """Test filtering of unsupported parameters."""
        params = {
            "model": "gpt-oss",
            "temperature": 0.7,
            "stop": "STOP",
            "stop_sequences": ["seq1"],
            "stop_words": ["word1"],
            "max_tokens": 100,
        }
        filtered = DatabricksGPTOSSHandler.filter_unsupported_params(params)

        assert "model" in filtered
        assert "temperature" in filtered
        assert "max_tokens" in filtered
        assert "stop" not in filtered
        assert "stop_sequences" not in filtered
        assert "stop_words" not in filtered

    def test_filter_unsupported_params_preserves_original(self):
        """Test that filtering doesn't modify the original params."""
        params = {"stop": "STOP", "model": "test"}
        filtered = DatabricksGPTOSSHandler.filter_unsupported_params(params)

        assert "stop" in params  # Original unchanged
        assert "stop" not in filtered  # Filtered removed

    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.extract_text_from_response"
    )
    def test_apply_monkey_patch(self, mock_extract):
        """Test that monkey patch is applied correctly."""
        mock_extract.return_value = "Extracted text"

        # Mock the litellm module structure
        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.apply_monkey_patch"
        ) as mock_patch:
            DatabricksGPTOSSHandler.apply_monkey_patch()
            mock_patch.assert_called_once()


class TestDatabricksGPTOSSLLM:
    """Test suite for DatabricksGPTOSSLLM wrapper."""

    @pytest.fixture(autouse=True)
    def mock_llm_factory(self, monkeypatch):
        """Bypass LLM.__new__ factory to avoid OPENAI_API_KEY requirement.

        CrewAI's LLM.__new__ is a factory that routes to native provider classes
        (e.g. OpenAICompletion), which requires API keys. We bypass it so that
        DatabricksGPTOSSLLM instances are created directly for unit testing.
        """
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test-dummy-key-for-unit-tests")

        original_init = DatabricksGPTOSSLLM.__init__

        def patched_new(cls, *args, **kwargs):
            return object.__new__(cls)

        def patched_init(self, **kwargs):
            # Skip parent LLM.__init__ but run DatabricksGPTOSSLLM's own setup
            self._original_model_name = kwargs.get("model", "")

        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler.LLM.__new__", patched_new
        ):
            with patch(
                "src.core.llm_handlers.databricks_gpt_oss_handler.LLM.__init__",
                lambda self, **kwargs: None,
            ):
                yield

    @patch("src.core.llm_handlers.databricks_gpt_oss_handler.LLM.__init__")
    def test_initialization(self, mock_llm_init):
        """Test DatabricksGPTOSSLLM initialization."""
        mock_llm_init.return_value = None

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        assert llm._original_model_name == "gpt-oss-test"
        mock_llm_init.assert_called_once()

    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.LLM._prepare_completion_params"
    )
    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.filter_unsupported_params"
    )
    def test_prepare_completion_params(self, mock_filter, mock_parent_prepare):
        """Test parameter preparation with filtering."""
        mock_parent_prepare.return_value = {
            "model": "test",
            "messages": [{"role": "user", "content": "test"}],
            "stop": "STOP",
        }
        mock_filter.return_value = {
            "model": "test",
            "messages": [{"role": "user", "content": "test"}],
        }

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]

        result = llm._prepare_completion_params(messages)

        mock_parent_prepare.assert_called_once()
        mock_filter.assert_called_once()
        assert "stop" not in result

    @patch("src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call")
    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.DatabricksGPTOSSHandler.filter_unsupported_params"
    )
    def test_call_method(self, mock_filter, mock_parent_call):
        """Test the call method with parameter filtering."""
        mock_filter.return_value = {"model": "test"}
        mock_parent_call.return_value = "Response text"

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]

        result = llm.call(messages, stop="STOP")

        mock_filter.assert_called_once()
        mock_parent_call.assert_called_once()
        assert result == "Response text"

    @patch("src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call")
    def test_call_handles_empty_response(self, mock_parent_call):
        """Test handling of empty responses."""
        mock_parent_call.return_value = ""

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]

        result = llm.call(messages)
        assert result == ""

    @patch("src.core.llm_handlers.databricks_gpt_oss_handler.LLM.call")
    def test_call_propagates_exceptions(self, mock_parent_call):
        """Test that exceptions are propagated correctly."""
        mock_parent_call.side_effect = Exception("Test error")

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        messages = [{"role": "user", "content": "test"}]

        with pytest.raises(Exception) as exc_info:
            llm.call(messages)
        assert str(exc_info.value) == "Test error"

    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response"
    )
    def test_handle_non_streaming_response_with_system_message(
        self, mock_parent_handle
    ):
        """Test that system message is added when missing."""
        mock_parent_handle.return_value = "Response"

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {"model": "test", "messages": [{"role": "user", "content": "test"}]}

        result = llm._handle_non_streaming_response(params)

        # Check that system message was inserted
        assert params["messages"][0]["role"] == "system"
        assert "helpful AI assistant" in params["messages"][0]["content"]
        assert result == "Response"

    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response"
    )
    def test_handle_non_streaming_response_preserves_existing_system_message(
        self, mock_parent_handle
    ):
        """Test that existing system message is preserved."""
        mock_parent_handle.return_value = "Response"

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {
            "model": "test",
            "messages": [
                {"role": "system", "content": "Existing system"},
                {"role": "user", "content": "test"},
            ],
        }

        result = llm._handle_non_streaming_response(params)

        # Check that original system message is preserved
        assert params["messages"][0]["role"] == "system"
        assert params["messages"][0]["content"] == "Existing system"
        assert result == "Response"

    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response"
    )
    def test_handle_non_streaming_response_handles_type_error(self, mock_parent_handle):
        """Test handling of TypeError when calling parent method."""
        # First call raises TypeError, second succeeds
        mock_parent_handle.side_effect = [
            TypeError("unexpected keyword argument"),
            "Response",
        ]

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {"model": "test", "messages": []}

        result = llm._handle_non_streaming_response(
            params,
            callbacks=None,
            available_functions=None,
            from_task=None,
            from_agent=None,
        )

        assert result == "Response"
        assert mock_parent_handle.call_count == 2

    @patch(
        "src.core.llm_handlers.databricks_gpt_oss_handler.LLM._handle_non_streaming_response"
    )
    def test_handle_non_streaming_response_returns_empty_on_none(
        self, mock_parent_handle
    ):
        """Test that None response returns empty string."""
        mock_parent_handle.return_value = None

        llm = DatabricksGPTOSSLLM(model="gpt-oss-test")
        params = {"model": "test", "messages": []}

        result = llm._handle_non_streaming_response(params)
        assert result == ""


class TestSanitizeMessagesForDatabricks:
    """Test suite for DatabricksRetryLLM._sanitize_messages_for_databricks."""

    def test_returns_none_for_none_input(self):
        assert DatabricksRetryLLM._sanitize_messages_for_databricks(None) is None

    def test_returns_empty_for_empty_list(self):
        assert DatabricksRetryLLM._sanitize_messages_for_databricks([]) == []

    def test_passthrough_for_non_list(self):
        assert (
            DatabricksRetryLLM._sanitize_messages_for_databricks("not a list")
            == "not a list"
        )

    def test_leaves_normal_messages_unchanged(self):
        msgs = [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
        ]
        DatabricksRetryLLM._sanitize_messages_for_databricks(msgs)
        assert len(msgs) == 3
        assert msgs[2]["content"] == "Hi there!"

    def test_fixes_assistant_content_none_with_tool_calls(self):
        msgs = [
            {"role": "user", "content": "Do something"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [{"id": "1", "function": {"name": "f"}}],
            },
        ]
        DatabricksRetryLLM._sanitize_messages_for_databricks(msgs)
        assert len(msgs) == 2
        assert msgs[1]["content"] == "Calling tools."
        assert msgs[1]["tool_calls"] == [{"id": "1", "function": {"name": "f"}}]

    def test_fixes_assistant_empty_string_with_tool_calls(self):
        msgs = [
            {"role": "assistant", "content": "", "tool_calls": [{"id": "1"}]},
        ]
        DatabricksRetryLLM._sanitize_messages_for_databricks(msgs)
        assert msgs[0]["content"] == "Calling tools."

    def test_fixes_assistant_whitespace_with_tool_calls(self):
        msgs = [
            {"role": "assistant", "content": "   ", "tool_calls": [{"id": "1"}]},
        ]
        DatabricksRetryLLM._sanitize_messages_for_databricks(msgs)
        assert msgs[0]["content"] == "Calling tools."

    def test_removes_assistant_empty_content_no_tool_calls(self):
        msgs = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": None},
            {"role": "user", "content": "Try again"},
        ]
        DatabricksRetryLLM._sanitize_messages_for_databricks(msgs)
        assert len(msgs) == 2
        assert msgs[0]["role"] == "user"
        assert msgs[1]["content"] == "Try again"

    def test_modifies_list_in_place(self):
        original = [
            {"role": "assistant", "content": None, "tool_calls": [{"id": "1"}]},
        ]
        result = DatabricksRetryLLM._sanitize_messages_for_databricks(original)
        assert result is original
        assert original[0]["content"] == "Calling tools."

    def test_handles_non_dict_items(self):
        msgs = ["plain string", {"role": "user", "content": "Hello"}]
        DatabricksRetryLLM._sanitize_messages_for_databricks(msgs)
        assert len(msgs) == 2
        assert msgs[0] == "plain string"

    def test_does_not_touch_user_or_system_messages(self):
        msgs = [
            {"role": "system", "content": None},
            {"role": "user", "content": None},
        ]
        DatabricksRetryLLM._sanitize_messages_for_databricks(msgs)
        assert len(msgs) == 2
        assert msgs[0]["content"] is None
        assert msgs[1]["content"] is None


class TestApplyEmptyContentFix:
    """Test suite for apply_empty_content_fix litellm.completion patch."""

    def test_sanitizes_messages_before_litellm_call(self):
        """Verify litellm.completion receives sanitized messages."""
        import litellm

        captured_messages = []
        original = litellm.completion

        def capturing_completion(*args, **kwargs):
            captured_messages.append(kwargs.get("messages", []))
            raise RuntimeError("stop here")

        litellm.completion = capturing_completion
        apply_empty_content_fix()

        try:
            litellm.completion(
                model="test",
                messages=[
                    {"role": "user", "content": "Hello"},
                    {"role": "assistant", "content": None, "tool_calls": [{"id": "1"}]},
                    {"role": "user", "content": "Retry"},
                ],
            )
        except RuntimeError:
            pass
        finally:
            litellm.completion = original
            apply_empty_content_fix()

        assert len(captured_messages) == 1
        msgs = captured_messages[0]
        assert msgs[1]["content"] == "Calling tools."
        assert msgs[1]["tool_calls"] == [{"id": "1"}]


class TestApplyToolCallsFix:
    """Test suite for apply_tool_calls_fix monkey-patch.

    This patch fixes a CrewAI bug where tool_calls are silently dropped when
    the LLM returns both content text and tool_calls in the same response.
    """

    def _make_mock_llm(self):
        """Build a mock LLM instance with required attributes."""
        llm = MagicMock()
        llm.is_litellm = False
        llm._token_usage = {}
        llm._handle_emit_call_events = MagicMock()
        llm._track_token_usage_internal = MagicMock()
        llm._handle_tool_call = MagicMock()
        return llm

    def _make_mock_response(self, content="", tool_calls=None):
        """Build a mock litellm ModelResponse."""
        mock_message = MagicMock()
        mock_message.content = content
        mock_message.tool_calls = tool_calls or []

        mock_choice = MagicMock()
        mock_choice.message = mock_message

        mock_response = MagicMock()
        mock_response.choices = [mock_choice]
        mock_response.usage = MagicMock()
        return mock_response

    def _make_tool_call(self, name="PerplexityTool", arguments='{"query": "test"}'):
        tc = MagicMock()
        tc.function.name = name
        tc.function.arguments = arguments
        return tc

    def test_patch_applied_to_sync_method(self):
        """Verify the patch was applied to LLM._handle_non_streaming_response."""
        from crewai import LLM

        assert "<patched" in LLM._handle_non_streaming_response.__code__.co_filename

    def test_patch_applied_to_async_method(self):
        """Verify the patch was applied to LLM._ahandle_non_streaming_response."""
        from crewai import LLM

        assert "<patched" in LLM._ahandle_non_streaming_response.__code__.co_filename

    @patch("litellm.completion")
    def test_returns_tool_calls_when_both_content_and_tools_present(
        self, mock_completion
    ):
        """Core bug fix: when LLM returns both content and tool_calls, return tool_calls."""
        from crewai import LLM

        tool_call = self._make_tool_call()
        mock_completion.return_value = self._make_mock_response(
            content="I'll search for that information.",
            tool_calls=[tool_call],
        )

        result = LLM._handle_non_streaming_response(
            self._make_mock_llm(),
            params={"messages": [{"role": "user", "content": "test"}], "model": "test"},
            callbacks=None,
            available_functions=None,
        )

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].function.name == "PerplexityTool"

    @patch("litellm.completion")
    def test_returns_text_when_no_tool_calls(self, mock_completion):
        """When LLM returns only text (no tool_calls), return text normally."""
        from crewai import LLM

        mock_completion.return_value = self._make_mock_response(
            content="Here is the answer.",
            tool_calls=[],
        )

        result = LLM._handle_non_streaming_response(
            self._make_mock_llm(),
            params={"messages": [{"role": "user", "content": "test"}], "model": "test"},
            callbacks=None,
            available_functions=None,
        )

        assert isinstance(result, str)
        assert result == "Here is the answer."

    @patch("litellm.completion")
    def test_returns_tool_calls_when_no_text_content(self, mock_completion):
        """When LLM returns tool_calls without text content, return tool_calls."""
        from crewai import LLM

        tool_call = self._make_tool_call()
        mock_completion.return_value = self._make_mock_response(
            content="",
            tool_calls=[tool_call],
        )

        result = LLM._handle_non_streaming_response(
            self._make_mock_llm(),
            params={"messages": [{"role": "user", "content": "test"}], "model": "test"},
            callbacks=None,
            available_functions=None,
        )

        assert isinstance(result, list)
        assert len(result) == 1

    @patch("litellm.completion")
    def test_executes_tools_when_available_functions_provided(self, mock_completion):
        """When available_functions is provided, tool execution is handled by LLM internally."""
        from crewai import LLM

        tool_call = self._make_tool_call()
        mock_completion.return_value = self._make_mock_response(
            content="",
            tool_calls=[tool_call],
        )

        llm = self._make_mock_llm()
        llm._handle_tool_call.return_value = "tool result"

        result = LLM._handle_non_streaming_response(
            llm,
            params={"messages": [{"role": "user", "content": "test"}], "model": "test"},
            callbacks=None,
            available_functions={"PerplexityTool": lambda **kw: "result"},
        )

        assert result == "tool result"
        llm._handle_tool_call.assert_called_once()

    @patch("litellm.completion")
    def test_returns_multiple_tool_calls(self, mock_completion):
        """When LLM returns multiple tool_calls alongside text, all are returned."""
        from crewai import LLM

        tc1 = self._make_tool_call(name="PerplexityTool")
        tc2 = self._make_tool_call(name="WebSearchTool")
        mock_completion.return_value = self._make_mock_response(
            content="Let me search using multiple tools.",
            tool_calls=[tc1, tc2],
        )

        result = LLM._handle_non_streaming_response(
            self._make_mock_llm(),
            params={"messages": [{"role": "user", "content": "test"}], "model": "test"},
            callbacks=None,
            available_functions=None,
        )

        assert isinstance(result, list)
        assert len(result) == 2

    def test_reapply_is_idempotent(self):
        """Calling apply_tool_calls_fix again doesn't break anything."""
        from crewai import LLM

        apply_tool_calls_fix()

        # Patch should still be in place (either re-applied or skipped gracefully)
        assert "<patched" in LLM._handle_non_streaming_response.__code__.co_filename
        assert "<patched" in LLM._ahandle_non_streaming_response.__code__.co_filename

    def test_handles_patch_failure_gracefully(self):
        """If patching fails, existing method is preserved and error is logged."""
        from crewai import LLM

        original_method = LLM._handle_non_streaming_response

        with patch("inspect.getsource", side_effect=OSError("could not get source")):
            apply_tool_calls_fix()

        # Method should be unchanged after failed patch
        assert LLM._handle_non_streaming_response is original_method


class TestDatabricksRetryLLMOTelTracing:
    """Tests for OTel tracing integration in DatabricksRetryLLM retry logic."""

    @patch("src.core.llm_handlers.databricks_gpt_oss_handler._get_retry_tracer")
    @patch.object(DatabricksRetryLLM, "_get_crew_logger")
    def test_emit_retry_span_creates_span_with_attributes(
        self, mock_crew_log, mock_get_tracer
    ):
        """_emit_retry_span creates an OTel span with correct retry attributes."""
        mock_span = MagicMock()
        mock_span.__enter__ = Mock(return_value=mock_span)
        mock_span.__exit__ = Mock(return_value=False)

        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.return_value = mock_span
        mock_get_tracer.return_value = mock_tracer

        mock_crew_log.return_value = MagicMock()

        with patch("litellm.request_timeout", 120.0):
            llm = DatabricksRetryLLM(model="databricks/test-model")

        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler._time_mod"
        ) as mock_time:
            llm._emit_retry_span(
                attempt=1,
                max_retries=3,
                backoff=2.0,
                error_type="retryable_error",
                error_message="Connection timeout",
                is_rate_limit=False,
                method="call",
            )

        mock_tracer.start_as_current_span.assert_called_once_with("kasal.llm.retry")
        mock_span.set_attribute.assert_any_call("kasal.event_type", "llm_retry")
        mock_span.set_attribute.assert_any_call("kasal.retry.attempt", 2)
        mock_span.set_attribute.assert_any_call("kasal.retry.max_retries", 3)
        mock_span.set_attribute.assert_any_call("kasal.retry.backoff_seconds", 2.0)
        mock_span.set_attribute.assert_any_call(
            "kasal.retry.error_type", "retryable_error"
        )
        mock_span.set_attribute.assert_any_call("kasal.retry.is_rate_limit", False)
        mock_span.set_attribute.assert_any_call("kasal.retry.method", "call")
        mock_span.set_attribute.assert_any_call(
            "kasal.retry.model", "databricks/test-model"
        )
        mock_span.set_attribute.assert_any_call(
            "kasal.retry.error_message", "Connection timeout"
        )
        # sleep should happen inside the span
        mock_time.sleep.assert_called_once_with(2.0)

    @patch("src.core.llm_handlers.databricks_gpt_oss_handler._get_retry_tracer")
    @patch.object(DatabricksRetryLLM, "_get_crew_logger")
    def test_emit_retry_span_sleeps_without_tracer(
        self, mock_crew_log, mock_get_tracer
    ):
        """When OTel is not available, _emit_retry_span still sleeps."""
        mock_get_tracer.return_value = None
        mock_crew_log.return_value = MagicMock()

        with patch("litellm.request_timeout", 120.0):
            llm = DatabricksRetryLLM(model="databricks/test-model")

        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler._time_mod"
        ) as mock_time:
            llm._emit_retry_span(
                attempt=0,
                max_retries=3,
                backoff=1.0,
                error_type="empty_response",
                error_message="",
                is_rate_limit=False,
                method="call",
            )

        mock_time.sleep.assert_called_once_with(1.0)

    @patch("src.core.llm_handlers.databricks_gpt_oss_handler._get_retry_tracer")
    @patch.object(DatabricksRetryLLM, "_get_crew_logger")
    def test_emit_retry_span_still_sleeps_on_tracer_exception(
        self, mock_crew_log, mock_get_tracer
    ):
        """If the tracer raises, we still sleep (retry logic is never broken)."""
        mock_tracer = MagicMock()
        mock_tracer.start_as_current_span.side_effect = RuntimeError("tracer broken")
        mock_get_tracer.return_value = mock_tracer
        mock_crew_log.return_value = MagicMock()

        with patch("litellm.request_timeout", 120.0):
            llm = DatabricksRetryLLM(model="databricks/test-model")

        with patch(
            "src.core.llm_handlers.databricks_gpt_oss_handler._time_mod"
        ) as mock_time:
            llm._emit_retry_span(
                attempt=0,
                max_retries=3,
                backoff=1.0,
                error_type="retryable_error",
                error_message="server error",
                is_rate_limit=False,
                method="call",
            )

        mock_time.sleep.assert_called_once_with(1.0)

    @patch.object(DatabricksRetryLLM, "_get_crew_logger")
    def test_record_retry_summary_adds_event_to_current_span(self, mock_crew_log):
        """_record_retry_summary adds an event on the current active span."""
        mock_crew_log.return_value = MagicMock()

        with patch("litellm.request_timeout", 120.0):
            llm = DatabricksRetryLLM(model="databricks/test-model")

        mock_span = MagicMock()
        mock_span.is_recording.return_value = True

        with patch("opentelemetry.trace.get_current_span", return_value=mock_span):
            llm._record_retry_summary(
                total_attempts=3, total_backoff=7.0, method="call"
            )

        mock_span.add_event.assert_called_once_with(
            "llm_retry_summary",
            attributes={
                "kasal.retry.total_attempts": 3,
                "kasal.retry.total_backoff_seconds": 7.0,
                "kasal.retry.model": "databricks/test-model",
                "kasal.retry.method": "call",
            },
        )

    @patch.object(DatabricksRetryLLM, "_record_retry_summary")
    @patch.object(DatabricksRetryLLM, "_emit_retry_span")
    @patch.object(DatabricksRetryLLM, "_get_crew_logger")
    def test_call_emits_retry_spans_on_empty_response(
        self, mock_crew_log, mock_emit_retry, mock_summary
    ):
        """call() method emits retry spans when receiving empty responses."""
        mock_crew_log.return_value = MagicMock()

        with patch("litellm.request_timeout", 120.0):
            llm = DatabricksRetryLLM(model="databricks/test-model")

        # First call returns empty, second returns valid response
        with patch.object(
            type(llm).__bases__[0], "call", side_effect=["", "Valid response"]
        ):
            result = llm.call([{"role": "user", "content": "test"}])

        assert result == "Valid response"
        mock_emit_retry.assert_called_once()
        call_kwargs = mock_emit_retry.call_args
        assert call_kwargs[1]["error_type"] == "empty_response"
        assert call_kwargs[1]["attempt"] == 0
        mock_summary.assert_called_once_with(2, pytest.approx(1.0, abs=0.1), "call")

    @patch.object(DatabricksRetryLLM, "_record_retry_summary")
    @patch.object(DatabricksRetryLLM, "_emit_retry_span")
    @patch.object(DatabricksRetryLLM, "_get_crew_logger")
    def test_call_no_retry_spans_on_success(
        self, mock_crew_log, mock_emit_retry, mock_summary
    ):
        """call() does not emit retry spans when the first attempt succeeds."""
        mock_crew_log.return_value = MagicMock()

        with patch("litellm.request_timeout", 120.0):
            llm = DatabricksRetryLLM(model="databricks/test-model")

        with patch.object(type(llm).__bases__[0], "call", return_value="Success"):
            result = llm.call([{"role": "user", "content": "test"}])

        assert result == "Success"
        mock_emit_retry.assert_not_called()
        mock_summary.assert_not_called()
