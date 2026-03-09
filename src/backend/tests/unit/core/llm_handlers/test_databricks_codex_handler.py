"""
Unit tests for DatabricksCodexCompletion handler.

Tests the specialized handling for gpt-5.3-codex on Databricks, which uses
the Responses API with phase preservation, stop-word suppression, and
diagnostic logging.
"""

import json
import logging
import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock


# ---------------------------------------------------------------------------
# Mocks for crewai internals that may not be installed in test environments
# ---------------------------------------------------------------------------

# Mock OpenAICompletion before importing the handler so we control the base
_mock_openai_completion_module = MagicMock()


class _FakeOpenAICompletion:
    """Minimal stand-in for crewai.llms.providers.openai.completion.OpenAICompletion."""

    def __init__(self, **kwargs):
        self.model = kwargs.get("model", "test-model")
        self.api_key = kwargs.get("api_key")
        self.base_url = kwargs.get("base_url")
        self.timeout = kwargs.get("timeout", 120)
        self.max_tokens = kwargs.get("max_tokens")
        self.auto_chain = False
        self.auto_chain_reasoning = False
        self.parse_tool_outputs = False
        self.client = MagicMock()
        self._last_response_id = None
        self._last_reasoning_items = []

    # Methods that the subclass may call via super()
    def _prepare_responses_params(self, messages, tools=None, response_model=None):
        params = {
            "model": self.model,
            "input": list(messages) if messages else [],
        }
        if tools:
            params["tools"] = tools
        return params

    def _extract_function_calls_from_response(self, response):
        calls = []
        if hasattr(response, "output") and response.output:
            for item in response.output:
                if getattr(item, "type", None) == "function_call":
                    calls.append({
                        "id": getattr(item, "id", ""),
                        "name": getattr(item, "name", ""),
                        "arguments": getattr(item, "arguments", "{}"),
                    })
        return calls

    def _extract_responses_token_usage(self, response):
        return {"prompt_tokens": 10, "completion_tokens": 20}

    def _track_token_usage_internal(self, usage):
        pass

    def _extract_reasoning_items(self, response):
        return []

    def _extract_builtin_tool_outputs(self, response):
        result = MagicMock()
        result.text = getattr(response, "output_text", "")
        return result

    def _apply_stop_words(self, text):
        return text

    def _validate_structured_output(self, content, response_model):
        return content

    def _handle_tool_execution(self, function_name, function_args, available_functions, from_task, from_agent):
        if function_name in (available_functions or {}):
            return f"result_{function_name}"
        return None

    def _emit_call_completed_event(self, **kwargs):
        pass

    def _emit_call_failed_event(self, **kwargs):
        pass

    def supports_function_calling(self):
        return True

    def supports_stop_words(self):
        return True


# Patch the imports before loading the handler module.
# We must use importlib to load the single file directly, avoiding __init__.py
# which would trigger heavy dependency chains (litellm, openai, etc.)
import sys
import importlib
import importlib.util

# ---------------------------------------------------------------------------
# Isolated module loading: force-install our mocks so the handler file sees
# _FakeOpenAICompletion as its base class (even if the real crewai module is
# already imported by other tests), then RESTORE sys.modules so we don't
# pollute other test files.
# ---------------------------------------------------------------------------
_MOCK_MODULES = {
    "crewai.llms.providers.openai.completion": MagicMock(
        OpenAICompletion=_FakeOpenAICompletion,
    ),
    "crewai.events.types.llm_events": MagicMock(
        LLMCallType=MagicMock(LLM_CALL="LLM_CALL", TOOL_CALL="TOOL_CALL"),
    ),
}
_HANDLER_MODULE_KEY = "src.core.llm_handlers.databricks_codex_handler"

# Save originals so we can restore them after loading
_saved_modules = {}
for _key in list(_MOCK_MODULES) + [_HANDLER_MODULE_KEY]:
    if _key in sys.modules:
        _saved_modules[_key] = sys.modules[_key]

# Force-install mocks (override real modules temporarily)
for _key, _mock_mod in _MOCK_MODULES.items():
    sys.modules[_key] = _mock_mod

# Load the module directly from its file path to bypass __init__.py
_handler_path = str(
    __import__("pathlib").Path(__file__).resolve().parents[4]
    / "src"
    / "core"
    / "llm_handlers"
    / "databricks_codex_handler.py"
)
_spec = importlib.util.spec_from_file_location(_HANDLER_MODULE_KEY, _handler_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_HANDLER_MODULE_KEY] = _mod
_spec.loader.exec_module(_mod)

# Extract the class we need (survives module cleanup because held by reference)
DatabricksCodexCompletion = _mod.DatabricksCodexCompletion

# Restore sys.modules — put back originals or remove entries we added
for _key in list(_MOCK_MODULES) + [_HANDLER_MODULE_KEY]:
    if _key in _saved_modules:
        sys.modules[_key] = _saved_modules[_key]
    else:
        sys.modules.pop(_key, None)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def handler():
    """Create a DatabricksCodexCompletion instance for testing."""
    return DatabricksCodexCompletion(
        model="databricks-gpt-5-3-codex",
        api_key="test-key",
        base_url="https://example.com/serving-endpoints",
        timeout=300,
        max_tokens=128000,
    )


@pytest.fixture
def make_response():
    """Factory for creating mock Responses API responses."""

    def _make(output_items=None, output_text="", status="completed", response_id="resp-123"):
        resp = MagicMock()
        resp.id = response_id
        resp.status = status
        resp.output_text = output_text

        if output_items is None:
            resp.output = []
        else:
            items = []
            for item_dict in output_items:
                item = MagicMock()
                item.type = item_dict.get("type", "message")
                item.phase = item_dict.get("phase")
                item.id = item_dict.get("id", "item-1")
                item.name = item_dict.get("name", "")
                item.arguments = item_dict.get("arguments", "{}")
                item.role = item_dict.get("role", "assistant")
                item.content = item_dict.get("content", [])

                # model_dump for serialization
                item.model_dump.return_value = dict(item_dict)

                # Make getattr work for phase
                for k, v in item_dict.items():
                    setattr(item, k, v)

                items.append(item)
            resp.output = items

        return resp

    return _make


# ---------------------------------------------------------------------------
# TestInit
# ---------------------------------------------------------------------------

class TestInit:
    """Test __init__ configuration."""

    def test_default_api_responses(self):
        """__init__ should default api to 'responses'."""
        handler = DatabricksCodexCompletion(model="test")
        # The api kwarg is set via setdefault — verify internal state
        assert handler._last_output_items == []
        assert handler._tool_call_count == 0
        assert handler._min_required_tool_calls is None

    def test_preserves_explicit_kwargs(self):
        """Explicit kwargs should be preserved."""
        handler = DatabricksCodexCompletion(
            model="my-model",
            api_key="key-123",
            base_url="https://example.com",
            timeout=999,
        )
        assert handler.model == "my-model"
        assert handler.api_key == "key-123"
        assert handler.timeout == 999

    def test_initial_state(self, handler):
        """Handler should start with empty output items and no tool calls."""
        assert handler._last_output_items == []
        assert handler._tool_call_count == 0
        assert handler._min_required_tool_calls is None


# ---------------------------------------------------------------------------
# TestCapabilityOverrides
# ---------------------------------------------------------------------------

class TestCapabilityOverrides:
    """Test capability method overrides."""

    def test_supports_function_calling(self, handler):
        """gpt-5.3-codex supports native function calling."""
        assert handler.supports_function_calling() is True

    def test_supports_stop_words(self, handler):
        """GPT-5 reasoning models reject stop — should return False."""
        assert handler.supports_stop_words() is False


# ---------------------------------------------------------------------------
# TestPrepareResponsesParams
# ---------------------------------------------------------------------------

class TestPrepareResponsesParams:
    """Test _prepare_responses_params method."""

    def test_basic_params(self, handler):
        """Should build basic Responses API params from messages."""
        messages = [{"role": "user", "content": "Hello"}]
        params = handler._prepare_responses_params(messages)
        assert params["model"] == "databricks-gpt-5-3-codex"
        assert len(params["input"]) >= 1

    def test_sanitizes_role_tool_to_function_call_output(self, handler):
        """role:tool messages should be converted to function_call_output items."""
        messages = [
            {"role": "tool", "tool_call_id": "call-1", "content": "tool result here"},
        ]
        params = handler._prepare_responses_params(messages)
        # Find the converted item
        fco_items = [it for it in params["input"] if isinstance(it, dict) and it.get("type") == "function_call_output"]
        assert len(fco_items) == 1
        assert fco_items[0]["call_id"] == "call-1"
        assert fco_items[0]["output"] == "tool result here"

    def test_sanitizes_assistant_tool_calls_to_function_call(self, handler):
        """assistant messages with tool_calls should become function_call items."""
        messages = [
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": "tc-1", "function": {"name": "my_tool", "arguments": '{"a": 1}'}},
                ],
            },
        ]
        params = handler._prepare_responses_params(messages)
        fc_items = [it for it in params["input"] if isinstance(it, dict) and it.get("type") == "function_call"]
        assert len(fc_items) == 1
        assert fc_items[0]["name"] == "my_tool"
        assert fc_items[0]["call_id"] == "tc-1"

    def test_null_content_replaced_with_empty_string(self, handler):
        """Messages with content: null should get content: '' instead."""
        messages = [{"role": "user", "content": None}]
        params = handler._prepare_responses_params(messages)
        user_items = [it for it in params["input"] if isinstance(it, dict) and it.get("role") == "user"]
        for item in user_items:
            assert item.get("content") is not None

    def test_truncates_long_ids(self, handler):
        """IDs longer than 64 chars should be truncated."""
        long_id = "x" * 100
        messages = [{"role": "user", "content": "test", "id": long_id}]
        params = handler._prepare_responses_params(messages)
        for item in params["input"]:
            if isinstance(item, dict) and "id" in item:
                assert len(item["id"]) <= 64

    def test_tool_choice_required_when_below_min(self, handler):
        """When tool_call_count < min, tool_choice should be 'required'."""
        handler._tool_call_count = 0
        handler._min_required_tool_calls = None  # will be computed
        messages = [{"role": "user", "content": "Do something"}]
        tools = [{"name": "my_tool", "type": "function"}]
        params = handler._prepare_responses_params(messages, tools=tools)
        assert params["tool_choice"] == "required"

    def test_tool_choice_auto_after_min_reached(self, handler):
        """After min tool calls reached, tool_choice should be 'auto'."""
        handler._tool_call_count = 10
        handler._min_required_tool_calls = 2
        messages = [{"role": "user", "content": "Do something"}]
        tools = [{"name": "my_tool", "type": "function"}]
        params = handler._prepare_responses_params(messages, tools=tools)
        assert params["tool_choice"] == "auto"

    def test_no_tool_choice_without_tools(self, handler):
        """Without tools, tool_choice should not be set."""
        messages = [{"role": "user", "content": "Hello"}]
        params = handler._prepare_responses_params(messages)
        assert "tool_choice" not in params

    def test_non_dict_items_passthrough(self, handler):
        """Non-dict items in the input should pass through unchanged."""
        messages = ["plain string item", {"role": "user", "content": "test"}]
        params = handler._prepare_responses_params(messages)
        assert "plain string item" in params["input"]

    def test_tool_role_with_none_content(self, handler):
        """role:tool with content=None should output empty string."""
        messages = [{"role": "tool", "tool_call_id": "call-1", "content": None}]
        params = handler._prepare_responses_params(messages)
        fco_items = [it for it in params["input"] if isinstance(it, dict) and it.get("type") == "function_call_output"]
        assert fco_items[0]["output"] == ""


# ---------------------------------------------------------------------------
# TestCaptureOutputItems
# ---------------------------------------------------------------------------

class TestCaptureOutputItems:
    """Test _capture_output_items method."""

    def test_capture_with_model_dump(self, handler, make_response):
        """Should capture items using model_dump."""
        response = make_response([
            {"type": "message", "role": "assistant", "content": [], "phase": "commentary"},
        ])
        handler._capture_output_items(response)
        assert len(handler._last_output_items) == 1
        assert handler._last_output_items[0].get("phase") == "commentary"

    def test_capture_empty_output(self, handler, make_response):
        """Should handle response with no output items."""
        response = make_response([])
        handler._capture_output_items(response)
        assert handler._last_output_items == []

    def test_capture_no_output_attribute(self, handler):
        """Should handle response without output attribute."""
        response = MagicMock(spec=[])  # No output attribute
        handler._capture_output_items(response)
        assert handler._last_output_items == []

    def test_capture_none_output(self, handler):
        """Should handle response with output=None."""
        response = MagicMock()
        response.output = None
        handler._capture_output_items(response)
        assert handler._last_output_items == []

    def test_capture_truncates_long_ids(self, handler, make_response):
        """IDs over 64 chars should be truncated."""
        long_id = "a" * 100
        response = make_response([{"type": "message", "id": long_id}])
        handler._capture_output_items(response)
        assert len(handler._last_output_items[0]["id"]) <= 64

    def test_capture_dict_item(self, handler):
        """Should handle items that are already dicts."""
        response = MagicMock()
        response.output = [{"type": "message", "phase": "final_answer"}]
        handler._capture_output_items(response)
        assert len(handler._last_output_items) == 1
        assert handler._last_output_items[0]["phase"] == "final_answer"

    def test_capture_item_with_to_dict(self, handler):
        """Should use to_dict if available and model_dump is not."""
        item = MagicMock(spec=["to_dict"])
        item.to_dict.return_value = {"type": "message", "phase": "commentary"}
        # Remove model_dump
        del item.model_dump

        response = MagicMock()
        response.output = [item]
        handler._capture_output_items(response)
        assert len(handler._last_output_items) == 1

    def test_capture_fallback_for_unknown_item(self, handler):
        """Items without model_dump, to_dict, or dict should use fallback."""

        class OpaqueItem:
            pass

        response = MagicMock()
        response.output = [OpaqueItem()]
        handler._capture_output_items(response)
        # Should not crash; may produce a fallback dict
        assert len(handler._last_output_items) == 1

    def test_capture_item_serialization_error(self, handler):
        """Items that fail serialization should be skipped gracefully."""
        item = MagicMock()
        item.model_dump.side_effect = Exception("serialize error")
        del item.to_dict  # Remove to_dict fallback

        # Also make dict() fail
        type(item).__iter__ = PropertyMock(side_effect=TypeError("not iterable"))

        response = MagicMock()
        response.output = [item]
        # Should not raise
        handler._capture_output_items(response)


# ---------------------------------------------------------------------------
# TestHandleResponses
# ---------------------------------------------------------------------------

class TestHandleResponses:
    """Test _handle_responses method."""

    def test_text_response(self, handler, make_response):
        """Should return text content from response."""
        response = make_response(
            output_items=[{"type": "message", "role": "assistant", "phase": "final_answer"}],
            output_text="Hello world!",
        )
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        result = handler._handle_responses(params)
        assert result == "Hello world!"

    def test_function_call_response_without_available_functions(self, handler, make_response):
        """Function calls without available_functions should return wrapped calls."""
        response = make_response(
            output_items=[{
                "type": "function_call",
                "id": "fc-1",
                "name": "search_tool",
                "arguments": '{"query": "test"}',
            }],
            output_text="",
        )
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        result = handler._handle_responses(params)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["function"]["name"] == "search_tool"
        assert handler._tool_call_count == 1

    def test_function_call_with_available_functions(self, handler, make_response):
        """Function calls with available_functions should execute them."""
        response = make_response(
            output_items=[{
                "type": "function_call",
                "id": "fc-1",
                "name": "my_tool",
                "arguments": '{"key": "val"}',
            }],
            output_text="",
        )
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        result = handler._handle_responses(
            params,
            available_functions={"my_tool": lambda **kw: "executed"},
        )
        assert result == "result_my_tool"

    def test_function_call_with_bad_json_arguments(self, handler, make_response):
        """Function calls with invalid JSON arguments should use empty dict."""
        response = make_response(
            output_items=[{
                "type": "function_call",
                "id": "fc-1",
                "name": "my_tool",
                "arguments": "not valid json",
            }],
            output_text="",
        )
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        # Should not crash
        result = handler._handle_responses(
            params,
            available_functions={"my_tool": lambda: "ok"},
        )

    def test_api_error_raises(self, handler):
        """API errors should be re-raised after logging."""
        handler.client.responses.create.side_effect = RuntimeError("API down")

        params = {"model": "test", "input": []}
        with pytest.raises(RuntimeError, match="API down"):
            handler._handle_responses(params)

    def test_empty_output_text(self, handler, make_response):
        """Response with empty output_text should return empty string."""
        response = make_response(output_items=[], output_text="")
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        result = handler._handle_responses(params)
        assert result == ""

    def test_structured_output_with_response_model(self, handler, make_response):
        """Should attempt structured output validation when response_model is given."""
        response = make_response(output_items=[], output_text='{"result": "ok"}')
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        result = handler._handle_responses(params, response_model=MagicMock())
        # _validate_structured_output returns content as-is in our mock
        assert result is not None

    def test_structured_output_validation_failure_falls_through(self, handler, make_response):
        """Failed structured output validation should fall through to text."""
        response = make_response(output_items=[], output_text="plain text")
        handler.client.responses.create.return_value = response

        # Make validation raise ValueError
        handler._validate_structured_output = MagicMock(side_effect=ValueError("bad"))

        params = {"model": "test", "input": []}
        result = handler._handle_responses(params, response_model=MagicMock())
        assert result == "plain text"

    def test_parse_tool_outputs_mode(self, handler, make_response):
        """When parse_tool_outputs is enabled, should return parsed result."""
        handler.parse_tool_outputs = True
        response = make_response(output_items=[], output_text="parsed output")
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        result = handler._handle_responses(params)
        # Returns the mock object from _extract_builtin_tool_outputs
        assert hasattr(result, "text")


# ---------------------------------------------------------------------------
# TestLogRequestParams
# ---------------------------------------------------------------------------

class TestLogRequestParams:
    """Test _log_request_params method."""

    def test_logs_basic_params(self, handler, caplog):
        """Should log model, input count, tool count."""
        params = {
            "model": "test-model",
            "input": [{"role": "user", "content": "hello"}],
            "tools": [{"name": "tool1"}],
            "tool_choice": "auto",
        }
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_request_params(params)

    def test_logs_tool_schemas(self, handler, caplog):
        """Should log tool schemas when tools are present."""
        params = {
            "model": "test-model",
            "input": [],
            "tools": [{"name": "my_tool", "type": "function", "parameters": {"a": "string"}}],
        }
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_request_params(params)

    def test_logs_without_tools(self, handler, caplog):
        """Should handle params without tools."""
        params = {"model": "test-model", "input": []}
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_request_params(params)

    def test_non_dict_tool_in_tools(self, handler, caplog):
        """Non-dict items in tools should not crash."""
        params = {
            "model": "test-model",
            "input": [],
            "tools": ["not_a_dict"],
        }
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_request_params(params)


# ---------------------------------------------------------------------------
# TestLogResponse
# ---------------------------------------------------------------------------

class TestLogResponse:
    """Test _log_response method."""

    def test_logs_text_response(self, handler, make_response, caplog):
        """Should log response with text content."""
        response = make_response(
            output_items=[{"type": "message", "phase": "final_answer"}],
            output_text="Short answer",
        )
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_response(response)

    def test_logs_function_call_response(self, handler, make_response, caplog):
        """Should log response with function calls."""
        response = make_response(
            output_items=[{"type": "function_call", "name": "search", "arguments": '{}'}],
            output_text="",
        )
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_response(response)

    def test_logs_empty_response(self, handler, make_response, caplog):
        """Should handle empty response gracefully."""
        response = make_response(output_items=[], output_text="")
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_response(response)

    def test_logs_long_text_response(self, handler, make_response, caplog):
        """Text over 500 chars should not be logged."""
        long_text = "x" * 600
        response = make_response(output_items=[], output_text=long_text)
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_response(response)

    def test_logs_response_without_output(self, handler, caplog):
        """Response without output attribute should not crash."""
        response = MagicMock(spec=["status"])
        response.status = "completed"
        with caplog.at_level(logging.INFO, logger="crew"):
            handler._log_response(response)


# ---------------------------------------------------------------------------
# TestMultiTurnPhasePreservation
# ---------------------------------------------------------------------------

class TestMultiTurnPhasePreservation:
    """Integration-style tests verifying phase is preserved across turns."""

    def test_output_items_captured_with_phase(self, handler, make_response):
        """Output items with phase should be captured for next turn."""
        response = make_response([
            {"type": "message", "role": "assistant", "phase": "commentary", "content": [{"type": "text", "text": "thinking..."}]},
            {"type": "message", "role": "assistant", "phase": "final_answer", "content": [{"type": "text", "text": "done"}]},
        ], output_text="done")

        handler._capture_output_items(response)

        assert len(handler._last_output_items) == 2
        phases = [it.get("phase") for it in handler._last_output_items if it.get("phase")]
        assert "commentary" in phases
        assert "final_answer" in phases

    def test_tool_call_count_tracks_state(self, handler, make_response):
        """_tool_call_count should increment when tools are called."""
        assert handler._tool_call_count == 0

        # Simulate a response with function calls (no available_functions)
        response = make_response([
            {"type": "function_call", "id": "fc-1", "name": "search", "arguments": "{}"},
        ], output_text="")
        handler.client.responses.create.return_value = response

        params = {"model": "test", "input": []}
        handler._handle_responses(params)

        assert handler._tool_call_count == 1

    def test_tool_choice_transitions(self, handler):
        """tool_choice should transition from required -> auto after min tool calls."""
        messages = [{"role": "user", "content": "test"}]
        tools = [{"name": "tool1"}]

        # First call: should be "required" (below min)
        handler._tool_call_count = 0
        handler._min_required_tool_calls = None  # will be computed
        params1 = handler._prepare_responses_params(messages, tools=tools)
        assert params1["tool_choice"] == "required"

        # After enough tool calls: should be "auto"
        handler._tool_call_count = handler._min_required_tool_calls
        params2 = handler._prepare_responses_params(messages, tools=tools)
        assert params2["tool_choice"] == "auto"


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Test edge cases and error boundaries."""

    def test_empty_messages(self, handler):
        """Should handle empty messages list."""
        params = handler._prepare_responses_params([])
        assert params["input"] == []

    def test_mixed_message_types(self, handler):
        """Should handle a mix of normal, tool, and assistant messages."""
        messages = [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": None, "tool_calls": [
                {"id": "tc-1", "function": {"name": "get_data", "arguments": "{}"}}
            ]},
            {"role": "tool", "tool_call_id": "tc-1", "content": "data result"},
            {"role": "user", "content": "Thanks"},
        ]
        params = handler._prepare_responses_params(messages)
        # Should have converted tool and assistant tool_calls
        types_in_input = [it.get("type") for it in params["input"] if isinstance(it, dict) and "type" in it]
        assert "function_call" in types_in_input
        assert "function_call_output" in types_in_input

    def test_multiple_tool_calls_in_single_assistant_message(self, handler):
        """Assistant message with multiple tool_calls should produce multiple function_call items."""
        messages = [
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": "tc-1", "function": {"name": "tool_a", "arguments": "{}"}},
                    {"id": "tc-2", "function": {"name": "tool_b", "arguments": '{"x":1}'}},
                ],
            },
        ]
        params = handler._prepare_responses_params(messages)
        fc_items = [it for it in params["input"] if isinstance(it, dict) and it.get("type") == "function_call"]
        assert len(fc_items) == 2
        assert fc_items[0]["name"] == "tool_a"
        assert fc_items[1]["name"] == "tool_b"


# ---------------------------------------------------------------------------
# TestMinRequiredToolCalls
# ---------------------------------------------------------------------------

class TestMinRequiredToolCalls:
    """Test the dynamic min_required_tool_calls computation."""

    def test_min_computed_from_few_tools(self, handler):
        """With few tools, min should be floor of 2."""
        messages = [{"role": "user", "content": "test"}]
        tools = [{"name": f"tool{i}"} for i in range(4)]  # 4 tools -> max(2, min(10, 4//4+1)) = max(2, 2) = 2
        handler._prepare_responses_params(messages, tools=tools)
        assert handler._min_required_tool_calls == 2

    def test_min_computed_from_many_tools(self, handler):
        """With 19 tools, min should be 5."""
        messages = [{"role": "user", "content": "test"}]
        tools = [{"name": f"tool{i}"} for i in range(19)]  # 19//4+1 = 5
        handler._prepare_responses_params(messages, tools=tools)
        assert handler._min_required_tool_calls == 5

    def test_min_capped_at_10(self, handler):
        """With 40+ tools, min should be capped at 10."""
        messages = [{"role": "user", "content": "test"}]
        tools = [{"name": f"tool{i}"} for i in range(40)]  # 40//4+1 = 11 -> min(10,11) = 10
        handler._prepare_responses_params(messages, tools=tools)
        assert handler._min_required_tool_calls == 10

    def test_min_not_recomputed(self, handler):
        """Once computed, min should not change on subsequent calls."""
        messages = [{"role": "user", "content": "test"}]
        tools4 = [{"name": f"tool{i}"} for i in range(4)]
        handler._prepare_responses_params(messages, tools=tools4)
        first_min = handler._min_required_tool_calls

        tools20 = [{"name": f"tool{i}"} for i in range(20)]
        handler._prepare_responses_params(messages, tools=tools20)
        assert handler._min_required_tool_calls == first_min  # Not recomputed

    def test_multiple_function_calls_increment_count(self, handler, make_response):
        """Multiple function calls in one response should increment by count."""
        response = make_response(
            output_items=[
                {"type": "function_call", "id": "fc-1", "name": "tool_a", "arguments": "{}"},
                {"type": "function_call", "id": "fc-2", "name": "tool_b", "arguments": "{}"},
            ],
            output_text="",
        )
        handler.client.responses.create.return_value = response
        params = {"model": "test", "input": []}
        handler._handle_responses(params)
        assert handler._tool_call_count == 2
