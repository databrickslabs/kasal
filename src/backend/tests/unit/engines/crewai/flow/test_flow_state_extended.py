"""
Extended unit tests for FlowStateManager to improve coverage.
"""
import pytest
from unittest.mock import patch, MagicMock

from src.engines.crewai.paths.flow.modules.flow_state import FlowStateManager


class TestFlowStateManagerInitializeState:
    def test_initialize_state_with_inputs(self):
        """initialize_state populates state from inputs."""
        inputs = {"key1": "val1", "key2": 42}
        state = FlowStateManager.initialize_state(inputs)
        assert state == {"key1": "val1", "key2": 42}

    def test_initialize_state_empty_inputs(self):
        """initialize_state returns empty dict when inputs is None."""
        state = FlowStateManager.initialize_state(None)
        assert state == {}

    def test_initialize_state_empty_dict(self):
        """initialize_state returns empty dict when inputs is empty dict."""
        state = FlowStateManager.initialize_state({})
        assert state == {}

    def test_initialize_state_returns_copy_of_inputs(self):
        """initialize_state data is separate from input reference."""
        inputs = {"a": 1}
        state = FlowStateManager.initialize_state(inputs)
        inputs["b"] = 2  # modify original
        assert "b" not in state  # state should not be affected


class TestFlowStateManagerUpdateState:
    def test_update_state_merges_updates(self):
        """update_state merges new keys into current state."""
        current = {"x": 1}
        result = FlowStateManager.update_state(current, {"y": 2})
        assert result["x"] == 1
        assert result["y"] == 2

    def test_update_state_overwrites_existing_key(self):
        """update_state overwrites existing keys."""
        current = {"x": 1}
        result = FlowStateManager.update_state(current, {"x": 99})
        assert result["x"] == 99

    def test_update_state_with_empty_updates(self):
        """update_state does not modify state with empty updates."""
        current = {"x": 1}
        result = FlowStateManager.update_state(current, {})
        assert result == {"x": 1}

    def test_update_state_returns_same_object(self):
        """update_state returns the same state dict object."""
        current = {"x": 1}
        result = FlowStateManager.update_state(current, {"y": 2})
        assert result is current


class TestFlowStateManagerParseCrewOutput:
    def test_parse_valid_json_dict(self):
        """parse_crew_output parses valid JSON dict."""
        output = '{"key": "value", "number": 42}'
        with patch("src.engines.crewai.security.scanner_pipeline.security_scanner"):
            result = FlowStateManager.parse_crew_output(output)
        assert result["key"] == "value"
        assert result["number"] == 42

    def test_parse_non_json_text(self):
        """parse_crew_output returns empty dict for plain text."""
        output = "This is just plain text output"
        with patch("src.engines.crewai.security.scanner_pipeline.security_scanner"):
            result = FlowStateManager.parse_crew_output(output)
        assert result == {}

    def test_parse_json_embedded_in_text(self):
        """parse_crew_output extracts JSON block from surrounding text."""
        output = 'Analysis complete. {"status": "done", "score": 0.9} End of report.'
        with patch("src.engines.crewai.security.scanner_pipeline.security_scanner"):
            result = FlowStateManager.parse_crew_output(output)
        assert result.get("status") == "done"
        assert result.get("score") == pytest.approx(0.9)

    def test_parse_empty_string(self):
        """parse_crew_output returns empty dict for empty string."""
        with patch("src.engines.crewai.security.scanner_pipeline.security_scanner"):
            result = FlowStateManager.parse_crew_output("")
        assert result == {}

    def test_parse_json_array_not_dict(self):
        """parse_crew_output returns empty dict when output is a JSON array (not dict)."""
        output = '[1, 2, 3]'
        with patch("src.engines.crewai.security.scanner_pipeline.security_scanner"):
            result = FlowStateManager.parse_crew_output(output)
        assert result == {}

    def test_security_scan_exception_is_suppressed(self):
        """Security scan errors do not bubble up."""
        output = '{"a": 1}'
        with patch("src.engines.crewai.security.scanner_pipeline.security_scanner") as mock_scanner:
            mock_scanner.scan.side_effect = RuntimeError("scan failed")
            result = FlowStateManager.parse_crew_output(output)
        # Should still parse despite security scan failure
        assert result.get("a") == 1

    def test_parse_multiple_json_blocks(self):
        """parse_crew_output merges multiple JSON blocks."""
        output = 'First: {"a": 1} Second: {"b": 2}'
        with patch("src.engines.crewai.security.scanner_pipeline.security_scanner"):
            result = FlowStateManager.parse_crew_output(output)
        assert result.get("a") == 1
        assert result.get("b") == 2


class TestFlowStateManagerEvaluateCondition:
    def test_returns_true_when_no_condition(self):
        """evaluate_condition returns True when condition is None."""
        result = FlowStateManager.evaluate_condition({}, None)
        assert result is True

    def test_returns_true_when_empty_string_condition(self):
        """evaluate_condition returns True when condition is empty string."""
        result = FlowStateManager.evaluate_condition({}, "")
        assert result is True

    def test_simple_true_condition(self):
        """evaluate_condition evaluates simple True condition."""
        state = {"score": 10}
        result = FlowStateManager.evaluate_condition(state, "state.get('score') > 5")
        assert result is True

    def test_simple_false_condition(self):
        """evaluate_condition evaluates simple False condition."""
        state = {"score": 3}
        result = FlowStateManager.evaluate_condition(state, "state.get('score') > 5")
        assert result is False

    def test_returns_false_on_eval_error(self):
        """evaluate_condition returns False when eval raises an exception."""
        state = {}
        result = FlowStateManager.evaluate_condition(state, "undefined_variable > 5")
        assert result is False

    def test_condition_with_builtin_len(self):
        """evaluate_condition can use len() builtin."""
        state = {"items": [1, 2, 3]}
        result = FlowStateManager.evaluate_condition(state, "len(state.get('items', [])) == 3")
        assert result is True

    def test_condition_with_bool_literals(self):
        """evaluate_condition handles True/False literals."""
        result = FlowStateManager.evaluate_condition({}, "True")
        assert result is True
        result2 = FlowStateManager.evaluate_condition({}, "False")
        assert result2 is False


class TestFlowStateManagerGetSetStateValue:
    def test_get_existing_key(self):
        """get_state_value retrieves existing key."""
        state = {"count": 5}
        result = FlowStateManager.get_state_value(state, "count")
        assert result == 5

    def test_get_missing_key_with_default(self):
        """get_state_value returns default when key missing."""
        state = {}
        result = FlowStateManager.get_state_value(state, "missing", default="fallback")
        assert result == "fallback"

    def test_get_missing_key_no_default(self):
        """get_state_value returns None when key missing and no default given."""
        state = {}
        result = FlowStateManager.get_state_value(state, "missing")
        assert result is None

    def test_set_state_value(self):
        """set_state_value sets value in state."""
        state = {}
        result = FlowStateManager.set_state_value(state, "new_key", "new_value")
        assert result["new_key"] == "new_value"

    def test_set_state_value_overwrites(self):
        """set_state_value overwrites existing key."""
        state = {"key": "old"}
        FlowStateManager.set_state_value(state, "key", "new")
        assert state["key"] == "new"

    def test_set_state_value_returns_same_object(self):
        """set_state_value returns the same state object."""
        state = {}
        returned = FlowStateManager.set_state_value(state, "k", "v")
        assert returned is state


class TestFlowStateManagerMergeState:
    def test_merge_without_prefix(self):
        """merge_state adds all keys without prefix."""
        state = {"existing": 1}
        result = FlowStateManager.merge_state(state, {"new": 2, "another": 3})
        assert result["existing"] == 1
        assert result["new"] == 2
        assert result["another"] == 3

    def test_merge_with_prefix(self):
        """merge_state prefixes all merged keys."""
        state = {}
        result = FlowStateManager.merge_state(state, {"a": 1, "b": 2}, prefix="crew_")
        assert result["crew_a"] == 1
        assert result["crew_b"] == 2
        assert "a" not in result

    def test_merge_empty_dict(self):
        """merge_state does nothing with empty merge_dict."""
        state = {"x": 1}
        result = FlowStateManager.merge_state(state, {})
        assert result == {"x": 1}


class TestFlowStateManagerGetStateSnapshot:
    def test_returns_deep_copy(self):
        """get_state_snapshot returns a deep copy, not the same reference."""
        state = {"nested": {"a": 1}}
        snapshot = FlowStateManager.get_state_snapshot(state)
        assert snapshot == state
        assert snapshot is not state
        snapshot["nested"]["a"] = 99
        assert state["nested"]["a"] == 1  # original not affected


class TestFlowStateManagerLogState:
    def test_log_state_does_not_raise(self):
        """log_state can be called without raising exceptions."""
        state = {"key": "value", "long": "x" * 200}
        # Should not raise
        FlowStateManager.log_state(state, "Test message")

    def test_log_state_handles_empty_state(self):
        """log_state handles empty state gracefully."""
        FlowStateManager.log_state({}, "Empty state")
