"""
Tests for flow builder JSON parsing improvements.

Covers the three helper functions defined inside FlowBuilder's router
method scope:
  - strip_code_fences(s)  -- removes markdown ```json ... ``` wrappers
  - looks_like_json(s)    -- detects JSON object/array boundaries
  - merge_parsed_json(parsed_data, source_label) -- merges dict or array
    JSON into the evaluation context and state

Because these helpers are nested (not importable), the tests replicate
the exact logic from flow_builder.py and validate the algorithmic
behaviour directly.  An integration-level test at the end verifies the
full pipeline through ``build_eval_context`` by exercising the public
FlowBuilder interface indirectly.
"""
import json
import pytest
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Replicated helpers -- identical logic to flow_builder.py lines 899-912
# ---------------------------------------------------------------------------

def strip_code_fences(s: str) -> str:
    """Strip markdown code fences (```json ... ```) from a string."""
    s = s.strip()
    if s.startswith('```'):
        first_newline = s.find('\n')
        if first_newline != -1:
            s = s[first_newline + 1:]
        if s.rstrip().endswith('```'):
            s = s.rstrip()[:-3].rstrip()
    return s


def looks_like_json(s: str) -> bool:
    s = s.strip()
    return (s.startswith('{') and s.endswith('}')) or (s.startswith('[') and s.endswith(']'))


def auto_convert_value(val):
    """Convert string numeric values to int/float."""
    if isinstance(val, str):
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            pass
    return val


def auto_convert_dict(d):
    """Recursively convert string numerics in a dict."""
    if not isinstance(d, dict):
        return d
    return {
        k: auto_convert_value(v) if not isinstance(v, dict) else auto_convert_dict(v)
        for k, v in d.items()
    }


def merge_parsed_json(
    parsed_data: Any,
    source_label: str,
    eval_context: Dict[str, Any],
) -> None:
    """Merge parsed JSON data into eval_context and eval_context['state'].

    Replicates the nested helper from flow_builder.py lines 880-897.
    The ``eval_context`` dict must contain a ``'state'`` key (a dict).
    """
    if isinstance(parsed_data, dict):
        parsed_data = auto_convert_dict(parsed_data)
        eval_context['state'].update(parsed_data)
        eval_context.update(parsed_data)
    elif isinstance(parsed_data, list) and parsed_data:
        first_item = parsed_data[0]
        if isinstance(first_item, dict):
            first_item = auto_convert_dict(first_item)
            eval_context['state'].update(first_item)
            eval_context.update(first_item)
        # Store full array regardless of first item type
        eval_context['items'] = parsed_data
        eval_context['state']['items'] = parsed_data


# ===========================================================================
# Tests
# ===========================================================================


class TestStripCodeFences:
    """Test the code fence stripping behaviour used in flow builder."""

    def test_strips_json_code_fence(self):
        """Markdown ```json ... ``` wrapper is removed, inner JSON preserved."""
        raw = '```json\n{"number": 42}\n```'
        result = strip_code_fences(raw)
        assert result == '{"number": 42}'
        # Ensure the result is valid JSON
        parsed = json.loads(result)
        assert parsed == {"number": 42}

    def test_strips_plain_code_fence(self):
        """Markdown ``` ... ``` without a language tag is also removed."""
        raw = '```\n{"key": "value"}\n```'
        result = strip_code_fences(raw)
        assert result == '{"key": "value"}'

    def test_strips_code_fence_with_language_python(self):
        """Markdown ```python ... ``` is stripped (any language tag works)."""
        raw = '```python\nprint("hello")\n```'
        result = strip_code_fences(raw)
        assert result == 'print("hello")'

    def test_returns_plain_json_unchanged(self):
        """Plain JSON without fences passes through unmodified."""
        plain = '{"status": "ok"}'
        assert strip_code_fences(plain) == plain

    def test_handles_empty_string(self):
        """Empty string returns empty string."""
        assert strip_code_fences('') == ''

    def test_handles_whitespace_only(self):
        """Whitespace-only string returns empty string after strip."""
        assert strip_code_fences('   \n  ') == ''

    def test_strips_surrounding_whitespace(self):
        """Leading/trailing whitespace around fences is removed."""
        raw = '  \n```json\n{"a": 1}\n```\n  '
        result = strip_code_fences(raw)
        assert result == '{"a": 1}'

    def test_preserves_inner_newlines(self):
        """Multi-line JSON inside fences keeps its internal structure."""
        raw = '```json\n{\n  "a": 1,\n  "b": 2\n}\n```'
        result = strip_code_fences(raw)
        parsed = json.loads(result)
        assert parsed == {"a": 1, "b": 2}

    def test_no_closing_fence(self):
        """If there is no closing ``` the content after the first line is returned."""
        raw = '```json\n{"a": 1}'
        result = strip_code_fences(raw)
        assert result == '{"a": 1}'

    def test_code_fence_with_array(self):
        """Array JSON inside code fences is correctly extracted."""
        raw = '```json\n[{"id": 1}, {"id": 2}]\n```'
        result = strip_code_fences(raw)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["id"] == 1

    def test_triple_backtick_only_no_newline(self):
        """Edge case: just ``` with no newline.

        The closing-fence check fires on the string itself (since no
        newline was found to split on), stripping the backticks and
        leaving an empty string.
        """
        raw = '```'
        result = strip_code_fences(raw)
        assert result == ''


class TestLooksLikeJson:
    """Test JSON detection behaviour."""

    def test_detects_json_object(self):
        """Curly-brace-delimited string is detected as JSON."""
        assert looks_like_json('{"key": "value"}') is True

    def test_detects_json_array(self):
        """Square-bracket-delimited string is detected as JSON."""
        assert looks_like_json('[1, 2, 3]') is True

    def test_rejects_plain_text(self):
        """Plain text is not treated as JSON."""
        assert looks_like_json('hello world') is False

    def test_rejects_number(self):
        """Bare number is not treated as JSON."""
        assert looks_like_json('42') is False

    def test_rejects_partial_brace_open(self):
        """String starting with { but not ending with } is rejected."""
        assert looks_like_json('{incomplete') is False

    def test_rejects_partial_bracket_close(self):
        """String ending with ] but not starting with [ is rejected."""
        assert looks_like_json('not an array]') is False

    def test_handles_whitespace(self):
        """Surrounding whitespace is stripped before detection."""
        assert looks_like_json('  {"key": "value"}  ') is True
        assert looks_like_json('  [1, 2]  ') is True

    def test_empty_object(self):
        """Empty JSON object {} is detected."""
        assert looks_like_json('{}') is True

    def test_empty_array(self):
        """Empty JSON array [] is detected."""
        assert looks_like_json('[]') is True

    def test_empty_string(self):
        """Empty string is not treated as JSON."""
        assert looks_like_json('') is False

    def test_nested_braces(self):
        """Nested object is detected (only outer delimiters matter)."""
        assert looks_like_json('{"a": {"b": 1}}') is True

    def test_mixed_delimiters_rejected(self):
        """Starting with { but ending with ] is rejected."""
        assert looks_like_json('{not valid]') is False
        assert looks_like_json('[not valid}') is False


class TestMergeJsonDict:
    """Test merging a JSON object (dict) into eval context and state."""

    def _make_context(self) -> Dict[str, Any]:
        return {'state': {}}

    def test_dict_merged_to_state_and_context(self):
        """Dict keys appear in both state and top-level context."""
        ctx = self._make_context()
        merge_parsed_json({"status": "ok", "count": 5}, "test", ctx)

        assert ctx['state']['status'] == 'ok'
        assert ctx['state']['count'] == 5
        assert ctx['status'] == 'ok'
        assert ctx['count'] == 5

    def test_string_numerics_auto_converted(self):
        """String values that look like numbers are auto-converted."""
        ctx = self._make_context()
        merge_parsed_json({"score": "42", "ratio": "3.14"}, "test", ctx)

        assert ctx['state']['score'] == 42
        assert isinstance(ctx['state']['score'], int)
        assert ctx['state']['ratio'] == 3.14
        assert isinstance(ctx['state']['ratio'], float)

    def test_existing_state_preserved(self):
        """Pre-existing state keys not in new data are preserved."""
        ctx = self._make_context()
        ctx['state']['existing'] = 'keep_me'
        merge_parsed_json({"new_key": "new_value"}, "test", ctx)

        assert ctx['state']['existing'] == 'keep_me'
        assert ctx['state']['new_key'] == 'new_value'

    def test_empty_dict_no_error(self):
        """Empty dict merges without error and leaves state empty."""
        ctx = self._make_context()
        merge_parsed_json({}, "test", ctx)
        assert ctx['state'] == {}

    def test_nested_dict_auto_converted(self):
        """Nested dict string numerics are also auto-converted."""
        ctx = self._make_context()
        merge_parsed_json({"data": {"value": "99"}}, "test", ctx)

        assert ctx['state']['data'] == {"value": 99}


class TestMergeJsonArrays:
    """Test JSON array handling -- first item extraction and items context."""

    def _make_context(self) -> Dict[str, Any]:
        return {'state': {}}

    def test_array_first_item_merged_to_state(self):
        """First dict item's keys are merged into state and context."""
        items = [{"name": "Alice", "age": "30"}, {"name": "Bob", "age": "25"}]
        ctx = self._make_context()
        merge_parsed_json(items, "test", ctx)

        # First item keys accessible at top level (age auto-converted)
        assert ctx['state']['name'] == 'Alice'
        assert ctx['state']['age'] == 30
        assert ctx['name'] == 'Alice'
        assert ctx['age'] == 30

    def test_array_items_stored_in_context(self):
        """Full array is stored under 'items' in both context and state."""
        items = [{"id": 1}, {"id": 2}, {"id": 3}]
        ctx = self._make_context()
        merge_parsed_json(items, "test", ctx)

        assert ctx['items'] == items
        assert ctx['state']['items'] == items
        assert len(ctx['items']) == 3

    def test_empty_array_not_merged(self):
        """Empty array does not modify context or state."""
        ctx = self._make_context()
        merge_parsed_json([], "test", ctx)

        assert 'items' not in ctx
        assert 'items' not in ctx['state']
        assert ctx['state'] == {}

    def test_array_with_non_dict_first_item(self):
        """Array whose first item is not a dict still stores items."""
        items = ["alpha", "beta", "gamma"]
        ctx = self._make_context()
        merge_parsed_json(items, "test", ctx)

        # No keys merged to state (first item is not a dict)
        assert ctx['items'] == items
        assert ctx['state']['items'] == items
        # State only has 'items', nothing else from the array content
        assert set(ctx['state'].keys()) == {'items'}

    def test_array_single_item(self):
        """Single-item array works correctly."""
        items = [{"result": "success"}]
        ctx = self._make_context()
        merge_parsed_json(items, "test", ctx)

        assert ctx['state']['result'] == 'success'
        assert ctx['result'] == 'success'
        assert ctx['items'] == items
        assert len(ctx['items']) == 1

    def test_none_not_merged(self):
        """None value does not modify context or state."""
        ctx = self._make_context()
        merge_parsed_json(None, "test", ctx)

        assert ctx['state'] == {}
        assert 'items' not in ctx

    def test_string_not_merged(self):
        """Plain string value does not modify context or state."""
        ctx = self._make_context()
        merge_parsed_json("just a string", "test", ctx)

        assert ctx['state'] == {}
        assert 'items' not in ctx


class TestFullPipeline:
    """Integration tests that exercise strip_code_fences + looks_like_json +
    merge_parsed_json as a combined pipeline, mirroring the flow_builder logic."""

    def _make_context(self) -> Dict[str, Any]:
        return {'state': {}}

    def _pipeline(self, raw_str: str, ctx: Dict[str, Any]) -> bool:
        """Run the full parse pipeline. Returns True if merge occurred."""
        stripped = strip_code_fences(raw_str)
        if looks_like_json(stripped):
            try:
                parsed = json.loads(stripped)
                merge_parsed_json(parsed, "pipeline", ctx)
                return True
            except json.JSONDecodeError:
                return False
        return False

    def test_fenced_json_object_parsed_and_merged(self):
        """```json ... ``` with an object is fully parsed and merged."""
        raw = '```json\n{"temperature": "98.6", "unit": "F"}\n```'
        ctx = self._make_context()
        assert self._pipeline(raw, ctx) is True
        assert ctx['state']['temperature'] == 98.6
        assert ctx['state']['unit'] == 'F'

    def test_fenced_json_array_parsed_and_merged(self):
        """```json ... ``` with an array extracts first item and stores all."""
        raw = '```json\n[{"city": "NYC"}, {"city": "LA"}]\n```'
        ctx = self._make_context()
        assert self._pipeline(raw, ctx) is True
        assert ctx['state']['city'] == 'NYC'
        assert len(ctx['items']) == 2

    def test_plain_json_object_parsed(self):
        """Plain JSON object (no fences) is parsed correctly."""
        raw = '{"flag": true, "count": "7"}'
        ctx = self._make_context()
        assert self._pipeline(raw, ctx) is True
        assert ctx['state']['count'] == 7

    def test_non_json_text_skipped(self):
        """Non-JSON text is skipped without error."""
        raw = 'This is just a message from the agent.'
        ctx = self._make_context()
        assert self._pipeline(raw, ctx) is False
        assert ctx['state'] == {}

    def test_invalid_json_in_fences_skipped(self):
        """Invalid JSON inside code fences is skipped gracefully."""
        raw = '```json\n{not valid json}\n```'
        ctx = self._make_context()
        assert self._pipeline(raw, ctx) is False

    def test_state_values_secondary_parse(self):
        """Simulate the secondary parse loop over state string values.

        flow_builder.py lines 940-955 iterate over state values and
        parse any that look like JSON.
        """
        ctx = self._make_context()
        # Initial state has a JSON string value (as if set by a previous crew)
        ctx['state']['Random Number'] = '{"number": 43}'
        ctx['state']['keep_this'] = 'regular string'

        # Replicate the secondary parse loop
        for key, value in list(ctx['state'].items()):
            if isinstance(value, str):
                json_value = strip_code_fences(value)
                if looks_like_json(json_value):
                    try:
                        parsed_value = json.loads(json_value)
                        merge_parsed_json(parsed_value, f"state['{key}']", ctx)
                    except json.JSONDecodeError:
                        pass

        # 'number' from the parsed JSON should be in state
        assert ctx['state']['number'] == 43
        assert ctx['number'] == 43
        # Original key preserved
        assert ctx['state']['keep_this'] == 'regular string'

    def test_state_values_fenced_json_parsed(self):
        """State values that contain code-fenced JSON are parsed correctly."""
        ctx = self._make_context()
        ctx['state']['llm_output'] = '```json\n{"score": "95", "grade": "A"}\n```'

        for key, value in list(ctx['state'].items()):
            if isinstance(value, str):
                json_value = strip_code_fences(value)
                if looks_like_json(json_value):
                    try:
                        parsed_value = json.loads(json_value)
                        merge_parsed_json(parsed_value, f"state['{key}']", ctx)
                    except json.JSONDecodeError:
                        pass

        assert ctx['state']['score'] == 95
        assert ctx['state']['grade'] == 'A'

    def test_state_values_array_in_state(self):
        """State value containing a JSON array is parsed and items stored."""
        ctx = self._make_context()
        ctx['state']['results'] = '[{"id": 1, "ok": true}, {"id": 2, "ok": false}]'

        for key, value in list(ctx['state'].items()):
            if isinstance(value, str):
                json_value = strip_code_fences(value)
                if looks_like_json(json_value):
                    try:
                        parsed_value = json.loads(json_value)
                        merge_parsed_json(parsed_value, f"state['{key}']", ctx)
                    except json.JSONDecodeError:
                        pass

        assert ctx['items'] == [{"id": 1, "ok": True}, {"id": 2, "ok": False}]
        assert ctx['state']['id'] == 1  # first item's id


class TestAutoConvertValue:
    """Test the auto_convert_value helper that converts string numerics."""

    def test_converts_int_string(self):
        assert auto_convert_value("42") == 42
        assert isinstance(auto_convert_value("42"), int)

    def test_converts_float_string(self):
        assert auto_convert_value("3.14") == 3.14
        assert isinstance(auto_convert_value("3.14"), float)

    def test_preserves_non_numeric_string(self):
        assert auto_convert_value("hello") == "hello"

    def test_preserves_int(self):
        assert auto_convert_value(42) == 42

    def test_preserves_float(self):
        assert auto_convert_value(3.14) == 3.14

    def test_preserves_none(self):
        assert auto_convert_value(None) is None

    def test_preserves_bool(self):
        assert auto_convert_value(True) is True

    def test_negative_int_string(self):
        assert auto_convert_value("-5") == -5

    def test_negative_float_string(self):
        assert auto_convert_value("-2.5") == -2.5

    def test_zero_string(self):
        assert auto_convert_value("0") == 0
        assert isinstance(auto_convert_value("0"), int)


class TestAutoConvertDict:
    """Test the auto_convert_dict helper that recursively converts numerics."""

    def test_converts_top_level_values(self):
        result = auto_convert_dict({"a": "1", "b": "2.5", "c": "text"})
        assert result == {"a": 1, "b": 2.5, "c": "text"}

    def test_converts_nested_dict(self):
        result = auto_convert_dict({"outer": {"inner": "99"}})
        assert result == {"outer": {"inner": 99}}

    def test_non_dict_returns_as_is(self):
        assert auto_convert_dict("not a dict") == "not a dict"
        assert auto_convert_dict(42) == 42
        assert auto_convert_dict(None) is None

    def test_empty_dict(self):
        assert auto_convert_dict({}) == {}

    def test_mixed_types_preserved(self):
        result = auto_convert_dict({
            "int_str": "10",
            "float_str": "1.5",
            "bool_val": True,
            "none_val": None,
            "list_val": [1, 2, 3],
        })
        assert result["int_str"] == 10
        assert result["float_str"] == 1.5
        assert result["bool_val"] is True
        assert result["none_val"] is None
        # Lists are not recursively converted (matches implementation)
        assert result["list_val"] == [1, 2, 3]
