"""Tests for extract_embedded_json — pulling router fields out of prose-wrapped JSON.

On the soft output_json path a crew often returns its structured answer inside a
```json ... ``` block surrounded by prose ("Based on my research… ```json {…} ```
## Summary …"). The router's direct-JSON check missed these, so has_results was
never read and the flow stopped without routing. This helper extracts the JSON.
"""

from src.engines.crewai.paths.flow.modules.flow_builder import extract_embedded_json

PROSE_WRAPPED = (
    "Based on my comprehensive online research, I have compiled the following:\n\n"
    "```json\n"
    '{"query": "Nehme Tohme", "results_found": 15, "has_results": true, '
    '"answer_found": true, "relevance_score": 0.98}\n'
    "```\n\n"
    "## Summary\nNehme Tohme is a Lebanese politician and businessman..."
)


def test_extracts_from_fenced_block_in_prose():
    out = extract_embedded_json(PROSE_WRAPPED)
    assert isinstance(out, dict)
    assert out["has_results"] is True
    assert out["results_found"] == 15


def test_extracts_bare_object_without_fences():
    out = extract_embedded_json('prefix text {"has_results": false} trailing')
    assert out == {"has_results": False}


def test_plain_prose_returns_none():
    assert extract_embedded_json("No JSON anywhere in this text.") is None


def test_handles_pure_json_string():
    assert extract_embedded_json('{"a": 1, "b": 2}') == {"a": 1, "b": 2}


def test_non_string_returns_none():
    assert extract_embedded_json(None) is None
    assert extract_embedded_json({"already": "dict"}) is None


def test_ignores_braces_inside_string_values():
    # The first balanced object is the real one; trailing prose with stray braces
    # must not corrupt extraction.
    out = extract_embedded_json('{"url": "https://x/y", "has_results": true} then } noise')
    assert out["has_results"] is True


def test_invalid_fenced_block_falls_through_to_brace_scan():
    # First fenced block is not valid JSON → skip it (continue) and recover the
    # bare object found later in the text.
    text = "```json\nnot valid json\n```\nand then {\"has_results\": true}"
    assert extract_embedded_json(text) == {"has_results": True}


def test_malformed_first_object_then_valid_one():
    # First {...} candidate fails json.loads → break and scan for the next {.
    assert extract_embedded_json('{nope not json} {"ok": 1}') == {"ok": 1}
