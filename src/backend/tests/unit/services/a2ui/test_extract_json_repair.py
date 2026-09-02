"""extract_json repairs the closing-bracket slips composer models make on
deeply nested surfaces — the exact failure that dropped every mindmap to
prose: `}` where `]` belonged, one character from the end."""

import json

from src.services.a2ui.compose import (
    _repair_brackets,
    extract_json,
    json_error_of,
    validate_surface,
)

GOOD = {
    "surfaceKind": "mindmap",
    "root": "root",
    "components": [{"id": "root", "component": "Mindmap", "root": {"path": "/root"}}],
    "dataModel": {
        "root": {
            "id": "l",
            "label": "Lakehouse",
            "children": [
                {
                    "id": "s",
                    "label": "Storage",
                    "children": [{"id": "d", "label": "Delta"}],
                },
                {
                    "id": "ai",
                    "label": "AI",
                    "children": [{"id": "g", "label": "Genie"}],
                },
            ],
        }
    },
}
CATALOG = {"components": {"Mindmap": {}}}


def _drop_one_closing_bracket(text: str) -> str:
    """The model's slip: the `]` closing root.children is missing, so the
    tail reads `}]}}}}` instead of `}]}]}}}`."""
    tail = "}]}]}}}"
    assert text.endswith(tail), text[-12:]
    return text[: -len(tail)] + "}]}}}}"


def test_the_models_missing_array_close_is_repaired():
    broken = _drop_one_closing_bracket(json.dumps(GOOD, separators=(",", ":")))
    try:
        json.loads(broken)
        assert False, "the fixture must be invalid JSON"
    except json.JSONDecodeError:
        pass
    parsed = extract_json(broken)
    assert parsed is not None
    assert parsed["surfaceKind"] == "mindmap"
    assert validate_surface(parsed, CATALOG)
    assert parsed["dataModel"]["root"]["children"][1]["children"][0]["label"] == "Genie"


def test_a_truncated_reply_is_closed_off():
    text = json.dumps(GOOD)[:-25]  # cut mid-structure
    parsed = extract_json(text)
    assert parsed is not None and parsed["surfaceKind"] == "mindmap"


def test_valid_json_is_untouched():
    text = json.dumps(GOOD)
    assert extract_json(text) == GOOD
    assert extract_json("```json\n" + text + "\n```") == GOOD


def test_brackets_inside_strings_are_not_structure():
    text = '{"a": "x } y ] z", "b": [1, 2]}'
    assert _repair_brackets(text) == text
    assert extract_json(text) == {"a": "x } y ] z", "b": [1, 2]}


def test_garbage_stays_none():
    assert extract_json("no json here") is None
    assert extract_json("") is None


def test_json_error_quotes_the_parser_with_context():
    broken = _drop_one_closing_bracket(json.dumps(GOOD, separators=(",", ":")))
    msg = json_error_of(broken)
    assert "Expecting" in msg and "at char" in msg and "near:" in msg
