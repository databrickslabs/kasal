"""Unit tests for server-side A2UI UI-document normalization."""

import json

import pytest

from src.engines.crewai.helpers.ui_document import (
    _balanced_block,
    _coerce_json,
    _find_ui_document,
    _is_ui_document,
    _repair_json_brackets,
    normalize_ui_document,
)


def _doc(components, *, summary=None, theme=None, extra_messages=None):
    """Build a minimal well-formed A2UI document object."""
    create_surface = {"surfaceId": "s1", "catalogId": "basic"}
    if theme is not None:
        create_surface["theme"] = theme
    messages = [
        {"createSurface": create_surface},
        {"updateComponents": {"surfaceId": "s1", "components": components}},
    ]
    if extra_messages:
        messages.extend(extra_messages)
    obj = {"messages": messages}
    if summary is not None:
        obj["summary"] = summary
    return obj


_ROOT_TEXT = [
    {"id": "root", "component": "Column", "children": ["t"]},
    {"id": "t", "component": "Text", "variant": "h1", "text": "Hello"},
]


# --- _balanced_block ------------------------------------------------------


def test_balanced_block_pulls_object_from_prose():
    text = 'Here is the UI: {"a": {"b": 1}} thanks'
    assert _balanced_block(text, "{", "}") == '{"a": {"b": 1}}'


def test_balanced_block_ignores_brackets_inside_strings():
    text = '{"a": "}"}'
    assert _balanced_block(text, "{", "}") == '{"a": "}"}'


def test_balanced_block_returns_none_when_unbalanced():
    assert _balanced_block('{"a": 1', "{", "}") is None


def test_balanced_block_returns_none_when_absent():
    assert _balanced_block("no brackets here", "{", "}") is None


# --- _repair_json_brackets ------------------------------------------------


def test_repair_drops_spurious_trailing_closer():
    assert json.loads(_repair_json_brackets('{"a": [1, 2]}]')) == {"a": [1, 2]}


def test_repair_autocloses_truncated_document():
    assert json.loads(_repair_json_brackets('{"a": [1, 2')) == {"a": [1, 2]}


def test_repair_leaves_brackets_inside_strings_untouched():
    src = '{"a": "}]"}'
    assert json.loads(_repair_json_brackets(src)) == {"a": "}]"}


# --- _coerce_json ---------------------------------------------------------


def test_coerce_passthrough_dict():
    obj = {"messages": []}
    assert _coerce_json(obj) is obj


def test_coerce_plain_json_string():
    assert _coerce_json('{"a": 1}') == {"a": 1}


def test_coerce_double_encoded_string():
    assert _coerce_json(json.dumps('{"a": 1}')) == {"a": 1}


def test_coerce_fenced_block():
    assert _coerce_json('```json\n{"a": 1}\n```') == {"a": 1}


def test_coerce_prose_preamble():
    assert _coerce_json('Here you go: {"a": 1}') == {"a": 1}


def test_coerce_repairs_mismatched_brackets():
    assert _coerce_json('{"a": [1, 2]}]') == {"a": [1, 2]}


def test_coerce_returns_none_for_non_json():
    assert _coerce_json("just some prose") is None


def test_coerce_returns_none_for_non_string_non_container():
    assert _coerce_json(42) is None


# --- _is_ui_document ------------------------------------------------------


def test_is_ui_document_true_for_valid_doc():
    assert _is_ui_document(_doc(_ROOT_TEXT)) is True


def test_is_ui_document_accepts_type_discriminator():
    components = [{"id": "root", "type": "Text", "text": "hi"}]
    assert _is_ui_document(_doc(components)) is True


def test_is_ui_document_false_without_recognized_component():
    components = [{"id": "root", "component": "Bogus", "text": "x"}]
    assert _is_ui_document(_doc(components)) is False


def test_is_ui_document_false_for_plain_object():
    assert _is_ui_document({"foo": "bar"}) is False


def test_is_ui_document_false_for_component_missing_id():
    components = [{"component": "Text", "text": "no id"}]
    assert _is_ui_document(_doc(components)) is False


# --- _find_ui_document ----------------------------------------------------


def test_find_ui_document_top_level():
    doc = _doc(_ROOT_TEXT)
    assert _find_ui_document(doc) is doc


def test_find_ui_document_nested_in_envelope():
    doc = _doc(_ROOT_TEXT)
    envelope = {"status": "ok", "result": {"output": doc}}
    assert _find_ui_document(envelope) is doc


def test_find_ui_document_none_for_non_a2ui():
    assert _find_ui_document({"status": "ok", "result": "plain text"}) is None


def test_find_ui_document_respects_depth_cap():
    # Bury a doc deeper than the walk cap (6) — must not be found.
    nested = _doc(_ROOT_TEXT)
    for _ in range(8):
        nested = {"wrap": nested}
    assert _find_ui_document(nested) is None


# --- normalize_ui_document ------------------------------------------------


def test_normalize_returns_none_for_non_a2ui_string():
    assert normalize_ui_document("The crew finished its analysis.") is None


def test_normalize_returns_none_for_non_a2ui_dict():
    assert normalize_ui_document({"answer": "42", "status": "done"}) is None


def test_normalize_clean_doc_roundtrips_to_equivalent_json():
    doc = _doc(_ROOT_TEXT, summary="Built a greeting.")
    out = normalize_ui_document(json.dumps(doc))
    assert out is not None
    assert json.loads(out) == doc


def test_normalize_is_idempotent():
    doc = _doc(_ROOT_TEXT, summary="x")
    first = normalize_ui_document(json.dumps(doc))
    second = normalize_ui_document(first)
    assert first == second


def test_normalize_strips_prose_preamble():
    doc = _doc(_ROOT_TEXT)
    wrapped = f"Here is your dashboard: {json.dumps(doc)}"
    out = normalize_ui_document(wrapped)
    assert out is not None
    assert json.loads(out) == doc


def test_normalize_unwraps_fence():
    doc = _doc(_ROOT_TEXT)
    out = normalize_ui_document(f"```json\n{json.dumps(doc)}\n```")
    assert json.loads(out) == doc


def test_normalize_unwraps_double_encoding():
    doc = _doc(_ROOT_TEXT)
    out = normalize_ui_document(json.dumps(json.dumps(doc)))
    assert json.loads(out) == doc


def test_normalize_repairs_trailing_bracket():
    doc = _doc(_ROOT_TEXT)
    broken = json.dumps(doc) + "]"
    out = normalize_ui_document(broken)
    assert out is not None
    assert json.loads(out) == doc


def test_normalize_preserves_summary_and_theme():
    theme = {"accent": "#fff", "font": "sans"}
    doc = _doc(_ROOT_TEXT, summary="A themed greeting.", theme=theme)
    out = normalize_ui_document(json.dumps(doc))
    parsed = json.loads(out)
    assert parsed["summary"] == "A themed greeting."
    assert parsed["messages"][0]["createSurface"]["theme"] == theme


def test_normalize_preserves_data_model_update_message():
    extra = [{"dataModelUpdate": {"surfaceId": "s1", "contents": {"x": 1}}}]
    doc = _doc(_ROOT_TEXT, extra_messages=extra)
    out = normalize_ui_document(json.dumps(doc))
    parsed = json.loads(out)
    assert any("dataModelUpdate" in m for m in parsed["messages"])


def test_normalize_extracts_doc_from_envelope():
    doc = _doc(_ROOT_TEXT)
    envelope = {"status": "COMPLETED", "result": {"output": doc}}
    out = normalize_ui_document(envelope)
    assert out is not None
    assert json.loads(out) == doc


def test_normalize_returns_none_on_unserializable(monkeypatch):
    # A document whose object cannot be re-serialized must fail safe to None.
    doc = _doc(_ROOT_TEXT)
    doc["messages"].append({"bad": {1, 2, 3}})  # set is not JSON-serializable
    assert normalize_ui_document(doc) is None
