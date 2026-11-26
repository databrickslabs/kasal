"""Layout round-trip, file parsing, and item IDs for Genie spaces."""

import json

import pytest

from src.services.tools.genie_space_sync_utils.ids import ensure_ids
from src.services.tools.genie_space_sync_utils.space_files import (
    SpaceFilesError,
    definition_title,
    files_to_definition,
    parse_space_files,
    serialized_to_files,
    summarize_definition,
)

from ._fakes import ID_A, ID_B, serialized_space


def _header() -> dict[str, object]:
    return {"space_id": "0" * 32, "title": "Sales", "description": "d"}


def test_round_trip_restores_the_serialized_sections():
    original = serialized_space()
    files = serialized_to_files(original, _header())
    header, rebuilt = files_to_definition(files)

    assert header == _header()
    assert rebuilt["version"] == 2
    for section in ("config", "data_sources", "instructions", "benchmarks"):
        assert rebuilt[section] == original[section], section


def test_multiline_text_is_written_as_a_block_scalar():
    serialized = serialized_space()
    serialized["instructions"]["text_instructions"] = [
        {"id": ID_A, "content": ["Line one.\nLine two."]}
    ]
    files = serialized_to_files(serialized, _header())

    assert "content: |" in files["instructions.yaml"]
    _header_out, rebuilt = files_to_definition(files)
    assert rebuilt["instructions"]["text_instructions"][0]["content"] == [
        "Line one.\nLine two."
    ]


def test_missing_files_read_as_empty_sections():
    header, rebuilt = files_to_definition({"header.yaml": "title: Only\n"})

    assert definition_title(header) == "Only"
    assert rebuilt["data_sources"] == {}
    assert rebuilt["benchmarks"] == {}


def test_a_title_is_required_and_stripped():
    assert definition_title({"title": "  Spaced  "}) == "Spaced"
    assert definition_title({}) == ""


def test_yaml_that_is_not_a_mapping_is_refused():
    with pytest.raises(SpaceFilesError):
        files_to_definition({"config.yaml": "- just\n- a list\n"})


def test_parse_space_files_accepts_json_text_and_dicts():
    text = json.dumps({"header.yaml": "title: T\n"})
    assert parse_space_files(text) == {"header.yaml": "title: T\n"}
    assert parse_space_files({"config.yaml": "{}\n"}) == {"config.yaml": "{}\n"}


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps({"secrets.yaml": "x: 1"}),
        json.dumps({"header.yaml": 42}),
        json.dumps(["header.yaml"]),
        "not json at all",
    ],
)
def test_parse_space_files_refuses_bad_input(raw):
    with pytest.raises(ValueError):
        parse_space_files(raw)


def test_summarize_definition_counts_each_kind():
    counts = summarize_definition(serialized_space())

    assert counts["tables"] == 1
    assert counts["metric_views"] == 1
    assert counts["sample_questions"] == 1
    assert counts["example_sqls"] == 1
    assert counts["sql_snippets"] == 1
    assert counts["benchmarks"] == 1


def test_ensure_ids_assigns_missing_ids_and_sorts():
    serialized = {
        "instructions": {
            "example_question_sqls": [
                {"id": ID_B, "question": ["second"], "sql": ["SELECT 2"]},
                {"question": ["first"], "sql": ["SELECT 1"]},
            ]
        }
    }
    result, assigned = ensure_ids(serialized)
    items = result["instructions"]["example_question_sqls"]

    assert assigned == 1
    assert all(len(item["id"]) == 32 for item in items)
    assert [item["id"] for item in items] == sorted(item["id"] for item in items)
    # The input is untouched.
    assert "id" not in serialized["instructions"]["example_question_sqls"][1]


def test_ensure_ids_leaves_a_complete_space_alone():
    result, assigned = ensure_ids(serialized_space())
    assert assigned == 0
    assert result["benchmarks"] == serialized_space()["benchmarks"]
