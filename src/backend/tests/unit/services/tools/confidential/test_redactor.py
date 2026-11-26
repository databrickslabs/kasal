"""The redactor: tables, nested JSON, free text and error messages."""

import json

import pytest

from src.services.tools.confidential.redactor import (
    NOTICE,
    is_error_text,
    redact_output,
    redact_text,
    scrub_error_text,
)

PLACEHOLDER = "<withheld>"

DAX_TABLE = (
    "| Plant | Qty | Margin | Day | Active |\n"
    "| --- | --- | --- | --- | --- |\n"
    "| North | 12 | 0.3311 | 2024-01-02 | true |\n"
    "| South | 7 | 14.0 | 2024-01-03 | false |\n"
    "| East | 3 | None | 2024-01-04 | true |"
)


# ── tables ───────────────────────────────────────────────────────────────


def test_table_keeps_headers_categorical_cells_and_row_count():
    out = redact_text(DAX_TABLE)
    lines = out.splitlines()
    assert lines[0] == "| Plant | Qty | Margin | Day | Active |"
    assert lines[1] == "| --- | --- | --- | --- | --- |"
    assert lines[2] == "| North | 12 | <withheld> | 2024-01-02 | true |"
    assert lines[3] == "| South | 7 | <withheld> | 2024-01-03 | false |"
    assert lines[4] == "| East | 3 | None | 2024-01-04 | true |"
    assert "3 rows" in lines[5]


def test_table_summary_names_each_column_type_and_withholds_fractions():
    summary = redact_text(DAX_TABLE).splitlines()[-1]
    assert "Plant (text)" in summary
    assert "Qty (int)" in summary
    assert "Margin (fractional, withheld)" in summary
    assert "Day (date)" in summary
    assert "Active (bool)" in summary


def test_table_strips_power_bi_bracket_names_from_headers():
    table = "| [Plant].[Name] | [Measures].[Revenue] |\n| --- | --- |\n| North | 0.5 |"
    out = redact_text(table)
    assert out.splitlines()[0] == "| Name | Revenue |"
    assert "0.5" not in out


def test_fraction_inside_a_text_cell_withholds_the_cell():
    table = "| Label | Note |\n| --- | --- |\n| A | margin 12.5 kg |"
    assert "| A | <withheld> |" in redact_text(table)


def test_table_without_separator_is_still_redacted():
    table = "| Plant | Revenue |\n| North | 1.5 |"
    out = redact_text(table)
    assert "| North | <withheld> |" in out


def test_two_tables_in_one_text_are_both_redacted():
    text = f"{DAX_TABLE}\n\n| A | B |\n| --- | --- |\n| x | 9.75 |"
    out = redact_text(text)
    assert "9.75" not in out
    assert out.count("<withheld>") >= 3


# ── prose and free text ──────────────────────────────────────────────────


def test_free_text_without_numbers_passes_through():
    text = "Plant North has the highest share of returns."
    assert redact_text(text) == text


def test_free_text_with_a_number_is_replaced_by_notice():
    assert redact_text("Revenue was 1234 last year.") == NOTICE


def test_failure_line_inside_prose_is_scrubbed_not_withheld():
    # The agent must still learn WHY a call failed; only the numbers go.
    text = (
        "Genie query failed. This may be due to syntax.\nGenie error: column 7 missing"
    )
    assert redact_text(text) == (
        "Genie query failed. This may be due to syntax.\nGenie error: column <n> missing"
    )


def test_repeated_notice_lines_collapse_to_one():
    text = "Line one has 5.\nLine two has 6.\nLine three is clean."
    assert redact_text(text) == f"{NOTICE}\nLine three is clean."


def test_prose_around_a_table_is_judged_line_by_line():
    text = f"Here is the table.\n{DAX_TABLE}\nTotal was 99."
    out = redact_text(text)
    assert out.splitlines()[0] == "Here is the table."
    assert "| North | 12 | <withheld> |" in out
    assert out.splitlines()[-1] == NOTICE


def test_genie_style_answer_loses_its_numbers_but_keeps_categories():
    answer = (
        "Which plants are active?\n"
        "| Plant | Share |\n| --- | --- |\n| A | 0.5 |\n"
        "Done with 3 steps."
    )
    out = redact_text(answer)
    assert "0.5" not in out
    assert "| A | <withheld> |" in out
    assert NOTICE in out


# ── JSON ─────────────────────────────────────────────────────────────────


def test_nested_json_keeps_keys_and_categories_and_withholds_fractions():
    payload = {
        "rows": [{"plant": "North", "qty": 3, "margin": 1.5, "day": "2024-01-02"}],
        "total": 2.0,
        "flags": [True, None],
    }
    out = json.loads(redact_text(json.dumps(payload)))
    assert out["rows"] == [
        {"plant": "North", "qty": 3, "margin": PLACEHOLDER, "day": "2024-01-02"}
    ]
    assert out["total"] == PLACEHOLDER
    assert out["flags"] == [True, None]


def test_json_string_values_with_fractions_are_withheld_whole():
    out = json.loads(redact_text('{"cell": "12.5 kg", "name": "Plant 1234"}'))
    assert out == {"cell": PLACEHOLDER, "name": "Plant 1234"}


def test_json_keys_that_are_fractions_are_withheld():
    out = json.loads(redact_text('{"0.25": "north"}'))
    assert out == {PLACEHOLDER: "north"}


def test_json_list_keeps_its_length_as_the_row_count():
    rows = [{"a": i, "b": i / 2} for i in range(1, 6)]
    out = json.loads(redact_text(json.dumps(rows)))
    assert len(out) == 5
    assert all(row["b"] == PLACEHOLDER for row in out)


def test_text_starting_with_bracket_but_not_json_is_prose():
    text = "[Sales].[Amount] is the measure"
    assert redact_text(text) == text


# ── errors ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "message",
    [
        "Error: HTTP 400 at row 12.5",
        "DAX execution failed: column 7 unknown",
        "Error obtaining Power BI access token: code 401",
        "Query timed out after 5 minutes",
    ],
)
def test_error_messages_are_recognised(message):
    assert is_error_text(message)


def test_error_message_numbers_are_scrubbed_and_text_kept():
    out = redact_text("Error: HTTP 400 at row 12.5 in table 3")
    assert out == "Error: HTTP <n> at row <n> in table <n>"


def test_error_scrub_is_applied_even_to_a_non_error_free_text_path():
    assert redact_output("DAX execution failed: column 7 unknown") == (
        "DAX execution failed: column <n> unknown"
    )


def test_scrub_error_text_replaces_scientific_and_grouped_numbers():
    assert scrub_error_text("got 1,234.50 and 2e-4") == "got <n> and <n>"


def test_ordinary_text_is_not_an_error():
    assert not is_error_text("The best plant is North.")


# ── structured and scalar outputs ────────────────────────────────────────


def test_structure_output_is_redacted_recursively():
    out = redact_output({"a": [1.25, "x"], "b": {"c": 7}})
    assert out == {"a": [PLACEHOLDER, "x"], "b": {"c": 7}}


def test_scalar_outputs():
    assert redact_output(4) == 4
    assert redact_output(None) is None
    assert redact_output(True) is True
    assert redact_output(0.5) == PLACEHOLDER


def test_arbitrary_object_is_judged_by_its_printed_form():
    class Frame:
        def __str__(self) -> str:
            return "a 1.5 b"

    class Label:
        def __str__(self) -> str:
            return "North"

    assert redact_output(Frame()) == NOTICE
    assert redact_output(Label()) == "North"


def test_plain_word_is_not_withheld():
    assert redact_text("North") == "North"
