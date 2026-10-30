"""EQUIVALENT needs proof, not partial similarity.

The old check returned EQUIVALENT whenever ANY similarity existed, so a measure
with a filter conjunct, a filter value, a column or a denominator wrong still
passed. Each of those is now REVIEW, while faithful rewrites stay EQUIVALENT.
"""

import json
import textwrap

import pytest

from src.services.tools.metric_view_validation_utils.data_input_handler import (
    DataInputHandler,
)
from src.services.tools.metric_view_validation_utils.expression_validator import (
    ExpressionValidator,
)

FILTERED = 'CALCULATE(SUM(Sales[Amount]), Sales[Region] = "EU")'

WRONG = [
    (
        "dropped_conjunct",
        "SUM(source.Amount) FILTER (WHERE source.Region = 'EU')",
        'CALCULATE(SUM(Sales[Amount]), Sales[Region] = "EU" && Sales[OrderDate] >= DATE(2024,1,1))',
    ),
    (
        "dropped_second_filter",
        "SUM(source.Amount) FILTER (WHERE source.Region = 'EU')",
        'CALCULATE(SUM(Sales[Amount]), Sales[Region] = "EU", Sales[Channel] = "Web")',
    ),
    (
        "wrong_value",
        "SUM(source.Amount) FILTER (WHERE source.Region = 'US')",
        FILTERED,
    ),
    ("filter_dropped", "SUM(source.Amount)", FILTERED),
    ("wrong_column", "SUM(source.Cost)", "SUM(Sales[Amount])"),
    ("wrong_agg_and_column", "COUNT(source.Cost)", "SUM(Sales[Amount])"),
    (
        "dropped_denominator",
        "SUM(source.Amount)",
        "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Qty]))",
    ),
]

FAITHFUL = [
    (
        "calculate_filter",
        "SUM(source.amount) FILTER (WHERE source.region = 'EU')",
        FILTERED,
    ),
    (
        "case_when_filter",
        "SUM(CASE WHEN source.region = 'EU' THEN source.amount END)",
        FILTERED,
    ),
    (
        "in_list",
        "SUM(source.Amount) FILTER (WHERE source.Region IN ('US', 'EU'))",
        'CALCULATE(SUM(Sales[Amount]), Sales[Region] IN {"EU", "US"})',
    ),
    ("case_only", "SUM(source.amount)", "SUM(Sales[Amount])"),
    ("countrows", "COUNT(1)", "COUNTROWS(Sales)"),
    ("distinct", "COUNT(DISTINCT source.Customer)", "DISTINCTCOUNT(Sales[Customer])"),
    (
        "dated_filter",
        "SUM(source.amount) FILTER (WHERE source.region = 'EU' "
        "AND source.orderdate >= DATE'2024-01-01')",
        'CALCULATE(SUM(Sales[Amount]), Sales[Region] = "EU" && Sales[OrderDate] >= DATE(2024,1,1))',
    ),
]


def _status(v: ExpressionValidator, db: str, dax: str) -> str:
    r = v.validate(db, dax)
    if r["is_valid"]:
        return "VALID"
    if v._is_equivalent(r):
        return "EQUIVALENT"
    return "REVIEW" if v._is_review_candidate(r) else "INVALID"


@pytest.fixture
def validator() -> ExpressionValidator:
    return ExpressionValidator(table_mappings={"Sales": "source"})


WRONG.append(
    (
        "and_or_regrouped",
        "SUM(source.Amount) FILTER (WHERE (source.A = 'x' OR source.B = 'y') "
        "AND source.C = 'z')",
        'CALCULATE(SUM(Sales[Amount]), Sales[A] = "x" || Sales[B] = "y" && Sales[C] = "z")',
    )
)
WRONG.append(
    (
        "sql_and_or_mix",
        "SUM(source.Amount) FILTER (WHERE source.A = 'x' OR source.B = 'y' "
        "AND source.C = 'z')",
        'CALCULATE(SUM(Sales[Amount]), (Sales[A] = "x" || Sales[B] = "y") && Sales[C] = "z")',
    )
)


WRONG.append(
    (
        # x is evaluated once, unfiltered; CALCULATE does not re-filter it
        "var_inside_calculate",
        "SUM(source.Amount) FILTER (WHERE source.Region = 'EU')",
        'VAR x = SUM(Sales[Amount]) RETURN CALCULATE(x, Sales[Region] = "EU")',
    )
)


@pytest.mark.parametrize("name,db,dax", WRONG, ids=[w[0] for w in WRONG])
def test_partial_similarity_is_not_equivalent(validator, name, db, dax):
    assert _status(validator, db, dax) not in ("VALID", "EQUIVALENT")


@pytest.mark.parametrize("name,db,dax", FAITHFUL, ids=[f[0] for f in FAITHFUL])
def test_true_equivalents_still_pass(validator, name, db, dax):
    assert _status(validator, db, dax) in ("VALID", "EQUIVALENT")


def _handler(tmp_path, expr: str, dax: str) -> DataInputHandler:
    yaml_file = tmp_path / "mv.yaml"
    yaml_file.write_text(textwrap.dedent(f"""\
            measures:
              - name: m
                expr: "{expr}"
        """))
    mapping = tmp_path / "mapping.json"
    mapping.write_text(json.dumps([{"measure_name": "m", "dax_expression": dax}]))
    return DataInputHandler(str(yaml_file), str(mapping))


def test_inverted_ratio_is_not_valid(tmp_path):
    h = _handler(
        tmp_path,
        "SUM(source.Qty) / NULLIF(SUM(source.Amount), 0)",
        "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Qty]))",
    )
    v = ExpressionValidator(data_handler=h, table_mappings={"Sales": "source"})
    assert v.validate_measure_by_name("m")["status"] == "REVIEW"


def test_plain_aggregate_for_a_filtered_dax_measure_is_not_valid(tmp_path):
    h = _handler(tmp_path, "SUM(source.Amount)", FILTERED)
    v = ExpressionValidator(data_handler=h, table_mappings={"Sales": "source"})
    assert v.validate_measure_by_name("m")["status"] == "REVIEW"


def test_base_measure_without_dax_is_still_valid(tmp_path):
    h = _handler(tmp_path, "SUM(source.Amount)", "Not available")
    v = ExpressionValidator(data_handler=h)
    assert v.validate_measure_by_name("m")["status"] == "VALID"
