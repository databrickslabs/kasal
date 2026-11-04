"""Tests for the display-format lint rules MV009 and MV010."""

from src.services.tools.metric_view_utils.metadata_lint import lint_metric_view
from src.services.tools.metric_view_utils.metadata_lint_format import (
    RULE_MISSING_FORMAT,
    RULE_PERCENT_DOUBLE_SCALED,
)

COMMENT = "A sufficiently long description of the measure."


def _spec(*measures):
    return {"dimensions": [], "measures": list(measures)}


def _m(name, expr="SUM(src.x)", fmt=None):
    entry = {"name": name, "expr": expr, "display_name": "D", "comment": COMMENT}
    if fmt is not None:
        entry["format"] = fmt
    return entry


def _rules(findings):
    return [f.rule_id for f in findings]


class TestMissingFormat:
    def test_measure_without_format_warns(self):
        findings = lint_metric_view(_spec(_m("total_sales")))
        hits = [f for f in findings if f.rule_id == RULE_MISSING_FORMAT]
        assert len(hits) == 1
        assert hits[0].field == "total_sales"
        assert hits[0].severity == "warning"

    def test_measure_with_format_is_clean(self):
        fmt = {"type": "number", "decimal_places": {"type": "max", "places": 2}}
        assert RULE_MISSING_FORMAT not in _rules(
            lint_metric_view(_spec(_m("total_sales", fmt=fmt)))
        )

    def test_format_without_type_still_warns(self):
        assert RULE_MISSING_FORMAT in _rules(
            lint_metric_view(_spec(_m("total_sales", fmt={})))
        )


class TestPercentDoubleScaled:
    def test_percentage_with_x100_is_error(self):
        pct = {"type": "percentage", "decimal_places": {"type": "exact", "places": 2}}
        findings = lint_metric_view(
            _spec(_m("fill_rate", "100*SUM(a)/SUM(b)", fmt=pct))
        )
        hits = [f for f in findings if f.rule_id == RULE_PERCENT_DOUBLE_SCALED]
        assert len(hits) == 1
        assert hits[0].severity == "error"

    def test_percentage_fraction_is_clean(self):
        pct = {"type": "percentage", "decimal_places": {"type": "exact", "places": 2}}
        findings = lint_metric_view(_spec(_m("fill_rate", "SUM(a)/SUM(b)", fmt=pct)))
        assert RULE_PERCENT_DOUBLE_SCALED not in _rules(findings)

    def test_number_with_x100_is_not_double_scaling(self):
        num = {"type": "number", "decimal_places": {"type": "max", "places": 2}}
        findings = lint_metric_view(_spec(_m("points", "100*SUM(a)/SUM(b)", fmt=num)))
        assert RULE_PERCENT_DOUBLE_SCALED not in _rules(findings)


class TestObjectInput:
    def test_object_measure_uses_derived_format(self):
        from types import SimpleNamespace

        spec = SimpleNamespace(
            dimensions=[],
            measures=[
                SimpleNamespace(
                    measure_name="total_cost",
                    sql_expr="SUM(src.cost)",
                    display_name="D",
                    synonyms=[],
                    comment=COMMENT,
                    skip_reason="",
                ),
                SimpleNamespace(
                    measure_name="mystery",
                    sql_expr="src.col",
                    display_name="D",
                    synonyms=[],
                    comment=COMMENT,
                    skip_reason="",
                ),
            ],
        )
        findings = lint_metric_view(spec)
        missing = [f.field for f in findings if f.rule_id == RULE_MISSING_FORMAT]
        assert missing == ["mystery"]
