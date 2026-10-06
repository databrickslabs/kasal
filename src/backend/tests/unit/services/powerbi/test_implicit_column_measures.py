"""Regression coverage for implicit_column_measures.py — promoting a raw
column to its own UCMV measure ONLY when it's aggregatable AND actually used
in a visual, with no PBI measure already covering it.
"""

from src.services.powerbi.implicit_column_measures import (
    derive_implicit_column_measures,
)

FACT_TABLES = {"Fact_OTC", "Fact_NPS"}


def _occ(page="OTC Scorecard", visual_type="card", role="drawn"):
    return [{"page": page, "visual_type": visual_type, "role": role}]


class TestImplicitColumnMeasures:
    def test_promotes_an_aggregatable_column_used_in_a_visual(self):
        visual_usage_index = {"NPS_Contribution_per_Driver": _occ(page="NPS Overview")}
        column_summarize_by = {
            "Fact_NPS": {"NPS_Contribution_per_Driver": "Sum"},
        }
        result = derive_implicit_column_measures(
            visual_usage_index, measures=[], column_summarize_by=column_summarize_by,
            fact_tables=FACT_TABLES,
        )
        assert "Fact_NPS" in result
        entry = result["Fact_NPS"][0]
        assert entry["original_name"] == "NPS_Contribution_per_Driver"
        assert entry["raw_expr"] == "SUM(source.nps_contribution_per_driver)"
        assert entry["category"] == "implicit_visual_column"
        assert entry["pbi_kind"] == "raw_column"
        assert entry["used_in_visuals"] == _occ(page="NPS Overview")
        assert "Aggregated column as measure" in entry["comment"]

    def test_maps_every_summarize_by_to_the_right_sql_aggregate(self):
        visual_usage_index = {
            "col_sum": _occ(), "col_avg": _occ(), "col_count": _occ(),
            "col_min": _occ(), "col_max": _occ(), "col_distinct": _occ(),
        }
        column_summarize_by = {
            "Fact_OTC": {
                "col_sum": "Sum", "col_avg": "Average", "col_count": "Count",
                "col_min": "Min", "col_max": "Max", "col_distinct": "DistinctCount",
            }
        }
        result = derive_implicit_column_measures(
            visual_usage_index, measures=[], column_summarize_by=column_summarize_by,
            fact_tables=FACT_TABLES,
        )
        by_name = {e["original_name"]: e["raw_expr"] for e in result["Fact_OTC"]}
        assert by_name["col_sum"] == "SUM(source.col_sum)"
        assert by_name["col_avg"] == "AVG(source.col_avg)"
        assert by_name["col_count"] == "COUNT(source.col_count)"
        assert by_name["col_min"] == "MIN(source.col_min)"
        assert by_name["col_max"] == "MAX(source.col_max)"
        assert by_name["col_distinct"] == "COUNT(DISTINCT source.col_distinct)"

    def test_skips_a_column_whose_summarize_by_is_none(self):
        """A text/key column dragged into a visual as a label, not a value —
        PBI itself doesn't aggregate it, so Kasal must not invent an aggregate."""
        visual_usage_index = {"region": _occ()}
        column_summarize_by = {"Fact_OTC": {"region": "None"}}
        result = derive_implicit_column_measures(
            visual_usage_index, measures=[], column_summarize_by=column_summarize_by,
            fact_tables=FACT_TABLES,
        )
        assert result == {}

    def test_skips_a_field_already_covered_by_a_named_measure(self):
        """The whole point: don't duplicate a measure that already exists —
        only promote columns with NO measure behind them."""
        visual_usage_index = {"Total Revenue": _occ()}
        measures = [{"measure_name": "total_revenue", "original_name": "Total Revenue"}]
        column_summarize_by = {"Fact_OTC": {"Total Revenue": "Sum"}}
        result = derive_implicit_column_measures(
            visual_usage_index, measures=measures, column_summarize_by=column_summarize_by,
            fact_tables=FACT_TABLES,
        )
        assert result == {}

    def test_skips_a_column_not_used_in_any_visual(self):
        """A column can be aggregatable and never actually be dragged into
        anything — must not be promoted just because it COULD be."""
        result = derive_implicit_column_measures(
            visual_usage_index={},
            measures=[],
            column_summarize_by={"Fact_OTC": {"unused_col": "Sum"}},
            fact_tables=FACT_TABLES,
        )
        assert result == {}

    def test_prefers_a_fact_table_when_the_same_column_name_exists_on_several_tables(self):
        visual_usage_index = {"amount": _occ()}
        column_summarize_by = {
            "Dim_Budget": {"amount": "Sum"},
            "Fact_OTC": {"amount": "Sum"},
        }
        result = derive_implicit_column_measures(
            visual_usage_index, measures=[], column_summarize_by=column_summarize_by,
            fact_tables=FACT_TABLES,
        )
        assert "Fact_OTC" in result
        assert "Dim_Budget" not in result

    def test_empty_inputs_return_empty(self):
        assert derive_implicit_column_measures({}, [], {}, set()) == {}
        assert derive_implicit_column_measures(
            {"x": _occ()}, [], {}, set()
        ) == {}
        assert derive_implicit_column_measures(
            None, [], {"T": {"c": "Sum"}}, set()
        ) == {}
