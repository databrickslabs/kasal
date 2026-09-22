"""process_table's Step 6d — implicit visual-column measures.

New test file rather than growing test_table_processor.py, which is already
over the file-size ceiling (see CLAUDE.md's ratchet — same pair for source
and tests).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.services.tools.metric_view_utils.data_classes import TableInfo
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator
from src.services.tools.metric_view_utils.table_processor import (
    TableProcessorContext,
    process_table,
)


def _make_table_info(**overrides) -> TableInfo:
    defaults = dict(
        table_name="fact_test",
        source_table="cat.sch.fact_test",
        is_fact=True,
        aggregate_columns=[{"name": "amount", "source_col": "amount"}],
        group_by_columns=["region"],
        calculated_columns=[],
        full_sql="",
        raw_transpiled_sql="",
        static_filters=[],
        dim_source_tables={},
    )
    defaults.update(overrides)
    return TableInfo(**defaults)


def _make_context(config) -> TableProcessorContext:
    jd = MagicMock()
    jd.detect.return_value = []
    jd.detect_fact_joins.return_value = []
    jd.get_dim_dimensions.return_value = []
    return TableProcessorContext(
        config=config,
        mquery_tables={},
        translator=DaxTranslator(),
        join_detector=jd,
        scan_data={},
        enrichment_joins={},
        inactive_rels=[],
        unflatten_tables=False,
        llm_config={},
        calc_groups=[],
        inner_dim_joins=False,
        dimension_exclusions={},
        cross_table_measures=[],
        filter_warnings=[],
        limitations={},
    )


def _noop_switch(defn, filter_sets=None):
    raise AssertionError("no switch decomposition configured for this test")


def _run(table_key, table_info, dax_measures, ctx):
    return process_table(
        table_key,
        table_info,
        dax_measures,
        ctx,
        build_switch_measure_fn=_noop_switch,
        resolve_var_chain_fn=lambda e: e,
        extract_divide_args_fn=lambda e: None,
        clean_unresolved_vars_fn=lambda s: s,
        validate_filter_consistency_fn=lambda measures: [],
    )


class TestImplicitColumnMeasureStep:
    def test_builds_a_translation_result_from_config(self):
        config = {
            "implicit_column_measures": {
                "fact_test": [
                    {
                        "name": "nps_contribution_per_driver",
                        "original_name": "NPS_Contribution_per_Driver",
                        "raw_expr": "SUM(source.nps_contribution_per_driver)",
                        "comment": "Aggregated column as measure (Sum) — no named PBI measure",
                        "used_in_visuals": [
                            {"page": "NPS Overview", "visual_type": "card", "role": "drawn"}
                        ],
                        "pbi_kind": "raw_column",
                        "pbi_sources": [
                            {"kind": "raw_column", "table": "fact_test", "column": "NPS_Contribution_per_Driver"}
                        ],
                    }
                ]
            }
        }
        spec = _run(
            "fact_test", _make_table_info(), [], _make_context(config)
        )
        implicit = [m for m in spec.measures if m.category == "implicit_visual_column"]
        assert len(implicit) == 1
        m = implicit[0]
        assert m.measure_name == "nps_contribution_per_driver"
        assert m.original_name == "NPS_Contribution_per_Driver"
        assert m.sql_expr == "SUM(source.nps_contribution_per_driver)"
        assert m.is_translatable is True
        assert m.pbi_kind == "raw_column"
        assert m.used_in_visuals == [
            {"page": "NPS Overview", "visual_type": "card", "role": "drawn"}
        ]
        assert spec.implicit_measure_count == 1

    def test_does_not_fire_for_a_different_table(self):
        config = {
            "implicit_column_measures": {
                "some_other_table": [
                    {"name": "x", "original_name": "X", "raw_expr": "SUM(source.x)"}
                ]
            }
        }
        spec = _run("fact_test", _make_table_info(), [], _make_context(config))
        assert spec.implicit_measure_count == 0
        assert not any(m.category == "implicit_visual_column" for m in spec.measures)

    def test_skips_a_name_collision_with_an_existing_base_measure(self):
        """The base measure already named 'amount' must win — never emit a
        second measure with the same name."""
        config = {
            "implicit_column_measures": {
                "fact_test": [
                    {"name": "amount", "original_name": "Amount", "raw_expr": "SUM(source.amount)"}
                ]
            }
        }
        spec = _run("fact_test", _make_table_info(), [], _make_context(config))
        implicit = [m for m in spec.measures if m.category == "implicit_visual_column"]
        assert implicit == []

    def test_absent_config_key_is_a_no_op(self):
        spec = _run("fact_test", _make_table_info(), [], _make_context({}))
        assert spec.implicit_measure_count == 0

    def test_included_in_the_emitted_yaml_with_its_own_section(self):
        from src.services.tools.metric_view_utils.yaml_emitter import emit_yaml

        config = {
            "implicit_column_measures": {
                "fact_test": [
                    {
                        "name": "nps_contribution_per_driver",
                        "original_name": "NPS_Contribution_per_Driver",
                        "raw_expr": "SUM(source.nps_contribution_per_driver)",
                        "comment": "Aggregated column as measure (Sum) — no named PBI measure",
                        "used_in_visuals": [
                            {"page": "NPS Overview", "visual_type": "card", "role": "drawn"}
                        ],
                    }
                ]
            }
        }
        spec = _run("fact_test", _make_table_info(), [], _make_context(config))
        yaml_text = emit_yaml(spec)
        assert "Aggregated Column Measures" in yaml_text
        assert "nps_contribution_per_driver" in yaml_text
        assert "Used on: NPS Overview" in yaml_text
