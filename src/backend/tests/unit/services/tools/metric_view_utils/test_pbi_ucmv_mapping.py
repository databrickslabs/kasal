"""pbi_ucmv_mapping.py — the deterministic PBI<->UCMV mapping draft.

Covers:
- Pre-existing pbi_kinds: raw_column (basic) and direct (default).
- 2026-09-29 additions:
  - pbi_aggregation on raw_column (emitted when non-Sum, omitted when Sum).
  - composite: composite_a/composite_b keys, Table[Column] pbi_column format,
    per-operand extra_filter, and proper degradation paths for unsupported
    shapes (divide, named-measure string pairs, EAV).
  - value_format and tolerance on every entry.
  - Measure-level extra_filter on raw_column entries.
  - switch pbi_kind (with and without selector info).
"""

from src.services.tools.metric_view_utils.data_classes import TranslationResult
from src.services.tools.metric_view_utils.pbi_ucmv_mapping import (
    _emit_measure_entry,
    _measure_pbi_kind,
)


def _measure(**overrides) -> TranslationResult:
    defaults = dict(
        measure_name="m",
        original_name="M",
        sql_expr="SUM(source.x)",
        is_translatable=True,
        skip_reason="",
        dax_expression="",
        confidence="high",
        category="dax_translated",
    )
    defaults.update(overrides)
    return TranslationResult(**defaults)


# ---------------------------------------------------------------------------
# Pre-existing: raw_column basics (kept intact)
# ---------------------------------------------------------------------------


class TestRawColumnPbiKind:
    def test_raw_column_resolves_to_table_and_column_not_a_measure_name(self):
        """The whole point of this pbi_kind: there is no PBI measure to name
        — unlike `direct`, which would wrongly claim original_name IS one."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "Fact_NPS",
                    "column": "NPS_Contribution_per_Driver",
                }
            ],
        )
        info = _measure_pbi_kind(m)
        assert info == {
            "kind": "raw_column",
            "pbi_table": "Fact_NPS",
            "pbi_column": "NPS_Contribution_per_Driver",
        }

    def test_raw_column_falls_back_to_original_name_with_no_sources(self):
        m = _measure(pbi_kind="raw_column", original_name="SomeColumn", pbi_sources=[])
        info = _measure_pbi_kind(m)
        assert info["kind"] == "raw_column"
        assert info["pbi_column"] == "SomeColumn"
        assert info.get("pbi_table") is None

    def test_emitted_yaml_entry_has_pbi_table_and_pbi_column_not_pbi_measure(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "Fact_NPS",
                    "column": "NPS_Contribution_per_Driver",
                }
            ],
        )
        lines = _emit_measure_entry(m)
        text = "\n".join(lines)
        assert "pbi_kind: raw_column" in text
        assert "pbi_table: Fact_NPS" in text
        assert "pbi_column: NPS_Contribution_per_Driver" in text
        assert "pbi_measure:" not in text

    def test_used_in_visuals_still_annotated_on_a_raw_column_entry(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[{"kind": "raw_column", "table": "Fact_NPS", "column": "X"}],
            used_in_visuals=[
                {"page": "NPS Overview", "visual_type": "card", "role": "drawn"}
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "used_in_visuals:" in text
        assert "NPS Overview" in text


# ---------------------------------------------------------------------------
# Pre-existing: direct default (kept intact)
# ---------------------------------------------------------------------------


class TestDirectPbiKindUnaffected:
    def test_ordinary_measure_with_no_stamped_kind_defaults_to_direct(self):
        m = _measure(pbi_kind=None)
        info = _measure_pbi_kind(m)
        assert info == {"kind": "direct", "pbi_measure": "M"}

    def test_unresolved_measure_takes_priority_over_any_stamped_kind(self):
        m = _measure(
            is_translatable=False,
            pbi_kind="raw_column",
            pbi_sources=[{"kind": "raw_column", "table": "T", "column": "C"}],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"


# ---------------------------------------------------------------------------
# 2026-09-29: pbi_aggregation on raw_column
# ---------------------------------------------------------------------------


class TestPbiAggregationOnRawColumn:
    """pbi_aggregation must be emitted when non-Sum and omitted when Sum
    (Sum is the documented default; emitting it for Sum would add noise to
    every draft mapping without adding information).
    """

    def test_non_sum_aggregation_present_in_info_dict(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "Fact_T",
                    "column": "col",
                    "summarize_by": "Average",
                }
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["pbi_aggregation"] == "Average"

    def test_sum_aggregation_not_in_info_dict(self):
        """Sum is the default — don't clutter the draft with it."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "Fact_T",
                    "column": "col",
                    "summarize_by": "Sum",
                }
            ],
        )
        info = _measure_pbi_kind(m)
        assert "pbi_aggregation" not in info

    def test_missing_summarize_by_treated_as_sum_default(self):
        """When pbi_sources carries no summarize_by (current default from
        implicit_column_measures.py), assume Sum and omit the field."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[{"kind": "raw_column", "table": "Fact_T", "column": "col"}],
        )
        info = _measure_pbi_kind(m)
        assert "pbi_aggregation" not in info

    def test_unrecognized_summarize_by_falls_back_to_sum(self):
        """An unrecognized value (data-quality issue upstream) should not crash
        — fall back to Sum and omit the field rather than emit an invalid one."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "Fact_T",
                    "column": "col",
                    "summarize_by": "WeirdValue",
                }
            ],
        )
        info = _measure_pbi_kind(m)
        assert "pbi_aggregation" not in info

    def test_count_aggregation_emitted_in_yaml(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "T",
                    "column": "C",
                    "summarize_by": "Count",
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "pbi_aggregation: Count" in text

    def test_max_aggregation_emitted_in_yaml(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "T",
                    "column": "C",
                    "summarize_by": "Max",
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "pbi_aggregation: Max" in text

    def test_sum_aggregation_absent_from_yaml(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "T",
                    "column": "C",
                    "summarize_by": "Sum",
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "pbi_aggregation" not in text

    def test_pbi_aggregation_positioned_after_pbi_column_before_value_format(self):
        """Emit order: pbi_column, pbi_aggregation (if any), value_format."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "T",
                    "column": "C",
                    "summarize_by": "DistinctCount",
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        col_pos = text.index("pbi_column:")
        agg_pos = text.index("pbi_aggregation:")
        fmt_pos = text.index("value_format:")
        assert col_pos < agg_pos < fmt_pos


# ---------------------------------------------------------------------------
# 2026-09-29: composite — new schema shape (composite_a/b + extra_filter)
# ---------------------------------------------------------------------------


class TestCompositeSchemaShape:
    """The 2026-09-29 schema uses composite_a/composite_b (not operand_a/b),
    pbi_column as "Table[Column]", and per-operand extra_filter lists."""

    def test_composite_uses_composite_a_b_keys(self):
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {"table": "Fact_SC", "column": "value"},
                {"table": "Fact_SC", "column": "value"},
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "composite"
        assert "composite_a" in info
        assert "composite_b" in info
        assert "operand_a" not in info
        assert "operand_b" not in info

    def test_composite_pbi_column_formatted_as_table_bracket(self):
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {"table": "Fact_SC", "column": "value"},
                {"table": "Fact_SC", "column": "budget"},
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["composite_a"]["pbi_column"] == "Fact_SC[value]"
        assert info["composite_b"]["pbi_column"] == "Fact_SC[budget]"

    def test_composite_without_extra_filter_emits_pbi_column_only(self):
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {"table": "Fact", "column": "col_a"},
                {"table": "Fact", "column": "col_b"},
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["composite_a"] == {"pbi_column": "Fact[col_a]"}
        assert info["composite_b"] == {"pbi_column": "Fact[col_b]"}

    def test_composite_per_operand_extra_filter_in_info_dict(self):
        ef_a = [{"table": "Fact_SC", "column": "bic_chversion", "values": ["0000"]}]
        ef_b = [{"table": "Fact_SC", "column": "bic_chversion", "values": ["B000"]}]
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {"table": "Fact_SC", "column": "value", "extra_filter": ef_a},
                {"table": "Fact_SC", "column": "value", "extra_filter": ef_b},
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["composite_a"]["extra_filter"] == ef_a
        assert info["composite_b"]["extra_filter"] == ef_b

    def test_composite_emitted_yaml_has_composite_a_b_keys(self):
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {"table": "Fact_SC", "column": "value"},
                {"table": "Fact_SC", "column": "value"},
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "composite_a:" in text
        assert "composite_b:" in text
        assert "operand_a:" not in text
        assert "operand_b:" not in text

    def test_composite_emitted_yaml_has_pbi_column_table_bracket(self):
        # _yaml_str quotes strings containing "["/"]" so the YAML value is
        # "Fact_SC[value]" (with surrounding double-quotes) — still valid YAML
        # that round-trips to the bare string Fact_SC[value].
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {"table": "Fact_SC", "column": "value"},
                {"table": "Fact_SC", "column": "value"},
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "Fact_SC[value]" in text

    def test_composite_emitted_yaml_includes_per_operand_filter(self):
        """This is the actual_vs_budget pattern: same column, different filter."""
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {
                    "table": "Fact_SC",
                    "column": "value",
                    "extra_filter": [
                        {
                            "table": "Fact_SC",
                            "column": "bic_chversion",
                            "values": ["0000"],
                        }
                    ],
                },
                {
                    "table": "Fact_SC",
                    "column": "value",
                    "extra_filter": [
                        {
                            "table": "Fact_SC",
                            "column": "bic_chversion",
                            "values": ["B000"],
                        }
                    ],
                },
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "composite_operator: subtract" in text
        assert "composite_a:" in text
        assert "composite_b:" in text
        assert "bic_chversion" in text
        assert "0000" in text
        assert "B000" in text

    def test_composite_add_operator_valid(self):
        m = _measure(
            pbi_kind="composite",
            pbi_operator="add",
            pbi_sources=[
                {"table": "T", "column": "a"},
                {"table": "T", "column": "b"},
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "composite"
        assert info["composite_operator"] == "add"

    def test_composite_divide_operator_degrades_to_unresolved(self):
        """divide is not in the 2026-09-29 schema; must NOT emit composite."""
        m = _measure(
            pbi_kind="composite",
            pbi_operator="divide",
            pbi_sources=[
                {
                    "table": "T",
                    "column": "a",
                    "value_column": "vc",
                    "filter_value": "v1",
                },
                {
                    "table": "T",
                    "column": "b",
                    "value_column": "vc",
                    "filter_value": "v2",
                },
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"
        assert "divide" in info["raw_hint"]

    def test_composite_string_sources_degrade_to_unresolved(self):
        """Two named-measure strings (from switch decomposition) are not
        expressible in the schema's composite shape — degrade honestly."""
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=["Actual", "Budget"],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"
        assert "composite of named measures" in info["raw_hint"]

    def test_composite_eav_shape_degrades_to_unresolved(self):
        """Dict sources with value_column/filter_value (fx_OTCKPI EAV) are
        not representable as extra_filter lists; degrade rather than emit
        a schema-invalid entry."""
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {
                    "table": "T",
                    "column": "C",
                    "value_column": "VC",
                    "filter_value": "V",
                },
                {
                    "table": "T",
                    "column": "C",
                    "value_column": "VC",
                    "filter_value": "V2",
                },
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"

    def test_composite_unexpected_source_count_degrades_to_unresolved(self):
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[{"table": "T", "column": "a"}],  # only one source
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"


# ---------------------------------------------------------------------------
# 2026-09-29: value_format and tolerance on every entry
# ---------------------------------------------------------------------------


class TestValueFormatAndTolerance:
    """Every emitted entry must include value_format and tolerance defaults
    so a reviewer sees all required fields without having to add them manually.
    TranslationResult carries no per-measure overrides today — all defaults."""

    def test_direct_measure_emits_value_format_raw_number(self):
        m = _measure(pbi_kind=None)
        text = "\n".join(_emit_measure_entry(m))
        assert "value_format: raw_number" in text

    def test_direct_measure_emits_absolute_tolerance(self):
        m = _measure(pbi_kind=None)
        text = "\n".join(_emit_measure_entry(m))
        assert "tolerance:" in text
        assert "kind: absolute" in text
        assert "value: 0.001" in text

    def test_raw_column_measure_emits_value_format(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[{"kind": "raw_column", "table": "T", "column": "C"}],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "value_format: raw_number" in text

    def test_unresolved_measure_emits_value_format(self):
        m = _measure(is_translatable=False, pbi_kind=None)
        text = "\n".join(_emit_measure_entry(m))
        assert "value_format: raw_number" in text

    def test_composite_measure_emits_value_format(self):
        m = _measure(
            pbi_kind="composite",
            pbi_operator="subtract",
            pbi_sources=[
                {"table": "T", "column": "a"},
                {"table": "T", "column": "b"},
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "value_format: raw_number" in text

    def test_tolerance_after_value_format_in_yaml(self):
        """tolerance: must follow value_format: in the YAML output."""
        m = _measure(pbi_kind=None)
        text = "\n".join(_emit_measure_entry(m))
        vf_pos = text.index("value_format:")
        tol_pos = text.index("tolerance:")
        assert vf_pos < tol_pos


# ---------------------------------------------------------------------------
# 2026-09-29: measure-level extra_filter on raw_column
# ---------------------------------------------------------------------------


class TestExtraFilterOnRawColumn:
    def test_raw_column_with_extra_filter_emits_it(self):
        """actual_value / budget_value pattern: same column, bic_chversion filter."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "Fact_SC",
                    "column": "value",
                    "extra_filter": [
                        {
                            "table": "Fact_SC",
                            "column": "bic_chversion",
                            "values": ["0000"],
                        }
                    ],
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "extra_filter:" in text
        assert "bic_chversion" in text
        assert "0000" in text

    def test_raw_column_without_extra_filter_has_no_extra_filter_key(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[{"kind": "raw_column", "table": "Fact", "column": "col"}],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "extra_filter" not in text

    def test_extra_filter_emitted_after_tolerance(self):
        """extra_filter must follow tolerance: in the YAML output."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "T",
                    "column": "C",
                    "extra_filter": [{"table": "T", "column": "k", "values": ["v"]}],
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        tol_pos = text.index("tolerance:")
        ef_pos = text.index("extra_filter:")
        assert tol_pos < ef_pos

    def test_extra_filter_multi_value_all_emitted(self):
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[
                {
                    "kind": "raw_column",
                    "table": "T",
                    "column": "C",
                    "extra_filter": [
                        {"table": "T", "column": "k", "values": ["v1", "v2", "v3"]}
                    ],
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "v1" in text
        assert "v2" in text
        assert "v3" in text


# ---------------------------------------------------------------------------
# 2026-09-29: switch pbi_kind
# ---------------------------------------------------------------------------


class TestSwitchPbiKind:
    def test_switch_with_full_selector_info_emits_switch_kind(self):
        m = _measure(
            pbi_kind="switch",
            pbi_sources=[
                {
                    "pbi_measure": "Prod_Exec_Line_Item_Actual",
                    "switch_selector_table": "Mapping_Line_Item",
                    "switch_selector_column": "KBI_Display_calculate",
                    "switch_selector_value": "Actual",
                }
            ],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "switch"
        assert info["pbi_measure"] == "Prod_Exec_Line_Item_Actual"
        assert info["switch_selector_table"] == "Mapping_Line_Item"
        assert info["switch_selector_column"] == "KBI_Display_calculate"
        assert info["switch_selector_value"] == "Actual"

    def test_switch_without_selector_info_degrades_to_unresolved(self):
        """Empty pbi_sources — we have no selector data to emit."""
        m = _measure(pbi_kind="switch", pbi_sources=[])
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"

    def test_switch_with_string_source_degrades_to_unresolved(self):
        """A plain string source (not a dict) carries no selector fields."""
        m = _measure(pbi_kind="switch", pbi_sources=["Some_Measure"])
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"

    def test_switch_missing_selector_table_degrades_to_unresolved(self):
        m = _measure(
            pbi_kind="switch",
            pbi_sources=[{"pbi_measure": "M", "switch_selector_column": "C"}],
        )
        info = _measure_pbi_kind(m)
        assert info["kind"] == "unresolved"

    def test_switch_emitted_yaml_has_selector_fields(self):
        m = _measure(
            pbi_kind="switch",
            pbi_sources=[
                {
                    "pbi_measure": "Some_Measure",
                    "switch_selector_table": "Sel_Table",
                    "switch_selector_column": "Sel_Col",
                    "switch_selector_value": "MyValue",
                }
            ],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "pbi_kind: switch" in text
        assert "switch_selector_table: Sel_Table" in text
        assert "switch_selector_column: Sel_Col" in text
        assert "switch_selector_value: MyValue" in text
