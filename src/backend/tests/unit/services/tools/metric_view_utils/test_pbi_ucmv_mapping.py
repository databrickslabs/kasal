"""pbi_ucmv_mapping.py — the deterministic PBI<->UCMV mapping draft.

No prior coverage existed for this module; this file focuses on the
`raw_column` pbi_kind added for implicit visual-column measures (a raw
column with PBI's own default aggregation and no named measure behind it —
see implicit_column_measures.py), while also pinning the pre-existing
`direct` default so the two don't collide.
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


class TestRawColumnPbiKind:
    def test_raw_column_resolves_to_table_and_column_not_a_measure_name(self):
        """The whole point of this pbi_kind: there is no PBI measure to name
        — unlike `direct`, which would wrongly claim original_name IS one."""
        m = _measure(
            pbi_kind="raw_column",
            pbi_sources=[{"kind": "raw_column", "table": "Fact_NPS", "column": "NPS_Contribution_per_Driver"}],
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
            pbi_sources=[{"kind": "raw_column", "table": "Fact_NPS", "column": "NPS_Contribution_per_Driver"}],
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
            used_in_visuals=[{"page": "NPS Overview", "visual_type": "card", "role": "drawn"}],
        )
        text = "\n".join(_emit_measure_entry(m))
        assert "used_in_visuals:" in text
        assert "NPS Overview" in text


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
