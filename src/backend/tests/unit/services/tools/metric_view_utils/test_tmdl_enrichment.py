"""Unit tests for metric_view_utils.tmdl_enrichment.

Covers S6, S9, rec#9, and rec#10.  All tests are pure (no I/O, no mocks).

The tmdl_enrichment module is imported via importlib.util (direct file path)
to avoid triggering src/engines/__init__.py → EngineFactory → crewai_tools
during collection in environments where crewai_tools is not installed.

Fixtures
--------
TMDL payloads are base64-encoded UTF-8 strings matching the format returned by
``generate_config.fetch_tmdl_parts``.  TMDL uses TAB indentation; all fixture
strings below use literal ``\\t`` characters.
"""
from __future__ import annotations

import base64
import importlib.util
import os
import re
import sys

import pytest

# ---------------------------------------------------------------------------
# Load tmdl_enrichment directly from the source file so we avoid the
# src.engines package import chain (which requires crewai_tools).
# ---------------------------------------------------------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# _THIS_DIR = …/tests/unit/engines/crewai/tools/custom
# Go up 6 dirs to reach src/backend root, then into src/engines/…
_BACKEND_ROOT = os.path.normpath(os.path.join(_THIS_DIR, *[".."] * 5))
_MODULE_PATH = os.path.join(
    _BACKEND_ROOT,
    "src", "services", "tools", "metric_view_utils", "tmdl_enrichment.py",
)

_spec = importlib.util.spec_from_file_location("tmdl_enrichment_mod", _MODULE_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

extract_calculated_columns_from_tmdl = _mod.extract_calculated_columns_from_tmdl
detect_fiscal_calendar_tables = _mod.detect_fiscal_calendar_tables
harvest_measure_metadata_from_tmdl = _mod.harvest_measure_metadata_from_tmdl
enrich_ucmv_measures_with_metadata = _mod.enrich_ucmv_measures_with_metadata
build_reconciliation_mapping_scaffolds = _mod.build_reconciliation_mapping_scaffolds
_parse_calculated_columns = _mod._parse_calculated_columns
_to_snake = _mod._to_snake
_humanize = _mod._humanize


# ---------------------------------------------------------------------------
# TMDL fixture helpers
# ---------------------------------------------------------------------------

def _b64(text: str) -> str:
    """Base64-encode a TMDL text string (UTF-8)."""
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


# --- Calendar445: fiscal table with calculated columns ---
CALENDAR445_TMDL_TEXT = (
    "table Calendar445\n"
    "\tlineageTag: abc-001\n"
    "\n"
    "\tcolumn date_id\n"
    "\t\tdataType: date\n"
    "\t\tlineageTag: col-001\n"
    "\n"
    "\tcolumn Month\n"
    "\t\tdataType: int64\n"
    "\t\tlineageTag: col-002\n"
    "\n"
    "\tcolumn 'Week445 Label'\n"
    "\t\tdataType: string\n"
    "\t\texpression: \"FW\" & FORMAT(WEEKNUM([date_id], 2), \"00\")\n"
    "\t\tlineageTag: col-003\n"
    "\n"
    "\tcolumn 'Latest Month Sort'\n"
    "\t\tdataType: int64\n"
    "\t\texpression: IF([Latest Month] = 1, 0, 1)\n"
    "\t\tlineageTag: col-004\n"
    "\n"
    "\tmeasure 'Latest Month' = LASTDATE(BLANK())\n"
    "\t\tdescription: The latest closed fiscal month in the model\n"
    "\t\tlineageTag: m-001\n"
)

CALENDAR445_PART = {
    "path": "definition/tables/Calendar445.tmdl",
    "payload": _b64(CALENDAR445_TMDL_TEXT),
}

# --- Fact_OTC: regular columns + measures with/without descriptions ---
FACT_OTC_TMDL_TEXT = (
    "table Fact_OTC\n"
    "\tlineageTag: abc-002\n"
    "\n"
    "\tcolumn fiscper\n"
    "\t\tdataType: string\n"
    "\t\tlineageTag: col-010\n"
    "\n"
    "\tcolumn country\n"
    "\t\tdataType: string\n"
    "\t\tlineageTag: col-011\n"
    "\n"
    "\tmeasure 'Order Accuracy %' = DIVIDE([Accurate Orders], [Total Orders])\n"
    "\t\tdescription: Percentage of orders delivered without issues\n"
    "\t\tformatString: \"0.00%\"\n"
    "\t\tlineageTag: m-010\n"
    "\n"
    "\tmeasure 'Sum OTC FLTP' = SUM(Fact_OTC[fltp])\n"
    "\t\tlineageTag: m-011\n"
)

FACT_OTC_PART = {
    "path": "definition/tables/Fact_OTC.tmdl",
    "payload": _b64(FACT_OTC_TMDL_TEXT),
}

# --- AI_Invoice: has a calculated join-key column ---
AI_INVOICE_TMDL_TEXT = (
    "table AI_Invoice\n"
    "\tlineageTag: abc-003\n"
    "\n"
    "\tcolumn invoice_id\n"
    "\t\tdataType: int64\n"
    "\t\tlineageTag: col-020\n"
    "\n"
    "\tcolumn country_lookup\n"
    "\t\tdataType: string\n"
    "\t\texpression: RELATED(Dim_Country[country])\n"
    "\t\tlineageTag: col-021\n"
    "\n"
    "\tmeasure 'Invoice Count' = COUNTROWS(AI_Invoice)\n"
    "\t\tlineageTag: m-020\n"
)

AI_INVOICE_PART = {
    "path": "definition/tables/AI_Invoice.tmdl",
    "payload": _b64(AI_INVOICE_TMDL_TEXT),
}

ALL_PARTS = [CALENDAR445_PART, FACT_OTC_PART, AI_INVOICE_PART]


# ---------------------------------------------------------------------------
# S6 — Calculated column extraction
# ---------------------------------------------------------------------------

class TestExtractCalculatedColumnsFromTmdl:
    """S6 — extract DAX calculated columns from TMDL parts."""

    def test_detects_calc_columns_in_calendar445(self):
        result = extract_calculated_columns_from_tmdl(ALL_PARTS)
        assert "Calendar445" in result, "Calendar445 must appear"
        names = {c["name"] for c in result["Calendar445"]}
        assert "Week445 Label" in names
        assert "Latest Month Sort" in names

    def test_regular_columns_not_included(self):
        result = extract_calculated_columns_from_tmdl(ALL_PARTS)
        cal = result.get("Calendar445", [])
        regular_names = {c["name"] for c in cal}
        assert "date_id" not in regular_names
        assert "Month" not in regular_names

    def test_expression_content_is_correct(self):
        result = extract_calculated_columns_from_tmdl([CALENDAR445_PART])
        cal_cols = {c["name"]: c for c in result["Calendar445"]}
        expr = cal_cols["Week445 Label"]["expression"]
        assert "WEEKNUM" in expr or "FORMAT" in expr

    def test_ai_invoice_join_key_column(self):
        result = extract_calculated_columns_from_tmdl([AI_INVOICE_PART])
        assert "AI_Invoice" in result
        names = {c["name"] for c in result["AI_Invoice"]}
        assert "country_lookup" in names

    def test_empty_parts_returns_empty(self):
        assert extract_calculated_columns_from_tmdl([]) == {}
        assert extract_calculated_columns_from_tmdl(None) == {}

    def test_non_table_parts_ignored(self):
        parts = [
            {"path": "definition/model.tmdl",
             "payload": _b64("model 'My Model'\n\tculture: en-US\n")},
            CALENDAR445_PART,
        ]
        result = extract_calculated_columns_from_tmdl(parts)
        assert "Calendar445" in result


class TestParseCalculatedColumns:
    """Direct unit tests for the internal _parse_calculated_columns function."""

    def test_inline_expression(self):
        content = "table T\n\tcolumn 'Inline' = [A] + [B]\n\t\tdataType: decimal\n"
        result = _parse_calculated_columns(content)
        assert len(result) == 1
        assert result[0]["name"] == "Inline"
        assert "[A] + [B]" in result[0]["expression"]

    def test_attribute_block_expression(self):
        content = (
            "table T\n"
            "\tcolumn Calc\n"
            "\t\tdataType: string\n"
            "\t\texpression: FORMAT([d], \"YYYY\")\n"
        )
        result = _parse_calculated_columns(content)
        assert len(result) == 1
        assert result[0]["name"] == "Calc"
        assert "FORMAT" in result[0]["expression"]

    def test_no_expression_not_returned(self):
        content = "table T\n\tcolumn plain\n\t\tdataType: int64\n"
        assert _parse_calculated_columns(content) == []

    def test_data_type_captured(self):
        content = (
            "table T\n"
            "\tcolumn 'MyCol'\n"
            "\t\tdataType: string\n"
            "\t\texpression: \"hello\"\n"
        )
        result = _parse_calculated_columns(content)
        assert result[0]["data_type"] == "string"


# ---------------------------------------------------------------------------
# S9 — Fiscal calendar detection
# ---------------------------------------------------------------------------

class TestDetectFiscalCalendarTables:
    """S9 — detect fiscal 4-4-5 calendar tables."""

    def test_calendar445_detected(self):
        result = detect_fiscal_calendar_tables(ALL_PARTS)
        assert "Calendar445" in result

    def test_pbi_period_format_key(self):
        result = detect_fiscal_calendar_tables(ALL_PARTS)
        assert result["Calendar445"]["pbi_period_format"] == "date_to_fiscper"

    def test_indicators_populated(self):
        result = detect_fiscal_calendar_tables(ALL_PARTS)
        indicators = result["Calendar445"]["indicators"]
        assert isinstance(indicators, list) and len(indicators) > 0

    def test_fact_table_not_flagged(self):
        result = detect_fiscal_calendar_tables([FACT_OTC_PART])
        assert "Fact_OTC" not in result

    def test_weeknum_calc_col_triggers_detection(self):
        """Fiscal calendar detected via WEEKNUM expression even without '445' in name."""
        tmdl = (
            "table FiscalCalendar\n"
            "\tcolumn 'FW Label'\n"
            "\t\tdataType: string\n"
            "\t\texpression: WEEKNUM([date_id], 2)\n"
        )
        parts = [{"path": "definition/tables/FiscalCalendar.tmdl",
                  "payload": _b64(tmdl)}]
        calc = extract_calculated_columns_from_tmdl(parts)
        result = detect_fiscal_calendar_tables(parts, calc)
        assert "FiscalCalendar" in result

    def test_empty_returns_empty(self):
        assert detect_fiscal_calendar_tables([]) == {}

    def test_detection_with_precomputed_calc_cols(self):
        calc = extract_calculated_columns_from_tmdl([CALENDAR445_PART])
        result = detect_fiscal_calendar_tables([CALENDAR445_PART], calc)
        assert "Calendar445" in result


# ---------------------------------------------------------------------------
# rec#9 — Measure metadata harvest
# ---------------------------------------------------------------------------

class TestHarvestMeasureMetadataFromTmdl:
    """rec#9 — extract measure descriptions from TMDL."""

    def test_description_harvested(self):
        result = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        assert "Order Accuracy %" in result

    def test_description_content(self):
        result = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        desc = result["Order Accuracy %"]["description"]
        assert "issues" in desc or "Percentage" in desc

    def test_format_string_harvested(self):
        result = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        assert "format_string" in result["Order Accuracy %"]

    def test_display_name_synthesised(self):
        result = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        dn = result["Order Accuracy %"]["display_name"]
        assert dn  # non-empty

    def test_measure_without_description_not_included(self):
        result = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        assert "Sum OTC FLTP" not in result

    def test_calendar_measure_with_description(self):
        result = harvest_measure_metadata_from_tmdl([CALENDAR445_PART])
        assert "Latest Month" in result

    def test_empty_parts(self):
        assert harvest_measure_metadata_from_tmdl([]) == {}


class TestEnrichUcmvMeasuresWithMetadata:
    """rec#9 — enrich ucmv_measures with TMDL metadata and PBI traceability tag."""

    def _make_measures(self):
        return [
            {
                "measure_name": "order_accuracy__pct",
                "original_name": "Order Accuracy %",
                "dax_expression": "DIVIDE([a], [b])",
                "proposed_allocation": "Fact_OTC",
            },
            {
                "measure_name": "sum_otc_fltp",
                "original_name": "Sum OTC FLTP",
                "dax_expression": "SUM(Fact_OTC[fltp])",
                "proposed_allocation": "Fact_OTC",
            },
        ]

    def test_comment_has_pbi_measure_tag(self):
        meta = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        measures = self._make_measures()
        enrich_ucmv_measures_with_metadata(measures, meta)
        acc_m = next(m for m in measures if m["original_name"] == "Order Accuracy %")
        assert "PBI measure: Order Accuracy %" in acc_m["comment"]

    def test_description_added(self):
        meta = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        measures = self._make_measures()
        enrich_ucmv_measures_with_metadata(measures, meta)
        acc_m = next(m for m in measures if m["original_name"] == "Order Accuracy %")
        assert "description" in acc_m

    def test_measure_without_metadata_unchanged(self):
        meta = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        measures = self._make_measures()
        # Sum OTC FLTP has no description in fixture
        before_keys = set(measures[1].keys())
        enrich_ucmv_measures_with_metadata(measures, meta)
        assert set(measures[1].keys()) == before_keys

    def test_existing_comment_not_overwritten(self):
        meta = harvest_measure_metadata_from_tmdl([FACT_OTC_PART])
        measures = [{"measure_name": "order_accuracy__pct",
                     "original_name": "Order Accuracy %",
                     "comment": "existing comment"}]
        enrich_ucmv_measures_with_metadata(measures, meta)
        assert measures[0]["comment"] == "existing comment"

    def test_tag_only_when_no_description(self):
        """Tag is appended even when metadata has no description."""
        meta = {"My KPI": {"display_name": "My KPI"}}
        measures = [{"original_name": "My KPI", "measure_name": "my_kpi"}]
        enrich_ucmv_measures_with_metadata(measures, meta)
        assert "PBI measure: My KPI" in measures[0]["comment"]


# ---------------------------------------------------------------------------
# rec#10 — Reconciliation mapping scaffolds
# ---------------------------------------------------------------------------

class TestBuildReconciliationMappingScaffolds:
    """rec#10 — dqa/kpi_reconciliation scaffolds, direct binding."""

    def _simple_config(self):
        return {
            "fact_join_map": {"Fact_OTC": {"alias": "fact_otc"}},
            "join_key_map": {
                "Dim_Country": {
                    "dim_column": "country",
                    "source_table": "cat.schema.dim_country",
                },
            },
            "period_dim_priority": ["fiscper"],
        }

    def _simple_measures(self):
        return [
            {"measure_name": "order_accuracy__pct",
             "original_name": "Order Accuracy %",
             "proposed_allocation": "Fact_OTC"},
            {"measure_name": "sum_otc_fltp",
             "original_name": "Sum OTC FLTP",
             "proposed_allocation": "Fact_OTC"},
            {"measure_name": "unassigned",
             "original_name": "Unassigned KPI",
             "proposed_allocation": "__unassigned__"},
        ]

    def test_one_scaffold_per_fact_table(self):
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-001", dataset_id="ds-001",
        )
        fact_tables = [s["pbi_fact_table"] for s in scaffolds]
        assert "Fact_OTC" in fact_tables

    def test_unassigned_measures_excluded(self):
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-001", dataset_id="ds-001",
        )
        otc = next(s for s in scaffolds if s["pbi_fact_table"] == "Fact_OTC")
        pbi_names = [m["pbi_measure"] for m in otc["measures"]]
        assert "Unassigned KPI" not in pbi_names

    def test_all_measures_direct(self):
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-001", dataset_id="ds-001",
        )
        for s in scaffolds:
            for m in s["measures"]:
                assert m["pbi_kind"] == "direct"

    def test_workspace_and_dataset_id(self):
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-abc", dataset_id="ds-xyz",
        )
        otc = next(s for s in scaffolds if s["pbi_fact_table"] == "Fact_OTC")
        assert otc["pbi_workspace_id"] == "ws-abc"
        assert otc["pbi_semantic_model_id"] == "ds-xyz"

    def test_ucmv_measure_snake_case(self):
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-001", dataset_id="ds-001",
        )
        otc = next(s for s in scaffolds if s["pbi_fact_table"] == "Fact_OTC")
        for m in otc["measures"]:
            snake = m["ucmv_measure"]
            assert re.match(r"^[a-z0-9_]+$", snake), f"not snake: {snake!r}"

    def test_fiscal_calendar_adds_pbi_period_format(self):
        fiscal = {"Calendar445": {"pbi_period_format": "date_to_fiscper",
                                  "indicators": ["table name matches '445'"]}}
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-001", dataset_id="ds-001",
            fiscal_calendars=fiscal,
        )
        otc = next(s for s in scaffolds if s["pbi_fact_table"] == "Fact_OTC")
        assert otc["time_dimension"]["pbi_period_format"] == "date_to_fiscper"

    def test_no_fiscal_calendar_no_period_format(self):
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-001", dataset_id="ds-001",
            fiscal_calendars={},
        )
        otc = next(s for s in scaffolds if s["pbi_fact_table"] == "Fact_OTC")
        assert "pbi_period_format" not in otc["time_dimension"]

    def test_tolerance_present(self):
        scaffolds = build_reconciliation_mapping_scaffolds(
            self._simple_measures(), self._simple_config(),
            workspace_id="ws-001", dataset_id="ds-001",
        )
        otc = next(s for s in scaffolds if s["pbi_fact_table"] == "Fact_OTC")
        for m in otc["measures"]:
            assert m["tolerance"]["kind"] == "absolute"

    def test_empty_measures_no_scaffolds(self):
        assert build_reconciliation_mapping_scaffolds([], {}, "ws", "ds") == []

    def test_multiple_fact_tables(self):
        measures = [
            {"measure_name": "m1", "original_name": "M1",
             "proposed_allocation": "Fact_A"},
            {"measure_name": "m2", "original_name": "M2",
             "proposed_allocation": "Fact_B"},
        ]
        scaffolds = build_reconciliation_mapping_scaffolds(
            measures, {}, workspace_id="ws", dataset_id="ds")
        assert {s["pbi_fact_table"] for s in scaffolds} == {"Fact_A", "Fact_B"}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

class TestToSnake:
    def test_basic(self):
        s = _to_snake("Order Accuracy %")
        assert re.match(r"^[a-z0-9_]+$", s)

    def test_already_snake(self):
        assert _to_snake("fiscper") == "fiscper"

    def test_special_chars(self):
        s = _to_snake("Cycle Time(order intake to settlement)")
        assert re.match(r"^[a-z0-9_]+$", s)


class TestHumanize:
    def test_snake_to_readable(self):
        result = _humanize("order_accuracy_pct")
        assert "Order" in result or "order" in result.lower()

    def test_already_readable(self):
        assert _humanize("Order Accuracy %") == "Order Accuracy %"
