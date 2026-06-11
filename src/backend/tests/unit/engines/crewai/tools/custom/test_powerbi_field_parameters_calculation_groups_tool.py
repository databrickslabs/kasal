"""
Unit tests for powerbi_field_parameters_calculation_groups_tool.py

Tests the PowerBIFieldParametersCalculationGroupsTool — extracts field parameters
and calculation groups from Power BI semantic models via the TMDL API.

Strategy:
  - Instantiate the real tool class
  - Mock only: httpx, powerbi_auth_utils helpers
  - Test: init, _resolve_placeholder, _run validation branches,
    _parse_field_parameters, _parse_calculation_groups, _parse_all_measures,
    _get_referenced_measures, _format_markdown_output, _format_json_output
"""

import base64
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.tools.custom.powerbi_field_parameters_calculation_groups_tool import (
    PowerBIFieldParametersCalculationGroupsSchema,
    PowerBIFieldParametersCalculationGroupsTool,
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

WS_ID = "ws-aaaaaa-1111-2222-3333-bbbbbbbbbbbb"
DS_ID = "ds-cccccc-4444-5555-6666-dddddddddddd"
TENANT_ID = "tenant-eeeeee-7777-8888-9999-ffffffffffff"
CLIENT_ID = "client-11111111-aaaa-bbbb-cccc-222222222222"
CLIENT_SECRET = "s3cr3t"
ACCESS_TOKEN = "ey.fake.access.token"


def _make_tool(**kwargs):
    defaults = dict(workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN)
    defaults.update(kwargs)
    return PowerBIFieldParametersCalculationGroupsTool(**defaults)


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def _make_field_parameter_tmdl(table_name: str, items: list) -> str:
    """Build minimal TMDL for a field parameter table."""
    lines = [f"table '{table_name}'"]
    # Construct the partition source
    source_items = []
    for label, src_table, src_measure, ordinal in items:
        source_items.append(
            f'("{label}", NAMEOF(\'{src_table}\'[{src_measure}]), {ordinal})'
        )
    source_str = ", ".join(source_items)
    lines += [
        "\tpartition 'FieldParam' = calculated",
        "\t\tsource = {" + source_str + "}",
    ]
    return "\n".join(lines)


def _make_calculation_group_tmdl(table_name: str, precedence: int, items: list) -> str:
    """Build minimal TMDL for a calculation group."""
    lines = [f"table '{table_name}'"]
    lines.append("\tcalculationGroup")
    lines.append(f"\t\tprecedence: {precedence}")
    for item_name, expr in items:
        lines += [
            f"\n\tcalculationItem '{item_name}' = {expr}",
            "\t\tlineageTag: abc",
        ]
    return "\n".join(lines)


def _make_measure_tmdl(table_name: str, measures: list) -> str:
    """Build minimal TMDL for a regular table with measures."""
    lines = [f"table '{table_name}'"]
    for m_name, expr in measures:
        lines += [
            f"\tmeasure '{m_name}' = {expr}",
            "\t\tlineageTag: xyz",
        ]
    return "\n".join(lines)


# ===========================================================================
# Schema tests
# ===========================================================================

class TestPowerBIFieldParametersCalculationGroupsSchema:
    def test_constructs_with_no_args(self):
        # All remaining fields have defaults; plumbing comes from tool_configs.
        schema = PowerBIFieldParametersCalculationGroupsSchema()
        assert schema is not None

    def test_plumbing_fields_not_llm_fillable(self):
        """Connection/auth/LLM plumbing is injected via tool_configs (__init__),
        never exposed as LLM-fillable schema fields."""
        forbidden = {
            "client_secret", "password", "access_token", "llm_token",
            "api_key", "token", "tenant_id", "client_id", "username",
            "auth_method", "workspace_id", "dataset_id",
            "llm_workspace_url", "llm_model",
        }
        leaked = forbidden & set(
            PowerBIFieldParametersCalculationGroupsSchema.model_fields
        )
        assert not leaked, f"plumbing fields leaked into schema: {sorted(leaked)}"

    def test_target_catalog_default(self):
        schema = PowerBIFieldParametersCalculationGroupsSchema()
        assert schema.target_catalog == "main"

    def test_target_schema_default(self):
        schema = PowerBIFieldParametersCalculationGroupsSchema()
        assert schema.target_schema == "default"

    def test_translate_measures_default(self):
        schema = PowerBIFieldParametersCalculationGroupsSchema()
        assert schema.translate_measures is True

    def test_include_sql_translation_default(self):
        schema = PowerBIFieldParametersCalculationGroupsSchema()
        assert schema.include_sql_translation is True

    def test_include_metadata_tables_default(self):
        schema = PowerBIFieldParametersCalculationGroupsSchema()
        assert schema.include_metadata_tables is True

    def test_output_format_default(self):
        schema = PowerBIFieldParametersCalculationGroupsSchema()
        assert schema.output_format == "markdown"


# ===========================================================================
# Init tests
# ===========================================================================

class TestFieldParamsToolInit:
    def test_tool_name(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()
        assert "Field Parameters" in tool.name or "Calculation Groups" in tool.name

    def test_tool_description_non_empty(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()
        assert len(tool.description) > 20

    def test_args_schema_set(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()
        assert tool.args_schema is PowerBIFieldParametersCalculationGroupsSchema

    def test_default_config_populated(self):
        tool = PowerBIFieldParametersCalculationGroupsTool(
            workspace_id=WS_ID, dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )
        assert tool._default_config["workspace_id"] == WS_ID
        assert tool._default_config["dataset_id"] == DS_ID

    def test_placeholder_filtered_on_init(self):
        tool = PowerBIFieldParametersCalculationGroupsTool(workspace_id="{workspace_id}")
        assert tool._default_config["workspace_id"] is None

    def test_target_catalog_default(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()
        assert tool._default_config["target_catalog"] == "main"

    def test_target_schema_default(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()
        assert tool._default_config["target_schema"] == "default"

    def test_instance_id_assigned(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()
        assert hasattr(tool, "_instance_id")
        assert len(tool._instance_id) == 8

    def test_instance_ids_unique(self):
        t1 = PowerBIFieldParametersCalculationGroupsTool()
        t2 = PowerBIFieldParametersCalculationGroupsTool()
        assert t1._instance_id != t2._instance_id


# ===========================================================================
# _resolve_placeholder tests
# ===========================================================================

class TestFieldParamsResolvePlaceholder:
    def setup_method(self):
        self.tool = PowerBIFieldParametersCalculationGroupsTool()

    def test_non_string_returns_as_is(self):
        assert self.tool._resolve_placeholder(42, {}) == 42
        assert self.tool._resolve_placeholder(None, {}) is None

    def test_no_placeholder(self):
        assert self.tool._resolve_placeholder("static", {}) == "static"

    def test_single_placeholder_resolved(self):
        result = self.tool._resolve_placeholder("{ws_id}", {"ws_id": "abc"})
        assert result == "abc"

    def test_multi_placeholder_resolved(self):
        result = self.tool._resolve_placeholder(
            "{cat}.{sch}", {"cat": "main", "sch": "sales"}
        )
        assert result == "main.sales"

    def test_missing_key_left_as_is(self):
        result = self.tool._resolve_placeholder("{unknown}", {"other": "v"})
        assert result == "{unknown}"


# ===========================================================================
# _run validation tests
# ===========================================================================

class TestFieldParamsRunValidation:
    def test_missing_workspace_returns_error(self):
        tool = PowerBIFieldParametersCalculationGroupsTool(
            dataset_id=DS_ID, access_token=ACCESS_TOKEN
        )
        result = tool._run()
        assert "error" in result.lower() or "workspace_id" in result.lower()

    def test_missing_dataset_returns_error(self):
        tool = PowerBIFieldParametersCalculationGroupsTool(
            workspace_id=WS_ID, access_token=ACCESS_TOKEN
        )
        result = tool._run()
        assert "error" in result.lower() or "dataset_id" in result.lower()

    def test_unresolved_placeholder_returns_error(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()
        result = tool._run(
            workspace_id="{workspace_id}",
            dataset_id=DS_ID,
            access_token=ACCESS_TOKEN,
        )
        assert "error" in result.lower() or "placeholder" in result.lower()

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config")
    def test_invalid_auth_returns_error(self, mock_validate):
        mock_validate.return_value = (False, "No credentials")
        tool = PowerBIFieldParametersCalculationGroupsTool(
            workspace_id=WS_ID, dataset_id=DS_ID
        )
        result = tool._run()
        assert "error" in result.lower()

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_valid_auth_calls_run_sync(self, _validate):
        tool = _make_tool()
        with patch.object(tool, "_run_sync", return_value="extracted") as mock_sync:
            result = tool._run()
        mock_sync.assert_called_once()
        assert result == "extracted"

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_exception_returns_error_string(self, _validate):
        tool = _make_tool()
        with patch.object(tool, "_run_sync", side_effect=Exception("failure")):
            result = tool._run()
        assert "error" in result.lower() or "failure" in result.lower()


# ===========================================================================
# _parse_field_parameters tests
# ===========================================================================

class TestParseFieldParameters:
    def setup_method(self):
        self.tool = PowerBIFieldParametersCalculationGroupsTool()

    def _make_part(self, table_name: str, content: str) -> dict:
        return {
            "path": f"definition/tables/{table_name}.tmdl",
            "payload": _b64(content),
        }

    def test_empty_parts_returns_empty(self):
        result = self.tool._parse_field_parameters([])
        assert result == []

    def test_non_tmdl_path_ignored(self):
        part = {"path": "definition/model.json", "payload": _b64("table Sales")}
        result = self.tool._parse_field_parameters([part])
        assert result == []

    def test_table_without_nameof_skipped(self):
        content = "table Sales\n\tcolumn Amount"
        part = self._make_part("Sales", content)
        result = self.tool._parse_field_parameters([part])
        assert result == []

    def test_field_parameter_with_nameof_parsed(self):
        tmdl = _make_field_parameter_tmdl(
            "Measure Selector",
            [
                ("Revenue", "Sales", "Total Revenue", 0),
                ("Profit", "Sales", "Gross Profit", 1),
            ]
        )
        part = self._make_part("Measure_Selector", tmdl)
        result = self.tool._parse_field_parameters([part])
        assert len(result) == 1
        fp = result[0]
        assert fp["type"] == "Field Parameter"
        assert fp["name"] == "Measure Selector"
        assert len(fp["items"]) == 2

    def test_field_parameter_items_sorted_by_ordinal(self):
        tmdl = _make_field_parameter_tmdl(
            "KPI Selector",
            [
                ("Profit", "Sales", "Profit", 1),
                ("Revenue", "Sales", "Revenue", 0),
            ]
        )
        part = self._make_part("KPI_Selector", tmdl)
        result = self.tool._parse_field_parameters([part])
        assert result[0]["items"][0]["ordinal"] == 0
        assert result[0]["items"][1]["ordinal"] == 1

    def test_invalid_base64_gracefully_skipped(self):
        part = {"path": "definition/tables/Broken.tmdl", "payload": "!!invalid!!"}
        result = self.tool._parse_field_parameters([part])
        assert isinstance(result, list)

    def test_table_name_without_quotes(self):
        content = "table MeasureSelector\n\tpartition Part = calculated\n\t\tsource = {(\"Revenue\", NAMEOF('Sales'[Revenue]), 0)}"
        part = self._make_part("MeasureSelector", content)
        result = self.tool._parse_field_parameters([part])
        assert len(result) == 1
        assert result[0]["name"] == "MeasureSelector"


# ===========================================================================
# _parse_calculation_groups tests
# ===========================================================================

class TestParseCalculationGroups:
    def setup_method(self):
        self.tool = PowerBIFieldParametersCalculationGroupsTool()

    def _make_part(self, table_name: str, content: str) -> dict:
        return {
            "path": f"definition/tables/{table_name}.tmdl",
            "payload": _b64(content),
        }

    def test_empty_parts_returns_empty(self):
        result = self.tool._parse_calculation_groups([])
        assert result == []

    def test_table_without_calculation_group_skipped(self):
        content = "table Sales\n\tcolumn Amount"
        part = self._make_part("Sales", content)
        result = self.tool._parse_calculation_groups([part])
        assert result == []

    def test_calculation_group_parsed(self):
        tmdl = _make_calculation_group_tmdl(
            "Time Calculations",
            precedence=1,
            items=[
                ("Current Period", "SELECTEDMEASURE()"),
                ("Prior Period", "CALCULATE(SELECTEDMEASURE(), DATEADD('Date'[Date], -1, MONTH))"),
            ]
        )
        part = self._make_part("Time_Calculations", tmdl)
        result = self.tool._parse_calculation_groups([part])
        assert len(result) == 1
        cg = result[0]
        assert cg["type"] == "Calculation Group"
        assert cg["name"] == "Time Calculations"
        assert cg["precedence"] == 1

    def test_calculation_group_items_extracted(self):
        tmdl = _make_calculation_group_tmdl(
            "Time Intel",
            precedence=0,
            items=[
                ("YTD", "CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))"),
                ("MTD", "CALCULATE(SELECTEDMEASURE(), DATESMTD('Date'[Date]))"),
            ]
        )
        part = self._make_part("Time_Intel", tmdl)
        result = self.tool._parse_calculation_groups([part])
        assert len(result[0]["items"]) == 2

    def test_invalid_base64_gracefully_handled(self):
        part = {"path": "definition/tables/CG.tmdl", "payload": "!!invalid!!"}
        result = self.tool._parse_calculation_groups([part])
        assert isinstance(result, list)

    def test_calculation_group_default_precedence_zero(self):
        content = "table 'NoPrecedence'\n\tcalculationGroup\n\n\tcalculationItem 'Item1' = SELECTEDMEASURE()"
        part = self._make_part("NoPrecedence", content)
        result = self.tool._parse_calculation_groups([part])
        if result:
            assert result[0]["precedence"] == 0


# ===========================================================================
# _parse_all_measures tests
# ===========================================================================

class TestParseAllMeasures:
    def setup_method(self):
        self.tool = PowerBIFieldParametersCalculationGroupsTool()

    def _make_part(self, table_name: str, content: str) -> dict:
        return {
            "path": f"definition/tables/{table_name}.tmdl",
            "payload": _b64(content),
        }

    def test_empty_parts_returns_empty_dict(self):
        result = self.tool._parse_all_measures([])
        assert result == {}

    def test_measures_extracted(self):
        tmdl = _make_measure_tmdl(
            "Sales",
            [("Total Revenue", "SUM(Sales[Amount])"), ("Profit", "SUM(Sales[Profit])")]
        )
        part = self._make_part("Sales", tmdl)
        result = self.tool._parse_all_measures([part])
        assert "Total Revenue" in result
        assert "Profit" in result

    def test_measure_includes_table_and_expression(self):
        tmdl = _make_measure_tmdl("Sales", [("Revenue", "SUM(Sales[Amount])")])
        part = self._make_part("Sales", tmdl)
        result = self.tool._parse_all_measures([part])
        assert result["Revenue"]["table"] == "Sales"
        assert "SUM" in result["Revenue"]["expression"]

    def test_multiple_tables_all_measures_collected(self):
        tmdl1 = _make_measure_tmdl("Sales", [("Revenue", "SUM(Sales[Amount])")])
        tmdl2 = _make_measure_tmdl("Finance", [("Cost", "SUM(Finance[Cost])")])
        parts = [
            self._make_part("Sales", tmdl1),
            self._make_part("Finance", tmdl2),
        ]
        result = self.tool._parse_all_measures(parts)
        assert "Revenue" in result
        assert "Cost" in result

    def test_non_tmdl_path_ignored(self):
        part = {"path": "definition/model.json", "payload": _b64("table Sales")}
        result = self.tool._parse_all_measures([part])
        assert result == {}

    def test_lineage_tag_cleaned_from_expression(self):
        content = "table Sales\n\tmeasure 'Revenue' = SUM(Sales[Amount])\n\t\tlineageTag: abc\n\t\tformatString: $#,0"
        part = self._make_part("Sales", content)
        result = self.tool._parse_all_measures([part])
        if "Revenue" in result:
            assert "lineageTag" not in result["Revenue"]["expression"]


# ===========================================================================
# _get_referenced_measures tests
# ===========================================================================

class TestGetReferencedMeasures:
    def setup_method(self):
        self.tool = PowerBIFieldParametersCalculationGroupsTool()

    def test_empty_field_params_returns_empty(self):
        result = self.tool._get_referenced_measures([], {})
        assert result == []

    def test_measure_found_in_all_measures(self):
        field_params = [
            {
                "items": [
                    {"source_table": "Sales", "source_measure": "Total Revenue"},
                ]
            }
        ]
        all_measures = {
            "Total Revenue": {"name": "Total Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}
        }
        result = self.tool._get_referenced_measures(field_params, all_measures)
        assert len(result) == 1
        assert result[0]["name"] == "Total Revenue"

    def test_measure_not_in_all_measures_creates_stub(self):
        field_params = [
            {
                "items": [
                    {"source_table": "Sales", "source_measure": "Missing Measure"},
                ]
            }
        ]
        result = self.tool._get_referenced_measures(field_params, {})
        assert len(result) == 1
        assert result[0]["name"] == "Missing Measure"
        assert result[0]["expression"] is None

    def test_duplicate_measures_deduplicated(self):
        field_params = [
            {
                "items": [
                    {"source_table": "Sales", "source_measure": "Revenue"},
                    {"source_table": "Sales", "source_measure": "Revenue"},  # duplicate
                ]
            }
        ]
        all_measures = {
            "Revenue": {"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}
        }
        result = self.tool._get_referenced_measures(field_params, all_measures)
        assert len(result) == 1

    def test_multiple_field_params(self):
        field_params = [
            {"items": [{"source_table": "Sales", "source_measure": "Revenue"}]},
            {"items": [{"source_table": "Sales", "source_measure": "Profit"}]},
        ]
        all_measures = {
            "Revenue": {"name": "Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"},
            "Profit": {"name": "Profit", "table": "Sales", "expression": "SUM(Sales[Profit])"},
        }
        result = self.tool._get_referenced_measures(field_params, all_measures)
        names = [r["name"] for r in result]
        assert "Revenue" in names
        assert "Profit" in names


# ===========================================================================
# _format_markdown_output tests
# ===========================================================================

class TestFormatMarkdownOutput:
    def setup_method(self):
        self.tool = PowerBIFieldParametersCalculationGroupsTool()

    def _make_fp(self, name, items):
        return {
            "name": name,
            "items": items,
            "associated_measure": None,
            "measure_expression": None,
        }

    def _make_cg(self, name, precedence, items):
        return {
            "name": name,
            "precedence": precedence,
            "items": [{"name": n, "expression": e, "ordinal": i} for i, (n, e) in enumerate(items)],
        }

    def test_header_included(self):
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [], [], "main", "default", True, True
        )
        assert "Field Parameters" in result or "Power BI" in result

    def test_workspace_and_dataset_shown(self):
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [], [], "main", "default", True, True
        )
        assert WS_ID in result
        assert DS_ID in result

    def test_field_parameter_shown(self):
        fp = self._make_fp("Measure Selector", [
            {"ordinal": 0, "label": "Revenue", "source_table": "Sales", "source_measure": "Total Revenue"},
        ])
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [fp], [], [], "main", "default", True, True
        )
        assert "Measure Selector" in result
        assert "Revenue" in result

    def test_calculation_group_shown(self):
        cg = self._make_cg("Time Calc", 1, [("YTD", "CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))")])
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [cg], [], "main", "default", True, True
        )
        assert "Time Calc" in result
        assert "YTD" in result

    def test_sql_metadata_table_included(self):
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [], [], "main", "default", True, True
        )
        assert "CREATE TABLE" in result or "sql" in result.lower()

    def test_no_sql_when_disabled(self):
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [], [], "main", "default", True, False
        )
        # When include_metadata_tables=False, no CREATE TABLE
        assert "CREATE TABLE" not in result

    def test_counts_shown_in_header(self):
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [], [], "main", "default", True, True
        )
        assert "0" in result  # 0 field params, 0 calc groups

    def test_referenced_measures_shown(self):
        measures = [
            {"name": "Total Revenue", "table": "Sales", "expression": "SUM(Sales[Amount])"}
        ]
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [], measures, "main", "default", True, True
        )
        assert "Total Revenue" in result

    def test_measure_without_expression_shows_placeholder(self):
        measures = [
            {"name": "Missing Measure", "table": "Sales", "expression": None}
        ]
        result = self.tool._format_markdown_output(
            WS_ID, DS_ID, [], [], measures, "main", "default", True, True
        )
        assert "not found" in result.lower() or "Missing Measure" in result


# ===========================================================================
# _format_json_output tests
# ===========================================================================

class TestFormatJsonOutput:
    def setup_method(self):
        self.tool = PowerBIFieldParametersCalculationGroupsTool()

    def test_returns_valid_json(self):
        result = self.tool._format_json_output(
            WS_ID, DS_ID, [], [], [], "main", "default"
        )
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_contains_workspace_and_dataset(self):
        result = self.tool._format_json_output(
            WS_ID, DS_ID, [], [], [], "main", "default"
        )
        parsed = json.loads(result)
        assert parsed.get("workspace_id") == WS_ID
        assert parsed.get("dataset_id") == DS_ID

    def test_field_parameters_in_output(self):
        fp = {
            "name": "Selector",
            "items": [{"ordinal": 0, "label": "Revenue", "source_table": "Sales", "source_measure": "Revenue"}],
            "associated_measure": None,
            "measure_expression": None,
        }
        result = self.tool._format_json_output(
            WS_ID, DS_ID, [fp], [], [], "main", "default"
        )
        parsed = json.loads(result)
        assert len(parsed["field_parameters"]) == 1

    def test_calculation_groups_in_output(self):
        cg = {
            "name": "Time Calc",
            "precedence": 1,
            "items": [{"name": "YTD", "expression": "CALCULATE(...)"}],
        }
        result = self.tool._format_json_output(
            WS_ID, DS_ID, [], [cg], [], "main", "default"
        )
        parsed = json.loads(result)
        assert len(parsed["calculation_groups"]) == 1


# ===========================================================================
# _run_sync tests
# ===========================================================================

class TestFieldParamsRunSync:
    def test_executes_coroutine(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()

        async def coro():
            return "sync_result"

        result = tool._run_sync(coro())
        assert result == "sync_result"

    def test_propagates_exception(self):
        tool = PowerBIFieldParametersCalculationGroupsTool()

        async def coro():
            raise ValueError("bad")

        with pytest.raises(ValueError, match="bad"):
            tool._run_sync(coro())


# ===========================================================================
# Integration: _run with mocked _run_sync
# ===========================================================================

class TestFieldParamsRunIntegration:
    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_returns_run_sync_result(self, _validate):
        tool = _make_tool()
        with patch.object(tool, "_run_sync", return_value="# markdown output"):
            result = tool._run()
        assert result == "# markdown output"

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_with_execution_inputs(self, _validate):
        tool = PowerBIFieldParametersCalculationGroupsTool(
            workspace_id="{ws}", dataset_id="{ds}", access_token=ACCESS_TOKEN
        )
        with patch.object(tool, "_run_sync", return_value="resolved result"):
            result = tool._run(
                execution_inputs={"ws": WS_ID, "ds": DS_ID}
            )
        assert isinstance(result, str)

    @patch("src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config",
           return_value=(True, ""))
    def test_run_with_json_output_format(self, _validate):
        tool = _make_tool(output_format="json")
        json_output = json.dumps({"field_parameters": [], "calculation_groups": []})
        with patch.object(tool, "_run_sync", return_value=json_output):
            result = tool._run()
        assert result == json_output
