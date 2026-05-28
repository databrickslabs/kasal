"""Unit tests for UCMVGenieConfigGeneratorTool (Tool 93)."""
import json
from unittest.mock import MagicMock, patch

import pytest

from src.engines.crewai.tools.custom.ucmv_genie_config_generator_tool import (
    UCMVGenieConfigGeneratorTool,
    UCMVGenieConfigGeneratorSchema,
)


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

SAMPLE_YAML = (
    "name: fact_sales_uc_metric_view\n"
    "source: main.metrics.fact_sales\n"
    "comment: Sales metric view\n"
    "dimensions:\n"
    "  - name: region\n"
    "    expr: region\n"
    "  - name: product\n"
    "    expr: product_name\n"
    "measures:\n"
    "  - name: total_revenue\n"
    "    expr: SUM(revenue)\n"
    "  - name: order_count\n"
    "    expr: COUNT(order_id)\n"
)

SAMPLE_YAML_WITH_JOINS = (
    "name: fact_orders_uc_metric_view\n"
    "source: main.metrics.fact_orders\n"
    "dimensions:\n"
    "  - name: customer_id\n"
    "    expr: customer_id\n"
    "measures:\n"
    "  - name: total_orders\n"
    "    expr: COUNT(order_id)\n"
    "joins:\n"
    "  - name: dim_customer\n"
    "    source: main.raw.dim_customer\n"
    "    on: fact_orders.customer_id = dim_customer.customer_id\n"
)

SAMPLE_UCMV_OUTPUT = json.dumps({
    "yaml": {
        "fact_sales": SAMPLE_YAML,
        "fact_orders": SAMPLE_YAML_WITH_JOINS,
    },
    "sql": {
        "fact_sales": "CREATE METRIC VIEW ...",
        "fact_orders": "CREATE METRIC VIEW ...",
    },
})

SAMPLE_UCMV_WITH_FILTER_TABLES = json.dumps({
    "yaml": {
        "fact_sales": SAMPLE_YAML,
    },
    "sql": {},
    "deployment_results": {},
})


def _mock_auth(workspace_url="https://test.azuredatabricks.net"):
    auth = MagicMock()
    auth.workspace_url = workspace_url
    auth.get_headers.return_value = {"Authorization": "Bearer test-token"}
    return auth


def _mock_litellm_response(content='{"text_instructions": "Test instructions", "sample_questions": "Q1\nQ2", "example_sqls_json": "[]"}'):
    mock_completion = MagicMock()
    mock_completion.choices[0].message.content = content
    return mock_completion


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestUCMVGenieConfigGeneratorSchema:
    def test_all_fields_optional(self):
        schema = UCMVGenieConfigGeneratorSchema()
        assert schema.ucmv_output is None
        assert schema.genie_config_override is None
        assert schema.space_title is None
        assert schema.catalog is None
        assert schema.schema_name is None
        assert schema.warehouse_id is None
        assert schema.databricks_host is None
        assert schema.llm_model is None

    def test_fields_accepted(self):
        schema = UCMVGenieConfigGeneratorSchema(
            ucmv_output=SAMPLE_UCMV_OUTPUT,
            space_title="My Genie Space",
            catalog="main",
            schema_name="metrics",
            warehouse_id="wh-123",
        )
        assert schema.ucmv_output == SAMPLE_UCMV_OUTPUT
        assert schema.space_title == "My Genie Space"


# ---------------------------------------------------------------------------
# Tool initialization
# ---------------------------------------------------------------------------

class TestToolInit:
    def test_tool_name(self):
        tool = UCMVGenieConfigGeneratorTool()
        assert tool.name == "UCMV Genie Space Config Generator"

    def test_description_present(self):
        tool = UCMVGenieConfigGeneratorTool()
        assert "Genie" in tool.description

    def test_ucmv_output_in_default_config(self):
        tool = UCMVGenieConfigGeneratorTool()
        assert "ucmv_output" in tool._default_config

    def test_static_config_stored_on_init(self):
        tool = UCMVGenieConfigGeneratorTool(
            space_title="Test Space",
            catalog="main",
            schema_name="metrics",
            warehouse_id="abc123",
        )
        assert tool._default_config["space_title"] == "Test Space"
        assert tool._default_config["catalog"] == "main"
        assert tool._default_config["warehouse_id"] == "abc123"


# ---------------------------------------------------------------------------
# Override path (skip LLM)
# ---------------------------------------------------------------------------

class TestGenieConfigOverride:
    def test_genie_config_override_skips_llm(self):
        """genie_config_override set → returns it directly, no LLM call."""
        override = json.dumps({
            "text_instructions": "Custom instructions",
            "sample_questions": "Custom question",
            "example_sqls_json": "[]",
        })
        tool = UCMVGenieConfigGeneratorTool()
        with patch("litellm.completion") as mock_llm:
            result = tool._run(
                genie_config_override=override,
                ucmv_output=SAMPLE_UCMV_OUTPUT,
                catalog="main",
                schema_name="metrics",
            )
            # LLM should NOT be called
            mock_llm.assert_not_called()
        data = json.loads(result)
        assert data["text_instructions"] == "Custom instructions"

    def test_override_merges_connection_params(self):
        """Override without catalog → catalog from _default_config is merged in."""
        override = json.dumps({
            "text_instructions": "Some instructions",
        })
        tool = UCMVGenieConfigGeneratorTool(catalog="injected_cat", schema_name="injected_sch")
        result = tool._run(genie_config_override=override)
        data = json.loads(result)
        # catalog should be merged in from defaults
        assert data.get("catalog") == "injected_cat"
        assert data.get("schema_name") == "injected_sch"

    def test_override_existing_keys_not_overwritten(self):
        """Override's own catalog should not be overwritten by defaults."""
        override = json.dumps({
            "text_instructions": "Some instructions",
            "catalog": "my_own_catalog",
        })
        tool = UCMVGenieConfigGeneratorTool(catalog="default_catalog")
        result = tool._run(genie_config_override=override)
        data = json.loads(result)
        # Override's catalog wins because setdefault is used
        assert data["catalog"] == "my_own_catalog"


# ---------------------------------------------------------------------------
# Error cases without override
# ---------------------------------------------------------------------------

class TestErrorCases:
    def test_no_ucmv_output_returns_error(self):
        """Neither ucmv_output nor override → error JSON."""
        tool = UCMVGenieConfigGeneratorTool()
        result = tool._run()
        data = json.loads(result)
        assert "error" in data
        assert "ucmv_output" in data["error"]

    def test_empty_ucmv_output_returns_error(self):
        """ucmv_output with empty yaml dict → error."""
        empty_ucmv = json.dumps({"yaml": {}, "sql": {}})
        tool = UCMVGenieConfigGeneratorTool()
        result = tool._run(ucmv_output=empty_ucmv)
        data = json.loads(result)
        assert "error" in data


# ---------------------------------------------------------------------------
# UCMV output parsing
# ---------------------------------------------------------------------------

class TestUcmvParsing:
    def test_extracts_yaml_specs_from_ucmv_output(self):
        """_extract_yaml_specs returns {table_key: yaml_str} from ucmv_output."""
        tool = UCMVGenieConfigGeneratorTool()
        specs = tool._extract_yaml_specs(SAMPLE_UCMV_OUTPUT)
        assert isinstance(specs, dict)
        assert "fact_sales" in specs
        assert "fact_orders" in specs

    def test_extract_yaml_specs_from_invalid_json(self):
        """Invalid JSON returns empty dict."""
        tool = UCMVGenieConfigGeneratorTool()
        specs = tool._extract_yaml_specs("not-json{{{")
        assert specs == {}

    def test_extract_yaml_specs_no_yaml_key(self):
        """ucmv_output without 'yaml' key returns empty dict."""
        tool = UCMVGenieConfigGeneratorTool()
        specs = tool._extract_yaml_specs(json.dumps({"sql": {}}))
        assert specs == {}


# ---------------------------------------------------------------------------
# YAML spec parsing
# ---------------------------------------------------------------------------

class TestParseYamlSpec:
    def test_parse_yaml_spec_extracts_measures(self):
        """_parse_yaml_spec() on valid YAML returns dict with measures."""
        tool = UCMVGenieConfigGeneratorTool()
        spec = tool._parse_yaml_spec(SAMPLE_YAML)
        assert isinstance(spec, dict)
        assert "measures" in spec
        measures = spec["measures"]
        assert len(measures) >= 1
        assert any(m.get("name") == "total_revenue" for m in measures)

    def test_parse_yaml_spec_extracts_dimensions(self):
        tool = UCMVGenieConfigGeneratorTool()
        spec = tool._parse_yaml_spec(SAMPLE_YAML)
        assert "dimensions" in spec
        dims = spec["dimensions"]
        assert any(d.get("name") == "region" for d in dims)

    def test_parse_yaml_spec_handles_invalid_yaml(self):
        """Invalid YAML returns empty dict."""
        tool = UCMVGenieConfigGeneratorTool()
        spec = tool._parse_yaml_spec("{not: valid: yaml:")
        assert isinstance(spec, dict)

    def test_parse_yaml_spec_empty_string(self):
        tool = UCMVGenieConfigGeneratorTool()
        spec = tool._parse_yaml_spec("")
        assert spec == {}


# ---------------------------------------------------------------------------
# Join specs extraction
# ---------------------------------------------------------------------------

class TestJoinSpecsExtraction:
    def test_join_specs_extracted_from_yaml(self):
        """YAML with joins section → join_specs_json populated in output."""
        tool = UCMVGenieConfigGeneratorTool()
        auth = _mock_auth()
        llm_resp = _mock_litellm_response()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("litellm.completion", return_value=llm_resp):
            result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, catalog="main", schema_name="metrics")

        data = json.loads(result)
        join_specs_raw = data.get("join_specs_json", "[]")
        join_specs = json.loads(join_specs_raw) if isinstance(join_specs_raw, str) else join_specs_raw
        # fact_orders has a join to dim_customer
        assert len(join_specs) >= 1
        assert any("dim_customer" in str(j) for j in join_specs)

    def test_dimension_tables_from_joins(self):
        """_extract_dimension_tables returns tables referenced in join sources."""
        tool = UCMVGenieConfigGeneratorTool()
        specs = {
            "fact_orders": SAMPLE_YAML_WITH_JOINS,
        }
        tables = tool._extract_dimension_tables(specs)
        assert "main.raw.dim_customer" in tables


# ---------------------------------------------------------------------------
# Dimension table filtering
# ---------------------------------------------------------------------------

class TestDimTableFiltering:
    def test_dim_tables_filtering(self):
        """Tables with dc_datalake_prod_001__ in name are excluded from additional_tables."""
        # Build YAML with joins that include filtered table names
        yaml_with_filtered = (
            "name: fact_test\n"
            "source: main.s.t\n"
            "joins:\n"
            "  - name: dim_ok\n"
            "    source: main.raw.dim_ok\n"
            "    on: t.id = dim_ok.id\n"
            "  - name: filtered_table\n"
            "    source: main.dc_datalake_prod_001__something.table\n"
            "    on: t.id = x.id\n"
        )
        ucmv = json.dumps({"yaml": {"fact_test": yaml_with_filtered}, "sql": {}})
        tool = UCMVGenieConfigGeneratorTool()
        auth = _mock_auth()
        llm_resp = _mock_litellm_response()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("litellm.completion", return_value=llm_resp):
            result = tool._run(ucmv_output=ucmv, catalog="main", schema_name="metrics")

        data = json.loads(result)
        additional_tables = data.get("additional_tables", "")
        # The filtered table should NOT appear in additional_tables
        assert "dc_datalake_prod_001__" not in additional_tables
        # The valid dim table should appear
        assert "dim_ok" in additional_tables


# ---------------------------------------------------------------------------
# LLM integration
# ---------------------------------------------------------------------------

class TestLLMIntegration:
    def test_llm_called_with_correct_model(self):
        """Mock litellm, verify model used."""
        tool = UCMVGenieConfigGeneratorTool(llm_model="databricks-claude-sonnet-4")
        auth = _mock_auth()
        llm_resp = _mock_litellm_response()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("litellm.completion", return_value=llm_resp) as mock_llm:
            tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, catalog="main", schema_name="metrics")

        mock_llm.assert_called_once()
        call_kwargs = mock_llm.call_args
        # Model should be prefixed with databricks/
        model_arg = call_kwargs[1].get("model") or call_kwargs[0][0]
        assert "databricks" in model_arg

    def test_llm_failure_returns_partial_config(self):
        """litellm raises exception → falls back to empty generated, still returns valid output."""
        tool = UCMVGenieConfigGeneratorTool()
        auth = _mock_auth()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("litellm.completion", side_effect=RuntimeError("LLM unavailable")):
            result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, catalog="main", schema_name="metrics")

        data = json.loads(result)
        # Should return valid JSON (no exception), even if LLM failed
        assert isinstance(data, dict)
        # Must not have "error" at top level (fallback path is used)
        # But it should have the structural keys
        assert "space_title" in data
        assert "ucmv_output" in data

    def test_output_contains_ucmv_output_passthrough(self):
        """Output JSON has 'ucmv_output' key with original ucmv_raw."""
        tool = UCMVGenieConfigGeneratorTool()
        auth = _mock_auth()
        llm_resp = _mock_litellm_response()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("litellm.completion", return_value=llm_resp):
            result = tool._run(ucmv_output=SAMPLE_UCMV_OUTPUT, catalog="main", schema_name="metrics")

        data = json.loads(result)
        assert "ucmv_output" in data
        assert data["ucmv_output"] == SAMPLE_UCMV_OUTPUT

    def test_output_has_connection_params(self):
        """Output JSON has catalog, schema_name, warehouse_id, space_title."""
        tool = UCMVGenieConfigGeneratorTool()
        auth = _mock_auth()
        llm_resp = _mock_litellm_response()

        with patch.object(tool, "_authenticate", return_value=auth), \
             patch("litellm.completion", return_value=llm_resp):
            result = tool._run(
                ucmv_output=SAMPLE_UCMV_OUTPUT,
                catalog="mycat",
                schema_name="mysch",
                warehouse_id="wh-abc",
                space_title="My Space",
            )

        data = json.loads(result)
        assert data["catalog"] == "mycat"
        assert data["schema_name"] == "mysch"
        assert data["warehouse_id"] == "wh-abc"
        assert data["space_title"] == "My Space"
