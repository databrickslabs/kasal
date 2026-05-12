"""Tests for ConfigGeneratorTool."""
import json
import pytest
from unittest.mock import patch, MagicMock


class TestConfigGeneratorSchema:
    def test_schema_defaults(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorSchema
        schema = ConfigGeneratorSchema()
        assert schema.measures_json is None
        assert schema.catalog is None

    def test_schema_with_values(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorSchema
        schema = ConfigGeneratorSchema(measures_json='[]', catalog='test')
        assert schema.measures_json == '[]'
        assert schema.catalog == 'test'


class TestConfigGeneratorTool:
    def test_tool_instantiation(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool()
        assert tool.name == "Config Generator"

    def test_run_empty_inputs(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool()
        result = tool._run(measures_json='[]', mquery_json='[]')
        parsed = json.loads(result)
        assert 'proposed_config' in parsed
        assert 'confidence' in parsed

    def test_run_returns_valid_json(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool()
        result = tool._run(measures_json='[]', mquery_json='[]')
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_run_with_measures(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool()
        measures = json.dumps([
            {'measure_name': 'Total Revenue', 'proposed_allocation': 'fact_sales',
             'dax_expression': 'SUM(fact_sales[revenue])'},
        ])
        mquery = json.dumps([
            {'table_name': 'fact_sales', 'table_type': 'fact',
             'transpiled_sql': 'SELECT SUM(revenue) AS revenue FROM raw.sales GROUP BY region',
             'all_table_refs': [], 'direct_fact_refs': []},
        ])
        result = tool._run(measures_json=measures, mquery_json=mquery)
        parsed = json.loads(result)
        assert 'proposed_config' in parsed

    def test_run_with_default_config(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool(catalog='my_catalog', schema_name='my_schema')
        result = tool._run(measures_json='[]', mquery_json='[]')
        parsed = json.loads(result)
        assert 'proposed_config' in parsed

    def test_run_error_handling(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool()
        result = tool._run(measures_json='invalid json{{{', mquery_json='[]')
        parsed = json.loads(result)
        assert 'error' in parsed

    def test_switch_decomposition_detection(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool()
        measures = json.dumps([
            {'measure_name': 'KBI_Wrapper', 'proposed_allocation': 'fact_plan',
             'dax_expression': 'var a = SELECTEDVALUE(Mapping[Index])\nSWITCH(TRUE(), a="Rev", [Revenue], a="Cost", [Cost])'},
        ])
        mquery = json.dumps([
            {'table_name': 'fact_plan', 'table_type': 'fact',
             'transpiled_sql': 'SELECT SUM(val) AS val FROM raw.plan GROUP BY key',
             'all_table_refs': [], 'direct_fact_refs': []},
        ])
        result = tool._run(measures_json=measures, mquery_json=mquery)
        parsed = json.loads(result)
        sw = parsed['proposed_config'].get('switch_decompositions', {})
        assert 'fact_plan' in sw
        assert len(sw['fact_plan']) > 0

    def test_summary_keys(self):
        from src.engines.crewai.tools.custom.config_generator_tool import ConfigGeneratorTool
        tool = ConfigGeneratorTool()
        result = tool._run(measures_json='[]', mquery_json='[]')
        parsed = json.loads(result)
        assert 'summary' in parsed
        assert 'keys_proposed' in parsed['summary']
        assert 'keys_total' in parsed['summary']
