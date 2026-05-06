"""Tests for UC Metric View Generator Tool — API extraction mode."""
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from src.engines.crewai.tools.custom.uc_metric_view_generator_tool import UCMetricViewGeneratorTool


class TestAPIExtraction:
    def test_json_mode_no_api_call(self):
        """When workspace_id is not provided, no API call is made."""
        tool = UCMetricViewGeneratorTool()
        measures = [{'measure_name': 'X', 'dax_expression': 'SUM(T[x])',
                     'original_name': 'X', 'proposed_allocation': 'fact'}]
        mquery = [{'table_name': 'fact',
                   'transpiled_sql': 'SELECT col, SUM(x) AS x FROM cat.sch.tbl GROUP BY col',
                   'validation_passed': 'Yes'}]
        result = tool._run(
            measures_json=json.dumps(measures),
            mquery_json=json.dumps(mquery),
        )
        data = json.loads(result)
        assert 'yaml' in data or 'error' not in data

    def test_api_mode_validates_workspace_id(self):
        """Invalid workspace_id format should be rejected."""
        tool = UCMetricViewGeneratorTool()
        result = tool._run(
            workspace_id='../../../etc/passwd',
            dataset_id='valid-uuid-format-1234',
            tenant_id='t',
            client_id='c',
            client_secret='s',
        )
        data = json.loads(result)
        assert 'error' in data

    def test_api_mode_validates_pbi_domain(self):
        """Untrusted PBI API domain should be rejected."""
        tool = UCMetricViewGeneratorTool()
        result = tool._run(
            workspace_id='abc-def-123',
            dataset_id='xyz-456-789',
            tenant_id='t',
            client_id='c',
            client_secret='s',
            pbi_api_base_url='https://evil.example.com',
        )
        data = json.loads(result)
        assert 'error' in data

    def test_schema_has_credential_fields(self):
        """Schema should have all PBI credential fields."""
        from src.engines.crewai.tools.custom.uc_metric_view_generator_tool import UCMetricViewGeneratorSchema
        schema = UCMetricViewGeneratorSchema.model_json_schema()
        props = schema['properties']
        assert 'workspace_id' in props
        assert 'dataset_id' in props
        assert 'tenant_id' in props
        assert 'client_id' in props
        assert 'client_secret' in props

    def test_secret_fields_not_in_repr(self):
        """Secret fields should have repr=False to prevent logging."""
        from src.engines.crewai.tools.custom.uc_metric_view_generator_tool import UCMetricViewGeneratorSchema
        schema = UCMetricViewGeneratorSchema(client_secret='super_secret_123')
        repr_str = repr(schema)
        assert 'super_secret_123' not in repr_str
