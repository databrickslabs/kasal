"""
Unit tests for engines/crewai/tools/custom/powerbi_connector_tool.py

Tests CrewAI integration tool for Power BI dataset extraction and conversion.
"""

import pytest
from unittest.mock import Mock, patch
from src.engines.crewai.tools.custom.powerbi_connector_tool import (
    PowerBIConnectorToolSchema,
    PowerBIConnectorTool
)


class TestPowerBIConnectorToolSchema:
    """Tests for PowerBIConnectorToolSchema Pydantic model"""

    def test_schema_initialization_minimal(self):
        """Test schema with minimal required parameters"""
        schema = PowerBIConnectorToolSchema(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789"
        )

        assert schema.semantic_model_id == "model123"
        assert schema.group_id == "workspace456"
        assert schema.access_token == "token789"
        assert schema.outbound_format == "dax"  # default
        assert schema.include_hidden is False  # default
        assert schema.filter_pattern is None
        assert schema.sql_dialect == "databricks"  # default
        assert schema.uc_catalog == "main"  # default
        assert schema.uc_schema == "default"  # default

    def test_schema_initialization_all_parameters(self):
        """Test schema with all parameters"""
        schema = PowerBIConnectorToolSchema(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="sql",
            include_hidden=True,
            filter_pattern="Sales.*",
            sql_dialect="postgresql",
            uc_catalog="test_catalog",
            uc_schema="test_schema",
            info_table_name="Custom Measures"
        )

        assert schema.outbound_format == "sql"
        assert schema.include_hidden is True
        assert schema.filter_pattern == "Sales.*"
        assert schema.sql_dialect == "postgresql"
        assert schema.uc_catalog == "test_catalog"
        assert schema.uc_schema == "test_schema"
        assert schema.info_table_name == "Custom Measures"


class TestPowerBIConnectorTool:
    """Tests for PowerBIConnectorTool CrewAI integration"""

    @pytest.fixture
    def tool(self):
        """Create PowerBIConnectorTool instance for testing"""
        return PowerBIConnectorTool()

    # ========== Initialization Tests ==========

    def test_tool_initialization(self, tool):
        """Test tool initializes correctly"""
        assert tool is not None
        assert tool.name == "Power BI Connector"
        assert "Extract measures from Power BI" in tool.description
        assert tool.args_schema == PowerBIConnectorToolSchema
        assert hasattr(tool, '_pipeline')

    def test_tool_has_required_attributes(self, tool):
        """Test tool has all required CrewAI tool attributes"""
        assert hasattr(tool, 'name')
        assert hasattr(tool, 'description')
        assert hasattr(tool, 'args_schema')
        assert hasattr(tool, '_run')

    # ========== Run Method Tests - Parameter Validation ==========

    def test_run_missing_semantic_model_id(self, tool):
        """Test _run fails with missing semantic_model_id"""
        result = tool._run(
            group_id="workspace456",
            access_token="token789"
        )

        assert "Error" in result
        assert "required" in result.lower()

    def test_run_missing_group_id(self, tool):
        """Test _run fails with missing group_id"""
        result = tool._run(
            semantic_model_id="model123",
            access_token="token789"
        )

        assert "Error" in result
        assert "required" in result.lower()

    def test_run_missing_access_token(self, tool):
        """Test _run fails with missing access_token"""
        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456"
        )

        assert "Error" in result
        assert "authentication" in result.lower()

    def test_run_invalid_outbound_format(self, tool):
        """Test _run fails with invalid outbound_format"""
        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="invalid_format"
        )

        assert "Error" in result
        assert "Invalid outbound_format" in result

    # ========== Run Method Tests - Success Paths ==========

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_dax_output_success(self, mock_pipeline, tool):
        """Test _run with DAX output format"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [
                {
                    "name": "Total Sales",
                    "expression": "SUM(Sales[Amount])",
                    "description": "Total sales amount"
                }
            ],
            "measure_count": 1,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="dax"
        )

        assert "Power BI Measures Converted to DAX" in result
        assert "Total Sales" in result
        assert "SUM(Sales[Amount])" in result
        assert "```dax" in result

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_sql_output_success(self, mock_pipeline, tool):
        """Test _run with SQL output format"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "SELECT SUM(amount) as total_sales FROM sales",
            "measure_count": 1,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="sql",
            sql_dialect="databricks"
        )

        assert "Power BI Measures Converted to SQL" in result
        assert "SELECT SUM(amount)" in result
        assert "```sql" in result

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_uc_metrics_output_success(self, mock_pipeline, tool):
        """Test _run with UC Metrics output format"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "version: 0.1\nmeasures:\n  - name: total_sales",
            "measure_count": 1,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="uc_metrics",
            uc_catalog="main",
            uc_schema="sales"
        )

        assert "Power BI Measures Converted to UC Metrics" in result
        assert "```yaml" in result
        assert "version: 0.1" in result

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_yaml_output_success(self, mock_pipeline, tool):
        """Test _run with YAML output format"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "kpis:\n  - technical_name: total_sales",
            "measure_count": 1,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="yaml"
        )

        assert "Power BI Measures Exported as YAML" in result
        assert "```yaml" in result

    # ========== Run Method Tests - Pipeline Parameters ==========

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_passes_correct_parameters_to_pipeline(self, mock_pipeline, tool):
        """Test _run passes all parameters correctly to pipeline"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [],
            "measure_count": 0,
            "errors": []
        }

        tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="sql",
            include_hidden=True,
            filter_pattern="Sales.*",
            sql_dialect="postgresql",
            info_table_name="Custom Measures"
        )

        # Verify pipeline.execute was called with correct parameters
        mock_pipeline.execute.assert_called_once()
        call_args = mock_pipeline.execute.call_args

        # Check inbound_params
        inbound_params = call_args[1]['inbound_params']
        assert inbound_params['semantic_model_id'] == "model123"
        assert inbound_params['group_id'] == "workspace456"
        assert inbound_params['access_token'] == "token789"
        assert inbound_params['info_table_name'] == "Custom Measures"

        # Check extract_params
        extract_params = call_args[1]['extract_params']
        assert extract_params['include_hidden'] is True
        assert extract_params['filter_pattern'] == "Sales.*"

        # Check outbound_params
        outbound_params = call_args[1]['outbound_params']
        assert outbound_params['dialect'] == "postgresql"

    # ========== Run Method Tests - Error Handling ==========

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_pipeline_failure(self, mock_pipeline, tool):
        """Test _run handles pipeline execution failure"""
        mock_pipeline.execute.return_value = {
            "success": False,
            "output": None,
            "measure_count": 0,
            "errors": ["Connection failed", "Authentication error"]
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789"
        )

        assert "Error" in result
        assert "Conversion failed" in result
        assert "Connection failed" in result or "Authentication error" in result

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_exception_handling(self, mock_pipeline, tool):
        """Test _run handles exceptions gracefully"""
        mock_pipeline.execute.side_effect = Exception("Unexpected error")

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789"
        )

        assert "Error" in result
        assert "Unexpected error" in result

    # ========== Run Method Tests - Multiple Measures ==========

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_multiple_measures_dax_output(self, mock_pipeline, tool):
        """Test _run with multiple measures in DAX format"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [
                {
                    "name": "Total Sales",
                    "expression": "SUM(Sales[Amount])",
                    "description": "Total sales amount"
                },
                {
                    "name": "Average Price",
                    "expression": "AVERAGE(Products[Price])",
                    "description": None
                }
            ],
            "measure_count": 2,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="dax"
        )

        assert "Extracted 2 measures" in result
        assert "Total Sales" in result
        assert "Average Price" in result
        assert "SUM(Sales[Amount])" in result
        assert "AVERAGE(Products[Price])" in result

    # ========== Edge Cases ==========

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_no_measures_extracted(self, mock_pipeline, tool):
        """Test _run when no measures are extracted"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [],
            "measure_count": 0,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="dax"
        )

        assert "0 measures" in result

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_case_insensitive_outbound_format(self, mock_pipeline, tool):
        """Test _run handles case-insensitive outbound format"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": [],
            "measure_count": 0,
            "errors": []
        }

        # Test uppercase
        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="DAX"
        )
        assert "Error" not in result

        # Test mixed case
        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="Sql"
        )
        assert "Error" not in result

    @patch.object(PowerBIConnectorTool, '_pipeline')
    def test_run_with_all_optional_parameters(self, mock_pipeline, tool):
        """Test _run with all optional parameters specified"""
        mock_pipeline.execute.return_value = {
            "success": True,
            "output": "SELECT * FROM table",
            "measure_count": 1,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="sql",
            include_hidden=True,
            filter_pattern="Revenue.*",
            sql_dialect="snowflake",
            uc_catalog="analytics",
            uc_schema="metrics",
            info_table_name="Info Measures Custom"
        )

        assert "Error" not in result
        assert "Power BI Measures Converted to SQL" in result
