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

    # ========== Run Method Tests - Success Paths ==========

    @patch('src.converters.pipeline.ConversionPipeline')
    def test_run_dax_output_success(self, mock_pipeline_class, tool):
        """Test _run with DAX output format"""
        mock_instance = Mock()
        mock_pipeline_class.return_value = mock_instance
        mock_instance.execute.return_value = {
            "success": True,
            "output": [
                {"name": "Total Sales", "expression": "SUM(Sales[Amount])", "description": "Total"}
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

    @patch('src.converters.pipeline.ConversionPipeline')
    def test_run_sql_output_success(self, mock_pipeline_class, tool):
        """Test _run with SQL output format"""
        mock_instance = Mock()
        mock_pipeline_class.return_value = mock_instance
        mock_instance.execute.return_value = {
            "success": True,
            "output": "SELECT SUM(amount) as total_sales FROM sales",
            "measure_count": 1,
            "errors": []
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789",
            outbound_format="sql"
        )

        assert "Power BI Measures Converted to SQL" in result
        assert "SELECT SUM(amount)" in result

    @patch('src.converters.pipeline.ConversionPipeline')
    def test_run_pipeline_failure(self, mock_pipeline_class, tool):
        """Test _run handles pipeline execution failure"""
        mock_instance = Mock()
        mock_pipeline_class.return_value = mock_instance
        mock_instance.execute.return_value = {
            "success": False,
            "output": None,
            "measure_count": 0,
            "errors": ["Connection failed"]
        }

        result = tool._run(
            semantic_model_id="model123",
            group_id="workspace456",
            access_token="token789"
        )

        assert "Error" in result
        assert "Conversion failed" in result
