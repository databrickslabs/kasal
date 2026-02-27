"""
Unit tests for engines/crewai/tools/custom/powerbi_field_parameters_calculation_groups_tool.py

Tests Power BI Field Parameters and Calculation Groups extraction tool for CrewAI.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.engines.crewai.tools.custom.powerbi_field_parameters_calculation_groups_tool import (
    PowerBIFieldParametersCalculationGroupsSchema,
    PowerBIFieldParametersCalculationGroupsTool
)


class TestPowerBIFieldParametersCalculationGroupsSchema:
    """Tests for PowerBIFieldParametersCalculationGroupsSchema Pydantic model"""

    def test_schema_initialization_minimal(self):
        """Test schema with minimal required parameters"""
        schema = PowerBIFieldParametersCalculationGroupsSchema(
            workspace_id="workspace123",
            dataset_id="dataset456"
        )

        assert schema.workspace_id == "workspace123"
        assert schema.dataset_id == "dataset456"

    def test_schema_initialization_with_auth(self):
        """Test schema with service principal authentication"""
        schema = PowerBIFieldParametersCalculationGroupsSchema(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345"
        )

        assert schema.tenant_id == "tenant789"
        assert schema.client_id == "client012"
        assert schema.client_secret == "secret345"


class TestPowerBIFieldParametersCalculationGroupsTool:
    """Tests for PowerBIFieldParametersCalculationGroupsTool"""

    def _make_tool(self, **kwargs):
        """Create a tool instance with the given config values baked into _default_config."""
        tool = PowerBIFieldParametersCalculationGroupsTool(**kwargs)
        return tool

    def test_tool_initialization(self):
        """Test tool initializes with correct name and description"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        assert tool.name == "Power BI Field Parameters & Calculation Groups Tool"
        assert "field parameters" in tool.description.lower() or "calculation groups" in tool.description.lower()
        assert tool.args_schema == PowerBIFieldParametersCalculationGroupsSchema

    def test_run_with_field_parameters_success(self):
        """Test successful extraction of field parameters"""
        tool = self._make_tool(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345",
        )

        mock_output = (
            "# Power BI Field Parameters & Calculation Groups Extraction Results\n"
            "**Field Parameters Found**: 1\n"
            "## Field Parameters\n"
            "### Measure Selection\n"
            "| Ordinal | Label | Source Table | Source Measure |\n"
            "| 0 | Sales | FactSales | Sales |\n"
        )

        with patch.object(tool, '_run_sync', return_value=mock_output):
            result = tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "Measure Selection" in result
            assert "Sales" in result or "field parameter" in result.lower()

    def test_run_with_calculation_groups_success(self):
        """Test successful extraction of calculation groups"""
        tool = self._make_tool(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345",
        )

        mock_output = (
            "# Power BI Field Parameters & Calculation Groups Extraction Results\n"
            "## Calculation Groups\n"
            "### Time Calculations\n"
            "#### Current Period\n"
            "```dax\nSELECTEDMEASURE()\n```\n"
            "#### Prior Period\n"
            "```dax\nCALCULATE(SELECTEDMEASURE(), DATEADD('Date'[Date], -1, MONTH))\n```\n"
            "#### Year to Date\n"
            "```dax\nCALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))\n```\n"
        )

        with patch.object(tool, '_run_sync', return_value=mock_output):
            result = tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "Time Calculations" in result
            assert "Prior Period" in result or "Year to Date" in result

    def test_run_with_both_features(self):
        """Test extraction of both field parameters and calculation groups"""
        tool = self._make_tool(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345",
        )

        mock_output = (
            "# Power BI Field Parameters & Calculation Groups Extraction Results\n"
            "## Field Parameters\n"
            "### Measure Selection\n"
            "| 0 | Sales | FactSales | Sales |\n"
            "## Calculation Groups\n"
            "### Time Calculations\n"
            "#### Current\n"
            "```dax\nSELECTEDMEASURE()\n```\n"
        )

        with patch.object(tool, '_run_sync', return_value=mock_output):
            result = tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "Measure Selection" in result
            assert "Time Calculations" in result

    def test_run_with_no_features(self):
        """Test when model has no field parameters or calculation groups"""
        tool = self._make_tool(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345",
        )

        mock_output = (
            "# Power BI Field Parameters & Calculation Groups Extraction Results\n"
            "**Field Parameters Found**: 0\n"
            "**Calculation Groups Found**: 0\n"
            "No field parameters or calculation groups found in this model.\n"
        )

        with patch.object(tool, '_run_sync', return_value=mock_output):
            result = tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "0" in result or "not found" in result.lower() or "no field parameters" in result.lower()

    def test_run_with_user_token(self):
        """Test extraction with user OAuth token"""
        tool = self._make_tool(
            workspace_id="workspace123",
            dataset_id="dataset456",
            access_token="user_oauth_token",
        )

        mock_output = (
            "# Power BI Field Parameters & Calculation Groups Extraction Results\n"
            "## Field Parameters\n"
            "### Measure Selection\n"
            "| 0 | Sales | FactSales | Sales |\n"
        )

        with patch.object(tool, '_run_sync', return_value=mock_output):
            result = tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                access_token="user_oauth_token"
            )

            assert "Measure Selection" in result

    def test_run_generates_databricks_equivalent(self):
        """Test that Databricks equivalent logic is generated"""
        tool = self._make_tool(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345",
        )

        mock_output = (
            "# Power BI Field Parameters & Calculation Groups Extraction Results\n"
            "## Field Parameters\n"
            "### Measure Selection\n"
            "| 0 | Sales Amount | FactSales | Sales Amount |\n"
            "| 1 | Profit | FactSales | Profit |\n"
            "| 2 | Quantity | FactSales | Quantity |\n"
            "## Unity Catalog SQL\n"
            "### Field Parameters Config Table\n"
            "```sql\nCREATE TABLE IF NOT EXISTS main.default._config_field_parameters ...\n```\n"
            "## Summary\n"
        )

        with patch.object(tool, '_run_sync', return_value=mock_output):
            result = tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            # Check for Databricks equivalent suggestion
            assert "sql" in result.lower() or "databricks" in result.lower() or "equivalent" in result.lower()
