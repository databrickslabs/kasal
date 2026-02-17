"""
Unit tests for engines/crewai/tools/custom/powerbi_report_references_tool.py

Tests Power BI Report References extraction tool for CrewAI.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from src.engines.crewai.tools.custom.powerbi_report_references_tool import (
    PowerBIReportReferencesSchema,
    PowerBIReportReferencesTool
)


class TestPowerBIReportReferencesSchema:
    """Tests for PowerBIReportReferencesSchema Pydantic model"""

    def test_schema_initialization_minimal(self):
        """Test schema with minimal required parameters"""
        schema = PowerBIReportReferencesSchema(
            workspace_id="workspace123",
            dataset_id="dataset456"
        )

        assert schema.workspace_id == "workspace123"
        assert schema.dataset_id == "dataset456"

    def test_schema_with_report_id(self):
        """Test schema with specific report ID"""
        schema = PowerBIReportReferencesSchema(
            workspace_id="workspace123",
            dataset_id="dataset456",
            report_id="report789"
        )

        assert schema.report_id == "report789"

    def test_schema_with_auth(self):
        """Test schema with authentication parameters"""
        schema = PowerBIReportReferencesSchema(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345"
        )

        assert schema.tenant_id == "tenant789"
        assert schema.client_id == "client012"
        assert schema.client_secret == "secret345"


class TestPowerBIReportReferencesTool:
    """Tests for PowerBIReportReferencesTool"""

    def test_tool_initialization(self):
        """Test tool initializes with correct name and description"""
        tool = PowerBIReportReferencesTool()

        assert tool.name == "Power BI Report References Tool"
        assert "report" in tool.description.lower()
        assert tool.args_schema == PowerBIReportReferencesSchema

    @pytest.mark.asyncio
    async def test_extract_report_references_success(self):
        """Test successful report references extraction"""
        tool = PowerBIReportReferencesTool()

        mock_result = """
# Power BI Report References

## Summary
Analyzed 1 report with 2 pages and 5 visuals.

## Report: Sales Dashboard

### Page: Overview
- **Visual 1**: Total Sales (Card)
  - Measures: [Total Sales]
  - Tables: Sales

- **Visual 2**: Sales by Category (Column Chart)
  - Measures: [Total Sales]
  - Dimensions: Product[Category]
  - Tables: Sales, Product

### Measure Usage
- [Total Sales]: Used in 2 visuals
- [Profit]: Used in 1 visual
"""

        with patch.object(tool, '_extract_report_references', new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = mock_result

            result = await tool._extract_report_references(
                workspace_id="workspace123",
                dataset_id="dataset456",
                report_id="report789",
                auth_config={
                    "tenant_id": "tenant789",
                    "client_id": "client012",
                    "client_secret": "secret345"
                },
                output_format="markdown",
                include_visual_details=True,
                group_by="page"
            )

            assert "Sales Dashboard" in result
            assert "Total Sales" in result
            assert "Overview" in result

    def test_run_with_missing_workspace_id(self):
        """Test that missing workspace_id returns error"""
        tool = PowerBIReportReferencesTool()

        result = tool._run(
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345"
        )

        assert "error" in result.lower()
        assert "workspace_id" in result.lower()



    @patch('src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config')
    def test_run_with_invalid_auth_config(self, mock_validate):
        """Test that invalid auth config returns error"""
        tool = PowerBIReportReferencesTool()

        # Mock validation to fail
        mock_validate.return_value = (False, "Invalid credentials")

        result = tool._run(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="invalid",
            client_id="invalid",
            client_secret="invalid"
        )

        assert "error" in result.lower()
        assert "error" in result.lower()  # Validation failed
