"""
Unit tests for engines/crewai/tools/custom/powerbi_field_parameters_calculation_groups_tool.py

Tests Power BI Field Parameters and Calculation Groups extraction tool for CrewAI.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
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

    def test_tool_initialization(self):
        """Test tool initializes with correct name and description"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        assert tool.name == "Power BI Field Parameters and Calculation Groups Tool"
        assert "field parameters" in tool.description.lower() or "calculation groups" in tool.description.lower()
        assert tool.args_schema == PowerBIFieldParametersCalculationGroupsSchema

    @pytest.mark.asyncio
    async def test_run_with_field_parameters_success(self):
        """Test successful extraction of field parameters"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        mock_tmdl = """
        table 'Measure Selection'
            lineageTag: field-parameter-table

            column 'Measure Fields'
                dataType: string

            column 'Measure Names'
                dataType: string

            measure 'Selected Measure' =
                SELECTEDVALUE('Measure Selection'[Measure Fields])
        """

        mock_field_params = [
            {
                'name': 'Measure Selection',
                'type': 'field_parameter',
                'measures': ['Sales', 'Profit', 'Quantity'],
                'default': 'Sales'
            }
        ]

        with patch.object(tool, '_get_access_token', new_callable=AsyncMock) as mock_token, \
             patch.object(tool, '_fetch_semantic_model_definition', new_callable=AsyncMock) as mock_fetch, \
             patch.object(tool, '_parse_field_parameters', return_value=mock_field_params):

            mock_token.return_value = "mock_access_token"
            mock_fetch.return_value = mock_tmdl

            result = await tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "Measure Selection" in result
            assert "Sales" in result or "field parameter" in result.lower()

    @pytest.mark.asyncio
    async def test_run_with_calculation_groups_success(self):
        """Test successful extraction of calculation groups"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        mock_tmdl = """
        table 'Time Intelligence'
            lineageTag: calculation-group

            calculationGroup 'Time Calculations'
                calculationItem 'Current Period' =
                    SELECTEDMEASURE()

                calculationItem 'Prior Period' =
                    CALCULATE(SELECTEDMEASURE(), DATEADD('Date'[Date], -1, MONTH))

                calculationItem 'Year to Date' =
                    CALCULATE(SELECTEDMEASURE(), DATESYTD('Date'[Date]))
        """

        mock_calc_groups = [
            {
                'name': 'Time Calculations',
                'items': [
                    {'name': 'Current Period', 'expression': 'SELECTEDMEASURE()'},
                    {'name': 'Prior Period', 'expression': 'CALCULATE(...)'},
                    {'name': 'Year to Date', 'expression': 'CALCULATE(...)'}
                ]
            }
        ]

        with patch.object(tool, '_get_access_token', new_callable=AsyncMock) as mock_token, \
             patch.object(tool, '_fetch_semantic_model_definition', new_callable=AsyncMock) as mock_fetch, \
             patch.object(tool, '_parse_calculation_groups', return_value=mock_calc_groups):

            mock_token.return_value = "mock_access_token"
            mock_fetch.return_value = mock_tmdl

            result = await tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "Time Calculations" in result
            assert "Prior Period" in result or "Year to Date" in result

    @pytest.mark.asyncio
    async def test_run_with_both_features(self):
        """Test extraction of both field parameters and calculation groups"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        mock_field_params = [
            {'name': 'Measure Selection', 'type': 'field_parameter', 'measures': ['Sales'], 'default': 'Sales'}
        ]
        mock_calc_groups = [
            {'name': 'Time Calculations', 'items': [{'name': 'Current', 'expression': 'SELECTEDMEASURE()'}]}
        ]

        with patch.object(tool, '_get_access_token', new_callable=AsyncMock) as mock_token, \
             patch.object(tool, '_fetch_semantic_model_definition', new_callable=AsyncMock) as mock_fetch, \
             patch.object(tool, '_parse_field_parameters', return_value=mock_field_params), \
             patch.object(tool, '_parse_calculation_groups', return_value=mock_calc_groups):

            mock_token.return_value = "mock_access_token"
            mock_fetch.return_value = "mock_tmdl"

            result = await tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "Measure Selection" in result
            assert "Time Calculations" in result

    @pytest.mark.asyncio
    async def test_run_with_no_features(self):
        """Test when model has no field parameters or calculation groups"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        with patch.object(tool, '_get_access_token', new_callable=AsyncMock) as mock_token, \
             patch.object(tool, '_fetch_semantic_model_definition', new_callable=AsyncMock) as mock_fetch, \
             patch.object(tool, '_parse_field_parameters', return_value=[]), \
             patch.object(tool, '_parse_calculation_groups', return_value=[]):

            mock_token.return_value = "mock_access_token"
            mock_fetch.return_value = "table Sales\n    column Revenue\n"

            result = await tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            assert "no field parameters" in result.lower() or "not found" in result.lower()

    @pytest.mark.asyncio
    async def test_run_with_user_token(self):
        """Test extraction with user OAuth token"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        mock_field_params = [
            {'name': 'Measure Selection', 'type': 'field_parameter', 'measures': ['Sales'], 'default': 'Sales'}
        ]

        with patch.object(tool, '_fetch_semantic_model_definition', new_callable=AsyncMock) as mock_fetch, \
             patch.object(tool, '_parse_field_parameters', return_value=mock_field_params), \
             patch.object(tool, '_parse_calculation_groups', return_value=[]):

            mock_fetch.return_value = "mock_tmdl"

            result = await tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                access_token="user_oauth_token"
            )

            assert "Measure Selection" in result

    @pytest.mark.asyncio
    async def test_run_generates_databricks_equivalent(self):
        """Test that Databricks equivalent logic is generated"""
        tool = PowerBIFieldParametersCalculationGroupsTool()

        mock_field_params = [
            {
                'name': 'Measure Selection',
                'type': 'field_parameter',
                'measures': ['Sales Amount', 'Profit', 'Quantity'],
                'default': 'Sales Amount'
            }
        ]

        with patch.object(tool, '_get_access_token', new_callable=AsyncMock) as mock_token, \
             patch.object(tool, '_fetch_semantic_model_definition', new_callable=AsyncMock) as mock_fetch, \
             patch.object(tool, '_parse_field_parameters', return_value=mock_field_params), \
             patch.object(tool, '_parse_calculation_groups', return_value=[]):

            mock_token.return_value = "mock_access_token"
            mock_fetch.return_value = "mock_tmdl"

            result = await tool._run(
                workspace_id="workspace123",
                dataset_id="dataset456",
                tenant_id="tenant789",
                client_id="client012",
                client_secret="secret345"
            )

            # Check for Databricks equivalent suggestion
            assert "databricks" in result.lower() or "sql" in result.lower() or "equivalent" in result.lower()
