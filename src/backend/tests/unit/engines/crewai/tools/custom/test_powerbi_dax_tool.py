"""
Unit tests for PowerBIDAXTool.

Tests the functionality of Power BI DAX tool including
schema validation, query execution, and error handling.
"""
import unittest
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import asyncio

from src.engines.crewai.tools.custom.powerbi_dax_tool import (
    PowerBIDAXTool,
    PowerBIDAXToolSchema
)
from src.schemas.powerbi_config import DAXQueryResponse


class TestPowerBIDAXToolSchema(unittest.TestCase):
    """Unit tests for PowerBIDAXToolSchema"""

    def test_valid_query_schema(self):
        """Test creating a valid query action schema"""
        schema = PowerBIDAXToolSchema(
            action="query",
            dax_query="EVALUATE 'Sales'",
            semantic_model_id="test-model"
        )

        assert schema.action == "query"
        assert schema.dax_query == "EVALUATE 'Sales'"
        assert schema.semantic_model_id == "test-model"

    def test_valid_analyze_schema(self):
        """Test creating a valid analyze action schema"""
        schema = PowerBIDAXToolSchema(
            action="analyze",
            questions=["What were sales last month?", "Top products?"]
        )

        assert schema.action == "analyze"
        assert len(schema.questions) == 2

    def test_schema_invalid_action(self):
        """Test schema validation with invalid action"""
        with pytest.raises(ValueError, match="Invalid action"):
            PowerBIDAXToolSchema(action="invalid")

    def test_schema_query_without_dax(self):
        """Test schema validation for query action without DAX"""
        with pytest.raises(ValueError, match="dax_query is required"):
            PowerBIDAXToolSchema(action="query")

    def test_schema_analyze_without_questions(self):
        """Test schema validation for analyze action without questions"""
        with pytest.raises(ValueError, match="questions are required"):
            PowerBIDAXToolSchema(action="analyze")

    def test_schema_case_insensitive_action(self):
        """Test that action validation is case-insensitive"""
        schema = PowerBIDAXToolSchema(
            action="QUERY",
            dax_query="EVALUATE 'Sales'"
        )
        assert schema.action == "QUERY"


class TestPowerBIDAXTool(unittest.TestCase):
    """Unit tests for PowerBIDAXTool"""

    def setUp(self):
        """Set up test environment"""
        self.tool = PowerBIDAXTool(group_id="test-group")

    def test_tool_initialization(self):
        """Test Power BI DAX tool initialization"""
        assert self.tool.name == "Power BI DAX Analyzer"
        assert self.tool._group_id == "test-group"
        assert "query" in self.tool.description.lower()

    def test_tool_initialization_no_group(self):
        """Test tool initialization without group_id"""
        tool = PowerBIDAXTool()
        assert tool._group_id is None

    def test_run_query_success(self):
        """Test successful query execution via _run"""
        mock_response = DAXQueryResponse(
            status="success",
            data=[{"Region": "East", "Total": 1000}],
            row_count=1,
            columns=["Region", "Total"],
            execution_time_ms=250
        )

        with patch.object(self.tool, '_execute_action', return_value="✅ Success") as mock_execute:
            result = self.tool._run(
                action="query",
                dax_query="EVALUATE 'Sales'",
                semantic_model_id="test-model"
            )

            assert "✅" in result or "Success" in result
            mock_execute.assert_called_once()

    def test_run_invalid_action(self):
        """Test _run with invalid action"""
        result = self.tool._run(action="invalid")
        assert "❌" in result

    @pytest.mark.asyncio
    async def test_execute_query_action_success(self):
        """Test successful query action execution"""
        mock_response = DAXQueryResponse(
            status="success",
            data=[{"Region": "East", "Total": 1000}],
            row_count=1,
            columns=["Region", "Total"],
            execution_time_ms=250
        )

        with patch('src.core.unit_of_work.UnitOfWork') as MockUOW:
            mock_uow = AsyncMock()
            MockUOW.return_value.__aenter__.return_value = mock_uow

            mock_service = AsyncMock()
            mock_service.execute_dax_query.return_value = mock_response

            with patch('src.services.powerbi_service.PowerBIService', return_value=mock_service):
                result = await self.tool._execute_query(
                    dax_query="EVALUATE 'Sales'",
                    semantic_model_id="test-model"
                )

                assert "✅" in result
                assert "1" in result  # row count
                assert "Region" in result

    @pytest.mark.asyncio
    async def test_execute_query_action_error(self):
        """Test query action execution with error"""
        mock_response = DAXQueryResponse(
            status="error",
            data=None,
            row_count=0,
            columns=None,
            error="Query failed",
            execution_time_ms=100
        )

        with patch('src.core.unit_of_work.UnitOfWork') as MockUOW:
            mock_uow = AsyncMock()
            MockUOW.return_value.__aenter__.return_value = mock_uow

            mock_service = AsyncMock()
            mock_service.execute_dax_query.return_value = mock_response

            with patch('src.services.powerbi_service.PowerBIService', return_value=mock_service):
                result = await self.tool._execute_query(
                    dax_query="INVALID DAX",
                    semantic_model_id="test-model"
                )

                assert "❌" in result
                assert "failed" in result.lower()

    @pytest.mark.asyncio
    async def test_execute_analyze_action(self):
        """Test analyze action execution"""
        result = await self.tool._analyze_questions(
            questions=["What were sales last month?"],
            semantic_model_id="test-model"
        )

        assert "📊" in result
        assert "Analysis" in result
        assert "sales last month" in result

    @pytest.mark.asyncio
    async def test_execute_action_exception_handling(self):
        """Test exception handling in _execute_action"""
        with patch.object(self.tool, '_execute_query', side_effect=Exception("Test error")):
            result = await self.tool._execute_action(
                action="query",
                dax_query="EVALUATE 'Sales'",
                semantic_model_id="test-model"
            )

            assert "❌" in result
            assert "Test error" in result


class TestPowerBIDAXToolIntegration:
    """Integration tests for PowerBIDAXTool"""

    def test_full_query_workflow(self):
        """Test complete query workflow from _run to result"""
        tool = PowerBIDAXTool(group_id="test-group")

        mock_response = DAXQueryResponse(
            status="success",
            data=[{"Sales": 1000}],
            row_count=1,
            columns=["Sales"],
            execution_time_ms=200
        )

        with patch('src.core.unit_of_work.UnitOfWork') as MockUOW:
            mock_uow = AsyncMock()
            MockUOW.return_value.__aenter__.return_value = mock_uow

            mock_service = AsyncMock()
            mock_service.execute_dax_query.return_value = mock_response

            with patch('src.services.powerbi_service.PowerBIService', return_value=mock_service):
                result = tool._run(
                    action="query",
                    dax_query="EVALUATE 'Sales'",
                    semantic_model_id="test-model"
                )

                assert isinstance(result, str)
                assert "✅" in result or "Success" in result

    def test_tool_uses_group_id(self):
        """Test that tool properly uses group_id"""
        tool = PowerBIDAXTool(group_id="specific-group")

        mock_response = DAXQueryResponse(
            status="success",
            data=[],
            row_count=0,
            columns=[],
            execution_time_ms=100
        )

        with patch('src.core.unit_of_work.UnitOfWork') as MockUOW:
            mock_uow = AsyncMock()
            MockUOW.return_value.__aenter__.return_value = mock_uow

            mock_service = AsyncMock()
            mock_service.execute_dax_query.return_value = mock_response

            with patch('src.services.powerbi_service.PowerBIService', return_value=mock_service) as MockService:
                tool._run(
                    action="query",
                    dax_query="EVALUATE 'Sales'",
                    semantic_model_id="test-model"
                )

                # Verify service was created with correct group_id
                MockService.assert_called()
                call_args = MockService.call_args
                assert call_args[1]['group_id'] == "specific-group"


if __name__ == '__main__':
    unittest.main()
