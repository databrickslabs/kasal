"""
Unit tests for ConversionPipeline orchestrator.

Tests the main conversion pipeline flow from inbound connectors
to outbound converters.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from typing import Dict, Any

from src.converters.pipeline import (
    ConversionPipeline,
    OutboundFormat,
)
from src.converters.base.connectors import ConnectorType
from src.converters.base.models import KPIDefinition, KPI


class TestConversionPipeline:
    """Test suite for ConversionPipeline class"""

    @pytest.fixture
    def pipeline(self):
        """Create a fresh pipeline instance for each test"""
        return ConversionPipeline()

    @pytest.fixture
    def sample_definition(self):
        """Create a sample KPIDefinition for testing"""
        return KPIDefinition(
            description="Test measures definition",
            technical_name="test_measures",
            kpis=[
                KPI(
                    description="Total sales amount",
                    formula="SUM(Sales[Amount])",
                    technical_name="Total_Sales"
                ),
                KPI(
                    description="Total cost amount",
                    formula="SUM(Cost[Amount])",
                    technical_name="Total_Cost"
                ),
            ]
        )

    @pytest.fixture
    def sample_metadata(self):
        """Create sample metadata"""
        from dataclasses import dataclass

        @dataclass
        class TestMetadata:
            source: str = "PowerBI"
            dataset_id: str = "test-dataset-123"
            extraction_time: str = "2026-01-16T12:00:00"

        return TestMetadata()

    # ========== Constructor Tests ==========

    def test_pipeline_initialization(self, pipeline):
        """Test pipeline initializes with logger"""
        assert pipeline.logger is not None
        assert pipeline.logger.name == "src.converters.pipeline"

    # ========== Inbound Connector Creation Tests ==========

    @patch('src.converters.pipeline.PowerBIConnector')
    def test_create_powerbi_connector(self, mock_connector_class, pipeline):
        """Test creating PowerBI connector"""
        # Arrange
        connection_params = {
            "semantic_model_id": "test-model-123",
            "group_id": "test-workspace-456",
            "access_token": "test-token"
        }

        # Act
        result = pipeline.create_inbound_connector(
            ConnectorType.POWERBI,
            connection_params
        )

        # Assert
        mock_connector_class.assert_called_once_with(**connection_params)
        assert result == mock_connector_class.return_value

    def test_create_unsupported_connector_raises_error(self, pipeline):
        """Test creating unsupported connector raises ValueError"""
        # Arrange
        unsupported_type = "TABLEAU"

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported connector type"):
            pipeline.create_inbound_connector(unsupported_type, {})

    # ========== Full Pipeline Execution Tests ==========

    @patch('src.converters.pipeline.PowerBIConnector')
    def test_execute_pipeline_success_powerbi_to_dax(
        self, mock_connector_class, pipeline, sample_definition, sample_metadata
    ):
        """Test successful pipeline execution: PowerBI → DAX"""
        # Arrange
        mock_connector = MagicMock()
        mock_connector.extract_to_definition.return_value = sample_definition
        mock_connector.get_metadata.return_value = sample_metadata
        mock_connector.__enter__.return_value = mock_connector
        mock_connector.__exit__.return_value = None
        mock_connector_class.return_value = mock_connector

        inbound_params = {
            "semantic_model_id": "model-123",
            "group_id": "workspace-456",
            "access_token": "token-789"
        }

        with patch.object(pipeline, '_convert_to_dax') as mock_convert:
            mock_convert.return_value = [
                {"name": "Total Sales", "expression": "SUM(...)", "description": "Test"}
            ]

            # Act
            result = pipeline.execute(
                inbound_type=ConnectorType.POWERBI,
                inbound_params=inbound_params,
                outbound_format=OutboundFormat.DAX,
                extract_params={"include_hidden": True}
            )

        # Assert
        assert result["success"] is True
        assert result["definition"] == sample_definition
        assert result["measure_count"] == 2
        assert "output" in result
        assert result["errors"] == []

        # Verify connector was called correctly
        mock_connector.extract_to_definition.assert_called_once()
        call_kwargs = mock_connector.extract_to_definition.call_args[1]
        assert call_kwargs["include_hidden"] is True

    @patch('src.converters.pipeline.PowerBIConnector')
    def test_execute_pipeline_failure_connector_error(
        self, mock_connector_class, pipeline
    ):
        """Test pipeline handles connector errors gracefully"""
        # Arrange
        mock_connector_class.side_effect = Exception("Connection failed")

        # Act
        result = pipeline.execute(
            inbound_type=ConnectorType.POWERBI,
            inbound_params={},
            outbound_format=OutboundFormat.DAX
        )

        # Assert
        assert result["success"] is False
        assert len(result["errors"]) > 0
        assert "Connection failed" in result["errors"][0]
        assert result["measure_count"] == 0

    @patch('src.converters.pipeline.PowerBIConnector')
    def test_execute_pipeline_failure_extraction_error(
        self, mock_connector_class, pipeline
    ):
        """Test pipeline handles extraction errors gracefully"""
        # Arrange
        mock_connector = MagicMock()
        mock_connector.extract_to_definition.side_effect = Exception("Extraction failed")
        mock_connector.__enter__.return_value = mock_connector
        mock_connector.__exit__.return_value = None
        mock_connector_class.return_value = mock_connector

        # Act
        result = pipeline.execute(
            inbound_type=ConnectorType.POWERBI,
            inbound_params={},
            outbound_format=OutboundFormat.DAX
        )

        # Assert
        assert result["success"] is False
        assert len(result["errors"]) > 0
        assert "Extraction failed" in result["errors"][0]

    # ========== Format Conversion Path Detection Tests ==========

    def test_convert_to_format_detects_transpilation_path_powerbi_to_sql(self, pipeline, sample_definition):
        """Test pipeline detects transpilation path for PowerBI → SQL"""
        # Arrange
        with patch.object(pipeline, '_convert_to_sql') as mock_convert_sql:
            mock_convert_sql.return_value = []

            # Act
            pipeline._convert_to_format(
                sample_definition,
                OutboundFormat.SQL,
                {},
                inbound_type=ConnectorType.POWERBI
            )

            # Assert
            # Should call _convert_to_sql with use_transpilation=True
            assert mock_convert_sql.called
            call_kwargs = mock_convert_sql.call_args[1]
            assert call_kwargs["use_transpilation"] is True

    def test_convert_to_format_detects_generation_path_non_powerbi_to_sql(self, pipeline, sample_definition):
        """Test pipeline detects generation path for non-PowerBI → SQL"""
        # Arrange
        with patch.object(pipeline, '_convert_to_sql') as mock_convert_sql:
            mock_convert_sql.return_value = []

            # Act
            pipeline._convert_to_format(
                sample_definition,
                OutboundFormat.SQL,
                {},
                inbound_type=None  # Non-PowerBI source
            )

            # Assert
            # Should call _convert_to_sql with use_transpilation=False
            assert mock_convert_sql.called
            call_kwargs = mock_convert_sql.call_args[1]
            assert call_kwargs["use_transpilation"] is False

    def test_convert_to_format_unsupported_format_raises_error(self, pipeline, sample_definition):
        """Test unsupported format raises ValueError"""
        # Arrange
        # Create an enum with an invalid value for testing
        from enum import Enum

        class InvalidFormat(str, Enum):
            INVALID = "invalid"

        # Act & Assert - The code will raise ValueError when checking format
        with pytest.raises((ValueError, AttributeError)):
            # Pass string to trigger the unsupported format path
            pipeline._convert_to_format(sample_definition, "xml", {})

    # ========== DAX Conversion Tests ==========

    @patch('src.converters.pipeline.SmartDAXGenerator')
    def test_convert_to_dax_success(self, mock_generator_class, pipeline, sample_definition):
        """Test successful conversion to DAX format"""
        # Arrange
        mock_generator = Mock()
        mock_dax_measure = Mock()
        mock_dax_measure.name = "Total Sales"
        mock_dax_measure.dax_formula = "SUM(Sales[Amount])"
        mock_dax_measure.description = "Total sales amount"
        mock_dax_measure.table = "Sales"

        mock_generator.generate_all_measures.return_value = [mock_dax_measure]
        mock_generator_class.return_value = mock_generator

        # Act
        result = pipeline._convert_to_dax(sample_definition, {})

        # Assert
        assert len(result) == 1
        assert result[0]["name"] == "Total Sales"
        assert result[0]["expression"] == "SUM(Sales[Amount])"
        assert result[0]["description"] == "Total sales amount"
        assert result[0]["table"] == "Sales"

    @patch('src.converters.pipeline.SmartDAXGenerator')
    def test_convert_to_dax_handles_generation_error(self, mock_generator_class, pipeline, sample_definition):
        """Test DAX conversion handles generator errors"""
        # Arrange
        mock_generator = Mock()
        mock_generator.generate_all_measures.side_effect = Exception("DAX generation failed")
        mock_generator_class.return_value = mock_generator

        # Act & Assert
        with pytest.raises(Exception, match="DAX generation failed"):
            pipeline._convert_to_dax(sample_definition, {})

    # ========== SQL Conversion Tests ==========

    @patch('src.converters.pipeline.SQLGenerator')
    def test_convert_to_sql_generation_path(self, mock_generator_class, pipeline, sample_definition):
        """Test SQL conversion using generation path (non-PowerBI source)"""
        # Arrange
        mock_generator = Mock()

        # Mock the result object returned by generate_sql_from_kbi_definition
        mock_result = Mock()
        mock_result.sql_queries = ["SELECT SUM(amount) as total_sales"]
        mock_result.to_output_string.return_value = "-- Generated SQL\nSELECT SUM(amount) as total_sales;"

        mock_generator.generate_sql_from_kbi_definition.return_value = mock_result
        mock_generator_class.return_value = mock_generator

        # Act
        result = pipeline._convert_to_sql(
            sample_definition,
            {"dialect": "databricks"},
            use_transpilation=False
        )

        # Assert
        assert isinstance(result, str)
        assert "SELECT" in result
        mock_generator.generate_sql_from_kbi_definition.assert_called_once_with(sample_definition)

    def test_convert_to_sql_transpilation_path(self, pipeline, sample_definition):
        """Test SQL conversion using transpilation path (PowerBI source)"""
        # Arrange
        # Add advanced parsing results to KPIs
        sample_definition.kpis[0]._advanced_parsing = {
            "sql_transpiled": "SUM(sales_table.amount)"
        }
        sample_definition.kpis[1]._advanced_parsing = {
            "sql_transpiled": "SUM(cost_table.amount)"
        }

        with patch.object(pipeline, '_format_transpiled_sql') as mock_format:
            mock_format.return_value = [
                {"name": "Total Sales", "sql": "SUM(...)"}
            ]

            # Act
            result = pipeline._convert_to_sql(
                sample_definition,
                {},
                use_transpilation=True
            )

            # Assert
            mock_format.assert_called_once()

    # ========== UC Metrics Conversion Tests ==========

    @patch('src.converters.pipeline.SmartUCMetricsGenerator')
    @patch('src.converters.pipeline.UCMetricsGenerator')
    def test_convert_to_uc_metrics_generation_path(
        self, mock_basic_gen_class, mock_generator_class, pipeline, sample_definition
    ):
        """Test UC Metrics conversion using generation path"""
        # Arrange
        mock_generator = Mock()

        # Mock the consolidated metrics output (dict structure)
        consolidated_metrics = {
            "version": "0.1",
            "source": "main.default.test_table",
            "measures": [
                {
                    "name": "total_sales",
                    "expr": "SUM(`amount`)",
                    "comment": "Total sales"
                }
            ]
        }

        mock_generator.generate_consolidated_uc_metrics.return_value = consolidated_metrics
        mock_generator_class.return_value = mock_generator

        # Mock the basic generator's format method
        mock_basic_gen = Mock()
        mock_basic_gen.format_consolidated_uc_metrics_yaml.return_value = "version: '0.1'\nmeasures:\n  - name: total_sales"
        mock_basic_gen_class.return_value = mock_basic_gen

        # Act
        result = pipeline._convert_to_uc_metrics(
            sample_definition,
            {"catalog": "main", "schema": "default"},
            use_transpilation=False
        )

        # Assert
        assert isinstance(result, str)
        assert "version" in result
        mock_generator.generate_consolidated_uc_metrics.assert_called_once()

    # ========== Parameter Handling Tests ==========

    def test_execute_uses_default_parameters(self, pipeline):
        """Test pipeline uses sensible defaults for optional parameters"""
        # Arrange
        with patch.object(pipeline, 'create_inbound_connector') as mock_create:
            mock_connector = MagicMock()
            mock_connector.extract_to_definition.return_value = KPIDefinition(
                description="Test",
                technical_name="test",
                kpis=[]
            )
            from dataclasses import dataclass

            @dataclass
            class EmptyMetadata:
                pass

            mock_connector.get_metadata.return_value = EmptyMetadata()
            mock_connector.__enter__.return_value = mock_connector
            mock_connector.__exit__.return_value = None
            mock_create.return_value = mock_connector

            with patch.object(pipeline, '_convert_to_dax') as mock_convert:
                mock_convert.return_value = []

                # Act
                result = pipeline.execute(
                    inbound_type=ConnectorType.POWERBI,
                    inbound_params={},
                    outbound_format=OutboundFormat.DAX
                    # No extract_params, outbound_params, or definition_name provided
                )

        # Assert
        assert result["success"] is True
        # Should use default definition name
        extract_call = mock_connector.extract_to_definition.call_args
        assert "definition_name" in extract_call[1]

    # ========== Metadata Extraction Tests ==========

    @patch('src.converters.pipeline.PowerBIConnector')
    def test_execute_includes_connector_metadata(
        self, mock_connector_class, pipeline, sample_definition
    ):
        """Test pipeline includes connector metadata in result"""
        # Arrange
        mock_connector = MagicMock()
        mock_connector.extract_to_definition.return_value = sample_definition

        from dataclasses import dataclass

        @dataclass
        class TestMetadata:
            source_type: str = "PowerBI"
            dataset_id: str = "test-123"
            measure_count: int = 2

        mock_connector.get_metadata.return_value = TestMetadata()

        mock_connector.__enter__.return_value = mock_connector
        mock_connector.__exit__.return_value = None
        mock_connector_class.return_value = mock_connector

        with patch.object(pipeline, '_convert_to_dax') as mock_convert:
            mock_convert.return_value = []

            # Act
            result = pipeline.execute(
                inbound_type=ConnectorType.POWERBI,
                inbound_params={},
                outbound_format=OutboundFormat.DAX
            )

        # Assert
        assert "metadata" in result
        assert result["metadata"]["source_type"] == "PowerBI"
        assert result["metadata"]["dataset_id"] == "test-123"

    # ========== Error Handling Tests ==========

    @patch('src.converters.pipeline.PowerBIConnector')
    def test_execute_logs_errors_and_returns_partial_result(
        self, mock_connector_class, pipeline
    ):
        """Test pipeline logs errors and returns partial result on failure"""
        # Arrange
        mock_connector_class.side_effect = Exception("Critical error")

        # Act
        result = pipeline.execute(
            inbound_type=ConnectorType.POWERBI,
            inbound_params={},
            outbound_format=OutboundFormat.DAX
        )

        # Assert
        assert result["success"] is False
        assert result["definition"] is None
        assert result["output"] is None
        assert result["measure_count"] == 0
        assert len(result["errors"]) > 0
