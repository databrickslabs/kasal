"""
Unit tests for base connector classes and enums.

Tests:
- ConnectorType enum
- InboundConnectorMetadata dataclass
- BaseInboundConnector abstract base class
"""

import pytest
from unittest.mock import Mock, patch
from dataclasses import asdict

from src.converters.base.connectors import (
    ConnectorType,
    InboundConnectorMetadata,
    BaseInboundConnector,
)
from src.converters.base.models import KPI, KPIDefinition


class TestConnectorType:
    """Tests for ConnectorType enum"""

    def test_connector_type_values(self):
        """Test all connector type values"""
        assert ConnectorType.POWERBI == "powerbi"
        assert ConnectorType.TABLEAU == "tableau"
        assert ConnectorType.LOOKER == "looker"
        assert ConnectorType.EXCEL == "excel"

    def test_connector_type_from_string(self):
        """Test creating ConnectorType from string"""
        assert ConnectorType("powerbi") == ConnectorType.POWERBI
        assert ConnectorType("tableau") == ConnectorType.TABLEAU

    def test_connector_type_invalid_value(self):
        """Test invalid connector type raises error"""
        with pytest.raises(ValueError):
            ConnectorType("invalid_type")


class TestInboundConnectorMetadata:
    """Tests for InboundConnectorMetadata dataclass"""

    def test_create_metadata_minimal(self):
        """Test creating metadata with minimal fields"""
        metadata = InboundConnectorMetadata(
            connector_type=ConnectorType.POWERBI,
            source_id="dataset-123"
        )

        assert metadata.connector_type == ConnectorType.POWERBI
        assert metadata.source_id == "dataset-123"
        assert metadata.connected is False
        assert metadata.measure_count is None

    def test_create_metadata_full(self):
        """Test creating metadata with all fields"""
        metadata = InboundConnectorMetadata(
            connector_type=ConnectorType.POWERBI,
            source_id="dataset-123",
            source_name="Sales Dataset",
            description="Production sales data",
            connected=True,
            measure_count=15,
            additional_info={"workspace": "Sales Workspace", "refreshed": "2024-01-15"}
        )

        assert metadata.source_name == "Sales Dataset"
        assert metadata.connected is True
        assert metadata.measure_count == 15
        assert metadata.additional_info["workspace"] == "Sales Workspace"

    def test_metadata_to_dict(self):
        """Test converting metadata to dictionary"""
        metadata = InboundConnectorMetadata(
            connector_type=ConnectorType.POWERBI,
            source_id="dataset-123",
            connected=True
        )

        metadata_dict = asdict(metadata)

        assert metadata_dict["source_id"] == "dataset-123"
        assert metadata_dict["connected"] is True


# Concrete implementation for testing
class TestConnector(BaseInboundConnector):
    """Concrete connector implementation for testing"""

    def connect(self) -> None:
        """Mock connect implementation"""
        if not self.connection_params.get("valid"):
            raise ConnectionError("Invalid connection parameters")
        self._connected = True

    def disconnect(self) -> None:
        """Mock disconnect implementation"""
        self._connected = False

    def extract_measures(self, **kwargs) -> list:
        """Mock extract implementation"""
        if not self._connected:
            raise RuntimeError("Not connected")

        # Return mock KPIs
        return [
            KPI(
                description="Total Sales",
                formula="SUM(Sales[Amount])",
                technical_name="total_sales"
            ),
            KPI(
                description="Total Cost",
                formula="SUM(Cost[Amount])",
                technical_name="total_cost"
            )
        ]

    def get_metadata(self) -> InboundConnectorMetadata:
        """Mock metadata implementation"""
        return InboundConnectorMetadata(
            connector_type=ConnectorType.POWERBI,
            source_id=self.connection_params.get("source_id", "test-123"),
            connected=self._connected,
            measure_count=2
        )


class TestBaseInboundConnector:
    """Tests for BaseInboundConnector abstract base class"""

    @pytest.fixture
    def connector(self):
        """Create test connector instance"""
        return TestConnector(connection_params={"valid": True, "source_id": "test-123"})

    def test_connector_initialization(self, connector):
        """Test connector initializes with parameters"""
        assert connector.connection_params == {"valid": True, "source_id": "test-123"}
        assert connector._connected is False
        assert connector.logger is not None

    def test_connector_connect_success(self, connector):
        """Test successful connection"""
        connector.connect()

        assert connector._connected is True

    def test_connector_connect_failure(self):
        """Test connection failure"""
        connector = TestConnector(connection_params={"valid": False})

        with pytest.raises(ConnectionError, match="Invalid connection parameters"):
            connector.connect()

    def test_connector_disconnect(self, connector):
        """Test disconnect"""
        connector.connect()
        assert connector._connected is True

        connector.disconnect()
        assert connector._connected is False

    def test_extract_measures_when_connected(self, connector):
        """Test extracting measures when connected"""
        connector.connect()

        measures = connector.extract_measures()

        assert len(measures) == 2
        assert measures[0].technical_name == "total_sales"
        assert measures[1].technical_name == "total_cost"

    def test_extract_measures_when_not_connected(self, connector):
        """Test extracting measures fails when not connected"""
        with pytest.raises(RuntimeError, match="Not connected"):
            connector.extract_measures()

    def test_get_metadata(self, connector):
        """Test getting metadata"""
        metadata = connector.get_metadata()

        assert metadata.connector_type == ConnectorType.POWERBI
        assert metadata.source_id == "test-123"
        assert metadata.measure_count == 2

    def test_extract_to_definition_success(self, connector):
        """Test extract_to_definition creates KPIDefinition"""
        connector.connect()

        definition = connector.extract_to_definition(
            definition_name="Sales Metrics",
            definition_description="Sales KPIs"
        )

        assert isinstance(definition, KPIDefinition)
        assert definition.description == "Sales KPIs"
        assert definition.technical_name == "sales_metrics"
        assert len(definition.kpis) == 2

    def test_extract_to_definition_uses_name_as_description(self, connector):
        """Test extract_to_definition uses name as description if not provided"""
        connector.connect()

        definition = connector.extract_to_definition(
            definition_name="Test Metrics"
        )

        assert definition.description == "Test Metrics"

    def test_extract_to_definition_normalizes_technical_name(self, connector):
        """Test technical name is normalized (lowercase, underscores)"""
        connector.connect()

        definition = connector.extract_to_definition(
            definition_name="Sales Metrics 2024"
        )

        assert definition.technical_name == "sales_metrics_2024"

    def test_extract_to_definition_fails_when_not_connected(self, connector):
        """Test extract_to_definition requires connection"""
        with pytest.raises(RuntimeError, match="Connector not connected"):
            connector.extract_to_definition(definition_name="Test")

    def test_extract_to_definition_passes_kwargs_to_extract(self, connector):
        """Test extract_to_definition passes kwargs to extract_measures"""
        connector.connect()

        with patch.object(connector, 'extract_measures', return_value=[]) as mock_extract:
            connector.extract_to_definition(
                definition_name="Test",
                include_hidden=True,
                filter_pattern="Sales.*"
            )

            mock_extract.assert_called_once_with(
                include_hidden=True,
                filter_pattern="Sales.*"
            )

    def test_context_manager_support(self, connector):
        """Test connector works as context manager"""
        with connector:
            assert connector._connected is True

        assert connector._connected is False

    def test_context_manager_on_exception(self, connector):
        """Test connector disconnects even on exception"""
        try:
            with connector:
                raise ValueError("Test error")
        except ValueError:
            pass

        assert connector._connected is False


class TestBaseInboundConnectorAbstract:
    """Test that BaseInboundConnector cannot be instantiated"""

    def test_cannot_instantiate_base_connector(self):
        """Test BaseInboundConnector is abstract and cannot be instantiated"""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseInboundConnector(connection_params={})
