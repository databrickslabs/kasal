"""
Base Inbound Connector
Abstract base class for all inbound connectors that extract measures from source systems
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum
import logging

from .models import KPI, KPIDefinition


class ConnectorType(str, Enum):
    """Supported inbound connector types"""
    POWERBI = "powerbi"
    TABLEAU = "tableau"
    LOOKER = "looker"
    EXCEL = "excel"
    # Future: Add more as needed


@dataclass
class InboundConnectorMetadata:
    """Metadata about an inbound connector"""
    connector_type: ConnectorType
    source_id: str  # Dataset ID, Workbook ID, etc.
    source_name: Optional[str] = None
    description: Optional[str] = None
    connected: bool = False
    measure_count: Optional[int] = None
    additional_info: Optional[Dict[str, Any]] = None


class BaseInboundConnector(ABC):
    """
    Abstract base class for inbound connectors.

    Inbound connectors extract measures/KPIs from source systems and convert them
    to the standardized KPIDefinition format that can be consumed by outbound converters.

    Flow:
    1. Connect to source system (authenticate, establish connection)
    2. Extract measures (query, parse, transform)
    3. Convert to KPIDefinition format
    4. Pass to outbound converter (DAX, SQL, UC Metrics, etc.)
    """

    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize connector with connection parameters.

        Args:
            connection_params: Connector-specific connection parameters
        """
        self.connection_params = connection_params
        self._connected = False
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to source system.

        Should handle authentication, token acquisition, session setup, etc.
        Sets self._connected = True on success.

        Raises:
            ConnectionError: If connection fails
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close connection to source system.

        Should clean up resources, invalidate tokens, close sessions, etc.
        Sets self._connected = False.
        """
        pass

    @abstractmethod
    def extract_measures(self, **kwargs) -> List[KPI]:
        """
        Extract measures from source system.

        Args:
            **kwargs: Connector-specific extraction parameters
                     (e.g., include_hidden, filter_pattern, folder_filter)

        Returns:
            List of KPI objects in standardized format

        Raises:
            RuntimeError: If not connected
            ValueError: If extraction parameters are invalid
        """
        pass

    @abstractmethod
    def get_metadata(self) -> InboundConnectorMetadata:
        """
        Get metadata about the connector and source.

        Returns:
            InboundConnectorMetadata with connector information
        """
        pass

    def extract_to_definition(
        self,
        definition_name: str,
        definition_description: Optional[str] = None,
        **extract_kwargs
    ) -> KPIDefinition:
        """
        Extract measures and wrap in KPIDefinition.

        This is the main entry point for the conversion pipeline.

        Args:
            definition_name: Name for the KPI definition
            definition_description: Description for the KPI definition
            **extract_kwargs: Passed to extract_measures()

        Returns:
            KPIDefinition containing all extracted measures
        """
        if not self._connected:
            raise RuntimeError(f"Connector not connected. Call connect() first.")

        self.logger.info(f"Extracting measures for definition: {definition_name}")

        # Extract measures
        kpis = self.extract_measures(**extract_kwargs)

        # Create KPIDefinition
        definition = KPIDefinition(
            description=definition_description or definition_name,
            technical_name=definition_name.lower().replace(' ', '_'),
            kpis=kpis,
            default_variables={},
            query_filters=[],
            structures={},
            filters=None  # FIXED: filters expects Optional[Dict], not list
        )

        self.logger.info(
            f"Created KPIDefinition with {len(kpis)} measures: {definition_name}"
        )

        return definition

    @property
    def is_connected(self) -> bool:
        """Check if connector is currently connected"""
        return self._connected

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
