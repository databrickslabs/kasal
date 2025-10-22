"""
PowerBI Connector Utility

This module provides connectivity to Power BI datasets via XMLA endpoints.
It's extracted from the reference implementation to provide DAX query generation
capabilities without the execution component (execution happens in Databricks notebooks).
"""

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def clean_dax_query(dax_query: str) -> str:
    """
    Remove HTML/XML tags and other artifacts from DAX queries.

    Args:
        dax_query: Raw DAX query string that may contain artifacts

    Returns:
        Cleaned DAX query string
    """
    # Remove HTML/XML tags like <oii>, </oii>, etc.
    cleaned = re.sub(r"<[^>]+>", "", dax_query)
    # Collapse extra whitespace
    cleaned = " ".join(cleaned.split())
    return cleaned


class PowerBIMetadataExtractor:
    """
    Extracts metadata from Power BI datasets without requiring ADOMD.NET.

    This class provides methods to discover tables, columns, measures, and relationships
    from Power BI datasets. It's designed to work with the metadata structure
    without executing queries.

    Note: Actual XMLA connection requires pyadomd which is Windows-only.
    For production use with Databricks, metadata should be cached or provided
    via configuration.
    """

    def __init__(self):
        """Initialize the metadata extractor."""
        self.metadata_cache: Dict[str, Any] = {}

    def set_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Set cached metadata for a Power BI dataset.

        Args:
            metadata: Dictionary containing tables, columns, measures, and relationships
        """
        self.metadata_cache = metadata
        logger.info(f"Metadata cache updated with {len(metadata.get('tables', []))} tables")

    def get_tables(self) -> List[Dict[str, Any]]:
        """
        Get list of tables from cached metadata.

        Returns:
            List of table dictionaries with name, description, and relationships
        """
        return self.metadata_cache.get('tables', [])

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get schema information for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table schema including columns and their types
        """
        tables = self.metadata_cache.get('tables', [])
        for table in tables:
            if table.get('name') == table_name:
                return table
        return None

    def get_relationships(self) -> List[Dict[str, Any]]:
        """
        Get all relationships from the dataset.

        Returns:
            List of relationship dictionaries
        """
        relationships = []
        tables = self.metadata_cache.get('tables', [])
        for table in tables:
            table_relationships = table.get('relationships', [])
            relationships.extend(table_relationships)
        return relationships

    def format_metadata_for_llm(self) -> str:
        """
        Format metadata as a readable string for LLM context.

        Returns:
            Formatted string describing the dataset structure
        """
        tables = self.get_tables()
        if not tables:
            return "No metadata available"

        output = "Power BI Dataset Structure:\n\n"

        for table in tables:
            table_name = table.get('name', 'Unknown')
            description = table.get('description', 'No description')
            columns = table.get('columns', [])
            relationships = table.get('relationships', [])

            output += f"Table: {table_name}\n"
            output += f"Description: {description}\n"

            if columns:
                output += "Columns:\n"
                for col in columns:
                    col_name = col.get('name', 'Unknown')
                    col_desc = col.get('description', 'No description')
                    col_type = col.get('data_type', 'Unknown')
                    output += f"  - {col_name} ({col_type}): {col_desc}\n"

            if relationships:
                output += "Relationships:\n"
                for rel in relationships:
                    related_table = rel.get('relatedTable', 'Unknown')
                    from_col = rel.get('fromColumn', 'Unknown')
                    to_col = rel.get('toColumn', 'Unknown')
                    rel_type = rel.get('relationshipType', 'Unknown')
                    output += f"  - {rel_type} with {related_table}: {from_col} → {to_col}\n"

            output += "\n"

        return output


class PowerBIConnectorConfig:
    """Configuration for Power BI connection."""

    def __init__(
        self,
        xmla_endpoint: str,
        dataset_name: str,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None
    ):
        """
        Initialize Power BI connection configuration.

        Args:
            xmla_endpoint: Power BI XMLA endpoint URL (e.g., powerbi://api.powerbi.com/v1.0/myorg/workspace)
            dataset_name: Name of the Power BI dataset
            tenant_id: Azure AD tenant ID (optional, for service principal auth)
            client_id: Service principal client ID (optional)
            client_secret: Service principal client secret (optional)
        """
        self.xmla_endpoint = xmla_endpoint
        self.dataset_name = dataset_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "xmla_endpoint": self.xmla_endpoint,
            "dataset_name": self.dataset_name,
            "tenant_id": self.tenant_id,
            "client_id": self.client_id,
            # Don't include client_secret in dict representation
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "PowerBIConnectorConfig":
        """Create configuration from dictionary."""
        return cls(
            xmla_endpoint=config_dict["xmla_endpoint"],
            dataset_name=config_dict["dataset_name"],
            tenant_id=config_dict.get("tenant_id"),
            client_id=config_dict.get("client_id"),
            client_secret=config_dict.get("client_secret")
        )
