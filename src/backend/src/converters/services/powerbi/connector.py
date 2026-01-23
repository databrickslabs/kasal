"""
Power BI Inbound Connector
Extracts measures from Power BI datasets via REST API
"""

import logging
import re
import requests
from typing import Dict, Any, List, Optional

from ...base.connectors import BaseInboundConnector, InboundConnectorMetadata, ConnectorType
from ...base.models import KPI
from .dax_parser import DAXExpressionParser
from .authentication import AadService


class PowerBIConnector(BaseInboundConnector):
    """
    Power BI Inbound Connector.

    Connects to Power BI dataset via REST API and extracts measures using either:
    1. $SYSTEM.MDSCHEMA_MEASURES system query (default, recommended)
    2. Custom Info Measures table (legacy, requires pre-existing table)

    The system schema approach is preferred as it works on any Power BI dataset
    without requiring additional setup.

    Authentication options (via AadService):
    1. Service Principal (client_id + client_secret + tenant_id) - Recommended
    2. Pre-obtained access token (OAuth flow from frontend)
    3. Database-stored credentials (future: project-specific with global fallback)

    Example usage:
        # Using system schema (default, recommended)
        connector = PowerBIConnector(
            semantic_model_id="abc123",
            group_id="workspace456",
            access_token="eyJ..."
        )

        # Using legacy Info Measures table
        connector = PowerBIConnector(
            semantic_model_id="abc123",
            group_id="workspace456",
            access_token="eyJ...",
            use_system_schema=False,
            info_table_name="Info Measures"
        )

        with connector:
            kpis = connector.extract_measures()
    """

    # Power BI API endpoint
    API_BASE = "https://api.powerbi.com/v1.0/myorg"

    def __init__(
        self,
        semantic_model_id: str,
        group_id: str,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        access_token: Optional[str] = None,
        project_id: Optional[str] = None,
        use_database: bool = False,
        use_system_schema: bool = True,
        info_table_name: str = "Info Measures",
        **kwargs
    ):
        """
        Initialize Power BI connector.

        Args:
            semantic_model_id: Power BI dataset/semantic model ID
            group_id: Workspace ID
            tenant_id: Azure AD tenant ID (optional if using access_token)
            client_id: Client ID for authentication (optional if using access_token)
            client_secret: Client secret for Service Principal auth (optional)
            access_token: Pre-obtained access token from frontend OAuth
            project_id: Project ID for database credential lookup (future)
            use_database: Enable database credential lookup (future)
            use_system_schema: Use $SYSTEM.MDSCHEMA_MEASURES (recommended, default True)
            info_table_name: Name of the Info Measures table (fallback if use_system_schema=False)
        """
        connection_params = {
            "semantic_model_id": semantic_model_id,
            "group_id": group_id,
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "access_token": access_token,
            "project_id": project_id,
            "use_database": use_database,
            "use_system_schema": use_system_schema,
            "info_table_name": info_table_name,
        }
        super().__init__(connection_params)

        self.semantic_model_id = semantic_model_id
        self.group_id = group_id
        self.use_system_schema = use_system_schema
        self.info_table_name = info_table_name
        self._access_token = access_token
        self.dax_parser = DAXExpressionParser()

        # Initialize AadService for authentication
        # Debug logging to help diagnose authentication issues
        self.logger.info(f"[POWERBI CONNECTOR DEBUG] Initializing with:")
        self.logger.info(f"[POWERBI CONNECTOR DEBUG]   tenant_id: '{tenant_id}'")
        self.logger.info(f"[POWERBI CONNECTOR DEBUG]   client_id: '{client_id}'")
        self.logger.info(f"[POWERBI CONNECTOR DEBUG]   client_secret length: {len(client_secret) if client_secret else 0}")
        self.aad_service = AadService(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id,
            access_token=access_token,
            project_id=project_id,
            use_database=use_database,
            logger=self.logger,
        )

    def _get_access_token(self) -> str:
        """
        Get access token for Power BI API.

        Delegates to AadService which handles:
        1. Pre-obtained tokens
        2. Service Principal authentication via MSAL
        3. Database credential lookup (future)

        Returns:
            str: Access token for Power BI API

        Raises:
            Exception: If authentication fails
        """
        return self.aad_service.get_access_token()

    def connect(self) -> None:
        """Establish connection by obtaining access token."""
        if self._connected:
            self.logger.warning("Already connected")
            return

        self.logger.info("Connecting to Power BI API")

        try:
            self._access_token = self._get_access_token()
            self.logger.info("Access token obtained successfully")
        except Exception as e:
            raise ConnectionError(f"Failed to obtain access token: {str(e)}")

        self._connected = True
        self.logger.info("Connected successfully")

    def disconnect(self) -> None:
        """Close connection."""
        self._connected = False
        # Note: We don't invalidate the token in case it came from frontend
        self.logger.info("Disconnected")

    def _execute_dax_query(self, dax_query: str) -> List[Dict[str, Any]]:
        """Execute DAX query against Power BI dataset."""
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")

        url = f"{self.API_BASE}/groups/{self.group_id}/datasets/{self.semantic_model_id}/executeQueries"

        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

        body = {
            "queries": [{"query": dax_query}],
            "serializerSettings": {"includeNulls": True}
        }

        self.logger.info(f"Executing DAX query against dataset {self.semantic_model_id}")
        response = requests.post(url, headers=headers, json=body, timeout=60)

        if response.status_code == 200:
            results = response.json().get("results", [])

            if results and results[0].get("tables"):
                rows = results[0]["tables"][0].get("rows", [])
                self.logger.info(f"Query returned {len(rows)} rows")
                return rows
            else:
                self.logger.warning("Query returned no tables")
                return []
        else:
            error_msg = f"Query failed ({response.status_code}): {response.text}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _extract_measures_from_system_schema(self) -> List[Dict[str, Any]]:
        """
        Extract measures using $SYSTEM.MDSCHEMA_MEASURES system query.

        This is the recommended approach as it works on any Power BI dataset
        without requiring a pre-existing Info Measures table.

        Returns:
            List of measure dictionaries with Name, Expression, Description, Table
        """
        self.logger.info("Extracting measures using $SYSTEM.MDSCHEMA_MEASURES")

        dax_query = """
        SELECT
            [MEASURE_NAME] as [Measure Name],
            [EXPRESSION] as [Expression],
            [DESCRIPTION] as [Description],
            [MEASUREGROUP_NAME] as [Table]
        FROM $SYSTEM.MDSCHEMA_MEASURES
        """

        rows = self._execute_dax_query(dax_query)

        # Filter out system measures like __Default measure
        filtered_rows = [
            row for row in rows
            if not row.get('[Measure Name]', row.get('Measure Name', '')).startswith('__')
        ]

        self.logger.info(
            f"Found {len(filtered_rows)} measures from system schema "
            f"(filtered {len(rows) - len(filtered_rows)} system measures)"
        )

        return filtered_rows

    def _extract_measures_from_info_table(self) -> List[Dict[str, Any]]:
        """
        Extract measures from a custom Info Measures table (legacy approach).

        Returns:
            List of measure dictionaries
        """
        self.logger.info(f"Extracting measures from '{self.info_table_name}' table")

        dax_query = f"""
        EVALUATE
        SELECTCOLUMNS(
            '{self.info_table_name}',
            "ID", '{self.info_table_name}'[ID],
            "Name", '{self.info_table_name}'[Name],
            "Table", '{self.info_table_name}'[Table],
            "Description", '{self.info_table_name}'[Description],
            "Expression", '{self.info_table_name}'[Expression],
            "IsHidden", '{self.info_table_name}'[IsHidden],
            "State", '{self.info_table_name}'[State],
            "DisplayFolder", '{self.info_table_name}'[DisplayFolder]
        )
        """

        return self._execute_dax_query(dax_query)

    def extract_measures(
        self,
        include_hidden: bool = False,
        filter_pattern: Optional[str] = None,
    ) -> List[KPI]:
        """
        Extract measures from Power BI dataset.

        Uses $SYSTEM.MDSCHEMA_MEASURES by default (recommended) or falls back
        to custom Info Measures table if use_system_schema=False.

        Args:
            include_hidden: Include hidden measures (only applicable for Info Table mode)
            filter_pattern: Regex pattern to filter measure names

        Returns:
            List of KPI objects
        """
        if not self._connected:
            raise RuntimeError("Not connected. Call connect() first.")

        # Extract measures using preferred method
        if self.use_system_schema:
            rows = self._extract_measures_from_system_schema()
            # System schema uses different column names
            name_col = '[Measure Name]'
            name_col_alt = 'Measure Name'
            table_col = '[Table]'
            table_col_alt = 'Table'
            desc_col = '[Description]'
            desc_col_alt = 'Description'
            expr_col = '[Expression]'
            expr_col_alt = 'Expression'
        else:
            rows = self._extract_measures_from_info_table()
            # Info table uses bracketed column names
            name_col = '[Name]'
            name_col_alt = 'Name'
            table_col = '[Table]'
            table_col_alt = 'Table'
            desc_col = '[Description]'
            desc_col_alt = 'Description'
            expr_col = '[Expression]'
            expr_col_alt = 'Expression'

        # Helper to get value from row with fallback column name
        def get_value(row: Dict, col: str, alt_col: str, default: Any = None) -> Any:
            return row.get(col, row.get(alt_col, default))

        # Build list of all measure names for advanced parsing
        measures_list = [get_value(row, name_col, name_col_alt, '') for row in rows]

        # Parse and convert to KPI
        kpis = []
        for row in rows:
            # Skip hidden measures if requested (only applicable for Info Table mode)
            if not self.use_system_schema and not include_hidden:
                if row.get('[IsHidden]', False):
                    continue

            # Get measure name
            measure_name = get_value(row, name_col, name_col_alt, '')

            # Apply filter pattern
            if filter_pattern and not re.match(filter_pattern, measure_name):
                continue

            # Parse DAX expression using ADVANCED mode
            expression = get_value(row, expr_col, expr_col_alt, '')
            parsed = self.dax_parser.parse_advanced(expression, measures_list)

            # Log transpilation info
            if parsed['is_transpilable'] and parsed['transpiled_sql']:
                self.logger.debug(f"Measure '{measure_name}' transpiled to SQL: {parsed['transpiled_sql']}")
            elif not parsed['is_transpilable']:
                self.logger.debug(f"Measure '{measure_name}' not transpilable: {parsed['transpilability_reason']}")

            # Generate technical name
            technical_name = measure_name.lower().replace(' ', '_').replace('-', '_')

            # PowerBI measures don't have separate filters - they're embedded in the DAX expression
            # Filters would only come from YAML-based definitions
            filters = []

            # Determine source table
            source_table = parsed['source_table'] or get_value(row, table_col, table_col_alt)

            # Get description
            description = get_value(row, desc_col, desc_col_alt) or measure_name

            # Create KPI with additional metadata from advanced parsing
            kpi = KPI(
                technical_name=technical_name,
                formula=parsed['base_formula'],
                description=description,
                source_table=source_table,
                aggregation_type=parsed['aggregation_type'],
                display_sign=1,
                filters=filters,
                apply_structures=[]
            )

            # Store advanced parsing metadata as additional attributes (optional)
            # This allows access to transpiled SQL, tokens, signature, etc.
            kpi._advanced_parsing = {
                'transpiled_sql': parsed['transpiled_sql'],
                'is_transpilable': parsed['is_transpilable'],
                'signature': parsed['generic_signature'],
                'tokens_count': len(parsed['tokens']),
                'functions_used': [f.value for f in parsed['functions']],
            }

            kpis.append(kpi)

        self.logger.info(f"Extracted {len(kpis)} measures")
        return kpis

    def get_metadata(self) -> InboundConnectorMetadata:
        """Get metadata about the connector."""
        measure_count = None
        if self._connected:
            try:
                measures = self.extract_measures()
                measure_count = len(measures)
            except Exception as e:
                self.logger.warning(f"Could not count measures: {e}")

        return InboundConnectorMetadata(
            connector_type=ConnectorType.POWERBI,
            source_id=self.semantic_model_id,
            source_name=f"Power BI Dataset {self.semantic_model_id}",
            description=f"Power BI semantic model in workspace {self.group_id}",
            connected=self._connected,
            measure_count=measure_count,
            additional_info={
                "info_table_name": self.info_table_name,
                "group_id": self.group_id,
            }
        )
