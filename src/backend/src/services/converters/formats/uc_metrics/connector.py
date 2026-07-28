"""
Databricks Unity Catalog Connector

Provides integration with Databricks Unity Catalog for UC Metrics operations.

This connector can be used to:
1. Validate UC Metrics definitions against a Databricks workspace
2. Deploy UC Metrics to Unity Catalog Metrics Store
3. Retrieve existing metrics from Unity Catalog
4. Manage UC Metrics lifecycle (create, update, delete)
"""

import logging
import requests
from typing import Dict, Any, List, Optional

from .authentication import DatabricksAuthService


class DatabricksConnector:
    """
    Databricks Unity Catalog Connector.

    Connects to Databricks workspace and provides UC Metrics management operations.

    Authentication options (via DatabricksAuthService):
    1. Personal Access Token (PAT) - Recommended for development
    2. Service Principal (client_id + client_secret) - Recommended for production
    3. Pre-obtained OAuth token (from frontend)
    4. Database-stored credentials (future)

    Example usage:
        # Personal Access Token
        connector = DatabricksConnector(
            workspace_url="https://dbc-12345.cloud.databricks.com",
            api_key="dapi123..."
        )

        # Service Principal
        connector = DatabricksConnector(
            workspace_url="https://dbc-12345.cloud.databricks.com",
            client_id="sp123",
            client_secret="secret"
        )

        # Validate connection
        if connector.validate_connection():
            print("Connected to Databricks successfully")

        # Deploy UC Metrics
        connector.deploy_uc_metrics(
            catalog="main",
            schema="analytics",
            metrics_definition=uc_metrics_yaml
        )
    """

    def __init__(
        self,
        workspace_url: str,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        project_id: Optional[str] = None,
        use_database: bool = False,
        **kwargs
    ):
        """
        Initialize Databricks connector.

        Args:
            workspace_url: Databricks workspace URL
            api_key: Personal Access Token (DATABRICKS_API_KEY from frontend)
            access_token: Pre-obtained OAuth token
            client_id: Service Principal client ID
            client_secret: Service Principal secret
            project_id: Project ID for database credential lookup (future)
            use_database: Enable database credential lookup (future)
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.logger = logging.getLogger(__name__)

        # Initialize authentication service
        self.auth_service = DatabricksAuthService(
            workspace_url=workspace_url,
            api_key=api_key,
            access_token=access_token,
            client_id=client_id,
            client_secret=client_secret,
            project_id=project_id,
            use_database=use_database,
            logger=self.logger,
        )

    def validate_connection(self) -> bool:
        """
        Validate connection to Databricks workspace.

        Tests authentication by making a simple API call to the workspace.

        Returns:
            bool: True if connection is valid and authenticated
        """
        try:
            headers = self.auth_service.get_headers()

            # Test connection with workspace info endpoint
            response = requests.get(
                f"{self.workspace_url}/api/2.0/workspace/get-status",
                headers=headers,
                params={"path": "/"},
            )

            if response.status_code == 200:
                self.logger.info("Successfully connected to Databricks workspace")
                return True
            else:
                self.logger.warning(
                    f"Connection validation failed with status {response.status_code}"
                )
                return False

        except Exception as ex:
            self.logger.error(f"Connection validation error: {str(ex)}")
            return False

    def get_catalogs(self) -> List[Dict[str, Any]]:
        """
        Get list of available Unity Catalog catalogs.

        Returns:
            List of catalog information dictionaries

        Raises:
            Exception: If API call fails
        """
        try:
            headers = self.auth_service.get_headers()

            response = requests.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/catalogs",
                headers=headers,
            )

            response.raise_for_status()
            result = response.json()

            catalogs = result.get("catalogs", [])
            self.logger.info(f"Found {len(catalogs)} catalogs")
            return catalogs

        except Exception as ex:
            self.logger.error(f"Failed to get catalogs: {str(ex)}")
            raise

    def get_schemas(self, catalog: str) -> List[Dict[str, Any]]:
        """
        Get list of schemas in a catalog.

        Args:
            catalog: Catalog name

        Returns:
            List of schema information dictionaries

        Raises:
            Exception: If API call fails
        """
        try:
            headers = self.auth_service.get_headers()

            response = requests.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/schemas",
                headers=headers,
                params={"catalog_name": catalog},
            )

            response.raise_for_status()
            result = response.json()

            schemas = result.get("schemas", [])
            self.logger.info(f"Found {len(schemas)} schemas in catalog '{catalog}'")
            return schemas

        except Exception as ex:
            self.logger.error(f"Failed to get schemas: {str(ex)}")
            raise

    def deploy_uc_metrics(
        self,
        catalog: str,
        schema: str,
        metrics_definition: str,
        metric_name: str = "generated_metrics",
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """
        Deploy UC Metrics definition to Unity Catalog.

        Note: This is a placeholder for future implementation.
        The actual deployment would use Databricks SQL or REST API
        to create/update metrics in Unity Catalog.

        Args:
            catalog: Target catalog name
            schema: Target schema name
            metrics_definition: UC Metrics YAML definition
            metric_name: Name for the metrics definition
            overwrite: Whether to overwrite existing metrics

        Returns:
            Dict with deployment result information

        Raises:
            NotImplementedError: Deployment not yet implemented
        """
        # TODO: Implement UC Metrics deployment
        # This would involve:
        # 1. Validating the metrics definition YAML
        # 2. Creating/updating metrics in Unity Catalog Metrics Store
        # 3. Handling versioning and rollback
        #
        # Example implementation might use:
        # - Databricks SQL API to execute CREATE METRIC statements
        # - Unity Catalog REST API for metrics management
        # - Delta tables for metrics storage

        self.logger.warning("UC Metrics deployment not yet implemented")
        raise NotImplementedError(
            "UC Metrics deployment to Databricks is not yet implemented. "
            "This will be added in a future release."
        )

    def get_metric_definitions(
        self,
        catalog: str,
        schema: str,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve existing metric definitions from Unity Catalog.

        Note: This is a placeholder for future implementation.

        Args:
            catalog: Catalog name
            schema: Schema name

        Returns:
            List of metric definition dictionaries

        Raises:
            NotImplementedError: Retrieval not yet implemented
        """
        # TODO: Implement metric retrieval
        # This would query Unity Catalog Metrics Store for existing metrics

        self.logger.warning("Metric retrieval not yet implemented")
        raise NotImplementedError(
            "Retrieving existing UC Metrics is not yet implemented. "
            "This will be added in a future release."
        )

    def validate_metric_definition(self, metrics_definition: str) -> Dict[str, Any]:
        """
        Validate UC Metrics definition without deploying.

        Checks YAML syntax and validates against UC Metrics schema.

        Args:
            metrics_definition: UC Metrics YAML definition

        Returns:
            Dict with validation results (valid: bool, errors: List[str])
        """
        import yaml

        try:
            # Parse YAML
            metrics_data = yaml.safe_load(metrics_definition)

            # Basic validation
            errors = []

            # Check required fields
            if "version" not in metrics_data:
                errors.append("Missing required field: version")

            if "measures" not in metrics_data:
                errors.append("Missing required field: measures")
            elif not isinstance(metrics_data["measures"], list):
                errors.append("Field 'measures' must be a list")

            # Validate each measure
            for idx, measure in enumerate(metrics_data.get("measures", [])):
                if not isinstance(measure, dict):
                    errors.append(f"Measure {idx} must be a dictionary")
                    continue

                if "name" not in measure:
                    errors.append(f"Measure {idx}: Missing required field 'name'")

                if "expr" not in measure:
                    errors.append(f"Measure {idx}: Missing required field 'expr'")

            # Return validation results
            is_valid = len(errors) == 0
            return {
                "valid": is_valid,
                "errors": errors,
                "message": "Validation passed" if is_valid else "Validation failed",
            }

        except yaml.YAMLError as ex:
            return {
                "valid": False,
                "errors": [f"YAML parsing error: {str(ex)}"],
                "message": "Invalid YAML syntax",
            }
        except Exception as ex:
            return {
                "valid": False,
                "errors": [f"Validation error: {str(ex)}"],
                "message": "Validation failed",
            }

    def __enter__(self):
        """Context manager entry - validate connection."""
        if not self.validate_connection():
            raise ConnectionError(
                f"Failed to connect to Databricks workspace: {self.workspace_url}"
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup if needed."""
        # No cleanup needed for REST API connections
        pass
