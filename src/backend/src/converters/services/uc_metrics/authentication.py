"""
Databricks Authentication Service

Provides authentication for Databricks REST API and Unity Catalog operations.

Supports multiple authentication methods:
1. Personal Access Token (PAT) - Direct API key
2. OAuth tokens (for user-on-behalf operations)
3. Service Principal (client credentials)
4. Database-stored credentials (project-specific with global fallback)
"""

import logging
from typing import Dict, Optional


class DatabricksAuthService:
    """
    Databricks Authentication Service.

    Handles various Databricks authentication methods to obtain access tokens
    for Databricks REST API and Unity Catalog operations.

    Authentication hierarchy:
    1. Pre-obtained access token (if provided)
    2. Personal Access Token (PAT/API key)
    3. Service Principal (OAuth client credentials)
    4. Database credentials (future: project-specific with global fallback)

    Example usage:
        # Personal Access Token
        service = DatabricksAuthService(
            workspace_url="https://dbc-12345.cloud.databricks.com",
            api_key="dapi123..."
        )
        token = service.get_access_token()

        # Pre-obtained OAuth token
        service = DatabricksAuthService(
            workspace_url="https://dbc-12345.cloud.databricks.com",
            access_token="eyJ..."
        )
        token = service.get_access_token()

        # Service Principal
        service = DatabricksAuthService(
            workspace_url="https://dbc-12345.cloud.databricks.com",
            client_id="sp123",
            client_secret="secret"
        )
        token = service.get_access_token()
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
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize Databricks Authentication Service.

        Args:
            workspace_url: Databricks workspace URL (e.g., https://dbc-xxx.cloud.databricks.com)
            api_key: Personal Access Token (PAT) or DATABRICKS_API_KEY from frontend
            access_token: Pre-obtained OAuth access token (bypasses authentication)
            client_id: Service Principal application/client ID
            client_secret: Service Principal client secret
            project_id: Project ID for database credential lookup (future)
            use_database: Enable database credential lookup (future)
            logger: Optional logger instance
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.api_key = api_key
        self._access_token = access_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.project_id = project_id
        self.use_database = use_database
        self.logger = logger or logging.getLogger(__name__)

    def get_access_token(self) -> str:
        """
        Get access token for Databricks API.

        Authentication priority:
        1. Use pre-obtained OAuth token if provided
        2. Use Personal Access Token (PAT) if provided
        3. Use Service Principal credentials if provided
        4. Fetch credentials from database if enabled
        5. Raise error if no valid credentials found

        Returns:
            str: Access token for Databricks API

        Raises:
            ValueError: If credentials are missing or invalid
        """
        # Priority 1: Use pre-obtained OAuth token
        if self._access_token:
            self.logger.info("Using pre-obtained OAuth access token")
            return self._access_token

        # Priority 2: Use Personal Access Token (PAT)
        if self.api_key:
            self.logger.info("Using Personal Access Token (PAT)")
            return self.api_key

        # Priority 3: Use Service Principal
        if self.client_id and self.client_secret:
            self.logger.info("Using Service Principal credentials")
            return self._acquire_token_with_service_principal()

        # Priority 4: Database credentials (future implementation)
        if self.use_database:
            self.logger.info("Fetching credentials from database")
            credentials = self._get_credentials_from_database()
            if credentials.get("api_key"):
                return credentials["api_key"]
            elif credentials.get("client_id") and credentials.get("client_secret"):
                return self._acquire_token_with_service_principal(
                    credentials["client_id"],
                    credentials["client_secret"]
                )

        # No valid credentials found
        raise ValueError(
            "No credentials available. Provide either:\n"
            "1. api_key (Personal Access Token)\n"
            "2. Pre-obtained access_token (OAuth)\n"
            "3. Service Principal (client_id + client_secret)\n"
            "4. Enable use_database with project_id"
        )

    def _acquire_token_with_service_principal(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None
    ) -> str:
        """
        Acquire access token using Databricks OAuth with Service Principal.

        This uses Databricks OAuth 2.0 M2M (machine-to-machine) authentication.

        Args:
            client_id: Service Principal client ID (uses self.client_id if not provided)
            client_secret: Service Principal secret (uses self.client_secret if not provided)

        Returns:
            str: Access token

        Raises:
            ValueError: If credentials are incomplete
            Exception: If token acquisition fails
        """
        import requests

        client_id = client_id or self.client_id
        client_secret = client_secret or self.client_secret

        if not all([client_id, client_secret]):
            raise ValueError(
                "Incomplete Service Principal credentials. Required: client_id, client_secret"
            )

        try:
            # Databricks OAuth token endpoint
            token_url = f"{self.workspace_url}/oidc/v1/token"

            self.logger.info(f"Acquiring token for workspace: {self.workspace_url}")

            # Request access token using client credentials grant
            response = requests.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "scope": "all-apis",  # Databricks scope for all APIs
                },
                auth=(client_id, client_secret),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            response.raise_for_status()
            result = response.json()

            if "access_token" in result:
                self.logger.info("Access token acquired successfully")
                return result["access_token"]
            else:
                raise Exception(
                    f"Failed to acquire access token. Response: {result}"
                )

        except requests.exceptions.RequestException as ex:
            self.logger.error(f"Token acquisition failed: {str(ex)}")
            raise Exception(f"Error retrieving access token: {str(ex)}")

    def _get_credentials_from_database(self) -> Optional[Dict[str, str]]:
        """
        Fetch credentials from database.

        Future implementation will:
        1. Query database for project-specific credentials
        2. Fall back to global credentials if project-specific not found
        3. Return credentials dictionary

        Returns:
            Dict with api_key or client_id/client_secret, or None

        Raises:
            NotImplementedError: Database credentials not yet implemented
        """
        # TODO: Implement database credential lookup
        # This would use the repository pattern to query credentials
        #
        # Example implementation:
        # credentials = await credential_repository.get_databricks_credentials(
        #     project_id=self.project_id,
        #     use_global_fallback=True
        # )
        # return credentials

        raise NotImplementedError(
            "Database credential storage not yet implemented. "
            "Use direct credentials (api_key or service principal)."
        )

    def validate_token(self, token: Optional[str] = None) -> bool:
        """
        Validate if a token exists and appears well-formed.

        Note: This does NOT validate token expiration or permissions.
        For full validation, attempt to use the token with Databricks API.

        Args:
            token: Token to validate, or use internal token if None

        Returns:
            bool: True if token exists and appears valid
        """
        check_token = token or self._access_token or self.api_key

        if not check_token:
            return False

        # Basic format checks
        # PATs typically start with "dapi" or similar
        # OAuth tokens are JWTs with 3 parts
        if check_token.startswith("dapi"):
            # Looks like a Databricks PAT
            return len(check_token) > 10
        else:
            # Might be an OAuth token (JWT format)
            parts = check_token.split(".")
            if len(parts) == 3:
                return True
            self.logger.warning("Token does not appear to be a valid PAT or JWT")
            return False

    def get_headers(self) -> Dict[str, str]:
        """
        Get HTTP headers with authentication for Databricks API requests.

        Returns:
            Dict with Authorization header and token
        """
        token = self.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
