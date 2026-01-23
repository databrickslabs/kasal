"""
Azure AD Authentication Service for Power BI API

Provides authentication using Azure Identity ClientSecretCredential
for Power BI App Owns Data scenarios.

Supports both direct credential parameters and database-stored credentials
with project-specific and global fallback options.
"""

import logging
from typing import Dict, Optional

# Optional azure.identity import
try:
    from azure.identity import ClientSecretCredential
    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    ClientSecretCredential = None  # type: ignore
    AZURE_IDENTITY_AVAILABLE = False
    logging.warning("azure-identity not available. Install with: pip install azure-identity")


class AadService:
    """
    Azure Active Directory Authentication Service for Power BI.

    Handles Service Principal authentication to obtain access tokens
    for Power BI REST API operations.

    Authentication hierarchy:
    1. Direct credentials (client_id, client_secret, tenant_id parameters)
    2. Database credentials (future: project-specific with global fallback)
    3. Pre-obtained access token (if provided)

    Example usage:
        # Direct credentials
        service = AadService(
            client_id="abc123",
            client_secret="secret",
            tenant_id="tenant789"
        )
        token = service.get_access_token()

        # Pre-obtained token
        service = AadService(access_token="eyJ...")
        token = service.get_access_token()

        # Database credentials (future)
        service = AadService(project_id="proj123", use_database=True)
        token = service.get_access_token()
    """

    # Power BI API scope
    POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

    # Microsoft login authority base URL
    AUTHORITY_BASE = "https://login.microsoftonline.com"

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
        access_token: Optional[str] = None,
        project_id: Optional[str] = None,
        use_database: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize Azure AD Service.

        Args:
            client_id: Azure AD application (client) ID
            client_secret: Azure AD application client secret
            tenant_id: Azure AD tenant ID
            access_token: Pre-obtained access token (bypasses authentication)
            project_id: Project ID for database credential lookup (future)
            use_database: Enable database credential lookup (future)
            logger: Optional logger instance
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self._access_token = access_token
        self.project_id = project_id
        self.use_database = use_database
        self.logger = logger or logging.getLogger(__name__)

    def get_access_token(self) -> str:
        """
        Get access token for Power BI API.

        Authentication priority:
        1. Use pre-obtained token if provided
        2. Use direct credentials if provided
        3. Fetch credentials from database if enabled
        4. Raise error if no valid credentials found

        Returns:
            str: Access token for Power BI API

        Raises:
            RuntimeError: If azure-identity library not available
            ValueError: If credentials are missing or invalid
            Exception: If token acquisition fails
        """
        # Priority 1: Use pre-obtained token
        if self._access_token:
            self.logger.info("Using pre-obtained access token")
            return self._access_token

        # Ensure azure-identity is available
        if not AZURE_IDENTITY_AVAILABLE:
            raise RuntimeError(
                "azure-identity library required for authentication. "
                "Install with: pip install azure-identity"
            )

        # Priority 2: Use direct credentials
        credentials = None
        if self.client_id and self.client_secret and self.tenant_id:
            self.logger.info("Using direct credential parameters")
            credentials = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "tenant_id": self.tenant_id,
            }
        # Priority 3: Database credentials (future implementation)
        elif self.use_database:
            self.logger.info("Fetching credentials from database")
            credentials = self._get_credentials_from_database()

        # Validate credentials exist
        if not credentials:
            raise ValueError(
                "No credentials available. Provide either:\n"
                "1. Direct credentials (client_id, client_secret, tenant_id)\n"
                "2. Pre-obtained access_token\n"
                "3. Enable use_database with project_id"
            )

        # Acquire token using ClientSecretCredential
        return self._acquire_token_with_client_credential(credentials)

    def _get_credentials_from_database(self) -> Optional[Dict[str, str]]:
        """
        Fetch credentials from database.

        Future implementation will:
        1. Query database for project-specific credentials
        2. Fall back to global credentials if project-specific not found
        3. Return credentials dictionary

        Returns:
            Dict with client_id, client_secret, tenant_id or None

        Raises:
            NotImplementedError: Database credentials not yet implemented
        """
        # TODO: Implement database credential lookup
        # This would use the repository pattern to query credentials
        #
        # Example implementation:
        # credentials = await credential_repository.get_by_project_id(
        #     project_id=self.project_id,
        #     use_global_fallback=True
        # )
        # return credentials

        raise NotImplementedError(
            "Database credential storage not yet implemented. "
            "Use direct credentials or pre-obtained access token."
        )

    def _acquire_token_with_client_credential(self, credentials: Dict[str, str]) -> str:
        """
        Acquire access token using Azure Identity ClientSecretCredential.

        Uses Service Principal (client credentials) flow which is
        Microsoft's recommended approach for Power BI App Owns Data scenarios.

        Args:
            credentials: Dict with client_id, client_secret, tenant_id

        Returns:
            str: Access token

        Raises:
            ValueError: If credentials are incomplete
            Exception: If token acquisition fails
        """
        # Validate required credentials
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        tenant_id = credentials.get("tenant_id")

        if not all([client_id, client_secret, tenant_id]):
            raise ValueError(
                "Incomplete credentials. Required: client_id, client_secret, tenant_id"
            )

        # Type assertions after validation
        assert client_id is not None
        assert client_secret is not None
        assert tenant_id is not None

        try:
            self.logger.info(f"Acquiring token for tenant: {tenant_id}, client_id: {client_id}")

            # Ensure ClientSecretCredential is available (should be checked earlier)
            if ClientSecretCredential is None:
                raise RuntimeError("azure-identity library not available")

            # Create ClientSecretCredential and get token
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )

            # Acquire token for Power BI API
            token_response = credential.get_token(self.POWERBI_SCOPE)

            self.logger.info("Access token acquired successfully")
            return token_response.token

        except Exception as ex:
            self.logger.error(f"Token acquisition failed: {str(ex)}")
            raise Exception(f"Error retrieving access token: {str(ex)}")

    def validate_token(self, token: Optional[str] = None) -> bool:
        """
        Validate if a token exists and appears well-formed.

        Note: This does NOT validate token expiration or signature.
        For full validation, attempt to use the token with Power BI API.

        Args:
            token: Token to validate, or use internal token if None

        Returns:
            bool: True if token exists and appears valid
        """
        check_token = token or self._access_token

        if not check_token:
            return False

        # Basic format check - JWT tokens have 3 parts separated by dots
        parts = check_token.split(".")
        if len(parts) != 3:
            self.logger.warning("Token does not appear to be a valid JWT")
            return False

        return True
