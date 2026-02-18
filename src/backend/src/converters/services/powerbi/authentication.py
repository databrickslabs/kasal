"""
Azure AD Authentication Service for Power BI API

Provides authentication using Azure Identity credentials
for Power BI App Owns Data scenarios.

Supports multiple authentication methods:
1. Pre-obtained access token (OAuth flow from frontend)
2. Service Principal (ClientSecretCredential) - App Owns Data
3. Service Account (UsernamePasswordCredential) - User credentials

Supports both direct credential parameters and database-stored credentials
with project-specific and global fallback options.
"""

import logging
import os
from typing import Dict, Optional

# Optional azure.identity import
try:
    from azure.identity import ClientSecretCredential, UsernamePasswordCredential
    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    ClientSecretCredential = None  # type: ignore
    UsernamePasswordCredential = None  # type: ignore
    AZURE_IDENTITY_AVAILABLE = False
    logging.warning("azure-identity not available. Install with: pip install azure-identity")


class AadService:
    """
    Azure Active Directory Authentication Service for Power BI.

    Handles multiple authentication methods to obtain access tokens
    for Power BI REST API operations.

    Authentication methods:
    1. Pre-obtained access token (OAuth flow from frontend) - highest priority
    2. Service Principal (client_id + client_secret + tenant_id) - App Owns Data
    3. Service Account (username + password + client_id + tenant_id) - User credentials
    4. Database credentials (future: project-specific with global fallback)

    Example usage:
        # Pre-obtained token
        service = AadService(access_token="eyJ...")
        token = service.get_access_token()

        # Service Principal credentials
        service = AadService(
            client_id="abc123",
            client_secret="secret",
            tenant_id="tenant789",
            auth_method="service_principal"
        )
        token = service.get_access_token()

        # Service Account credentials
        service = AadService(
            client_id="abc123",
            tenant_id="tenant789",
            username="user@domain.com",
            password="password",
            auth_method="service_account"
        )
        token = service.get_access_token()

        # Service Account with env var names
        service = AadService(
            client_id="abc123",
            tenant_id="tenant789",
            username_env="POWERBI_USERNAME",
            password_env="POWERBI_PASSWORD",
            auth_method="service_account"
        )
        token = service.get_access_token()

        # Database credentials (future)
        service = AadService(project_id="proj123", use_database=True)
        token = service.get_access_token()
    """

    # Power BI API scope
    POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

    # Microsoft login authority base URL
    AUTHORITY_BASE = "https://login.microsoftonline.com"

    # Valid authentication methods
    AUTH_METHOD_SERVICE_PRINCIPAL = "service_principal"
    AUTH_METHOD_SERVICE_ACCOUNT = "service_account"
    AUTH_METHOD_TOKEN = "token"

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
        access_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        username_env: Optional[str] = None,
        password_env: Optional[str] = None,
        auth_method: Optional[str] = None,
        project_id: Optional[str] = None,
        use_database: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize Azure AD Service.

        Args:
            client_id: Azure AD application (client) ID
            client_secret: Azure AD application client secret (for service principal)
            tenant_id: Azure AD tenant ID
            access_token: Pre-obtained access token (bypasses authentication)
            username: Service account username/UPN (for service account auth)
            password: Service account password (for service account auth)
            username_env: Environment variable name containing username (for service account)
            password_env: Environment variable name containing password (for service account)
            auth_method: Authentication method: 'service_principal', 'service_account', or 'token'
                        If not specified, auto-detected based on provided credentials
            project_id: Project ID for database credential lookup (future)
            use_database: Enable database credential lookup (future)
            logger: Optional logger instance
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self._access_token = access_token
        self.username = username
        self.password = password
        self.username_env = username_env
        self.password_env = password_env
        self.auth_method = auth_method
        self.project_id = project_id
        self.use_database = use_database
        self.logger = logger or logging.getLogger(__name__)

    def get_access_token(self) -> str:
        """
        Get access token for Power BI API.

        Authentication priority:
        1. Use pre-obtained token if provided
        2. Use specified auth_method if set:
           - 'service_principal': ClientSecretCredential (client_id, client_secret, tenant_id)
           - 'service_account': UsernamePasswordCredential (username, password, client_id, tenant_id)
        3. Auto-detect based on available credentials
        4. Fetch credentials from database if enabled
        5. Raise error if no valid credentials found

        Returns:
            str: Access token for Power BI API

        Raises:
            RuntimeError: If azure-identity library not available
            ValueError: If credentials are missing or invalid
            Exception: If token acquisition fails
        """
        # Priority 1: Use pre-obtained token
        if self._access_token:
            self.logger.info("=" * 80)
            self.logger.info("🔑 AUTHENTICATION METHOD: User OAuth (pre-obtained access token)")
            self.logger.info("=" * 80)
            return self._access_token

        # Ensure azure-identity is available
        if not AZURE_IDENTITY_AVAILABLE:
            raise RuntimeError(
                "azure-identity library required for authentication. "
                "Install with: pip install azure-identity"
            )

        # Determine authentication method
        effective_auth_method = self._determine_auth_method()
        self.logger.info(f"Using authentication method: {effective_auth_method}")

        # Priority 2: Service Account authentication (username/password)
        if effective_auth_method == self.AUTH_METHOD_SERVICE_ACCOUNT:
            return self._acquire_token_with_username_password()

        # Priority 3: Service Principal authentication (client credentials)
        if effective_auth_method == self.AUTH_METHOD_SERVICE_PRINCIPAL:
            credentials = self._get_service_principal_credentials()
            return self._acquire_token_with_client_credential(credentials)

        # Priority 4: Database credentials (future implementation)
        if self.use_database:
            self.logger.info("Fetching credentials from database")
            credentials = self._get_credentials_from_database()
            return self._acquire_token_with_client_credential(credentials)

        # No valid authentication method found
        raise ValueError(
            "No credentials available. Provide either:\n"
            "1. Pre-obtained access_token\n"
            "2. Service Principal credentials (client_id, client_secret, tenant_id)\n"
            "3. Service Account credentials (username, password, client_id, tenant_id)\n"
            "4. Enable use_database with project_id"
        )

    def _determine_auth_method(self) -> Optional[str]:
        """
        Determine the authentication method to use based on explicit setting or available credentials.

        Returns:
            str: Authentication method ('service_principal', 'service_account', or None)
        """
        # If auth_method is explicitly set, use it
        if self.auth_method:
            return self.auth_method

        # Auto-detect based on available credentials
        # Check for service principal credentials first (more specific)
        if self.client_id and self.client_secret and self.tenant_id:
            return self.AUTH_METHOD_SERVICE_PRINCIPAL

        # Check for service account credentials
        username = self._resolve_username()
        password = self._resolve_password()
        if username and password and self.client_id and self.tenant_id:
            return self.AUTH_METHOD_SERVICE_ACCOUNT

        return None

    def _resolve_username(self) -> Optional[str]:
        """Resolve username from direct value or environment variable."""
        if self.username:
            return self.username
        if self.username_env:
            return os.getenv(self.username_env)
        return None

    def _resolve_password(self) -> Optional[str]:
        """Resolve password from direct value or environment variable."""
        if self.password:
            return self.password
        if self.password_env:
            return os.getenv(self.password_env)
        return None

    def _get_service_principal_credentials(self) -> Dict[str, str]:
        """Get service principal credentials from instance variables."""
        if not all([self.client_id, self.client_secret, self.tenant_id]):
            raise ValueError(
                "Incomplete Service Principal credentials. "
                "Required: client_id, client_secret, tenant_id"
            )
        # Type assertions after validation
        assert self.client_id is not None
        assert self.client_secret is not None
        assert self.tenant_id is not None
        return {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "tenant_id": self.tenant_id,
        }

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
            self.logger.info("=" * 80)
            self.logger.info("🔑 AUTHENTICATION METHOD: Service Principal (client credentials)")
            self.logger.info("=" * 80)
            self.logger.info(f"[AUTH DEBUG] Acquiring token with:")
            self.logger.info(f"[AUTH DEBUG]   tenant_id: '{tenant_id}' (type: {type(tenant_id).__name__})")
            self.logger.info(f"[AUTH DEBUG]   client_id: '{client_id}' (type: {type(client_id).__name__})")
            self.logger.info(f"[AUTH DEBUG]   client_secret length: {len(client_secret) if client_secret else 0}")

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

            self.logger.info("✅ Service Principal access token acquired successfully")
            return token_response.token

        except Exception as ex:
            self.logger.error(f"Token acquisition failed: {str(ex)}")
            raise Exception(f"Error retrieving access token: {str(ex)}")

    def _acquire_token_with_username_password(self) -> str:
        """
        Acquire access token using Azure Identity UsernamePasswordCredential.

        Uses Service Account (ROPC) flow which authenticates with user credentials.
        This is useful for scenarios where Service Principal doesn't have
        sufficient permissions to read Power BI data.

        Returns:
            str: Access token

        Raises:
            ValueError: If credentials are incomplete
            RuntimeError: If azure-identity not available
            Exception: If token acquisition fails
        """
        # Resolve username and password
        username = self._resolve_username()
        password = self._resolve_password()

        # Validate required credentials
        if not all([username, password, self.client_id, self.tenant_id]):
            missing = []
            if not username:
                missing.append("username")
            if not password:
                missing.append("password")
            if not self.client_id:
                missing.append("client_id")
            if not self.tenant_id:
                missing.append("tenant_id")
            raise ValueError(
                f"Incomplete Service Account credentials. Missing: {', '.join(missing)}"
            )

        # Type assertions after validation
        assert username is not None
        assert password is not None
        assert self.client_id is not None
        assert self.tenant_id is not None

        try:
            self.logger.info("=" * 80)
            self.logger.info("🔑 AUTHENTICATION METHOD: Service Account (username/password)")
            self.logger.info("=" * 80)
            self.logger.info(f"[AUTH DEBUG] Acquiring token with service account:")
            self.logger.info(f"[AUTH DEBUG]   tenant_id: '{self.tenant_id}'")
            self.logger.info(f"[AUTH DEBUG]   client_id: '{self.client_id}'")
            self.logger.info(f"[AUTH DEBUG]   username: '{username}'")
            self.logger.info(f"[AUTH DEBUG]   password length: {len(password)}")
            self.logger.info(f"[AUTH DEBUG]   password stripped length: {len(password.strip())}")
            self.logger.info(f"[AUTH DEBUG]   password has whitespace: {password != password.strip()}")

            # Strip whitespace from credentials (common copy-paste issue)
            username = username.strip()

            # Handle base64-encoded passwords (for special characters like \$ that get escaped)
            # If password starts with 'base64:', decode it
            if password.strip().startswith('base64:'):
                import base64
                encoded_password = password.strip()[7:]  # Remove 'base64:' prefix
                password = base64.b64decode(encoded_password).decode('utf-8')
                self.logger.info(f"[AUTH DEBUG] Decoded base64 password - length: {len(password)}")
            else:
                password = password.strip()

                # CRITICAL FIX: Unescape common escape sequences that appear in passwords
                # When passwords are stored in env vars/JSON/DB, backslash escape sequences
                # like \$ get stored literally instead of being interpreted
                original_length = len(password)
                password = password.replace('\\$', '$')  # \$ -> $
                password = password.replace('\\\\', '\\')  # \\ -> \
                password = password.replace('\\n', '\n')  # \n -> newline (unlikely but handle it)
                password = password.replace('\\t', '\t')  # \t -> tab (unlikely but handle it)

                if len(password) != original_length:
                    self.logger.info(f"[AUTH DEBUG] Unescaped password - original length: {original_length}, new length: {len(password)}")
                else:
                    self.logger.info(f"[AUTH DEBUG] Using raw password - length: {len(password)}")

            # Ensure UsernamePasswordCredential is available
            if UsernamePasswordCredential is None:
                raise RuntimeError("azure-identity library not available")

            # Create UsernamePasswordCredential and get token
            # Note: Service Account authentication can use both public (no secret) or confidential (with secret) clients
            # Include client_secret if available (matches working implementation)
            credential_kwargs = {
                'client_id': self.client_id,
                'username': username,
                'password': password,
                'tenant_id': self.tenant_id
            }

            # Include client_secret if provided (required for confidential client apps)
            if self.client_secret:
                credential_kwargs['client_secret'] = self.client_secret
                self.logger.info(f"[AUTH DEBUG] Including client_secret (confidential client)")
            else:
                self.logger.info(f"[AUTH DEBUG] No client_secret (public client)")

            credential = UsernamePasswordCredential(**credential_kwargs)

            # Acquire token for Power BI API
            token_response = credential.get_token(self.POWERBI_SCOPE)

            self.logger.info("✅ Service Account access token acquired successfully")
            return token_response.token

        except Exception as ex:
            self.logger.error(f"Service account token acquisition failed: {str(ex)}")
            raise Exception(f"Error retrieving access token with service account: {str(ex)}")

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
