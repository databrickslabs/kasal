import logging
import os
import time
from typing import Dict, List, Optional
import requests
from azure.identity import UsernamePasswordCredential
from fastapi import HTTPException

from src.repositories.powerbi_config_repository import PowerBIConfigRepository
from src.schemas.powerbi_config import DAXQueryRequest, DAXQueryResponse

# Set up logger
logger = logging.getLogger(__name__)


class PowerBIService:
    """Service for Power BI DAX operations."""

    def __init__(self, session, group_id: Optional[str] = None):
        self.session = session
        self.repository = PowerBIConfigRepository(session)
        self.group_id = group_id
        self._secrets_service = None  # Lazy load to avoid circular deps

    @property
    def secrets_service(self):
        """Lazy load secrets_service to avoid circular dependency."""
        if self._secrets_service is None:
            from src.services.api_keys_service import ApiKeysService
            self._secrets_service = ApiKeysService(self.session)
        return self._secrets_service

    async def execute_dax_query(self, query_request: DAXQueryRequest) -> DAXQueryResponse:
        """
        Execute DAX query against Power BI semantic model.

        Args:
            query_request: DAX query request with query and optional semantic model ID

        Returns:
            DAXQueryResponse with results or error information
        """
        start_time = time.time()

        try:
            # Get active Power BI configuration
            config = await self.repository.get_active_config(group_id=self.group_id)
            if not config:
                raise HTTPException(
                    status_code=404,
                    detail="No active Power BI configuration found. Please configure Power BI connection first."
                )

            if not config.is_enabled:
                raise HTTPException(
                    status_code=400,
                    detail="Power BI integration is disabled. Please enable it in settings."
                )

            # Use provided semantic model ID or default from config
            semantic_model_id = query_request.semantic_model_id or config.semantic_model_id
            if not semantic_model_id:
                raise HTTPException(
                    status_code=400,
                    detail="Semantic model ID is required. Provide it in the request or configure a default."
                )

            # Generate authentication token
            token = await self._generate_token(config)

            # Execute DAX query
            results = await self._execute_query(
                token=token,
                semantic_model_id=semantic_model_id,
                dax_query=query_request.dax_query
            )

            # Process results
            data = self._postprocess_data(results)

            execution_time_ms = int((time.time() - start_time) * 1000)

            return DAXQueryResponse(
                status="success",
                data=data,
                row_count=len(data),
                columns=list(data[0].keys()) if data else [],
                execution_time_ms=execution_time_ms
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error executing DAX query: {e}", exc_info=True)
            execution_time_ms = int((time.time() - start_time) * 1000)
            return DAXQueryResponse(
                status="error",
                data=None,
                row_count=0,
                columns=None,
                error=str(e),
                execution_time_ms=execution_time_ms
            )

    async def _generate_token(self, config) -> str:
        """
        Generate authentication token for Power BI API.

        Args:
            config: PowerBIConfig model instance

        Returns:
            Authentication token string
        """
        try:
            tenant_id = config.tenant_id
            client_id = config.client_id

            # Attempt to get credentials from different sources
            # Priority: API Keys Service > Environment Variables
            username = None
            password = None
            client_secret = None

            # Try to get from API Keys Service
            if self._secrets_service:
                try:
                    username = await self.secrets_service.get_api_key("POWERBI_USERNAME")
                    password = await self.secrets_service.get_api_key("POWERBI_PASSWORD")
                    client_secret = await self.secrets_service.get_api_key("POWERBI_CLIENT_SECRET")
                except Exception as e:
                    logger.warning(f"Could not get Power BI credentials from API Keys Service: {e}")

            # Fallback to environment variables
            if not username:
                username = os.getenv("POWERBI_USERNAME") or os.getenv("SADATAMESHPOWERBIUSERNAME")
            if not password:
                password = os.getenv("POWERBI_PASSWORD") or os.getenv("SADATAMESHPOWERBIPASSWORD")
            if not client_secret:
                client_secret = os.getenv("POWERBI_CLIENT_SECRET")

            # Validate credentials
            if not all([username, password, client_id]):
                raise ValueError(
                    "Missing required credentials. Please provide username, password, and client_id "
                    "through API Keys Service or environment variables."
                )

            logger.info(f"Authenticating with username length: {len(username)}, password length: {len(password)}")

            # Create credential and get token
            credential = UsernamePasswordCredential(
                client_id=client_id,
                username=username,
                password=password,
                tenant_id=tenant_id,
                client_secret=client_secret if client_secret else None,
            )

            # Token generation for Power BI API
            token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
            return token.token

        except Exception as e:
            logger.error(f"Error generating Power BI token: {e}", exc_info=True)
            raise HTTPException(
                status_code=401,
                detail=f"Failed to authenticate with Power BI: {str(e)}"
            )

    async def _execute_query(self, token: str, semantic_model_id: str, dax_query: str) -> List:
        """
        Execute DAX query against Power BI API.

        Args:
            token: Authentication token
            semantic_model_id: Power BI semantic model (dataset) ID
            dax_query: DAX query string

        Returns:
            Raw query results from Power BI API
        """
        try:
            datasets_url = f"https://api.powerbi.com/v1.0/myorg/datasets/{semantic_model_id}/executeQueries"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            }

            body = {"queries": [{"query": dax_query}]}

            logger.info(f"Executing DAX query against semantic model: {semantic_model_id}")
            logger.debug(f"Query: {dax_query[:200]}...")  # Log first 200 chars

            response = requests.post(datasets_url, headers=headers, json=body, timeout=30)

            if response.status_code != 200:
                error_msg = f"Power BI API error (status {response.status_code}): {response.text}"
                logger.error(error_msg)
                raise HTTPException(status_code=response.status_code, detail=error_msg)

            logger.info(f"Successfully fetched response with status: {response.status_code}")

            results = response.json().get("results", [])
            return results

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error when calling Power BI API: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to execute DAX query: {str(e)}"
            )

    def _postprocess_data(self, results: List) -> List[Dict]:
        """
        Post-process DAX query results into a list of dictionaries.

        Args:
            results: Raw results from Power BI API

        Returns:
            List of dictionaries representing rows
        """
        if not results:
            logger.info("No results found in the response.")
            return []

        tables = results[0].get("tables", [])
        if not tables:
            logger.info("No tables found in the response.")
            return []

        rows = tables[0].get("rows", [])
        if not rows:
            logger.info("No rows found in the response.")
            return []

        return rows
