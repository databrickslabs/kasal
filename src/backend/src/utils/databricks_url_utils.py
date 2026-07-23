"""
Utility module for normalizing and constructing Databricks URLs.

This module provides centralized URL handling to ensure consistent
construction of Databricks API endpoints throughout the application.

LLM inference traffic can be routed two ways, selected by the
``DATABRICKS_ENABLE_AI_GATEWAY`` environment variable (sourced from the
``ai_gateway_enabled`` flag on DatabricksConfig):

- Serving endpoints (default): ``/serving-endpoints`` — for chat LiteLLM
  appends ``/chat/completions`` (model in body); direct REST callers hit
  ``/serving-endpoints/<model>/invocations`` (model in URL path).
- AI Gateway: ``/ai-gateway/mlflow/v1`` — OpenAI-compatible, model always
  in the request body. Chat → ``/ai-gateway/mlflow/v1/chat/completions``,
  embeddings → ``/ai-gateway/mlflow/v1/embeddings``.

Both routes accept the same Databricks endpoint names and bearer tokens, so
switching is purely a matter of base-path + where the model name travels.
"""
import os
import re
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class DatabricksURLUtils:
    """Utility class for normalizing and constructing Databricks URLs."""

    # Base path segments for the two LLM routing modes.
    SERVING_ENDPOINTS_PATH = "serving-endpoints"
    AI_GATEWAY_PATH = "ai-gateway/mlflow/v1"
    # The Responses API (gpt-5-codex models) is served under a DIFFERENT gateway
    # path than chat/embeddings — the OpenAI-compatible /ai-gateway/openai/v1.
    AI_GATEWAY_RESPONSES_PATH = "ai-gateway/openai/v1"

    # Environment variable that mirrors DatabricksConfig.ai_gateway_enabled.
    AI_GATEWAY_ENV_VAR = "DATABRICKS_ENABLE_AI_GATEWAY"

    @staticmethod
    def is_ai_gateway_enabled() -> bool:
        """
        Whether LLM/embedding traffic should be routed through the Databricks
        AI Gateway instead of per-endpoint serving-endpoints invocations.

        Sourced from the DATABRICKS_ENABLE_AI_GATEWAY environment variable,
        which is set from the ai_gateway_enabled flag on DatabricksConfig when
        the configuration is loaded or saved.

        Returns:
            True if the AI Gateway should be used, False otherwise.
        """
        return os.getenv(DatabricksURLUtils.AI_GATEWAY_ENV_VAR, "false").strip().lower() in (
            "true", "1", "yes", "on"
        )

    @staticmethod
    def normalize_workspace_url(url: Optional[str]) -> Optional[str]:
        """
        Normalize a workspace URL to the base format without paths.
        
        This method ensures that:
        - The URL has the https:// protocol
        - Any path components (like /serving-endpoints) are removed
        - The result is just the base workspace URL
        
        Args:
            url: The workspace URL to normalize (can be in various formats)
            
        Returns:
            Normalized workspace URL (e.g., https://workspace.databricks.com) or None if invalid
            
        Examples:
            >>> DatabricksURLUtils.normalize_workspace_url("workspace.databricks.com")
            'https://workspace.databricks.com'
            >>> DatabricksURLUtils.normalize_workspace_url("https://workspace.databricks.com/serving-endpoints")
            'https://workspace.databricks.com'
        """
        if not url:
            return None
            
        # Remove whitespace
        url = url.strip()
        
        # Add https:// if missing
        if not url.startswith(('http://', 'https://')):
            url = f"https://{url}"
        
        # Remove any path components (including /serving-endpoints)
        # Extract just the protocol and hostname
        match = re.match(r'(https?://[^/]+)', url)
        if match:
            normalized_url = match.group(1)
            if url != normalized_url:
                logger.debug(f"Normalized URL from '{url}' to '{normalized_url}'")
            return normalized_url
        
        logger.warning(f"Could not normalize URL: {url}")
        return None
    
    @staticmethod
    def construct_serving_endpoints_url(workspace_url: Optional[str]) -> Optional[str]:
        """
        Construct the serving endpoints base URL from a workspace URL.
        
        This method normalizes the workspace URL first, then appends the
        /serving-endpoints path to create the base URL for all serving endpoint APIs.
        
        Args:
            workspace_url: The workspace URL (will be normalized)
            
        Returns:
            Full serving endpoints URL or None if invalid
            
        Example:
            >>> DatabricksURLUtils.construct_serving_endpoints_url("workspace.databricks.com")
            'https://workspace.databricks.com/serving-endpoints'
        """
        normalized_url = DatabricksURLUtils.normalize_workspace_url(workspace_url)
        if not normalized_url:
            return None
            
        serving_url = f"{normalized_url}/serving-endpoints"
        logger.debug(f"Constructed serving endpoints URL: {serving_url}")
        return serving_url

    @staticmethod
    def construct_llm_base_url(workspace_url: Optional[str]) -> Optional[str]:
        """
        Construct the base URL passed to LiteLLM as ``api_base`` for chat and
        embedding calls, honoring the AI Gateway toggle.

        LiteLLM's Databricks provider appends ``/chat/completions`` (chat) or
        ``/embeddings`` (embeddings) to this base and sends the model name in
        the request body, so the only difference between modes is the base path:

        - AI Gateway on:  ``https://host/ai-gateway/mlflow/v1``
        - AI Gateway off: ``https://host/serving-endpoints``

        Args:
            workspace_url: The workspace URL (will be normalized)

        Returns:
            Base URL for LiteLLM api_base, or None if invalid.
        """
        normalized_url = DatabricksURLUtils.normalize_workspace_url(workspace_url)
        if not normalized_url:
            return None

        if DatabricksURLUtils.is_ai_gateway_enabled():
            base_url = f"{normalized_url}/{DatabricksURLUtils.AI_GATEWAY_PATH}"
            logger.debug(f"Constructed AI Gateway LLM base URL: {base_url}")
        else:
            base_url = f"{normalized_url}/{DatabricksURLUtils.SERVING_ENDPOINTS_PATH}"
            logger.debug(f"Constructed serving-endpoints LLM base URL: {base_url}")
        return base_url

    @staticmethod
    def construct_responses_base_url(workspace_url: Optional[str]) -> Optional[str]:
        """
        Construct the base URL for the OpenAI Responses API, used by gpt-5-codex
        models (via DatabricksCodexCompletion). The OpenAI client appends
        ``/responses`` to this base.

        IMPORTANT: the Responses API is NOT served under the mlflow chat gateway
        path. When the AI Gateway is enabled it lives at ``/ai-gateway/openai/v1``;
        otherwise it's the standard ``/serving-endpoints`` (→ /serving-endpoints/responses).

        - AI Gateway on:  ``https://host/ai-gateway/openai/v1``
        - AI Gateway off: ``https://host/serving-endpoints``

        Args:
            workspace_url: The workspace URL (will be normalized)

        Returns:
            Base URL for the Responses API, or None if invalid.
        """
        normalized_url = DatabricksURLUtils.normalize_workspace_url(workspace_url)
        if not normalized_url:
            return None

        if DatabricksURLUtils.is_ai_gateway_enabled():
            base_url = f"{normalized_url}/{DatabricksURLUtils.AI_GATEWAY_RESPONSES_PATH}"
            logger.debug(f"Constructed AI Gateway Responses base URL: {base_url}")
        else:
            base_url = f"{normalized_url}/{DatabricksURLUtils.SERVING_ENDPOINTS_PATH}"
            logger.debug(f"Constructed serving-endpoints Responses base URL: {base_url}")
        return base_url

    @staticmethod
    def construct_chat_completions_url(
        workspace_url: Optional[str], model_name: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Build the full chat-completions URL for hand-rolled REST callers
        (i.e. callers not going through LiteLLM), honoring the AI Gateway toggle.

        Returns a ``(url, model_for_body)`` tuple:

        - AI Gateway on:  ``(https://host/ai-gateway/mlflow/v1/chat/completions, <model>)``
          The caller MUST add ``"model": model_for_body`` to the request body.
        - AI Gateway off: ``(https://host/serving-endpoints/<model>/invocations, None)``
          The model travels in the URL path; ``model_for_body`` is None.

        Args:
            workspace_url: The workspace URL (will be normalized)
            model_name: The serving endpoint / model name (``databricks/`` prefix stripped)

        Returns:
            Tuple of (chat completions URL, model name to inject into body or None).
        """
        normalized_url = DatabricksURLUtils.normalize_workspace_url(workspace_url)
        clean_model_name = model_name.replace('databricks/', '') if model_name else ''
        if not normalized_url or not clean_model_name:
            logger.warning("Cannot construct chat completions URL: missing workspace URL or model")
            return None, None

        if DatabricksURLUtils.is_ai_gateway_enabled():
            url = f"{normalized_url}/{DatabricksURLUtils.AI_GATEWAY_PATH}/chat/completions"
            return url, clean_model_name
        url = f"{normalized_url}/{DatabricksURLUtils.SERVING_ENDPOINTS_PATH}/{clean_model_name}/invocations"
        return url, None

    @staticmethod
    def construct_embeddings_url(
        workspace_url: Optional[str], model_name: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Build the full embeddings URL for hand-rolled REST callers, honoring the
        AI Gateway toggle. Same ``(url, model_for_body)`` contract as
        :meth:`construct_chat_completions_url`.

        - AI Gateway on:  ``(https://host/ai-gateway/mlflow/v1/embeddings, <model>)``
        - AI Gateway off: ``(https://host/serving-endpoints/<model>/invocations, None)``

        Args:
            workspace_url: The workspace URL (will be normalized)
            model_name: The embedding endpoint / model name (``databricks/`` prefix stripped)

        Returns:
            Tuple of (embeddings URL, model name to inject into body or None).
        """
        normalized_url = DatabricksURLUtils.normalize_workspace_url(workspace_url)
        clean_model_name = model_name.replace('databricks/', '') if model_name else ''
        if not normalized_url or not clean_model_name:
            logger.warning("Cannot construct embeddings URL: missing workspace URL or model")
            return None, None

        if DatabricksURLUtils.is_ai_gateway_enabled():
            url = f"{normalized_url}/{DatabricksURLUtils.AI_GATEWAY_PATH}/embeddings"
            return url, clean_model_name
        url = f"{normalized_url}/{DatabricksURLUtils.SERVING_ENDPOINTS_PATH}/{clean_model_name}/invocations"
        return url, None

    @staticmethod
    def construct_model_invocation_url(
        workspace_url: Optional[str], 
        model_name: str,
        served_model_name: Optional[str] = None
    ) -> Optional[str]:
        """
        Construct the full model invocation URL.
        
        Supports both direct endpoint invocation and served model invocation formats:
        - Direct: /serving-endpoints/<endpoint_name>/invocations
        - Served: /serving-endpoints/<endpoint_name>/served-models/<served_model_name>/invocations
        
        Args:
            workspace_url: The workspace URL (will be normalized)
            model_name: The endpoint name (databricks/ prefix will be removed if present)
            served_model_name: Optional served model name for the served model format
            
        Returns:
            Full invocation URL or None if invalid
            
        Examples:
            >>> DatabricksURLUtils.construct_model_invocation_url("workspace.databricks.com", "databricks-gte-large-en")
            'https://workspace.databricks.com/serving-endpoints/databricks-gte-large-en/invocations'
        """
        serving_url = DatabricksURLUtils.construct_serving_endpoints_url(workspace_url)
        if not serving_url:
            return None
        
        # Clean model name of any provider prefixes
        clean_model_name = model_name.replace('databricks/', '') if model_name else ''
        
        if not clean_model_name:
            logger.warning("Model name is empty after cleaning")
            return None
        
        # Construct URL based on whether we have a served model name
        if served_model_name:
            invocation_url = f"{serving_url}/{clean_model_name}/served-models/{served_model_name}/invocations"
        else:
            invocation_url = f"{serving_url}/{clean_model_name}/invocations"
            
        logger.debug(f"Constructed invocation URL: {invocation_url}")
        return invocation_url
    
    @staticmethod
    def extract_workspace_from_endpoint(endpoint_url: Optional[str]) -> Optional[str]:
        """
        Extract the workspace URL from a full endpoint URL.
        
        This is useful when you have a full endpoint URL and need to get
        back to the base workspace URL.
        
        Args:
            endpoint_url: A full endpoint URL
            
        Returns:
            The workspace URL or None if invalid
            
        Example:
            >>> DatabricksURLUtils.extract_workspace_from_endpoint("https://workspace.databricks.com/serving-endpoints/model/invocations")
            'https://workspace.databricks.com'
        """
        if not endpoint_url:
            return None
            
        # First normalize to ensure we have a clean URL
        normalized = DatabricksURLUtils.normalize_workspace_url(endpoint_url)
        return normalized
    
    @staticmethod
    async def validate_and_fix_environment() -> bool:
        """
        Validate and auto-fix Databricks environment variables.

        This method checks common environment variables and ensures they
        contain the correct format. It will auto-fix issues when possible.

        Note: Now uses unified authentication via get_auth_context() instead of
        directly reading environment variables.

        Returns:
            True if environment is valid (or was fixed), False otherwise
        """
        import os

        issues_found = False

        # Use unified authentication to get current workspace URL
        try:
            from src.utils.databricks_auth import get_auth_context
            auth = await get_auth_context()

            if not auth:
                logger.warning("Failed to get authentication context for validation")
                return False

            workspace_url = auth.workspace_url

            # Check DATABRICKS_HOST - ensure it matches the workspace URL from auth context
            host = os.getenv("DATABRICKS_HOST")
            if host:
                if "/serving-endpoints" in host or "/api" in host:
                    logger.warning(f"DATABRICKS_HOST contains path components: {host}")
                    logger.info("Auto-correcting DATABRICKS_HOST to base workspace URL")
                    normalized = DatabricksURLUtils.normalize_workspace_url(host)
                    if normalized:
                        os.environ["DATABRICKS_HOST"] = normalized
                        logger.info(f"DATABRICKS_HOST corrected to: {normalized}")
                        issues_found = True
                    else:
                        logger.error("Could not auto-correct DATABRICKS_HOST")
                        return False
                elif host.rstrip('/') != workspace_url.rstrip('/'):
                    logger.warning(f"DATABRICKS_HOST ({host}) differs from auth context ({workspace_url})")
                    logger.info("Synchronizing DATABRICKS_HOST with auth context")
                    os.environ["DATABRICKS_HOST"] = workspace_url
                    issues_found = True
            else:
                # Set DATABRICKS_HOST from auth context if not present
                logger.info(f"Setting DATABRICKS_HOST from auth context: {workspace_url}")
                os.environ["DATABRICKS_HOST"] = workspace_url
                issues_found = True

            # Check DATABRICKS_ENDPOINT
            endpoint = os.getenv("DATABRICKS_ENDPOINT")
            if endpoint:
                # This one might legitimately contain /serving-endpoints
                # but let's ensure it's properly formatted
                if endpoint.count("/serving-endpoints") > 1:
                    logger.warning(f"DATABRICKS_ENDPOINT has duplicate /serving-endpoints: {endpoint}")
                    # Try to fix by normalizing and reconstructing
                    workspace = DatabricksURLUtils.extract_workspace_from_endpoint(endpoint)
                    if workspace:
                        fixed_endpoint = DatabricksURLUtils.construct_serving_endpoints_url(workspace)
                        if fixed_endpoint:
                            os.environ["DATABRICKS_ENDPOINT"] = fixed_endpoint
                            logger.info(f"DATABRICKS_ENDPOINT corrected to: {fixed_endpoint}")
                            issues_found = True

            if issues_found:
                logger.info("Environment variables were auto-corrected")
            else:
                logger.debug("Databricks environment variables are properly formatted")

            return True

        except Exception as e:
            logger.error(f"Error validating environment: {str(e)}")
            return False