"""
Kasal User-Agent Module

Provides centralized User-Agent / application name configuration for Databricks API calls.
Follows the Databricks Partner Well-Architected Framework guidelines.

User-Agent Format: <isv-name_product-name>/<product-version>

See: https://github.com/databrickslabs/partner-architecture/docs/isv-partners/usage-patterns
"""

import asyncio
import logging
import uuid
from typing import Any, Dict, Optional

import aiohttp

from src.config.settings import settings

VERSION = settings.VERSION

# Base product name
KASAL_BASE = "kasal"

logger = logging.getLogger(__name__)


class KasalProduct:
    """Product identifiers for specific Databricks integrations requiring granular tracking."""

    # Infrastructure integrations
    JOBS = "jobs"
    GENIE = "genie"
    VECTORSEARCH = "vectorsearch"
    
    # LLM call types (for distinguishing agent vs guardrail vs generation calls)
    AGENT = "agent"
    GUARDRAIL = "guardrail"
    EMBEDDING = "embedding"
    
    # Generic use for token usage telemetry
    LLM = "llm"
    
    # AI generation services
    NAME_GENERATION = "name_gen"
    TASK_GENERATION = "task_gen"
    AGENT_GENERATION = "agent_gen"
    CREW_GENERATION = "crew_gen"
    TEMPLATE_GENERATION = "template_gen"
    
    # Other services
    INTENT_DETECTION = "intent"
    CONNECTION_TEST = "conn_test"
    
    # Model serving
    AGENTBRICKS = "agentbricks"


def get_user_agent(product: str = None) -> str:
    """
    Generate User-Agent string for Databricks REST API calls.

    Args:
        product: Optional specific Kasal product/integration identifier

    Returns:
        User-Agent string in format: Kasal/<version> or Kasal_<product>/<version>

    Examples:
        >>> get_user_agent()
        'Kasal/0.1.0'
        >>> get_user_agent(KasalProduct.JOBS)
        'Kasal_jobs/0.1.0'
    """
    if product:
        return f"{KASAL_BASE}_{product}/{VERSION}"
    return f"{KASAL_BASE}/{VERSION}"


def get_user_agent_header(product: str = None) -> dict:
    """
    Get User-Agent as a header dictionary for REST API calls.

    Args:
        product: Optional specific Kasal product/integration identifier

    Returns:
        Dictionary with User-Agent header

    Examples:
        >>> get_user_agent_header()
        {'User-Agent': 'Kasal/0.1.0'}
        >>> get_user_agent_header(KasalProduct.GENIE)
        {'User-Agent': 'Kasal_genie/0.1.0'}
    """
    return {"User-Agent": get_user_agent(product)}


def get_application_name() -> str:
    """
    Get application name for database connections (PostgreSQL application_name).

    Returns:
        Application name string: Kasal/<version>
    """
    return f"{KASAL_BASE}/{VERSION}"
    

async def send_logfood_telemetry(
    usage: Dict[str, Any],
    model: str,
    product_context: str,
    group_context: Optional[Any] = None,
    execution_id: Optional[str] = None,
    skip_db_auth: bool = False,
) -> None:
    """
    Send token usage telemetry to Databricks logfood (Two-Request Pattern).
    
    Makes a lightweight GET request to /api/2.0/serving-endpoints with token
    usage data in custom headers. This gets logged in Databricks logfood.
    
    Args:
        usage: Token usage dict with prompt_tokens, completion_tokens, total_tokens
        model: Model name used for the LLM call
        product_context: Product context (e.g., 'crew_gen', 'agent', 'guardrail')
        group_context: Optional GroupContext for authentication
        execution_id: Optional unique execution identifier
        skip_db_auth: If True, skip authentication methods that require database access
                      (use this when called from callbacks during database transactions)
    
    Example:
        >>> await send_logfood_telemetry(
        ...     usage={"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
        ...     model="databricks-llama-4-maverick",
        ...     product_context=KasalProduct.CREW_GENERATION,
        ...     group_context=group_context
        ... )
    """
    
    try:
        # Import here to avoid circular imports
        from src.utils.databricks_auth import get_auth_context
        
        # Get user token from group_context if available
        user_token = getattr(group_context, 'user_token', None) if group_context else None
        
        # Get authentication using the unified auth chain
        # When skip_db_auth=True, we only use OBO or SPN auth (no database PAT lookup)
        auth = await get_auth_context(user_token=user_token, skip_db_auth=skip_db_auth)
        
        if not auth:
            logger.warning("Logfood telemetry: No authentication available")
            return
        
        # Generate execution ID if not provided
        exec_id = execution_id or str(uuid.uuid4())
        
        # Build telemetry headers with token usage
        telemetry_headers = {
            "Authorization": f"Bearer {auth.token}",
            "User-Agent": f"{KASAL_BASE}_telemetry/{VERSION}/{product_context}/model={model}/input_tokens={usage.get('prompt_tokens', 0)}/output_tokens={usage.get('completion_tokens', 0)}",
        }
        
        # Make a lightweight GET request (logged in Databricks logfood)
        telemetry_url = f"{auth.workspace_url}/api/2.0/serving-endpoints"
        
        logger.info(
            f"Sending logfood telemetry: {product_context}, "
            f"tokens={usage.get('total_tokens', 0)}, model={model}"
        )
        
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(
                telemetry_url,
                headers=telemetry_headers,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as response:
                if response.status == 200:
                    logger.info(
                        f"Logfood telemetry sent - execution_id={exec_id}, "
                        f"tokens={usage.get('total_tokens', 0)}, context={product_context}"
                    )
                else:
                    text = await response.text()
                    logger.warning(
                        f"Logfood telemetry returned status {response.status}: {text[:200]}"
                    )
                    
    except asyncio.TimeoutError:
        logger.warning("Logfood telemetry request timed out")
    except Exception as e:
        # Telemetry failures should not affect main flow
        logger.warning(f"Failed to send logfood telemetry: {str(e)}")


def _extract_token(token_value: str) -> str:
    """Extract access token from either plain token or JSON format."""
    if not token_value:
        return ""
    
    # Check if it's a JSON object (Databricks CLI format)
    token_value = token_value.strip()
    if token_value.startswith('{'):
        try:
            import json
            token_data = json.loads(token_value)
            return token_data.get('access_token', token_value)
        except (json.JSONDecodeError, TypeError):
            pass
    
    return token_value


def send_logfood_telemetry_sync(
    usage: Dict[str, Any],
    model: str,
    product_context: str,
    workspace_url: str,
    token: str,
    execution_id: Optional[str] = None,
) -> None:
    """
    Synchronous version of logfood telemetry for non-async contexts (e.g., embeddings).
    
    Args:
        usage: Token usage dict with prompt_tokens, completion_tokens, total_tokens
        model: Model name used
        product_context: Product context (e.g., 'embedding', 'agent')
        workspace_url: Databricks workspace URL
        token: Databricks auth token (can be plain token or JSON with access_token)
        execution_id: Optional unique execution identifier
    """
    try:
        import requests
        
        # Extract actual token if it's in JSON format (Databricks CLI format)
        actual_token = _extract_token(token)
        
        if not actual_token:
            logger.debug("No valid token found, skipping telemetry")
            return
        
        # Generate execution ID if not provided
        exec_id = execution_id or str(uuid.uuid4())
        
        # Ensure workspace URL has https://
        if not workspace_url.startswith("http"):
            workspace_url = f"https://{workspace_url}"
        
        # Build telemetry headers with token usage
        telemetry_headers = {
            "Authorization": f"Bearer {actual_token}",
            "User-Agent": f"{KASAL_BASE}_telemetry/{VERSION}/{product_context}/model={model}/input_tokens={usage.get('prompt_tokens', 0)}/output_tokens={usage.get('completion_tokens', 0)}",
        }
        
        # Make a lightweight GET request (logged in Databricks logfood)
        telemetry_url = f"{workspace_url}/api/2.0/serving-endpoints"
        
        logger.info(
            f"Sending logfood telemetry (sync): {product_context}, "
            f"tokens={usage.get('total_tokens', 0)}, model={model}"
        )
        
        response = requests.get(telemetry_url, headers=telemetry_headers, timeout=5)
        
        if response.status_code == 200:
            logger.info(
                f"Logfood telemetry sent - execution_id={exec_id}, "
                f"tokens={usage.get('total_tokens', 0)}, context={product_context}"
            )
        else:
            logger.warning(
                f"Logfood telemetry returned status {response.status_code}: {response.text[:200]}"
            )
            
    except Exception as e:
        # Telemetry failures should not affect main flow
        logger.warning(f"Failed to send logfood telemetry (sync): {str(e)}")

