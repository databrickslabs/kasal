"""
Kasal User-Agent Module

Provides centralized User-Agent / application name configuration for Databricks API calls.
Follows the Databricks Partner Well-Architected Framework guidelines.

User-Agent Format: <isv-name_product-name>/<product-version>

See: https://github.com/databrickslabs/partner-architecture/docs/isv-partners/usage-patterns
"""

from src.config.settings import settings

VERSION = settings.VERSION

# Base product name
KASAL_BASE = "Kasal"


class KasalProduct:
    """Product identifiers for specific Databricks integrations requiring granular tracking."""

    JOBS = "jobs"
    GENIE = "genie"
    VECTORSEARCH = "vectorsearch"


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
    Used for Lakebase telemetry.

    Returns:
        Application name string: Kasal/<version>
    """
    return f"{KASAL_BASE}/{VERSION}"


def get_litellm_user_agent() -> str:
    """
    Get User-Agent for LiteLLM Databricks calls.

    LiteLLM formats this as: {partner}_litellm/{litellm_version}
    So we just pass the base name "Kasal".

    Result in API calls: Kasal_litellm/1.79.1 (or current LiteLLM version)

    Returns:
        Base Kasal string for LiteLLM to format
    """
    return KASAL_BASE

