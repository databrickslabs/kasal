"""Use the app identity only for endpoints explicitly assigned at installation."""

import asyncio
import os
from functools import lru_cache

from src.core.databricks_app import DatabricksAppInstallation


@lru_cache(maxsize=4)
def _client(host: str, client_id: str, client_secret: str):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.useragent import with_product

    from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct

    with_product(f"{KASAL_BASE}_{KasalProduct.LLM}", VERSION)
    if not client_id or not client_secret:
        raise ValueError("Databricks App credentials are unavailable")
    return WorkspaceClient(
        host=host,
        client_id=client_id,
        client_secret=client_secret,
        auth_type="oauth-m2m",
    )


def is_installed_model(model: str) -> bool:
    installation = DatabricksAppInstallation.from_env()
    return bool(
        installation.hosted
        and model
        and model.removeprefix("databricks/") == installation.default_model
    )


def get_app_client():
    """Blocking SDK construction; async callers must offload it."""
    installation = DatabricksAppInstallation.from_env()
    if not installation.hosted:
        raise ValueError("Databricks App authentication requires a hosted installation")
    return _client(
        installation.host,
        os.getenv("DATABRICKS_CLIENT_ID", ""),
        os.getenv("DATABRICKS_CLIENT_SECRET", ""),
    )


def get_app_headers():
    """SDK authentication refreshes expiring credentials for long-running jobs."""
    return get_app_client().config.authenticate()


async def get_model_auth_context(model: str, *, user_token=None, group_id=None):
    from src.utils.databricks_auth import AuthContext, get_auth_context

    if not is_installed_model(model):
        return await get_auth_context(user_token=user_token, group_id=group_id)
    installation = DatabricksAppInstallation.from_env()
    headers = await asyncio.to_thread(get_app_headers)
    bearer = headers.get("Authorization", "")
    if not bearer.startswith("Bearer "):
        raise ValueError(
            "Databricks App authentication did not return a bearer credential"
        )
    return AuthContext(
        token=bearer[7:],
        workspace_url=installation.host,
        auth_method="service_principal",
    )
