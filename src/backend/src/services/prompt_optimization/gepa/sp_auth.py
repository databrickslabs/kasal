"""Authenticate a UC registry call as the app SP with a SINGLE auth method.

On a Databricks App the platform injects the app service principal's OAuth
credentials (``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET``). Kasal's
LLM auth path also exports ``DATABRICKS_TOKEN`` (a PAT) for LLM SDK
compatibility. With BOTH present the Databricks SDK refuses to choose —

    ValueError: validate: more than one authorization method configured:
    oauth and pat

— and MLflow falls back to "legacy authentication", so the registry call is NOT
made as the app SP that actually holds the Unity Catalog grant, yielding a
misleading ``PERMISSION_DENIED`` even after the correct grant.

Fix, matching the pattern in ``services/mlflow/service.py._setup_mlflow_auth``:
derive the SP's own bearer token from its OAuth creds and present that as the
SINGLE method (set ``DATABRICKS_TOKEN``, remove the OAuth env vars) for the
duration of the call, restoring the original env afterwards. No-op when OAuth SP
creds are not present (local dev / PAT-only), so those paths are unaffected.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator, Optional

logger = logging.getLogger(__name__)

_SWAP_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_API_KEY",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
)


def _derive_sp_bearer(host: str, client_id: str, client_secret: str) -> Optional[str]:
    """Exchange the SP's OAuth creds for a bearer token (same as MLflowService)."""
    try:
        import requests
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        adder = w.config.authenticate()  # returns a callable that adds the header
        dummy = requests.Request("GET", host)
        adder(dummy)
        bearer = dummy.headers.get("Authorization", "")
        return bearer[len("Bearer ") :] if bearer.startswith("Bearer ") else None
    except Exception as exc:  # noqa: BLE001 — caller falls back to ambient env
        logger.warning(f"Could not derive SP bearer token for registry call: {exc}")
        return None


@contextmanager
def sp_single_auth() -> Iterator[bool]:
    """Present the app SP as the single Databricks auth method for one call.

    Yields ``True`` when the SP-token swap is active, ``False`` when it is a
    no-op (no OAuth SP creds in the env). Always restores the original env.
    """
    host = os.environ.get("DATABRICKS_HOST")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not (host and client_id and client_secret):
        yield False
        return

    bearer = _derive_sp_bearer(host, client_id, client_secret)
    if not bearer:
        yield False
        return

    saved = {k: os.environ.get(k) for k in _SWAP_KEYS}
    logger.info(
        "Registry call: authenticating as the app service principal via a single "
        "token method (OAuth creds temporarily removed to avoid the SDK's "
        "'more than one authorization method' error)."
    )
    try:
        os.environ["DATABRICKS_TOKEN"] = bearer
        os.environ.pop("DATABRICKS_API_KEY", None)
        os.environ.pop("DATABRICKS_CLIENT_ID", None)
        os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
        yield True
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value
            elif key in os.environ:
                del os.environ[key]
