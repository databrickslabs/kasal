"""App service-principal single-method auth for Databricks MLflow calls.

THE one place SP-token auth lives. Consolidates logic that was triplicated
across ``mlflow/service.py._setup_mlflow_auth``,
``prompt_optimization/gepa/sp_auth.py`` and
``prompt_optimization/gepa/mlflow_session.py`` — a duplication that let the
SAME bug (calling ``config.authenticate()`` as if it were a callable) ship in
more than one copy. MLflow is used across the app (tracing, evaluation, prompt
registry, judges), so this belongs in the MLflow service layer, not under
prompt_optimization.

Why "single method": on a Databricks App the platform injects the app service
principal's OAuth credentials (``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET``).
Kasal's LLM auth path ALSO exports ``DATABRICKS_TOKEN`` (a PAT) for LLM SDK
compatibility. With BOTH present the Databricks SDK refuses to choose —

    ValueError: validate: more than one authorization method configured:
    oauth and pat

— and MLflow falls back to "legacy authentication", so the registry/tracing call
is NOT made as the app SP that holds the Unity Catalog grant, yielding a
misleading ``PERMISSION_DENIED`` (or ``Invalid Token``) even after the correct
grant. The fix: present the SP's own bearer token as the SINGLE method (set
``DATABRICKS_TOKEN``, remove the OAuth env vars) for the duration of the call,
restoring the original env afterwards.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

logger = logging.getLogger(__name__)

#: Every Databricks auth env var the swap touches. Saved and restored as a unit
#: so a call leaves the process env exactly as it found it.
SWAP_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_API_KEY",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
)


def derive_sp_bearer(host: str, client_id: str, client_secret: str) -> Optional[str]:
    """Exchange the SP's OAuth creds for a bearer token.

    ``Config.authenticate()`` returns a ``{"Authorization": "Bearer <tok>"}``
    dict (a set of fresh auth headers) — NOT a callable. An earlier version
    called the result as ``adder(dummy)``, which raised ``TypeError: 'dict'
    object is not callable``; that was swallowed, so this returned None, the
    single-auth swap silently no-op'd, and the call fell back to the ambient PAT
    — the ``403 Invalid Token`` on the UC prompts endpoint. Read the header out
    of the dict.

    Returns None (caller falls back to ambient env) if creds are unusable.
    """
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret)
        headers: Dict[str, str] = w.config.authenticate() or {}
        bearer = headers.get("Authorization", "")
        return bearer[len("Bearer ") :] if bearer.startswith("Bearer ") else None
    except Exception as exc:  # noqa: BLE001 — caller falls back to ambient env
        logger.warning(f"Could not derive SP bearer token: {exc}")
        return None


@contextmanager
def single_auth_env(
    *, host: Optional[str] = None, token: Optional[str] = None
) -> Iterator[None]:
    """Present ``token`` as the SINGLE Databricks auth method for one call.

    Sets ``DATABRICKS_TOKEN`` (and ``DATABRICKS_HOST`` when given), removes the
    OAuth env vars, and restores every :data:`SWAP_KEYS` var afterwards. Use when
    a bearer is ALREADY in hand (e.g. an ``AuthContext.token`` derived earlier).
    For the derive-from-ambient-creds case, use :func:`sp_single_auth`.
    """
    saved = {k: os.environ.get(k) for k in SWAP_KEYS}
    try:
        if host is not None:
            os.environ["DATABRICKS_HOST"] = host
        if token is not None:
            os.environ["DATABRICKS_TOKEN"] = token
        os.environ.pop("DATABRICKS_API_KEY", None)
        os.environ.pop("DATABRICKS_CLIENT_ID", None)
        os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
        yield
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value
            elif key in os.environ:
                del os.environ[key]


@contextmanager
def sp_single_auth() -> Iterator[bool]:
    """Derive the app SP bearer from ambient OAuth creds and present it alone.

    Yields ``True`` when the SP-token swap is active, ``False`` when it is a
    no-op (no OAuth SP creds in the env — local dev / PAT-only), so those paths
    are unaffected. Always restores the original env.
    """
    host = os.environ.get("DATABRICKS_HOST")
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if not (host and client_id and client_secret):
        yield False
        return

    bearer = derive_sp_bearer(host, client_id, client_secret)
    if not bearer:
        yield False
        return

    logger.info(
        "Registry call: authenticating as the app service principal via a single "
        "token method (OAuth creds temporarily removed to avoid the SDK's "
        "'more than one authorization method' error)."
    )
    with single_auth_env(token=bearer):
        yield True
