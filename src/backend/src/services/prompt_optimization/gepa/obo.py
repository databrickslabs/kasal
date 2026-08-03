"""On-behalf-of (OBO) auth for MLflow registry writes on Databricks.

The prompt registry write (``register_prompt``) authenticates via the ambient
Databricks env. In a deployed app that env is the APP SERVICE PRINCIPAL
(``DATABRICKS_CLIENT_ID`` / ``DATABRICKS_CLIENT_SECRET`` /
``DATABRICKS_AUTH_TYPE=oauth-m2m``). If that SP lacks UC ``CREATE MODEL`` on the
target schema the call 403s — even though the signed-in USER usually does have
access.

This swaps the process env to the user's OBO token for the duration of one
MLflow operation, then restores it. Mirrors the pattern already used in
``services/mlflow/service.py`` for tracing, with one addition: the SP's
``CLIENT_ID``/``CLIENT_SECRET``/``AUTH_TYPE`` are cleared inside the window,
because MLflow's Databricks auth prefers them over ``DATABRICKS_TOKEN`` and would
otherwise keep using the SP. A missing token is a no-op (the ``with`` block runs
under the unchanged SP env), so callers get SP behavior when no user token is
available.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)

# Env vars that select the app SP; cleared inside the OBO window so a
# DATABRICKS_TOKEN is actually honored rather than shadowed by SP OAuth.
_SP_ENV_KEYS = (
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_AUTH_TYPE",
)


@contextmanager
def obo_databricks_env(
    user_token: Optional[str], workspace_url: Optional[str] = None
) -> Iterator[bool]:
    """Run a block authenticating to Databricks as the user, not the app SP.

    Yields ``True`` when the OBO swap is active, ``False`` when it is a no-op
    (no user token) so the caller can log which identity it used. Always
    restores the original env, even on error.
    """
    if not user_token:
        yield False
        return

    saved = {
        k: os.environ.get(k)
        for k in (*_SP_ENV_KEYS, "DATABRICKS_TOKEN", "DATABRICKS_HOST")
    }
    try:
        for key in _SP_ENV_KEYS:
            os.environ.pop(key, None)
        os.environ["DATABRICKS_TOKEN"] = user_token
        if workspace_url:
            os.environ["DATABRICKS_HOST"] = workspace_url
        yield True
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value
            else:
                os.environ.pop(key, None)


def _grant_hint(prompt_name: str) -> str:
    """A copy-pasteable fix for the UC PERMISSION_DENIED, resolved to the
    actual catalog/schema in play (prompt_name is ``catalog.schema.name``)."""
    parts = prompt_name.split(".")
    target = ".".join(parts[:2]) if len(parts) >= 2 else "<catalog>.<schema>"
    catalog = parts[0] if parts else "<catalog>"
    return (
        f"The app's service principal cannot write to the MLflow prompt registry "
        f"in Unity Catalog schema '{target}'. Grant it (as a catalog admin), "
        f"using the app SP's application id shown as client_id in the error: "
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `<app-sp-application-id>`; "
        f"GRANT USE SCHEMA, CREATE MODEL ON SCHEMA {target} "
        f"TO `<app-sp-application-id>`;  (see docs: prompt-optimization-setup)"
    )


def register_prompt_obo(
    registry_uri: str,
    prompt_name: str,
    template: str,
    user_token: Optional[str],
    workspace_url: Optional[str] = None,
    commit_message: str = "crew baseline registered by Kasal prompt optimization",
) -> Any:
    """Register a prompt, authenticating as the user (OBO) when possible.

    Builds the MlflowClient INSIDE the OBO env window so it picks up the user's
    token rather than the app SP's cached OAuth creds. On a UC permission
    denial, re-raise as a ValueError carrying the exact GRANT to run — so the
    failure surfaces to the user with a fix instead of a raw stack trace.
    """
    import mlflow

    workspace_url = workspace_url or os.environ.get("DATABRICKS_HOST")
    try:
        with obo_databricks_env(user_token, workspace_url) as obo_active:
            logger.info(
                "Registering prompt %s as %s",
                prompt_name,
                "user (OBO)" if obo_active else "app service principal",
            )
            client = mlflow.MlflowClient(registry_uri=registry_uri)
            return client.register_prompt(
                name=prompt_name, template=template, commit_message=commit_message
            )
    except Exception as e:  # noqa: BLE001 — classify UC permission errors
        if "PERMISSION_DENIED" in str(e) or "Permission denied" in str(e):
            raise ValueError(_grant_hint(prompt_name)) from e
        raise
