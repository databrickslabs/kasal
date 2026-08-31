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
grant. The fix: present the SP's own bearer token as the method to use (set
``DATABRICKS_TOKEN`` and pin ``DATABRICKS_AUTH_TYPE=pat``) for the duration of
the call, restoring the original env afterwards.

The OAuth variables are deliberately LEFT IN PLACE. The SDK raises "more than
one authorization method" only when no auth type is chosen
(``Config._validate``); an explicit ``DATABRICKS_AUTH_TYPE`` is enough. An
earlier version also popped ``DATABRICKS_CLIENT_ID``/``SECRET`` from the
process-global env for the whole window — and because these windows run on
worker threads (a judge listing on Unity Catalog is many REST calls long),
every concurrent reader saw ``spn_id=no, spn_cred=no``: the dispatcher and chat
kickoff skipped MLflow tracing, a crew subprocess spawned in the window
inherited the stripped env for its lifetime, and Lakebase engine creation
failed with "cannot configure default credentials" (issue #8).
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

logger = logging.getLogger(__name__)

#: Every Databricks auth env var the swap touches. Saved and restored as a unit
#: so a call leaves the process env exactly as it found it.
#:
#: DATABRICKS_AUTH_TYPE matters: Databricks Apps inject it as "oauth-m2m". If we
#: set DATABRICKS_TOKEN and drop CLIENT_ID/SECRET but leave AUTH_TYPE=oauth-m2m,
#: any bare ``WorkspaceClient()`` built inside the window (e.g. MLflow's
#: ``get_trace`` -> ``_resolve_sql_warehouse_id`` during ``optimize_prompts``)
#: obeys oauth-m2m, finds no m2m creds, and dies with "cannot configure default
#: credentials ... auth_type=oauth-m2m". Pinning it to "pat" makes the bare
#: client use the token instead.
SWAP_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_API_KEY",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_AUTH_TYPE",
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
def pat_auth_env() -> Iterator[bool]:
    """Pin ``DATABRICKS_AUTH_TYPE=pat`` for the duration, WITHOUT stripping the
    OAuth SP creds.

    Use around calls that internally build a bare ``WorkspaceClient()`` AND also
    run other Databricks work that may still need the SP creds — the GEPA
    ``optimize_prompts`` call is exactly this: MLflow's per-eval ``get_trace``
    resolves a SQL warehouse via a bare client (which dies under the app-injected
    ``oauth-m2m`` when no m2m creds resolve), while ``predict_fn`` executes the
    crew whose LLM auth may fall back to SPN. Pinning ``auth_type=pat``
    disambiguates for the bare client (it uses ``DATABRICKS_TOKEN``) without
    removing the creds the crew path might use — the same disambiguation the
    explicit ``WorkspaceClient(..., auth_type="pat")`` in ``databricks_auth`` uses.

    Yields ``True`` when a token is present to pin against, ``False`` (no-op)
    otherwise. Restores the original ``DATABRICKS_AUTH_TYPE`` after.
    """
    if not os.environ.get("DATABRICKS_TOKEN"):
        yield False
        return
    saved = os.environ.get("DATABRICKS_AUTH_TYPE")
    try:
        os.environ["DATABRICKS_AUTH_TYPE"] = "pat"
        yield True
    finally:
        if saved is not None:
            os.environ["DATABRICKS_AUTH_TYPE"] = saved
        elif "DATABRICKS_AUTH_TYPE" in os.environ:
            del os.environ["DATABRICKS_AUTH_TYPE"]


@contextmanager
def single_auth_env(
    *, host: Optional[str] = None, token: Optional[str] = None
) -> Iterator[None]:
    """Present ``token`` as the SINGLE Databricks auth method for one call.

    Sets ``DATABRICKS_TOKEN`` (and ``DATABRICKS_HOST`` when given), pins
    ``DATABRICKS_AUTH_TYPE=pat``, and restores every :data:`SWAP_KEYS` var
    afterwards. The OAuth SP variables are NOT removed — see the module
    docstring: the pinned auth type already makes the SDK ignore them, and
    removing them starved every concurrent reader of the process env. Use when
    a bearer is ALREADY in hand (e.g. an ``AuthContext.token`` derived earlier).
    For the derive-from-ambient-creds case, use :func:`sp_single_auth`.
    """
    saved = {k: os.environ.get(k) for k in SWAP_KEYS}
    try:
        if host is not None:
            os.environ["DATABRICKS_HOST"] = host
        if token is not None:
            os.environ["DATABRICKS_TOKEN"] = token
        # Pin token auth: the SDK then uses DATABRICKS_TOKEN and skips its
        # "more than one authorization method" validation, and a bare
        # WorkspaceClient() built during the window (MLflow's get_trace
        # warehouse resolution) uses the token rather than oauth-m2m.
        os.environ["DATABRICKS_AUTH_TYPE"] = "pat"
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

    bearer = (
        derive_sp_bearer(host, client_id, client_secret)
        if (host and client_id and client_secret)
        else None
    )

    if bearer:
        logger.info(
            "MLflow call: authenticating as the app service principal via its "
            "bearer token (auth type pinned to 'pat'; OAuth creds left in place)."
        )
        with single_auth_env(token=bearer):
            yield True
        return

    # No SP bearer, but if a PAT is already in the env, still pin token auth so a
    # bare WorkspaceClient() built inside the window (MLflow get_trace warehouse
    # resolution during optimize_prompts) uses it instead of the app-injected
    # oauth-m2m it can no longer satisfy. Only a true PAT-less/token-less env is a
    # genuine no-op.
    if os.environ.get("DATABRICKS_TOKEN"):
        with single_auth_env():  # keeps existing token, pins AUTH_TYPE=pat
            yield True
        return

    yield False
