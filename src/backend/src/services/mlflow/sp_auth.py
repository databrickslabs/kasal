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

This module is the ONLY place a token is written into ``os.environ``
(``tests/unit/architecture/test_no_secrets_in_environ.py`` holds that line),
because MLflow reads its credentials from nowhere else. Windows for different
credentials are mutually exclusive (see ``_pinned``), so a token written for one
request is never visible to a concurrent MLflow call made for another.

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
import threading
from contextlib import contextmanager
from typing import Dict, Iterator, Optional, Tuple

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

        # auth_type names the credentials passed, so neither a PAT in the env
        # nor a pinned DATABRICKS_AUTH_TYPE from a concurrent window can
        # redirect this client (issue #8: with the env pinned to "pat" and the
        # PAT momentarily absent, the exchange failed with "cannot configure
        # default credentials").
        w = WorkspaceClient(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            auth_type="oauth-m2m",
        )
        headers: Dict[str, str] = w.config.authenticate() or {}
        bearer = headers.get("Authorization", "")
        return bearer[len("Bearer ") :] if bearer.startswith("Bearer ") else None
    except Exception as exc:  # noqa: BLE001 — caller falls back to ambient env
        logger.warning(f"Could not derive SP bearer token: {exc}")
        return None


#: Guards every field below. A Condition, because a window whose credential
#: differs from the active one WAITS for it to close rather than overwrite it.
_PIN_COND = threading.Condition(threading.RLock())
_PIN_DEPTH = 0
#: The credential the open windows share (``None`` while none is open).
_PIN_ACTIVE: Optional[Tuple[Optional[str], Optional[str]]] = None
#: SWAP_KEYS as they were before the FIRST active window; restored by the last.
_PIN_ORIGINAL: Dict[str, Optional[str]] = {}
_PIN_THREAD = threading.local()

#: Window key for the app service principal. SP bearers are minted per call, so
#: two SP windows carry different token strings for the SAME identity — keying
#: them by identity lets them overlap instead of queueing behind each other.
_APP_SP = "app-service-principal"


@contextmanager
def _pinned(
    *,
    host: Optional[str] = None,
    token: Optional[str] = None,
    identity: Optional[str] = None,
) -> Iterator[None]:
    """Pin token auth for the duration of a window.

    MLflow (and the bare ``WorkspaceClient()`` it builds) reads Databricks auth
    ONLY from the process environment — there is no per-call credential. The
    environment is shared by every workspace and user this server serves, so a
    window is the only place a token may be written there, and windows for
    DIFFERENT credentials are mutually exclusive: the second waits for the first
    to close. Two concurrent requests therefore can never see each other's token.
    Windows for the same credential overlap (reference counted), and a window
    nested in one this thread already holds proceeds and restores on exit.

    Blocking — callers run in worker threads (``asyncio.to_thread``), never on
    the event loop.
    """
    global _PIN_DEPTH, _PIN_ACTIVE
    key = (host, identity or token)
    with _PIN_COND:
        nested = getattr(_PIN_THREAD, "depth", 0) > 0
        while _PIN_DEPTH > 0 and not nested and _PIN_ACTIVE != key:
            _PIN_COND.wait()
        if _PIN_DEPTH == 0:
            _PIN_ORIGINAL.clear()
            _PIN_ORIGINAL.update({k: os.environ.get(k) for k in SWAP_KEYS})
            _PIN_ACTIVE = key
        outer = {k: os.environ.get(k) for k in SWAP_KEYS} if nested else None
        _PIN_DEPTH += 1
        _PIN_THREAD.depth = getattr(_PIN_THREAD, "depth", 0) + 1
        if host is not None:
            os.environ["DATABRICKS_HOST"] = host
        if token is not None:
            # The one sanctioned write of a token into os.environ: MLflow can
            # only read it from there, and the exclusion above scopes it.
            os.environ["DATABRICKS_TOKEN"] = token
        # The SDK then uses DATABRICKS_TOKEN, skips its "more than one
        # authorization method" validation, and a bare WorkspaceClient() built
        # in the window (MLflow's get_trace warehouse resolution) uses the
        # token rather than oauth-m2m.
        os.environ["DATABRICKS_AUTH_TYPE"] = "pat"
    try:
        yield
    finally:
        with _PIN_COND:
            _PIN_DEPTH -= 1
            _PIN_THREAD.depth -= 1
            restore = _PIN_ORIGINAL if _PIN_DEPTH == 0 else outer
            for k, value in (restore or {}).items():
                if value is not None:
                    os.environ[k] = value
                elif k in os.environ:
                    del os.environ[k]
            if _PIN_DEPTH == 0:
                _PIN_ORIGINAL.clear()
                _PIN_ACTIVE = None
                _PIN_COND.notify_all()


@contextmanager
def pat_auth_env() -> Iterator[bool]:
    """Pin token auth as the app service principal for the duration.

    Use around calls that internally build a bare ``WorkspaceClient()`` AND also
    run other Databricks work — the GEPA ``optimize_prompts`` call is exactly
    this: MLflow's per-eval ``get_trace`` resolves a SQL warehouse via a bare
    client. Pinning ``auth_type=pat`` with the SP's own bearer disambiguates for
    the bare client, the same disambiguation the explicit
    ``WorkspaceClient(..., auth_type="pat")`` in ``databricks_auth`` uses.

    This used to pin whatever ``DATABRICKS_TOKEN`` was already in the process
    environment. Nothing puts one there any more (a token in the shared env
    belongs to no workspace in particular), so the bearer is derived from the
    platform SP credentials instead (or, in local dev only, a PAT the developer
    exported). Yields ``False`` (no-op) where there is neither.
    """
    with sp_single_auth() as active:
        yield active


@contextmanager
def single_auth_env(
    *, host: Optional[str] = None, token: Optional[str] = None
) -> Iterator[None]:
    """Present ``token`` as the Databricks auth method for one call.

    Sets ``DATABRICKS_TOKEN`` (and ``DATABRICKS_HOST`` when given) and pins
    ``DATABRICKS_AUTH_TYPE=pat``; the original :data:`SWAP_KEYS` come back when
    the last overlapping window exits. The OAuth SP variables are NOT removed —
    see the module docstring: the pinned auth type already makes the SDK ignore
    them, and removing them starved every concurrent reader of the process env.
    Use when a bearer is ALREADY in hand (e.g. an ``AuthContext.token`` derived
    earlier). For the derive-from-ambient-creds case, use :func:`sp_single_auth`.
    """
    with _pinned(host=host, token=token):
        yield


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
        with _pinned(token=bearer, identity=_APP_SP):
            yield True
        return

    # No SP bearer. In LOCAL DEV a PAT the developer exported is still pinned as
    # the single method (nothing is written; the window only fixes the auth
    # type), so a bare WorkspaceClient() built inside uses it. Inside Apps there
    # is no such fallback: Kasal never puts a token in the shared environment.
    from src.utils.databricks_auth import local_dev_pat

    if local_dev_pat():
        with _pinned(identity="local-dev-pat"):
            yield True
        return

    yield False
