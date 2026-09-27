"""Per-call Databricks credentials for MLflow — without touching ``os.environ``.

THE one place MLflow's Databricks auth is decided. Consolidates logic that was
triplicated across ``mlflow/service.py._setup_mlflow_auth``,
``prompt_optimization/gepa/sp_auth.py`` and
``prompt_optimization/gepa/mlflow_session.py``. Callers (all of them
``with``-blocks run in worker threads):

* ``mlflow/experiment_setup.py`` — experiment create on settings save;
* ``mlflow/service.py`` — trace deep link (``get_experiment_by_name``);
* ``mlflow/evaluation_runner.py`` — create_run / ``mlflow.genai.evaluate``;
* ``prompt_optimization/gepa/mlflow_session.py`` — judges (they live in the
  MLflow Prompt Registry / scorer store) and judge alignment;
* ``prompt_optimization/crew_runner.py`` — prompt-registry registration and
  the GEPA ``optimize_prompts`` call.

Why this exists
---------------
MLflow has no per-call credential parameter for Databricks: every tracking,
registry and trace-storage request resolves auth through
``mlflow.utils.databricks_utils.get_databricks_host_creds``, which reads
MLflow's legacy ``EnvironmentVariableConfigProvider`` and builds a bare
``databricks.sdk.WorkspaceClient()`` — and MLflow builds more bare clients of
its own (SQL-warehouse resolution in ``get_trace``, dspy judge alignment). All
of them read the PROCESS environment.

The previous implementation therefore wrote the caller's token into
``os.environ`` and serialized every "window" behind one process-wide lock. A
long GEPA or evaluation run held it for minutes, every other MLflow call for a
different credential waited up to 60 s and then failed, and the token sat in the
shared environment — visible to any thread, and inherited by any subprocess
spawned meanwhile.

The design now
--------------
The credential lives in a :class:`contextvars.ContextVar`, set for the duration
of the ``with`` block and visible only to code running in that context. Three
narrow hooks, installed once on first use, consult it BEFORE the environment:

1. ``databricks.sdk.config.Config._load_from_env`` — a ``Config`` built with no
   explicit credential inside a scope gets the scope's host + token and
   ``auth_type="pat"``. That also settles the Apps-specific problem this module
   was first written for: with the platform's OAuth SP variables AND a token
   present the SDK refuses "more than one authorization method"; an explicit
   ``auth_type`` makes it use the token. A client given explicit credentials
   (``derive_sp_bearer`` below) is left alone.
2. MLflow's ``EnvironmentVariableConfigProvider.get_config`` — returns the
   scope's host + token, so ``MlflowHostCreds`` (and MLflow's per-token
   ``get_workspace_client`` cache key) carry this caller's identity.
3. ``concurrent.futures.ThreadPoolExecutor.submit`` — work submitted from inside
   a scope runs in a copy of the submitter's context, the same thing
   ``asyncio.to_thread`` does. ``mlflow.genai.evaluate`` and
   ``optimize_prompts`` fan out onto their own pools; without this their scorer
   and predict threads would see no credential. Outside a scope ``submit`` is
   untouched.

Nothing is written to ``os.environ``, there is no lock, and two identities can
run side by side for as long as they like: each sees only its own token.

What it does NOT cover: a plain ``threading.Thread`` started inside a scope
does not inherit it (Python threads start with an empty context) and falls back
to the ambient environment — the app SP's own OAuth variables on Databricks
Apps, which is the identity :func:`sp_single_auth` presents anyway. Non-secret
MLflow state that is still process-global (``mlflow.set_tracking_uri``,
``MLFLOW_TRACING_SQL_WAREHOUSE_ID``) is out of this module's scope.
"""

from __future__ import annotations

import contextvars
import logging
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Iterator, Optional, ParamSpec, TypeVar

if TYPE_CHECKING:
    from databricks.sdk.config import Config
    from mlflow.legacy_databricks_cli.configure.provider import (
        DatabricksConfig,
        EnvironmentVariableConfigProvider,
    )

logger = logging.getLogger(__name__)

_P = ParamSpec("_P")
_T = TypeVar("_T")
_F = TypeVar("_F", bound=Callable[..., object])


@dataclass(frozen=True)
class ScopedCredentials:
    """The Databricks identity MLflow calls in the current context use."""

    host: Optional[str]
    token: str

    def __repr__(self) -> str:  # never print the token
        return f"ScopedCredentials(host={self.host!r}, token=***)"


_CREDENTIALS: contextvars.ContextVar[Optional[ScopedCredentials]] = (
    contextvars.ContextVar("kasal_mlflow_databricks_credentials", default=None)
)


def current_credentials() -> Optional[ScopedCredentials]:
    """The credential scoped to the current context, or None outside a scope."""
    return _CREDENTIALS.get()


# ---------------------------------------------------------------------------
# Hooks — installed once, inert outside a scope
# ---------------------------------------------------------------------------

#: ``Config`` attributes that mean "the caller chose its credential"; a Config
#: built with any of them is never overridden by the scope.
_EXPLICIT_AUTH_ATTRS = (
    "token",
    "client_id",
    "client_secret",
    "auth_type",
    "profile",
    "username",
    "password",
    "azure_client_id",
    "azure_client_secret",
    "google_credentials",
    "google_service_account",
)

_HOOKS_LOCK = threading.Lock()
#: Set on every wrapper, so installation is idempotent by inspecting the live
#: target rather than a flag — a module reloaded (or stubbed, in tests) since
#: the last install simply gets hooked again.
_MARK = "_kasal_scoped_credentials"


def _mark(wrapper: _F, original: object) -> _F:
    setattr(wrapper, "__wrapped__", original)
    setattr(wrapper, _MARK, True)
    return wrapper


def _hook_sdk_config() -> None:
    try:
        from databricks.sdk.config import Config
        from databricks.sdk.credentials_provider import DefaultCredentials
    except ImportError:  # no SDK: nothing reads Databricks auth
        return
    original = Config._load_from_env
    if getattr(original, _MARK, False):
        return

    def _load_from_env(self: Config) -> None:
        creds = _CREDENTIALS.get()
        if (
            creds is not None
            # A caller-supplied credentials_strategy is an explicit choice too.
            and isinstance(self._credentials_strategy, DefaultCredentials)
            and not any(a in self._inner for a in _EXPLICIT_AUTH_ATTRS)
        ):
            # attributes() names the ConfigAttribute descriptors on first call;
            # before that a descriptor write lands under the key None.
            self.attributes()
            if creds.host and "host" not in self._inner:
                self.host = creds.host
            self.token = creds.token
            self.auth_type = "pat"
        original(self)

    # setattr, not assignment: mypy rightly rejects assigning to a method.
    setattr(Config, "_load_from_env", _mark(_load_from_env, original))


def _hook_mlflow_env_provider() -> None:
    try:
        from mlflow.legacy_databricks_cli.configure import provider
    except ImportError:  # MLflow not importable (yet): retried next scope
        return
    original = provider.EnvironmentVariableConfigProvider.get_config
    if getattr(original, _MARK, False):
        return

    def get_config(
        self: EnvironmentVariableConfigProvider,
    ) -> Optional[DatabricksConfig]:
        creds = _CREDENTIALS.get()
        # MLflow's provider module is untyped; these name what it returns.
        config: Optional[DatabricksConfig]
        if creds is None or not creds.host:
            config = original(self)
        else:
            config = provider.DatabricksConfig.from_token(creds.host, creds.token)
        return config

    setattr(
        provider.EnvironmentVariableConfigProvider,
        "get_config",
        _mark(get_config, original),
    )


def _hook_thread_pool_submit() -> None:
    original = ThreadPoolExecutor.submit
    if getattr(original, _MARK, False):
        return

    def submit(
        self: ThreadPoolExecutor,
        fn: Callable[_P, _T],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Future[_T]:
        if _CREDENTIALS.get() is None:
            return original(self, fn, *args, **kwargs)
        # A fresh copy per task: one Context cannot be entered by two threads.
        ctx = contextvars.copy_context()
        return original(self, lambda: ctx.run(fn, *args, **kwargs))

    setattr(ThreadPoolExecutor, "submit", _mark(submit, original))


def install_hooks() -> None:
    """Install the three read hooks (idempotent). Called on every scope entry."""
    with _HOOKS_LOCK:
        _hook_sdk_config()
        _hook_mlflow_env_provider()
        _hook_thread_pool_submit()


@contextmanager
def _scoped(host: Optional[str], token: str) -> Iterator[None]:
    install_hooks()
    handle = _CREDENTIALS.set(ScopedCredentials(host=host, token=token))
    try:
        yield
    finally:
        _CREDENTIALS.reset(handle)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def derive_sp_bearer(host: str, client_id: str, client_secret: str) -> Optional[str]:
    """Exchange the SP's OAuth creds for a bearer token.

    ``Config.authenticate()`` returns a ``{"Authorization": "Bearer <tok>"}``
    dict (a set of fresh auth headers) — NOT a callable. An earlier version
    called the result as ``adder(dummy)``, which raised ``TypeError: 'dict'
    object is not callable``; that was swallowed, so this returned None and the
    call fell back to the ambient PAT — the ``403 Invalid Token`` on the UC
    prompts endpoint. Read the header out of the dict.

    Returns None (caller falls back to ambient env) if creds are unusable.
    """
    try:
        from databricks.sdk import WorkspaceClient

        # auth_type names the credentials passed, so neither a PAT in the env
        # nor an enclosing credential scope can redirect this client.
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
        logger.warning("Could not derive SP bearer token: %s", exc)
        return None


@contextmanager
def single_auth_env(
    *, host: Optional[str] = None, token: Optional[str] = None
) -> Iterator[None]:
    """Make ``token`` (at ``host``) the Databricks credential for this block.

    Use when a bearer is ALREADY in hand (e.g. an ``AuthContext.token`` derived
    earlier). Scoped to the current context and anything it submits to a thread
    pool — never written to the process environment, never visible to a
    concurrent call. Without a token this is a no-op (ambient auth applies).
    For the derive-from-ambient-creds case, use :func:`sp_single_auth`.

    The name is historical: this used to swap the process env.
    """
    if not token:
        yield
        return
    with _scoped(host, token):
        yield


@contextmanager
def sp_single_auth() -> Iterator[bool]:
    """Authenticate this block as the app service principal.

    Derives the SP's bearer from the platform-injected OAuth credentials and
    scopes it. Yields ``True`` when a credential is scoped, ``False`` when there
    is none to scope (no OAuth SP creds and no local-dev PAT), so those paths
    are unaffected.
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
            "bearer token (scoped to this call)."
        )
        with _scoped(host, bearer):
            yield True
        return

    # No SP bearer. In LOCAL DEV a PAT the developer exported is scoped as the
    # single method, so a bare WorkspaceClient() built inside uses it even with
    # stray OAuth variables around. Inside Apps there is no such fallback:
    # local_dev_pat() returns None there.
    from src.utils.databricks_auth import local_dev_pat

    pat = local_dev_pat()
    if pat:
        with _scoped(host, pat):
            yield True
        return

    yield False


@contextmanager
def pat_auth_env() -> Iterator[bool]:
    """Alias of :func:`sp_single_auth` for the GEPA ``optimize_prompts`` call.

    MLflow's per-eval ``get_trace`` resolves a SQL warehouse via a bare
    ``WorkspaceClient()``; on Apps the injected ``DATABRICKS_AUTH_TYPE=oauth-m2m``
    made it fail. Inside the scope that client uses the SP bearer instead.
    """
    with sp_single_auth() as active:
        yield active
