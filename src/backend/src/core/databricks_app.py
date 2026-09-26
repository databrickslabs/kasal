"""Databricks Apps installation defaults, independent of tenant configuration.

Ambient SDK credentials (and the dev entrypoint's synthetic app name) are not
evidence that Kasal is hosted. Resource bindings only apply inside the platform.
"""

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Optional


@dataclass(frozen=True)
class DatabricksAppInstallation:
    hosted: bool
    host: str = ""
    app_name: str = ""
    workspace_id: str = ""
    warehouse_id: str = ""
    output_volume: str = ""
    default_model: str = ""

    @classmethod
    def from_env(
        cls, env: Optional[Mapping[str, str]] = None
    ) -> "DatabricksAppInstallation":
        env = os.environ if env is None else env

        def value(key: str) -> str:
            return (env.get(key) or "").strip()

        hosted = value("KASAL_DEPLOYMENT_MODE").lower() != "local" and all(
            value(key)
            for key in (
                "DATABRICKS_APP_NAME",
                "DATABRICKS_APP_PORT",
                "DATABRICKS_WORKSPACE_ID",
                "DATABRICKS_HOST",
            )
        )
        if not hosted:
            return cls(hosted=False)
        return cls(
            hosted=True,
            host=value("DATABRICKS_HOST").rstrip("/"),
            app_name=value("DATABRICKS_APP_NAME"),
            workspace_id=value("DATABRICKS_WORKSPACE_ID"),
            warehouse_id=value("KASAL_SQL_WAREHOUSE_ID"),
            output_volume=value("KASAL_OUTPUT_VOLUME"),
            default_model=value("KASAL_DEFAULT_MODEL"),
        )

    def experiment_name(self, group_id: str) -> str:
        """Stable personal/teamspace destination across installation resource changes."""
        if not self.hosted or not group_id:
            raise ValueError("A hosted installation and teamspace are required")
        installation = hashlib.sha256(
            f"{self.workspace_id}:{self.app_name}".encode()
        ).hexdigest()[:16]
        tenant = hashlib.sha256(group_id.encode()).hexdigest()[:24]
        return f"/Shared/kasal-{installation}-{tenant}-traces-uc"

    def output_path(self, group_id: str) -> str:
        if not group_id or not self.output_volume.startswith("/Volumes/"):
            raise ValueError("Output storage requires an assigned volume and teamspace")
        scope = hashlib.sha256(
            f"{self.workspace_id}:{self.app_name}:{group_id}".encode()
        ).hexdigest()[:32]
        return f"{self.output_volume.rstrip('/')}/kasal/{scope}"


def is_databricks_app() -> bool:
    return DatabricksAppInstallation.from_env().hosted


def on_databricks_apps() -> bool:
    """Running on the Databricks Apps platform, for SAFETY decisions.

    Broader than :func:`is_databricks_app` (which also needs the workspace id
    and port for installation-scoped defaults): the platform's own
    ``DATABRICKS_APP_NAME`` alone is enough to refuse development behaviour,
    so a partial environment fails closed.
    """
    return bool(os.getenv("DATABRICKS_APP_NAME", "").strip()) or is_databricks_app()


def is_production() -> bool:
    """Whether Kasal must behave as production.

    DERIVED inside Databricks Apps, never configured: there Kasal is production
    whatever ``ENVIRONMENT`` says (the platform sets ``DATABRICKS_APP_NAME``;
    ``ENVIRONMENT`` is not in app.yaml, and its "development" default used to
    put every deployment in local-dev mode). Outside Apps,
    ``ENVIRONMENT=production``/``prod`` opts in.
    """
    if on_databricks_apps():
        return True
    return os.getenv("ENVIRONMENT", "").strip().lower() in ("production", "prod")


def is_local_dev() -> bool:
    """A developer's machine: never inside Apps, and ENVIRONMENT (default
    ``development``) is development/dev/local."""
    if is_production():
        return False
    return os.getenv("ENVIRONMENT", "development").strip().lower() in (
        "development",
        "dev",
        "local",
    )


@dataclass(frozen=True)
class LakebaseAppResource:
    host: str
    database: str
    user: str
    port: int = 5432
    endpoint: str = ""

    @classmethod
    def from_env(cls) -> Optional["LakebaseAppResource"]:
        if not is_databricks_app():
            return None
        host, database, user = (
            os.getenv(key, "").strip() for key in ("PGHOST", "PGDATABASE", "PGUSER")
        )
        if not (host or database or user):
            return None
        if not all((host, database, user)):
            raise ValueError(
                "The Lakebase app resource must supply PGHOST, PGDATABASE and PGUSER"
            )
        port = int(os.getenv("PGPORT", "5432"))
        if not 1 <= port <= 65535:
            raise ValueError("Invalid PGPORT in the Lakebase app resource")
        return cls(
            host, database, user, port, os.getenv("KASAL_LAKEBASE_RESOURCE", "").strip()
        )

    def configuration(self) -> dict:
        return {
            "enabled": True,
            "database_type": "lakebase",
            "instance_status": "READY",
            "endpoint": self.host,
            "instance_name": self.endpoint or self.host,
            "database_name": self.database,
            "installation_managed": True,
        }


def fallback_trace_experiment(group_id: Optional[str]) -> Optional[str]:
    """The crew-traces experiment when the MLflow configuration names none.

    Callers use the configured experiment first (``mlflow_config`` /
    ``MLflowService.configured_crew_traces_experiment``). Without one:

    - inside Databricks Apps, the per-teamspace installation experiment
      (:meth:`DatabricksAppInstallation.experiment_name`) — private to the
      teamspace, unlike the old shared ``/Shared/kasal-crew-execution-traces``
      that every workspace user could read;
    - otherwise ``None``: the caller skips or reports it, never invents a path.
      Outside Apps the experiment comes from Configuration → MLflow (callers go
      through ``MLflowService.configured_crew_traces_experiment``).
    """
    installation = DatabricksAppInstallation.from_env()
    if installation.hosted:
        return installation.experiment_name(group_id) if group_id else None
    return None


def apps_data_dir(name: str) -> Optional[Path]:
    """App-relative data directory inside Databricks Apps, else ``None``.

    The home directory of an Apps container is not where an app keeps data
    (and is recreated with it), so inside Apps local stores default under the
    app's own tree instead of ``~``. This is still the container's filesystem:
    it does NOT survive a redeploy — anything that must persist belongs in
    Lakebase or a Unity Catalog volume.
    """
    if not on_databricks_apps():
        return None
    from src.core.paths import BACKEND_ROOT

    return BACKEND_ROOT / "data" / name


def resolve_lakebase_instance_name(configured: Optional[str] = None) -> str:
    """The Lakebase instance to connect to. The ONE place this is decided.

    1. ``configured``: the database setting (the ``database_configs`` Lakebase
       row, or a memory backend's ``lakebase_config.instance_name``);
    2. the Databricks Apps binding: ``KASAL_LAKEBASE_RESOURCE``, else the bound
       resource's ``PGHOST``.

    There is no invented fallback (it used to be ``"kasal-lakebase"``, which
    matched nothing a user created): with neither, this raises
    :class:`~src.core.exceptions.LakebaseNotConfiguredError`.
    """
    name = (configured or "").strip()
    if name:
        return name
    binding = os.getenv("KASAL_LAKEBASE_RESOURCE", "").strip()
    if binding:
        return binding
    installed = LakebaseAppResource.from_env()
    if installed is not None:
        return installed.endpoint or installed.host
    from src.core.exceptions import LakebaseNotConfiguredError

    raise LakebaseNotConfiguredError()


def lakebase_instance_from_config(config: Optional[Mapping[str, object]]) -> str:
    """:func:`resolve_lakebase_instance_name` for a Lakebase config dict."""
    configured = (config or {}).get("instance_name")
    return resolve_lakebase_instance_name(
        configured if isinstance(configured, str) else None
    )


# ---------------------------------------------------------------------------
# What a child process may inherit.
#
# Crew/flow subprocesses and the MCP tool helper are Kasal code running for ONE
# workspace; the server that spawns them serves MANY. A child therefore gets an
# ALLOW-LISTED environment — platform facts, Kasal's own settings, process
# control, database connection and logging — and never a workspace credential.
# Children fetch their workspace's keys through the services (ApiKeysService /
# get_auth_context), exactly like the parent.
# ---------------------------------------------------------------------------

#: Read by the OS, Python and HTTP/TLS libraries rather than by Kasal code.
_PROCESS_ENV_NAMES = frozenset(
    {
        # Process basics.
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "TERM",
        "LANG",
        "LANGUAGE",
        "TZ",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SYSTEMROOT",
        "PYTHONPATH",
        "PYTHONUNBUFFERED",
        "PYTHONHASHSEED",
        "PYTHONIOENCODING",
        "VIRTUAL_ENV",
        # TLS trust and egress proxies (corporate networks).
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
    }
)

#: Kasal's own settings without a namespace prefix (see config/settings.py).
#: Each must be read somewhere in ``src/`` — tests/unit/core checks it.
_KASAL_ENV_NAMES = frozenset(
    {
        "SYNC_DATABASE_URI",
        "MCP_SERVER_ENABLED",
        "ENCRYPTION_KEY",  # the app's at-rest key: children decrypt DB secrets
        "ENVIRONMENT",
        "DEBUG_MODE",
        "USE_NULLPOOL",
        "SQL_DEBUG",
        "SEED_DEBUG",
        "LOCAL_DEV_AUTH",
    }
)

#: Exact names a child inherits.
CHILD_ENV_NAMES = _PROCESS_ENV_NAMES | _KASAL_ENV_NAMES

#: Prefixes a child inherits: every ``DATABRICKS_*`` the Apps platform injects
#: (the app service principal included, so a child authenticates as the app),
#: the Lakebase ``PG*`` binding, the app.yaml ``KASAL_*`` bindings and process
#: control, and database/logging/tracing configuration. Kasal's tuning knobs are
#: not here: they are Configuration settings a child loads from the database
#: (``engine_settings``), not variables it inherits. Only list a prefix that
#: something in ``src/`` actually reads — ``tests/unit/core/`` checks it.
CHILD_ENV_PREFIXES = (
    "DATABRICKS_",
    "PG",
    "KASAL_",
    "POSTGRES_",
    "DATABASE_",
    "SQLITE_",
    "LOG_",
    "LC_",
    "MLFLOW_",
    "OTEL_",
    "CREWAI_",
)

#: A name shaped like a credential. Even under an allowed prefix it is dropped
#: unless listed in :data:`CHILD_ENV_CREDENTIALS`.
_CREDENTIAL_NAME = re.compile(r"(_API_KEY|_TOKEN|_SECRET|PASSWORD)$")

#: Credential-shaped names a child still needs: the app service principal the
#: platform injects, and the operator's own database password (local Postgres).
CHILD_ENV_CREDENTIALS = frozenset({"DATABRICKS_CLIENT_SECRET", "POSTGRES_PASSWORD"})

#: A PAT a developer exported in their shell. Passed to children OUTSIDE
#: Databricks Apps only (single-user local dev); never inside, where the
#: environment is shared by every workspace the server serves.
LOCAL_DEV_PAT_NAMES = frozenset({"DATABRICKS_TOKEN", "DATABRICKS_API_KEY"})


def child_env_allowed(name: str, *, hosted: Optional[bool] = None) -> bool:
    """Whether a child process may inherit the variable ``name``."""
    if name in LOCAL_DEV_PAT_NAMES:
        return not (is_databricks_app() if hosted is None else hosted)
    if _CREDENTIAL_NAME.search(name) and name not in CHILD_ENV_CREDENTIALS:
        return False
    return name in CHILD_ENV_NAMES or name.startswith(CHILD_ENV_PREFIXES)


def child_environment(env: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """An allow-listed copy of ``env`` (default ``os.environ``) for a child."""
    source = os.environ if env is None else env
    hosted = DatabricksAppInstallation.from_env(source).hosted
    return {k: v for k, v in source.items() if child_env_allowed(k, hosted=hosted)}


def restrict_environment_to_child_allow_list() -> List[str]:
    """Drop every non-allow-listed variable from THIS process's environment.

    The first thing a spawned crew/flow interpreter does. ``multiprocessing``
    spawn cannot be handed an environment, so the child applies the allow-list
    to what it inherited. Returns the names removed (for a debug log; values
    are never logged).
    """
    hosted = is_databricks_app()
    removed = [k for k in list(os.environ) if not child_env_allowed(k, hosted=hosted)]
    for name in removed:
        os.environ.pop(name, None)
    return removed
