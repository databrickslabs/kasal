"""Databricks Apps installation defaults, independent of tenant configuration.

Ambient SDK credentials (and the dev entrypoint's synthetic app name) are not
evidence that Kasal is hosted. Resource bindings only apply inside the platform.
"""

import hashlib
import os
import re
from dataclasses import dataclass
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

#: Exact names a child inherits.
CHILD_ENV_NAMES = frozenset(
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
        # Kasal settings without a namespace prefix (see config/settings.py).
        "PROJECT_NAME",
        "PROJECT_DESCRIPTION",
        "VERSION",
        "API_V1_STR",
        "AUTO_SEED_DATABASE",
        "BACKEND_CORS_ORIGINS",
        "SYNC_DATABASE_URI",
        "SERVER_HOST",
        "SERVER_PORT",
        "MCP_SERVER_ENABLED",
        "ENCRYPTION_KEY",  # the app's at-rest key: children decrypt DB secrets
        "ENVIRONMENT",
        "DEBUG_MODE",
        "DOCS_ENABLED",
        "CORS_ORIGINS",
        "USE_NULLPOOL",
        "SQL_DEBUG",
        "SEED_DEBUG",
        "DB_FILE_PATH",
        "FRONTEND_STATIC_DIR",
        "LOCAL_DEV_AUTH",
        "LOCAL_DEV_USER_EMAIL",
        "GROUP_MEMBERSHIP_CACHE_TTL",
        "SSE_HEARTBEAT_SECONDS",
        "INSTRUCTOR_MODEL_NAME",
        "JEV_API_BASE",
        "DAX_LLM_BATCH_SIZE",
        # Provider endpoint URLs and model-name overrides (not credentials).
        "ANTHROPIC_API_BASE",
        "GEMINI_API_BASE",
        "DEEPSEEK_ENDPOINT",
        "KIMI_ENDPOINT",
        "OPENAI_BASE_URL",
        "AGENT_MODEL",
        "CONNECTION_MODEL",
        "CREW_MODEL",
        "TASK_MODEL",
        "PROMPT_IMPROVE_MODEL",
    }
)

#: Prefixes a child inherits: every ``DATABRICKS_*`` the Apps platform injects
#: (the app service principal included, so a child authenticates as the app),
#: the Lakebase ``PG*`` binding, the app.yaml ``KASAL_*`` bindings and process
#: control, database/logging/tracing configuration and Kasal's tuning knobs.
CHILD_ENV_PREFIXES = (
    "DATABRICKS_",
    "PG",
    "KASAL_",
    "POSTGRES_",
    "DATABASE_",
    "SQLITE_",
    "LAKEBASE_",
    "LOG_",
    "LC_",
    "MLFLOW_",
    "OTEL_",
    "UVICORN_",
    "CREWAI_",
    "CREW_",
    "FLOW_",
    "A2UI_",
    "CHAT_",
    "KNOWLEDGE_",
    "EMBEDDING_",
    "WORKFLOW_RECIPE_",
    "SCRAPE_",
    "GEPA_",
    "DISPATCHER_",
    "DEFAULT_",
    "VLLM_",
    "KAT_",
    "OLLAMA_",
    "LITELLM_",
    "RATE_LIMIT_",
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
