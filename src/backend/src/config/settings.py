import logging
import os
from typing import Any, List, Optional, Union

from pydantic import AnyHttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.paths import BACKEND_ROOT


class Settings(BaseSettings):
    PROJECT_NAME: str = "Modern Backend"
    PROJECT_DESCRIPTION: str = "A modern backend API for the Kasal application"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/api/v1"

    # BACKEND_CORS_ORIGINS is a comma-separated list of origins
    # e.g: "http://localhost,http://localhost:8080"
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    # The localhost dev-server origins are the default OUTSIDE Databricks Apps
    # only (see _apply_databricks_apps_policy): there the SPA is served from the
    # app's own origin, and a credentialed CORS allowance for localhost is
    # something any page on the viewer's machine could use.
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3002",
        "http://127.0.0.1:3002",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    # Database settings
    #
    # 'sqlite' or 'postgres'. The default is SQLite, the same as ``run.sh`` and
    # the Databricks Apps entrypoint. It used to be "postgres" here while run.sh
    # defaulted to SQLite, so the documented companion commands that do not go
    # through run.sh (``alembic upgrade head``, ``python run_seeders.py``) hit
    # postgres@localhost while the server used app.db. Every entry point must
    # agree; opt in to PostgreSQL with DATABASE_TYPE=postgres.
    DATABASE_TYPE: str = os.getenv("DATABASE_TYPE", "sqlite")
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "kasal"
    POSTGRES_PORT: str = "5432"

    # Database file path for SQLite.
    #
    # The default is ABSOLUTE on purpose. It used to be "./app.db", which
    # resolves against the process CWD — so a script run from the repo root or
    # from src/frontend/ silently created its own empty database there instead
    # of opening the real one. Three such strays existed in the tree.
    #
    # DECLARED BEFORE DATABASE_URI, and that ordering is load-bearing. A pydantic
    # "before" validator only sees fields declared ABOVE it, via ``info.data``.
    # While this sat below, ``assemble_db_connection``'s
    # ``info.data.get("SQLITE_DB_PATH", <default>)`` never found the field and
    # silently used the default — so SQLITE_DB_PATH was inert: setting it in the
    # environment changed this attribute and nothing else, and every connection
    # still went to BACKEND_ROOT/app.db. That is how a test suite configured for
    # ``:memory:`` opened, and wrote to, the real development database.
    SQLITE_DB_PATH: Optional[str] = os.getenv(
        "SQLITE_DB_PATH", str(BACKEND_ROOT / "app.db")
    )
    DB_FILE_PATH: Optional[str] = os.getenv("DB_FILE_PATH", "sqlite.db")

    DATABASE_URI: Optional[str] = None
    SYNC_DATABASE_URI: Optional[str] = None

    @field_validator("DATABASE_URI", mode="before")
    def assemble_db_connection(cls, v: Optional[str], info) -> Any:
        if isinstance(v, str):
            return v

        # Check database type to determine URI format
        db_type = info.data.get("DATABASE_TYPE", "sqlite")

        if db_type.lower() == "sqlite":
            sqlite_path = info.data.get("SQLITE_DB_PATH", str(BACKEND_ROOT / "app.db"))
            return f"sqlite+aiosqlite:///{sqlite_path}"
        else:
            # PostgreSQL - return string instead of PostgresDsn to avoid validation issues
            return f"postgresql+asyncpg://{info.data.get('POSTGRES_USER')}:{info.data.get('POSTGRES_PASSWORD')}@{info.data.get('POSTGRES_SERVER')}:{info.data.get('POSTGRES_PORT', 5432)}/{info.data.get('POSTGRES_DB') or ''}"

    @field_validator("SYNC_DATABASE_URI", mode="before")
    def assemble_sync_db_connection(cls, v: Optional[str], info) -> Any:
        if isinstance(v, str):
            return v

        # Check database type to determine URI format
        db_type = info.data.get("DATABASE_TYPE", "sqlite")

        if db_type.lower() == "sqlite":
            sqlite_path = info.data.get("SQLITE_DB_PATH", str(BACKEND_ROOT / "app.db"))
            return f"sqlite:///{sqlite_path}"
        else:
            # Use asyncpg for sync operations too - avoid psycopg2 dependency
            return f"postgresql+asyncpg://{info.data.get('POSTGRES_USER')}:{info.data.get('POSTGRES_PASSWORD')}@{info.data.get('POSTGRES_SERVER')}:{info.data.get('POSTGRES_PORT', 5432)}/{info.data.get('POSTGRES_DB') or ''}"

    # API Documentation
    DOCS_ENABLED: bool = True

    # Logging
    # Support both old LOG_LEVEL and new KASAL_LOG_LEVEL environment variables
    LOG_LEVEL: str = os.getenv("KASAL_LOG_LEVEL", os.getenv("LOG_LEVEL", "INFO"))

    # Server settings
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8000
    DEBUG_MODE: bool = False

    # Local development fallback user: the identity a request WITHOUT an
    # identity header runs as, when LOCAL_DEV_AUTH=true (run.sh sets it; see
    # main._local_dev_auth_enabled). Empty means "dev@localhost". Ignored in
    # Databricks Apps and with ENVIRONMENT=production, where the platform proxy
    # provides X-Forwarded-Email.
    LOCAL_DEV_USER_EMAIL: str = os.getenv("LOCAL_DEV_USER_EMAIL", "")

    # Base URL of the Jev decisions API (services/decisions/provider.py), e.g.
    # "https://jev.example.com". Deployment-owned: never taken from a prompt or
    # a tool result. Empty (the default) means the provider is not configured,
    # so decisions stay off for every workspace and admins cannot enable them.
    JEV_API_BASE: str = os.getenv("JEV_API_BASE", "")

    # Add the following setting to control database seeding
    AUTO_SEED_DATABASE: bool = True

    # Deliver trigger webhooks to loopback/private addresses (skips the SSRF
    # check). A local-dev convenience for a localhost receiver; refused inside
    # Databricks Apps.
    KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS: bool = False

    # Response caching for the legacy LiteLLM completion_with_usage path.
    # Native chat/crew/flow calls use the transport layer. Disk caching is
    # disabled because its default serializer reads pickle from writable files.
    LITELLM_CACHE_ENABLED: bool = (
        os.getenv("LITELLM_CACHE_ENABLED", "true").lower() == "true"
    )
    # Supported backends: "local" (in-memory, default) and "redis" (shared).
    LITELLM_CACHE_TYPE: str = os.getenv("LITELLM_CACHE_TYPE", "local")
    LITELLM_CACHE_TTL: int = int(os.getenv("LITELLM_CACHE_TTL", "3600"))
    # Redis connection (only used when LITELLM_CACHE_TYPE == "redis").
    LITELLM_CACHE_REDIS_HOST: Optional[str] = os.getenv("LITELLM_CACHE_REDIS_HOST")
    LITELLM_CACHE_REDIS_PORT: Optional[str] = os.getenv("LITELLM_CACHE_REDIS_PORT")
    LITELLM_CACHE_REDIS_PASSWORD: Optional[str] = os.getenv(
        "LITELLM_CACHE_REDIS_PASSWORD"
    )

    @model_validator(mode="after")
    def _apply_databricks_apps_policy(self) -> "Settings":
        """Inside Databricks Apps, development-only flags do nothing.

        ``DEBUG_MODE`` (cross-workspace debug endpoints),
        ``KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS`` (disables the webhook
        SSRF check) and ``DOCS_ENABLED`` (serves the OpenAPI schema) are forced
        off, with an error logged when one was set; ``CORS_ORIGINS`` keeps its
        localhost default only outside Apps. Hosting is DERIVED from the
        platform's own variables, never from ``ENVIRONMENT``.
        """
        from src.core.databricks_app import on_databricks_apps

        if not on_databricks_apps():
            return self
        log = logging.getLogger(__name__)
        for flag in (
            "DEBUG_MODE",
            "KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS",
            "DOCS_ENABLED",
        ):
            if flag in self.model_fields_set and getattr(self, flag):
                log.error(
                    "%s is set but ignored inside Databricks Apps (unsafe there)",
                    flag,
                )
            setattr(self, flag, False)
        if "CORS_ORIGINS" not in self.model_fields_set:
            self.CORS_ORIGINS = []
        if os.getenv("ENVIRONMENT", "").strip().lower() not in (
            "",
            "production",
            "prod",
        ):
            log.warning(
                "ENVIRONMENT=%s is ignored: inside Databricks Apps Kasal always "
                "runs as production",
                os.getenv("ENVIRONMENT"),
            )
        return self

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


settings = Settings()
