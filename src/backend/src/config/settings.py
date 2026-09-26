import logging
import os
from typing import Any, ClassVar, List, Optional, Tuple, Type

from pydantic import field_validator, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from src.core.paths import BACKEND_ROOT

#: The ONLY fields the environment (or a local .env file) may set. Every one is
#: set by the Databricks Apps launcher (``entrypoint.py``), by Kasal itself for a
#: child process, or by local development (``run.sh`` / ``./run.sh postgres``).
#: Every other field is a constant: a setting nobody sets in Databricks Apps is
#: a setting that is never set — it belongs in Configuration or in the code.
ENV_FIELDS = frozenset(
    {
        "DATABASE_TYPE",
        "DATABASE_URI",
        "SYNC_DATABASE_URI",
        "SQLITE_DB_PATH",
        "POSTGRES_SERVER",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "POSTGRES_PORT",
        # Development switches; forced off inside Databricks Apps (see below).
        "DEBUG_MODE",
        "KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS",
    }
)


class _OnlyEnvFields:
    """Mixin for a settings source: ignore every field not in ENV_FIELDS."""

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> Tuple[Any, str, bool]:
        if field_name not in ENV_FIELDS:
            return None, field_name, False
        value: Tuple[Any, str, bool] = super().get_field_value(  # type: ignore[misc]
            field, field_name
        )
        return value


class _EnvSource(_OnlyEnvFields, EnvSettingsSource):
    pass


class _DotEnvSource(_OnlyEnvFields, DotEnvSettingsSource):
    pass


class Settings(BaseSettings):
    PROJECT_NAME: ClassVar[str] = "Modern Backend"
    PROJECT_DESCRIPTION: ClassVar[str] = (
        "A modern backend API for the Kasal application"
    )
    VERSION: ClassVar[str] = "0.1.0"
    API_V1_STR: ClassVar[str] = "/api/v1"

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

    # Logging (KASAL_LOG_LEVEL is set by app.yaml and run.sh).
    LOG_LEVEL: str = os.getenv("KASAL_LOG_LEVEL", "INFO")

    # Server settings (``python -m src.main`` only; run.sh and Apps run uvicorn).
    SERVER_HOST: ClassVar[str] = "0.0.0.0"
    SERVER_PORT: ClassVar[int] = 8000
    DEBUG_MODE: bool = False

    # Local development fallback user: the identity a request WITHOUT an
    # identity header runs as, when LOCAL_DEV_AUTH=true (run.sh sets it; see
    # main._local_dev_auth_enabled). Ignored in Databricks Apps and with
    # ENVIRONMENT=production, where the platform proxy provides
    # X-Forwarded-Email. The dev frontend always sends its own header
    # (VITE_DEV_USER_EMAIL), which wins over this.
    LOCAL_DEV_USER_EMAIL: ClassVar[str] = "dev@localhost"

    # Seed the database at startup (the seeders are idempotent).
    AUTO_SEED_DATABASE: ClassVar[bool] = True

    # Deliver trigger webhooks to loopback/private addresses (skips the SSRF
    # check). A local-dev convenience for a localhost receiver; refused inside
    # Databricks Apps.
    KASAL_EVENT_TRIGGERS_ALLOW_PRIVATE_WEBHOOKS: bool = False

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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """Constructor arguments, then ENV_FIELDS from the environment / .env."""
        return (
            init_settings,
            _EnvSource(settings_cls),
            _DotEnvSource(settings_cls),
        )


settings = Settings()
