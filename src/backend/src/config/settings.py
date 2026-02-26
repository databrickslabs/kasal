from typing import Any, Dict, List, Optional, Union
import os
from pathlib import Path
from urllib.parse import quote_plus

from pydantic import AnyHttpUrl, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = "Modern Backend"
    PROJECT_DESCRIPTION: str = "A modern backend API for the Kasal application"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/api/v1"
    
    # BACKEND_CORS_ORIGINS is a comma-separated list of origins
    # e.g: "http://localhost,http://localhost:8080"
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:3002", "http://127.0.0.1:3002"]

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    # Database settings
    DATABASE_TYPE: str = os.getenv("DATABASE_TYPE", "postgres")  # 'postgres' or 'sqlite'
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "kasal"
    POSTGRES_PORT: str = "5432"
    POSTGRES_SSL: bool = os.getenv("POSTGRES_SSL", "false").lower() == "true"
    DATABASE_URI: Optional[str] = None
    SYNC_DATABASE_URI: Optional[str] = None
    
    # Database file path for SQLite
    SQLITE_DB_PATH: Optional[str] = os.getenv("SQLITE_DB_PATH", "./app.db")
    DB_FILE_PATH: Optional[str] = os.getenv("DB_FILE_PATH", "sqlite.db")

    @field_validator("DATABASE_URI", mode="before")
    def assemble_db_connection(cls, v: Optional[str], info) -> Any:
        if isinstance(v, str):
            return v
        
        # Check database type to determine URI format
        db_type = info.data.get("DATABASE_TYPE", "postgres")
        
        if db_type.lower() == "sqlite":
            sqlite_path = info.data.get("SQLITE_DB_PATH", "./app.db")
            return f"sqlite+aiosqlite:///{sqlite_path}"
        else:
            # Default to PostgreSQL - URL-encode user/password so special chars (e.g. @ in emails) are safe
            ssl_suffix = "?ssl=require" if info.data.get("POSTGRES_SSL") else ""
            user = quote_plus(info.data.get('POSTGRES_USER') or '')
            password = quote_plus(info.data.get('POSTGRES_PASSWORD') or '')
            return f"postgresql+asyncpg://{user}:{password}@{info.data.get('POSTGRES_SERVER')}:{info.data.get('POSTGRES_PORT', 5432)}/{info.data.get('POSTGRES_DB') or ''}{ssl_suffix}"

    @field_validator("SYNC_DATABASE_URI", mode="before")
    def assemble_sync_db_connection(cls, v: Optional[str], info) -> Any:
        if isinstance(v, str):
            return v

        # Check database type to determine URI format
        db_type = info.data.get("DATABASE_TYPE", "postgres")

        if db_type.lower() == "sqlite":
            sqlite_path = info.data.get("SQLITE_DB_PATH", "./app.db")
            return f"sqlite:///{sqlite_path}"
        else:
            # Use asyncpg for sync operations too - URL-encode user/password for special chars
            ssl_suffix = "?ssl=require" if info.data.get("POSTGRES_SSL") else ""
            user = quote_plus(info.data.get('POSTGRES_USER') or '')
            password = quote_plus(info.data.get('POSTGRES_PASSWORD') or '')
            return f"postgresql+asyncpg://{user}:{password}@{info.data.get('POSTGRES_SERVER')}:{info.data.get('POSTGRES_PORT', 5432)}/{info.data.get('POSTGRES_DB') or ''}{ssl_suffix}"

    # API Documentation
    DOCS_ENABLED: bool = True
    
    # Logging
    # Support both old LOG_LEVEL and new KASAL_LOG_LEVEL environment variables
    LOG_LEVEL: str = os.getenv("KASAL_LOG_LEVEL", os.getenv("LOG_LEVEL", "INFO"))

    # Server settings
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8000
    DEBUG_MODE: bool = False

    # Local development fallback user.
    # Set this in your .env file when running outside Databricks Apps.
    # Leave empty (the default) in production — the platform provides X-Forwarded-Email.
    LOCAL_DEV_USER_EMAIL: str = os.getenv("LOCAL_DEV_USER_EMAIL", "")

    # Add the following setting to control database seeding
    AUTO_SEED_DATABASE: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


settings = Settings() 