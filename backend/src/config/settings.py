from typing import Any, Dict, List, Optional, Union
import os
from pathlib import Path

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
    CORS_ORIGINS: List[str] = ["*"]  # Default to allow all origins

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
    POSTGRES_DB: str = "app"
    POSTGRES_PORT: str = "5432"
    DATABASE_URI: Optional[str] = None
    SYNC_DATABASE_URI: Optional[str] = None
    
    # Database file path for SQLite
    SQLITE_DB_PATH: str = os.getenv("SQLITE_DB_PATH", "./app.db")
    DB_FILE_PATH: str = os.getenv("DB_FILE_PATH", "sqlite.db")

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
            # Default to PostgreSQL - return string instead of PostgresDsn to avoid validation issues
            return f"postgresql+asyncpg://{info.data.get('POSTGRES_USER')}:{info.data.get('POSTGRES_PASSWORD')}@{info.data.get('POSTGRES_SERVER')}:{info.data.get('POSTGRES_PORT', 5432)}/{info.data.get('POSTGRES_DB') or ''}"

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
            # Use asyncpg for sync operations too - avoid psycopg2 dependency
            return f"postgresql+asyncpg://{info.data.get('POSTGRES_USER')}:{info.data.get('POSTGRES_PASSWORD')}@{info.data.get('POSTGRES_SERVER')}:{info.data.get('POSTGRES_PORT', 5432)}/{info.data.get('POSTGRES_DB') or ''}"

    # Security settings
    SECRET_KEY: str = "development_secret_key"
    ALGORITHM: str = "HS256"
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    
    # API Documentation
    DOCS_ENABLED: bool = True
    
    # Logging
    LOG_LEVEL: str = "INFO"

    # Server settings
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8000
    DEBUG_MODE: bool = True

    # Add the following setting to control database seeding
    AUTO_SEED_DATABASE: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


settings = Settings() 