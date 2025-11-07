"""
Centralized logging configuration for the Kasal application.

This module provides a unified logging configuration system that works across
all entry points (run.sh, main.py, entrypoint.py) and environments (dev/prod).

IMPORTANT: This module must be imported and configured BEFORE any other modules that create loggers.
"""

import logging
import logging.config
import logging.handlers
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

from src.core.logger import LoggerManager

# Suppress known deprecation warnings from third-party libraries
warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="chromadb")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="websockets")
warnings.filterwarnings("ignore", message=".*Use 'content=.*' to upload raw bytes/text content.*")
warnings.filterwarnings("ignore", message=".*Accessing the 'model_fields' attribute on the instance is deprecated.*")
warnings.filterwarnings("ignore", message=".*remove second argument of ws_handler.*")

# Log file naming
current_date = datetime.now().strftime("%Y-%m-%d")
log_filename = f"backend.{current_date}.log"
error_log_filename = f"backend.error.{current_date}.log"

# Log formatting
VERBOSE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
SIMPLE_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


def get_logging_config(env: str = "development") -> Dict[str, Any]:
    """
    Returns logging configuration based on environment.

    Args:
        env: The environment. One of development, staging, production.

    Returns:
        Dict with logging configuration.
    """
    is_prod = env.lower() == "production"
    is_dev = env.lower() == "development"

    # Get the log directory from LoggerManager
    logger_manager = LoggerManager.get_instance()
    if not logger_manager._log_dir:
        # Initialize with the environment variable if available
        log_dir = os.environ.get("LOG_DIR")
        if log_dir:
            logger_manager.initialize(log_dir)
        else:
            logger_manager.initialize()

    logs_dir = logger_manager._log_dir

    # Base configuration
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "verbose": {
                "format": VERBOSE_FORMAT
            },
            "simple": {
                "format": SIMPLE_FORMAT
            },
        },
        "handlers": {
            "console": {
                "level": "DEBUG" if is_dev else "INFO",
                "class": "logging.StreamHandler",
                "formatter": "simple" if is_dev else "verbose",
                "stream": sys.stdout,
            },
            "file": {
                "level": "INFO",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "verbose",
                "filename": os.path.join(logs_dir, log_filename),
                "maxBytes": 10485760,  # 10 MB
                "backupCount": 5,
                "encoding": "utf-8",
            },
            "error_file": {
                "level": "ERROR",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "verbose",
                "filename": os.path.join(logs_dir, error_log_filename),
                "maxBytes": 10485760,  # 10 MB
                "backupCount": 5,
                "encoding": "utf-8",
            },
            "sqlalchemy_file": {
                "level": "INFO",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "verbose",
                "filename": os.path.join(logs_dir, "sqlalchemy.log"),
                "maxBytes": 10485760,  # 10 MB
                "backupCount": 5,
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "": {  # Root logger
                "handlers": ["console", "file", "error_file"],
                "level": "DEBUG" if is_dev else "INFO",
                "propagate": True,
            },
            "uvicorn": {
                "handlers": ["console", "file", "error_file"],
                "level": "INFO",
                "propagate": False,
            },
            "sqlalchemy.engine": {
                "handlers": ["sqlalchemy_file"],
                "level": "INFO",
                "propagate": False,
            },
            "alembic": {
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False,
            },
        },
    }

    # If in production, add options for more secure and robust logging
    if is_prod:
        # Additional prod-specific handlers could be added here
        # Such as Sentry, ELK, Datadog, etc.
        pass

    return config


def setup_logging(env: str = "development") -> None:
    """
    Sets up logging configuration for the application.

    Args:
        env: The environment. One of development, staging, production.
    """
    config = get_logging_config(env)
    logging.config.dictConfig(config)

    # Log that logging has been configured
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured for {env} environment")


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger with the given name.

    Args:
        name: The name of the logger, typically __name__

    Returns:
        A configured logger instance
    """
    return logging.getLogger(name)


class CentralizedLoggingConfig:
    """
    Centralized logging configuration that works across all entry points.

    This class configures all loggers based on environment variables with proper
    hierarchy and precedence.
    """

    # Logger hierarchy mapping
    LOGGER_HIERARCHY = {
        # Application modules
        'app': ['src'],
        'api': ['src.api'],
        'services': ['src.services'],
        'repositories': ['src.repositories'],
        'engines': ['src.engines'],
        'seeds': ['src.seeds'],
        'utils': ['src.utils'],
        'core': ['src.core'],
        'models': ['src.models'],
        'schemas': ['src.schemas'],
        'config': ['src.config'],
        'db': ['src.db'],
        'dependencies': ['src.dependencies'],

        # Special domains
        'crew': ['crew'],
        'system': ['system'],
        'llm': ['llm', 'LiteLLM', 'backendcrew.llm_config'],
        'scheduler': ['scheduler', 'backendcrew.scheduler',
                     'apscheduler.scheduler', 'apscheduler.executors'],
        'database': ['database'],

        # Third-party libraries
        'sqlalchemy': ['sqlalchemy', 'sqlalchemy.engine', 'sqlalchemy.pool',
                      'aiosqlite', 'alembic'],
        'uvicorn': ['uvicorn', 'uvicorn.error', 'uvicorn.access'],
        'httpx': ['httpx', 'httpcore'],
        'urllib3': ['urllib3', 'requests'],
        'crewai': ['crewai'],
        'mlflow': ['mlflow', 'mlflow.tracing', 'mlflow.models'],
        'litellm': ['litellm', 'LiteLLM'],
    }

    @classmethod
    def parse_log_level(cls, level_str: Optional[str]) -> Optional[int]:
        """Parse a log level string to a logging level constant."""
        if not level_str:
            return None

        level_str = level_str.upper().strip()

        if level_str == "OFF":
            return logging.CRITICAL + 1
        elif level_str == "DEBUG":
            return logging.DEBUG
        elif level_str == "INFO":
            return logging.INFO
        elif level_str in ["WARNING", "WARN"]:
            return logging.WARNING
        elif level_str == "ERROR":
            return logging.ERROR
        elif level_str == "CRITICAL":
            return logging.CRITICAL
        else:
            return None

    @classmethod
    def configure_early(cls):
        """
        Configure logging as early as possible in application startup.
        This should be called BEFORE any module imports that create loggers.
        """
        # Get configuration from environment
        global_level = cls.parse_log_level(
            os.getenv('KASAL_LOG_LEVEL', os.getenv('LOG_LEVEL', 'INFO'))
        )
        debug_all = os.getenv('KASAL_DEBUG_ALL', '').lower() in ['true', '1', 'yes']
        app_level = cls.parse_log_level(os.getenv('KASAL_LOG_APP'))
        third_party_level = cls.parse_log_level(os.getenv('KASAL_LOG_THIRD_PARTY', 'WARNING'))

        # Configure root logger
        if debug_all:
            root_level = logging.DEBUG
        else:
            root_level = global_level or logging.INFO

        logging.basicConfig(
            level=root_level,
            format=SIMPLE_FORMAT,
            force=True
        )

        # Configure application modules
        if app_level is not None or debug_all:
            level = logging.DEBUG if debug_all else (app_level or global_level or logging.INFO)
            for module in ['src', 'backend', 'backendcrew']:
                logger = logging.getLogger(module)
                logger.setLevel(level)

        # Configure third-party libraries
        third_party_libs = [
            'sqlalchemy', 'sqlalchemy.engine', 'sqlalchemy.pool', 'aiosqlite',
            'uvicorn', 'uvicorn.error', 'uvicorn.access',
            'httpx', 'httpcore', 'urllib3', 'requests',
            'crewai', 'mlflow', 'litellm', 'LiteLLM',
            'asyncio', 'PIL', 'matplotlib', 'langchain', 'opentelemetry',
            'chromadb', 'websockets'
        ]

        for lib in third_party_libs:
            logger = logging.getLogger(lib)
            if debug_all:
                logger.setLevel(logging.DEBUG)
            else:
                logger.setLevel(third_party_level or logging.WARNING)

            # If level is OFF, disable the logger
            if (third_party_level or logging.WARNING) > logging.CRITICAL:
                logger.handlers = []
                logger.propagate = False

        # Configure domain-specific loggers
        domains = ['crew', 'system', 'llm', 'api', 'database', 'scheduler']
        for domain in domains:
            env_var = f'KASAL_LOG_{domain.upper()}'
            domain_level = cls.parse_log_level(os.getenv(env_var))

            if domain_level is not None or debug_all:
                level = logging.DEBUG if debug_all else (domain_level or global_level or logging.INFO)

                # Get logger names for this domain
                logger_names = cls.LOGGER_HIERARCHY.get(domain, [domain])

                for logger_name in logger_names:
                    logger = logging.getLogger(logger_name)
                    logger.setLevel(level)

                    # If level is OFF, disable the logger
                    if level > logging.CRITICAL:
                        logger.handlers = []
                        logger.propagate = False

        # Optionally suppress console output
        if os.getenv('KASAL_LOG_CONSOLE', 'true').lower() == 'false':
            root = logging.getLogger()
            root.handlers = [h for h in root.handlers if not isinstance(h, logging.StreamHandler)]

    @classmethod
    def get_configuration_summary(cls) -> str:
        """Get a summary of the current logging configuration."""
        lines = [
            "\n=== Kasal Logging Configuration ===",
            f"Global Level: {os.getenv('KASAL_LOG_LEVEL', 'INFO')}",
            f"Debug All: {os.getenv('KASAL_DEBUG_ALL', 'false')}",
            f"Console Output: {os.getenv('KASAL_LOG_CONSOLE', 'true')}",
            f"File Output: {os.getenv('KASAL_LOG_FILE', 'true')}",
            f"App Level: {os.getenv('KASAL_LOG_APP', 'follows global')}",
            f"Third-Party Level: {os.getenv('KASAL_LOG_THIRD_PARTY', 'WARNING')}",
        ]

        # Check for domain overrides
        overrides = []
        for domain in ['crew', 'system', 'llm', 'api', 'database', 'scheduler']:
            env_var = f'KASAL_LOG_{domain.upper()}'
            if os.getenv(env_var):
                overrides.append(f"{domain}={os.getenv(env_var)}")

        if overrides:
            lines.append(f"Domain Overrides: {', '.join(overrides)}")

        lines.append("===================================\n")
        return "\n".join(lines)


def configure_early_logging():
    """
    Configure logging as early as possible.
    This should be called BEFORE any module imports that create loggers.
    """
    CentralizedLoggingConfig.configure_early()