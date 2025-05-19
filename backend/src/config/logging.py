"""
Logging configuration for the backend application.
This module sets up structured logging with proper formatting and routing to
appropriate handlers based on the environment.
"""

import logging
import logging.config
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Import LoggerManager to use its log directory
from src.core.logger import LoggerManager

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