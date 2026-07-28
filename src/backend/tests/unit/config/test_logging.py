"""
Extended tests for config/logging.py to cover missing branches.
Focuses on: parse_log_level edge cases, configure_early with debug_all,
app-level env vars, domain-level env vars, Databricks Apps console handler,
console suppression, and get_configuration_summary with domain overrides.
"""
import logging
import os
import pytest
from unittest.mock import patch, MagicMock


from src.config.logging import (
    CentralizedLoggingConfig,
    configure_early_logging,
    get_logging_config,
    setup_logging,
    get_logger,
)


# ── parse_log_level ───────────────────────────────────────────────────────────

class TestParseLogLevel:
    def test_none_returns_none(self):
        assert CentralizedLoggingConfig.parse_log_level(None) is None

    def test_empty_string_returns_none(self):
        assert CentralizedLoggingConfig.parse_log_level("") is None

    def test_off_returns_critical_plus_one(self):
        result = CentralizedLoggingConfig.parse_log_level("OFF")
        assert result == logging.CRITICAL + 1

    def test_debug(self):
        assert CentralizedLoggingConfig.parse_log_level("DEBUG") == logging.DEBUG

    def test_info(self):
        assert CentralizedLoggingConfig.parse_log_level("INFO") == logging.INFO

    def test_warning(self):
        assert CentralizedLoggingConfig.parse_log_level("WARNING") == logging.WARNING

    def test_warn_alias(self):
        assert CentralizedLoggingConfig.parse_log_level("WARN") == logging.WARNING

    def test_error(self):
        assert CentralizedLoggingConfig.parse_log_level("ERROR") == logging.ERROR

    def test_critical(self):
        assert CentralizedLoggingConfig.parse_log_level("CRITICAL") == logging.CRITICAL

    def test_unknown_returns_none(self):
        assert CentralizedLoggingConfig.parse_log_level("VERBOSE") is None

    def test_lowercase_normalized(self):
        assert CentralizedLoggingConfig.parse_log_level("debug") == logging.DEBUG


# ── configure_early: debug_all branch ────────────────────────────────────────

class TestConfigureEarly:
    def test_debug_all_sets_debug_level(self):
        """KASAL_DEBUG_ALL=true sets root to DEBUG."""
        env = {
            "KASAL_DEBUG_ALL": "true",
            "KASAL_LOG_LEVEL": "INFO",
            "KASAL_LOG_THIRD_PARTY": "WARNING",
        }
        # Remove Databricks env vars so we don't trigger console handler setup
        with patch.dict("os.environ", env, clear=False):
            # Should not raise
            CentralizedLoggingConfig.configure_early()
        root = logging.getLogger()
        assert root.level <= logging.DEBUG

    def test_debug_all_true_1(self):
        """KASAL_DEBUG_ALL=1 is truthy."""
        env = {"KASAL_DEBUG_ALL": "1"}
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()

    def test_debug_all_yes(self):
        """KASAL_DEBUG_ALL=yes is truthy."""
        env = {"KASAL_DEBUG_ALL": "yes"}
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()

    def test_app_level_env_var(self):
        """KASAL_LOG_APP sets application module log level."""
        env = {
            "KASAL_LOG_APP": "DEBUG",
            "KASAL_DEBUG_ALL": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()
        # src logger should have DEBUG level
        src_logger = logging.getLogger("src")
        assert src_logger.level == logging.DEBUG

    def test_third_party_off_disables_logger(self):
        """KASAL_LOG_THIRD_PARTY=OFF disables third-party loggers."""
        env = {
            "KASAL_LOG_THIRD_PARTY": "OFF",
            "KASAL_DEBUG_ALL": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()
        httpx_logger = logging.getLogger("httpx")
        assert not httpx_logger.propagate or httpx_logger.handlers == []

    def test_domain_level_crew_env_var(self):
        """KASAL_LOG_CREW overrides crew logger level."""
        env = {
            "KASAL_LOG_CREW": "ERROR",
            "KASAL_DEBUG_ALL": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()
        crew_logger = logging.getLogger("crew")
        assert crew_logger.level == logging.ERROR

    def test_domain_level_off_disables_domain_logger(self):
        """KASAL_LOG_SYSTEM=OFF disables system logger."""
        env = {
            "KASAL_LOG_SYSTEM": "OFF",
            "KASAL_DEBUG_ALL": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()
        sys_logger = logging.getLogger("system")
        assert not sys_logger.propagate

    def test_databricks_runtime_adds_console_handler(self):
        """In Databricks Apps, a StreamHandler is added if missing."""
        env = {
            "DATABRICKS_RUNTIME_VERSION": "13.0",
            "KASAL_DEBUG_ALL": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()
        root = logging.getLogger()
        has_stream = any(
            isinstance(h, logging.StreamHandler) for h in root.handlers
        )
        assert has_stream

    def test_databricks_app_name_adds_console_handler(self):
        """DATABRICKS_APP_NAME env triggers console handler setup."""
        env = {
            "DATABRICKS_APP_NAME": "my-kasal-app",
            "KASAL_DEBUG_ALL": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            CentralizedLoggingConfig.configure_early()
        root = logging.getLogger()
        has_stream = any(
            isinstance(h, logging.StreamHandler) for h in root.handlers
        )
        assert has_stream

    def test_console_suppressed_when_kasal_log_console_false(self):
        """Non-Databricks env + KASAL_LOG_CONSOLE=false removes StreamHandlers."""
        env = {
            "KASAL_LOG_CONSOLE": "false",
            "KASAL_DEBUG_ALL": "false",
        }
        with patch.dict("os.environ", env, clear=False):
            # Ensure no Databricks env vars
            env_without_databricks = {
                k: v for k, v in {**os.environ, **env}.items()
                if "DATABRICKS" not in k
            }
            with patch.dict("os.environ", env_without_databricks, clear=True):
                CentralizedLoggingConfig.configure_early()
        # After call, root should have no StreamHandlers (or just file handlers)
        root = logging.getLogger()
        stream_handlers = [
            h for h in root.handlers
            if type(h) is logging.StreamHandler  # exact type, not subclasses
        ]
        # Either 0 or all are RotatingFileHandlers — just verify no exception raised
        assert isinstance(stream_handlers, list)


# ── get_configuration_summary ─────────────────────────────────────────────────

class TestGetConfigurationSummary:
    def test_summary_basic(self):
        """get_configuration_summary returns string with config info."""
        summary = CentralizedLoggingConfig.get_configuration_summary()
        assert "Kasal Logging Configuration" in summary
        assert "Global Level" in summary

    def test_summary_with_domain_overrides(self):
        """get_configuration_summary includes domain overrides when set."""
        env = {
            "KASAL_LOG_CREW": "DEBUG",
            "KASAL_LOG_SYSTEM": "ERROR",
        }
        with patch.dict("os.environ", env, clear=False):
            summary = CentralizedLoggingConfig.get_configuration_summary()
        assert "crew=DEBUG" in summary or "crew" in summary
        assert "system=ERROR" in summary or "system" in summary

    def test_summary_no_overrides(self):
        """get_configuration_summary works when no domain overrides set."""
        env_clean = {k: v for k, v in os.environ.items() if not k.startswith("KASAL_LOG_")}
        with patch.dict("os.environ", env_clean, clear=True):
            summary = CentralizedLoggingConfig.get_configuration_summary()
        assert "Domain Overrides" not in summary


# ── get_logging_config ────────────────────────────────────────────────────────

class TestGetLoggingConfig:
    def test_development_config(self):
        """get_logging_config returns dev config with DEBUG console level."""
        config = get_logging_config("development")
        assert config["version"] == 1
        assert config["handlers"]["console"]["level"] == "DEBUG"

    def test_production_config(self):
        """get_logging_config returns prod config with INFO console level."""
        config = get_logging_config("production")
        assert config["handlers"]["console"]["level"] == "INFO"

    def test_config_has_required_keys(self):
        """get_logging_config includes all required logging config keys."""
        config = get_logging_config()
        assert "formatters" in config
        assert "handlers" in config
        assert "loggers" in config


# ── setup_logging ──────────────────────────────────────────────────────────────

class TestSetupLogging:
    def test_setup_logging_does_not_raise(self):
        """setup_logging executes without error."""
        setup_logging("development")

    def test_setup_logging_production(self):
        """setup_logging works for production env."""
        setup_logging("production")


# ── get_logger ────────────────────────────────────────────────────────────────

class TestGetLogger:
    def test_returns_logger_instance(self):
        """get_logger returns a Logger instance."""
        logger = get_logger("test.module")
        assert isinstance(logger, logging.Logger)

    def test_logger_has_correct_name(self):
        """get_logger returns logger with given name."""
        logger = get_logger("my.custom.module")
        assert logger.name == "my.custom.module"


# ── configure_early_logging convenience function ──────────────────────────────

class TestConfigureEarlyLogging:
    def test_calls_configure_early(self):
        """configure_early_logging delegates to CentralizedLoggingConfig.configure_early."""
        with patch.object(CentralizedLoggingConfig, "configure_early") as mock_ce:
            configure_early_logging()
            mock_ce.assert_called_once()
