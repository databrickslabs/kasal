"""
Unit tests for src/services/otel_tracing/otel_config.py.

Covers:
  - is_otel_tracing_enabled()
  - create_kasal_tracer_provider()
  - shutdown_provider()

All OTel SDK dependencies are mocked to keep tests fast and isolated.
"""

import logging
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _reset_active_provider():
    """Force module-level _active_provider back to None between tests."""
    import src.services.otel_tracing.otel_config as mod
    mod._active_provider = None


# ---------------------------------------------------------------------------
# is_otel_tracing_enabled
# ---------------------------------------------------------------------------


class TestIsOtelTracingEnabled:
    """Tests for is_otel_tracing_enabled()."""

    def test_returns_true_when_env_not_set(self, monkeypatch):
        """Default (no env var) should be enabled."""
        monkeypatch.delenv("KASAL_OTEL_TRACING", raising=False)
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is True

    def test_returns_true_when_env_is_true(self, monkeypatch):
        """Explicit 'true' should be enabled."""
        monkeypatch.setenv("KASAL_OTEL_TRACING", "true")
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is True

    def test_returns_true_when_env_is_TRUE_uppercase(self, monkeypatch):
        """Case-insensitive comparison: 'TRUE' should also be enabled."""
        monkeypatch.setenv("KASAL_OTEL_TRACING", "TRUE")
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is True

    def test_returns_true_when_env_is_True_mixed(self, monkeypatch):
        """Case-insensitive comparison: 'True' should also be enabled."""
        monkeypatch.setenv("KASAL_OTEL_TRACING", "True")
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is True

    def test_returns_false_when_env_is_false(self, monkeypatch):
        """'false' should disable tracing."""
        monkeypatch.setenv("KASAL_OTEL_TRACING", "false")
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is False

    def test_returns_false_when_env_is_FALSE_uppercase(self, monkeypatch):
        """'FALSE' should disable tracing."""
        monkeypatch.setenv("KASAL_OTEL_TRACING", "FALSE")
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is False

    def test_returns_false_when_env_is_zero(self, monkeypatch):
        """'0' is not 'true', should disable tracing."""
        monkeypatch.setenv("KASAL_OTEL_TRACING", "0")
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is False

    def test_returns_false_when_env_is_empty_string(self, monkeypatch):
        """Empty string is not 'true', should disable tracing."""
        monkeypatch.setenv("KASAL_OTEL_TRACING", "")
        from src.services.otel_tracing.otel_config import is_otel_tracing_enabled
        assert is_otel_tracing_enabled() is False


# ---------------------------------------------------------------------------
# create_kasal_tracer_provider
# ---------------------------------------------------------------------------


class TestCreateKasalTracerProvider:
    """Tests for create_kasal_tracer_provider()."""

    def setup_method(self):
        _reset_active_provider()

    def teardown_method(self):
        _reset_active_provider()

    @patch("src.services.otel_tracing.otel_config.TracerProvider")
    @patch("src.services.otel_tracing.otel_config.Resource")
    def test_returns_tracer_provider_instance(self, mock_resource_cls, mock_tp_cls):
        """Should return the TracerProvider instance created."""
        mock_provider = MagicMock()
        mock_tp_cls.return_value = mock_provider

        from src.services.otel_tracing.otel_config import create_kasal_tracer_provider
        result = create_kasal_tracer_provider("job-abc", "my-service")

        assert result is mock_provider

    @patch("src.services.otel_tracing.otel_config.TracerProvider")
    @patch("src.services.otel_tracing.otel_config.Resource")
    def test_resource_created_with_correct_attributes(self, mock_resource_cls, mock_tp_cls):
        """Resource.create() must receive service.name and kasal.job_id."""
        mock_resource_cls.create.return_value = MagicMock()
        mock_tp_cls.return_value = MagicMock()

        from src.services.otel_tracing.otel_config import create_kasal_tracer_provider
        create_kasal_tracer_provider("job-xyz", "svc-name")

        call_kwargs = mock_resource_cls.create.call_args[0][0]
        assert call_kwargs["service.name"] == "svc-name"
        assert call_kwargs["kasal.job_id"] == "job-xyz"
        assert "kasal.process_id" in call_kwargs

    @patch("src.services.otel_tracing.otel_config.TracerProvider")
    @patch("src.services.otel_tracing.otel_config.Resource")
    def test_default_service_name_is_kasal_crew_engine(self, mock_resource_cls, mock_tp_cls):
        """Default service_name should be 'kasal-crew-engine'."""
        mock_resource_cls.create.return_value = MagicMock()
        mock_tp_cls.return_value = MagicMock()

        from src.services.otel_tracing.otel_config import create_kasal_tracer_provider
        create_kasal_tracer_provider("job-1")

        call_kwargs = mock_resource_cls.create.call_args[0][0]
        assert call_kwargs["service.name"] == "kasal-crew-engine"

    @patch("src.services.otel_tracing.otel_config.TracerProvider")
    @patch("src.services.otel_tracing.otel_config.Resource")
    def test_sets_module_level_active_provider(self, mock_resource_cls, mock_tp_cls):
        """_active_provider module variable must be set to the new provider."""
        mock_provider = MagicMock()
        mock_tp_cls.return_value = mock_provider
        mock_resource_cls.create.return_value = MagicMock()

        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import create_kasal_tracer_provider

        create_kasal_tracer_provider("job-set-check")
        assert mod._active_provider is mock_provider

    @patch("src.services.otel_tracing.otel_config.TracerProvider")
    @patch("src.services.otel_tracing.otel_config.Resource")
    def test_tracer_provider_receives_resource(self, mock_resource_cls, mock_tp_cls):
        """TracerProvider must be initialised with the created Resource."""
        fake_resource = MagicMock()
        mock_resource_cls.create.return_value = fake_resource
        mock_tp_cls.return_value = MagicMock()

        from src.services.otel_tracing.otel_config import create_kasal_tracer_provider
        create_kasal_tracer_provider("job-res")

        mock_tp_cls.assert_called_once_with(resource=fake_resource)

    @patch("src.services.otel_tracing.otel_config.TracerProvider")
    @patch("src.services.otel_tracing.otel_config.Resource")
    def test_logs_info_message(self, mock_resource_cls, mock_tp_cls, caplog):
        """An INFO log line mentioning the job_id should be emitted."""
        mock_resource_cls.create.return_value = MagicMock()
        mock_tp_cls.return_value = MagicMock()

        from src.services.otel_tracing.otel_config import create_kasal_tracer_provider
        with caplog.at_level(logging.INFO, logger="src.services.otel_tracing.otel_config"):
            create_kasal_tracer_provider("job-log-test")

        assert "job-log-test" in caplog.text

    @patch("src.services.otel_tracing.otel_config.TracerProvider")
    @patch("src.services.otel_tracing.otel_config.Resource")
    def test_subsequent_call_overwrites_active_provider(self, mock_resource_cls, mock_tp_cls):
        """Calling twice should replace _active_provider with the newer instance."""
        first_provider = MagicMock(name="first")
        second_provider = MagicMock(name="second")
        mock_tp_cls.side_effect = [first_provider, second_provider]
        mock_resource_cls.create.return_value = MagicMock()

        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import create_kasal_tracer_provider

        create_kasal_tracer_provider("job-first")
        create_kasal_tracer_provider("job-second")

        assert mod._active_provider is second_provider


# ---------------------------------------------------------------------------
# shutdown_provider
# ---------------------------------------------------------------------------


class TestShutdownProvider:
    """Tests for shutdown_provider()."""

    def setup_method(self):
        _reset_active_provider()

    def teardown_method(self):
        _reset_active_provider()

    def test_calls_shutdown_on_active_provider(self):
        """Should call shutdown() on the active TracerProvider."""
        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import shutdown_provider

        mock_provider = MagicMock()
        mod._active_provider = mock_provider

        shutdown_provider()

        mock_provider.shutdown.assert_called_once()

    def test_sets_active_provider_to_none_after_shutdown(self):
        """_active_provider must be None after successful shutdown."""
        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import shutdown_provider

        mod._active_provider = MagicMock()
        shutdown_provider()

        assert mod._active_provider is None

    def test_no_error_when_active_provider_is_none(self):
        """shutdown_provider() should be a no-op when no provider exists."""
        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import shutdown_provider

        mod._active_provider = None
        # Should not raise
        shutdown_provider()

    def test_sets_active_provider_none_even_if_shutdown_raises(self):
        """_active_provider must still be cleared when shutdown() throws."""
        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import shutdown_provider

        mock_provider = MagicMock()
        mock_provider.shutdown.side_effect = RuntimeError("oops")
        mod._active_provider = mock_provider

        # Should not propagate the error
        shutdown_provider()

        assert mod._active_provider is None

    def test_logs_warning_when_shutdown_raises(self, caplog):
        """A WARNING should be logged when the shutdown call fails."""
        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import shutdown_provider

        mock_provider = MagicMock()
        mock_provider.shutdown.side_effect = Exception("bad shutdown")
        mod._active_provider = mock_provider

        with caplog.at_level(logging.WARNING, logger="src.services.otel_tracing.otel_config"):
            shutdown_provider()

        assert "bad shutdown" in caplog.text

    def test_logs_info_on_successful_shutdown(self, caplog):
        """An INFO message should be logged when shutdown succeeds."""
        import src.services.otel_tracing.otel_config as mod
        from src.services.otel_tracing.otel_config import shutdown_provider

        mod._active_provider = MagicMock()

        with caplog.at_level(logging.INFO, logger="src.services.otel_tracing.otel_config"):
            shutdown_provider()

        assert "shutdown" in caplog.text.lower()
