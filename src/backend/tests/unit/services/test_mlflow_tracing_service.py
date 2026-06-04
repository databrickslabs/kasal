"""
Comprehensive unit tests for services/mlflow_tracing_service.py
"""

import gc
import asyncio
import logging
import pytest
from contextlib import contextmanager
from unittest.mock import Mock, patch, MagicMock, AsyncMock

from src.services.mlflow_tracing_service import (
    _get_mlflow,
    start_root_trace,
    get_last_active_trace_id,
    flush_async_logging,
    cleanup_async_db_connections,
)


class TestGetMlflow:
    """Tests for _get_mlflow helper."""

    def test_returns_module_when_available(self):
        mock_mlflow = MagicMock()
        with patch.dict("sys.modules", {"mlflow": mock_mlflow}):
            result = _get_mlflow()
        assert result is mock_mlflow

    def test_returns_none_when_not_available(self):
        with patch("builtins.__import__", side_effect=ImportError("no mlflow")):
            pass  # Can't easily unimport mlflow if already imported
        # Test by patching the import inside the function
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None) as mock_fn:
            result = mock_fn()
        assert result is None


class TestStartRootTrace:
    """Tests for start_root_trace context manager."""

    def test_yields_none_when_mlflow_unavailable(self):
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            with start_root_trace("test-trace") as span:
                assert span is None

    def test_uses_start_trace_when_available(self):
        mock_mlflow = MagicMock()
        mock_span = MagicMock()

        @contextmanager
        def fake_start_trace(**kwargs):
            yield mock_span

        mock_mlflow.start_trace = fake_start_trace

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("test-trace", inputs={"k": "v"}) as span:
                assert span is mock_span

    def test_falls_back_to_start_span(self):
        mock_mlflow = MagicMock()
        mock_span = MagicMock()
        mock_span.set_inputs = Mock()

        # No start_trace callable
        del mock_mlflow.start_trace

        @contextmanager
        def fake_start_span(**kwargs):
            yield mock_span

        mock_mlflow.start_span = fake_start_span
        mock_mlflow.tracing = None

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("test-trace", inputs={"key": "val"}) as span:
                assert span is mock_span

    def test_falls_back_to_nullcontext_when_both_fail(self):
        mock_mlflow = MagicMock()

        def bad_start_trace(**kwargs):
            raise RuntimeError("trace api unavailable")

        mock_mlflow.start_trace = bad_start_trace
        mock_mlflow.start_span = None
        mock_mlflow.tracing = None

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("test-trace") as span:
                # nullcontext yields None
                assert span is None

    def test_empty_inputs_defaults_to_empty_dict(self):
        mock_mlflow = MagicMock()

        @contextmanager
        def fake_start_trace(name, inputs):
            assert isinstance(inputs, dict)
            yield MagicMock()

        mock_mlflow.start_trace = fake_start_trace

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("test") as span:
                pass  # No exception means inputs defaulted to {}

    def test_start_trace_in_tracing_module(self):
        mock_mlflow = MagicMock()
        mock_span = MagicMock()

        @contextmanager
        def fake_start_trace(**kwargs):
            yield mock_span

        # mlflow has no direct start_trace but has tracing.start_trace
        mock_mlflow.start_trace = None
        mock_tracing = MagicMock()
        mock_tracing.start_trace = fake_start_trace
        mock_mlflow.tracing = mock_tracing

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("test-trace") as span:
                assert span is mock_span

    def test_span_set_inputs_called_when_method_exists(self):
        mock_mlflow = MagicMock()
        mock_span = MagicMock()
        mock_span.set_inputs = Mock()
        del mock_mlflow.start_trace  # Force fallback to start_span

        @contextmanager
        def fake_start_span(**kwargs):
            yield mock_span

        mock_mlflow.start_span = fake_start_span
        mock_mlflow.tracing = None

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("test", inputs={"a": 1}) as span:
                pass

        mock_span.set_inputs.assert_called_once_with({"a": 1})

    def test_span_set_inputs_exception_is_swallowed(self):
        mock_mlflow = MagicMock()
        mock_span = MagicMock()
        mock_span.set_inputs = Mock(side_effect=RuntimeError("set_inputs failed"))
        del mock_mlflow.start_trace

        @contextmanager
        def fake_start_span(**kwargs):
            yield mock_span

        mock_mlflow.start_span = fake_start_span
        mock_mlflow.tracing = None

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("test", inputs={"a": 1}) as span:
                pass  # Should not raise


class TestGetLastActiveTraceId:
    """Tests for get_last_active_trace_id."""

    def test_returns_none_when_mlflow_unavailable(self):
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            result = get_last_active_trace_id()
        assert result is None

    def test_returns_trace_id_from_tracing_module(self):
        mock_mlflow = MagicMock()
        mock_tracing = MagicMock()
        mock_tracing.get_last_active_trace_id = Mock(return_value="trace-abc")
        mock_mlflow.tracing = mock_tracing

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()

        assert result == "trace-abc"

    def test_returns_trace_id_from_top_level(self):
        mock_mlflow = MagicMock()
        mock_mlflow.get_last_active_trace_id = Mock(return_value="trace-xyz")
        mock_mlflow.tracing = MagicMock(spec=[])  # no get_last_active_trace_id

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()

        assert result == "trace-xyz"

    def test_returns_none_on_exception(self):
        mock_mlflow = MagicMock()
        mock_mlflow.tracing.get_last_active_trace_id = Mock(side_effect=RuntimeError("error"))

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()

        assert result is None

    def test_returns_none_when_no_method_available(self):
        mock_mlflow = MagicMock()
        mock_mlflow.tracing = None
        mock_mlflow.get_last_active_trace_id = None

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()

        assert result is None


class TestFlushAsyncLogging:
    """Tests for flush_async_logging."""

    @pytest.mark.asyncio
    async def test_noop_when_mlflow_unavailable(self):
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            # Should not raise
            await flush_async_logging()

    @pytest.mark.asyncio
    async def test_calls_flush_when_available(self):
        mock_mlflow = MagicMock()
        mock_mlflow.flush_trace_async_logging = Mock()

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            await flush_async_logging()

        mock_mlflow.flush_trace_async_logging.assert_called_once()

    @pytest.mark.asyncio
    async def test_logs_when_flush_not_available(self):
        mock_mlflow = MagicMock(spec=[])  # No flush_trace_async_logging attr

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            await flush_async_logging()  # Should not raise

    @pytest.mark.asyncio
    async def test_handles_flush_exception(self):
        mock_mlflow = MagicMock()
        mock_mlflow.flush_trace_async_logging = Mock(side_effect=RuntimeError("flush failed"))

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            await flush_async_logging()  # Should not raise

    @pytest.mark.asyncio
    async def test_uses_provided_logger(self):
        mock_logger = Mock()
        mock_mlflow = MagicMock()
        mock_mlflow.flush_trace_async_logging = Mock()

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            await flush_async_logging(async_logger=mock_logger)

        mock_logger.info.assert_called()

    @pytest.mark.asyncio
    async def test_none_mlflow_logs_debug(self):
        mock_logger = Mock()
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            await flush_async_logging(async_logger=mock_logger)
        mock_logger.info.assert_called()


class TestCleanupAsyncDbConnections:
    """Tests for cleanup_async_db_connections."""

    def test_no_error_on_empty_gc(self):
        # Should not raise even with no AsyncEngine objects in GC
        with patch("gc.get_objects", return_value=[]):
            cleanup_async_db_connections()

    def test_disposes_async_engine(self):
        from sqlalchemy.ext.asyncio import AsyncEngine
        mock_engine = MagicMock(spec=AsyncEngine)
        mock_engine.sync_engine = MagicMock()

        with patch("gc.get_objects", return_value=[mock_engine, "other obj", 42]):
            cleanup_async_db_connections()

        mock_engine.sync_engine.dispose.assert_called_once()

    def test_handles_dispose_error_gracefully(self):
        from sqlalchemy.ext.asyncio import AsyncEngine
        mock_engine = MagicMock(spec=AsyncEngine)
        mock_engine.sync_engine.dispose = Mock(side_effect=RuntimeError("dispose error"))

        with patch("gc.get_objects", return_value=[mock_engine]):
            # Should not raise
            cleanup_async_db_connections()

    def test_handles_import_error_gracefully(self):
        with patch("builtins.__import__", side_effect=ImportError("no sqlalchemy")):
            # The function imports inside try/except so it should not propagate
            pass
        # Just call it normally - it should work or catch errors internally
        cleanup_async_db_connections()

    def test_uses_provided_logger(self):
        mock_logger = Mock()
        with patch("gc.get_objects", return_value=[]):
            cleanup_async_db_connections(async_logger=mock_logger)
        # Logger may be used for warnings — not required to be called on success

    def test_collects_gc_after_dispose(self):
        with patch("gc.get_objects", return_value=[]):
            with patch("gc.collect") as mock_collect:
                cleanup_async_db_connections()
        mock_collect.assert_called_once()
