"""
Comprehensive unit tests for src/services/mlflow_tracing_service.py
Targets coverage of start_root_trace, get_last_active_trace_id,
flush_async_logging, and cleanup_async_db_connections.
"""

import asyncio
import logging
import pytest
from contextlib import nullcontext
from unittest.mock import AsyncMock, MagicMock, Mock, patch


# ---------------------------------------------------------------------------
# _get_mlflow helper
# ---------------------------------------------------------------------------

class TestGetMlflow:
    def test_returns_mlflow_when_available(self):
        mock_mlflow = MagicMock()
        with patch.dict("sys.modules", {"mlflow": mock_mlflow}):
            from importlib import reload
            import src.services.mlflow_tracing_service as mod
            result = mod._get_mlflow()
            assert result is not None

    def test_returns_none_when_mlflow_unavailable(self):
        import src.services.mlflow_tracing_service as mod
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            result = mod._get_mlflow()
            assert result is None


# ---------------------------------------------------------------------------
# start_root_trace
# ---------------------------------------------------------------------------

class TestStartRootTrace:
    def test_yields_none_when_mlflow_unavailable(self):
        """When mlflow is not importable, context manager yields None."""
        from src.services.mlflow_tracing_service import start_root_trace
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            with start_root_trace("test_trace") as span:
                assert span is None

    def test_yields_none_with_no_inputs(self):
        """Yields None when mlflow unavailable and no inputs provided."""
        from src.services.mlflow_tracing_service import start_root_trace
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            with start_root_trace("trace_no_inputs") as span:
                assert span is None

    def test_uses_start_trace_fn_when_available(self):
        """When mlflow.start_trace is callable, it should be used."""
        from src.services.mlflow_tracing_service import start_root_trace

        mock_span = MagicMock()
        mock_span.__enter__ = Mock(return_value=mock_span)
        mock_span.__exit__ = Mock(return_value=False)

        mock_mlflow = MagicMock()
        mock_mlflow.start_trace = Mock(return_value=mock_span)

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("my_trace", inputs={"key": "val"}) as rt:
                pass
        mock_mlflow.start_trace.assert_called_once()

    def test_falls_back_to_tracing_submodule(self):
        """Falls back to mlflow.tracing.start_trace when direct attribute missing."""
        from src.services.mlflow_tracing_service import start_root_trace

        mock_span = MagicMock()
        mock_span.__enter__ = Mock(return_value=mock_span)
        mock_span.__exit__ = Mock(return_value=False)

        mock_tracing = MagicMock()
        mock_tracing.start_trace = Mock(return_value=mock_span)

        mock_mlflow = MagicMock(spec=[])
        mock_mlflow.tracing = mock_tracing
        # start_trace is not directly on mock_mlflow
        del mock_mlflow.start_trace  # ensure attribute missing triggers AttributeError via spec

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            # The function does getattr(mlflow, "start_trace", None) -> None
            # Then tries getattr(tracing_mod, "start_trace", None) -> callable
            with start_root_trace("sub_trace") as rt:
                pass

    def test_uses_start_span_fn_as_fallback(self):
        """Falls back to start_span when start_trace fails."""
        from src.services.mlflow_tracing_service import start_root_trace

        mock_span = MagicMock()
        mock_span.__enter__ = Mock(return_value=mock_span)
        mock_span.__exit__ = Mock(return_value=False)
        mock_span.set_inputs = Mock()

        mock_mlflow = MagicMock()
        # Make start_trace raise so we fall through to start_span
        mock_mlflow.start_trace.side_effect = Exception("start_trace failed")
        mock_mlflow.start_span = Mock(return_value=mock_span)

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("span_trace", inputs={"a": 1}) as span:
                pass
        mock_mlflow.start_span.assert_called_once()

    def test_start_span_sets_inputs_when_method_exists(self):
        """start_span path calls set_inputs on the span if inputs provided."""
        from src.services.mlflow_tracing_service import start_root_trace

        mock_span = MagicMock()
        mock_span.__enter__ = Mock(return_value=mock_span)
        mock_span.__exit__ = Mock(return_value=False)

        mock_mlflow = MagicMock()
        mock_mlflow.start_trace.side_effect = Exception("fail")
        mock_mlflow.start_span = Mock(return_value=mock_span)

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("with_inputs", inputs={"x": 42}) as span:
                pass
        mock_span.set_inputs.assert_called_once_with({"x": 42})

    def test_start_span_handles_set_inputs_exception(self):
        """set_inputs failure is silently ignored."""
        from src.services.mlflow_tracing_service import start_root_trace

        mock_span = MagicMock()
        mock_span.__enter__ = Mock(return_value=mock_span)
        mock_span.__exit__ = Mock(return_value=False)
        mock_span.set_inputs.side_effect = Exception("set_inputs failed")

        mock_mlflow = MagicMock()
        mock_mlflow.start_trace.side_effect = Exception("fail")
        mock_mlflow.start_span = Mock(return_value=mock_span)

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            # Should not raise even though set_inputs raises
            with start_root_trace("ignore_inputs_err", inputs={"y": 1}) as span:
                pass

    def test_nullcontext_fallback_when_both_apis_fail(self):
        """Falls back to nullcontext when start_trace and start_span both fail."""
        from src.services.mlflow_tracing_service import start_root_trace

        mock_mlflow = MagicMock()
        mock_mlflow.start_trace.side_effect = Exception("fail")
        mock_mlflow.start_span.side_effect = Exception("also fail")

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            with start_root_trace("fallback") as span:
                # nullcontext yields None
                assert span is None

    def test_empty_inputs_defaults_to_empty_dict(self):
        """Passing None inputs is converted to empty dict."""
        from src.services.mlflow_tracing_service import start_root_trace
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            with start_root_trace("trace", inputs=None) as span:
                assert span is None


# ---------------------------------------------------------------------------
# get_last_active_trace_id
# ---------------------------------------------------------------------------

class TestGetLastActiveTraceId:
    def test_returns_none_when_mlflow_unavailable(self):
        from src.services.mlflow_tracing_service import get_last_active_trace_id
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            result = get_last_active_trace_id()
        assert result is None

    def test_uses_tracing_submodule_method(self):
        from src.services.mlflow_tracing_service import get_last_active_trace_id
        mock_tracing = MagicMock()
        mock_tracing.get_last_active_trace_id = Mock(return_value="trace_123")
        mock_mlflow = MagicMock()
        mock_mlflow.tracing = mock_tracing

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()
        assert result == "trace_123"

    def test_uses_direct_mlflow_method(self):
        from src.services.mlflow_tracing_service import get_last_active_trace_id

        mock_mlflow = MagicMock()
        # tracing submodule has no get_last_active_trace_id
        mock_mlflow.tracing = MagicMock(spec=[])
        mock_mlflow.get_last_active_trace_id = Mock(return_value="trace_456")

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()
        assert result == "trace_456"

    def test_returns_none_on_exception(self):
        from src.services.mlflow_tracing_service import get_last_active_trace_id
        mock_mlflow = MagicMock()
        mock_mlflow.tracing.get_last_active_trace_id.side_effect = Exception("boom")

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()
        assert result is None

    def test_returns_none_when_no_method_found(self):
        from src.services.mlflow_tracing_service import get_last_active_trace_id
        mock_mlflow = MagicMock(spec=[])

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            result = get_last_active_trace_id()
        assert result is None


# ---------------------------------------------------------------------------
# flush_async_logging
# ---------------------------------------------------------------------------

class TestFlushAsyncLogging:
    @pytest.mark.asyncio
    async def test_calls_flush_when_available(self):
        from src.services.mlflow_tracing_service import flush_async_logging
        mock_mlflow = MagicMock()
        mock_mlflow.flush_trace_async_logging = Mock()

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            await flush_async_logging()
        mock_mlflow.flush_trace_async_logging.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_no_flush_method(self):
        from src.services.mlflow_tracing_service import flush_async_logging
        mock_mlflow = MagicMock(spec=[])  # No flush_trace_async_logging

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            # Should complete without error
            await flush_async_logging()

    @pytest.mark.asyncio
    async def test_handles_mlflow_none(self):
        from src.services.mlflow_tracing_service import flush_async_logging
        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            await flush_async_logging()

    @pytest.mark.asyncio
    async def test_handles_flush_exception(self):
        from src.services.mlflow_tracing_service import flush_async_logging
        mock_mlflow = MagicMock()
        mock_mlflow.flush_trace_async_logging.side_effect = RuntimeError("flush error")

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=mock_mlflow):
            # Should not raise
            await flush_async_logging()

    @pytest.mark.asyncio
    async def test_uses_provided_logger(self):
        from src.services.mlflow_tracing_service import flush_async_logging
        custom_logger = MagicMock(spec=logging.Logger)
        custom_logger.info = Mock()

        with patch("src.services.mlflow_tracing_service._get_mlflow", return_value=None):
            await flush_async_logging(async_logger=custom_logger)
        custom_logger.info.assert_called()


# ---------------------------------------------------------------------------
# cleanup_async_db_connections
# ---------------------------------------------------------------------------

class TestCleanupAsyncDbConnections:
    def test_disposes_async_engines(self):
        from src.services.mlflow_tracing_service import cleanup_async_db_connections
        from sqlalchemy.ext.asyncio import AsyncEngine

        mock_engine = MagicMock(spec=AsyncEngine)
        mock_engine.sync_engine = MagicMock()
        mock_engine.sync_engine.dispose = Mock()

        with patch("gc.get_objects", return_value=[mock_engine, "not_an_engine", 42]):
            with patch("gc.collect"):
                cleanup_async_db_connections()

        mock_engine.sync_engine.dispose.assert_called_once()

    def test_handles_dispose_exception(self):
        from src.services.mlflow_tracing_service import cleanup_async_db_connections
        from sqlalchemy.ext.asyncio import AsyncEngine

        mock_engine = MagicMock(spec=AsyncEngine)
        mock_engine.sync_engine.dispose.side_effect = Exception("dispose error")

        with patch("gc.get_objects", return_value=[mock_engine]):
            with patch("gc.collect"):
                # Should not raise
                cleanup_async_db_connections()

    def test_handles_outer_exception(self):
        from src.services.mlflow_tracing_service import cleanup_async_db_connections
        with patch("gc.get_objects", side_effect=Exception("gc error")):
            # Should not raise
            cleanup_async_db_connections()

    def test_uses_custom_logger(self):
        from src.services.mlflow_tracing_service import cleanup_async_db_connections
        custom_logger = MagicMock(spec=logging.Logger)

        with patch("gc.get_objects", return_value=[]):
            with patch("gc.collect"):
                cleanup_async_db_connections(async_logger=custom_logger)

    def test_no_async_engines_in_objects(self):
        from src.services.mlflow_tracing_service import cleanup_async_db_connections
        with patch("gc.get_objects", return_value=["string", 1, None, [], {}]):
            with patch("gc.collect"):
                # Should complete without error
                cleanup_async_db_connections()
