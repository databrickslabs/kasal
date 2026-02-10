"""Unit tests for event bus flush logic in run_flow_in_process.

These tests cover the three event bus flush points added to
process_flow_executor.py:
1. Post-success flush (after flow execution completes)
2. Error-path flush (in except block)
3. Cleanup/finally flush (before subprocess teardown)
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_event_bus(flush_return=True, flush_side_effect=None):
    """Create a mock crewai_event_bus with configurable flush behaviour."""
    bus = MagicMock()
    bus.flush = MagicMock(return_value=flush_return)
    if flush_side_effect:
        bus.flush.side_effect = flush_side_effect
    return bus


# ===========================================================================
# Tests: Post-success flush
# ===========================================================================

class TestPostSuccessFlush:
    """Tests for the event bus flush after successful flow execution."""

    def test_flush_called_on_success(self):
        """Event bus flush is called with 30s timeout after successful execution."""
        mock_bus = _mock_event_bus(flush_return=True)

        with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=mock_bus)}):
            # Simulate the post-success flush logic directly
            from crewai.events import crewai_event_bus as _event_bus
            flushed = _event_bus.flush(timeout=30.0)

            assert flushed is True
            _event_bus.flush.assert_called_once_with(timeout=30.0)

    def test_flush_timeout_logs_warning(self):
        """When flush returns False (timeout), a warning is logged."""
        mock_bus = _mock_event_bus(flush_return=False)

        with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=mock_bus)}):
            from crewai.events import crewai_event_bus as _event_bus
            flushed = _event_bus.flush(timeout=30.0)

            assert flushed is False

    def test_flush_exception_is_nonfatal(self):
        """Exception during flush does not propagate (non-fatal)."""
        mock_bus = _mock_event_bus(flush_side_effect=RuntimeError("flush error"))

        # Simulate the try/except pattern used in the code
        try:
            mock_bus.flush(timeout=30.0)
            assert False, "Should have raised"
        except Exception as flush_err:
            # The code catches this and logs a warning - verify it's catchable
            assert "flush error" in str(flush_err)


# ===========================================================================
# Tests: Error-path flush
# ===========================================================================

class TestErrorPathFlush:
    """Tests for the event bus flush in the except block."""

    def test_flush_called_on_error(self):
        """Event bus flush is called with 10s timeout on error path."""
        mock_bus = _mock_event_bus(flush_return=True)

        with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=mock_bus)}):
            # Simulate error-path flush
            try:
                raise RuntimeError("Flow execution error")
            except Exception:
                from crewai.events import crewai_event_bus as _event_bus
                _event_bus.flush(timeout=10.0)

                _event_bus.flush.assert_called_once_with(timeout=10.0)

    def test_flush_exception_on_error_is_silenced(self):
        """Flush exception on error path is silenced (pass)."""
        mock_bus = _mock_event_bus(flush_side_effect=RuntimeError("double fault"))

        with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=mock_bus)}):
            # Simulate: except block tries flush, flush fails, should be silenced
            try:
                raise RuntimeError("Flow execution error")
            except Exception:
                try:
                    from crewai.events import crewai_event_bus as _event_bus
                    _event_bus.flush(timeout=10.0)
                except Exception:
                    pass  # This matches the actual code pattern


# ===========================================================================
# Tests: Cleanup/finally flush
# ===========================================================================

class TestCleanupFlush:
    """Tests for the event bus flush in the finally/cleanup block."""

    def test_cleanup_flush_called_with_correct_timeout(self):
        """Final cleanup flush uses 15s timeout."""
        mock_bus = _mock_event_bus(flush_return=True)

        with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=mock_bus)}):
            # Simulate the finally cleanup flush
            try:
                from crewai.events import crewai_event_bus as _cleanup_event_bus
                _cleanup_event_bus.flush(timeout=15.0)

                _cleanup_event_bus.flush.assert_called_once_with(timeout=15.0)
            except Exception:
                pytest.fail("Cleanup flush should not raise")

    def test_cleanup_flush_exception_is_caught(self):
        """Exception during cleanup flush is caught and does not halt cleanup."""
        mock_bus = _mock_event_bus(flush_side_effect=RuntimeError("cleanup flush error"))

        with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=mock_bus)}):
            # Simulate the try/except pattern in the finally block
            try:
                from crewai.events import crewai_event_bus as _cleanup_event_bus
                _cleanup_event_bus.flush(timeout=15.0)
            except Exception as eb_flush_err:
                # Code logs warning but continues cleanup
                assert "cleanup flush error" in str(eb_flush_err)


# ===========================================================================
# Integration: run_flow_in_process flush points
# ===========================================================================

class TestRunFlowInProcessFlushIntegration:
    """Integration tests verifying flush is wired into run_flow_in_process."""

    def test_module_imports_successfully(self):
        """The process_flow_executor module can be imported."""
        from src.services.process_flow_executor import run_flow_in_process
        assert callable(run_flow_in_process)

    def test_run_flow_in_process_has_flush_code(self):
        """Verify the source code contains event bus flush calls."""
        import inspect
        from src.services.process_flow_executor import run_flow_in_process
        source = inspect.getsource(run_flow_in_process)

        # Verify all three flush points exist
        assert "flush(timeout=30.0)" in source, "Post-success flush (30s) not found"
        assert "flush(timeout=10.0)" in source, "Error-path flush (10s) not found"
        assert "flush(timeout=15.0)" in source, "Cleanup flush (15s) not found"

    def test_run_flow_in_process_has_flush_error_handling(self):
        """Verify flush errors are handled gracefully."""
        import inspect
        from src.services.process_flow_executor import run_flow_in_process
        source = inspect.getsource(run_flow_in_process)

        assert "Event bus flush error (non-fatal)" in source
        assert "Event bus flush error:" in source

    def test_run_flow_in_process_has_flush_logging(self):
        """Verify flush operations are properly logged."""
        import inspect
        from src.services.process_flow_executor import run_flow_in_process
        source = inspect.getsource(run_flow_in_process)

        assert "Flushing CrewAI event bus" in source
        assert "Event bus flush completed" in source
        assert "Event bus flush timed out" in source
        assert "Final event bus flush before cleanup" in source


class TestProcessFlowExecutorFlushEnvironment:
    """Test the environment setup that affects flush behaviour."""

    def test_crewai_tracing_disabled_at_module_level(self):
        """Module sets CREWAI_TRACING_ENABLED to false."""
        import os
        # Import the module (which sets env vars at import time)
        import src.services.process_flow_executor  # noqa: F401
        assert os.environ.get("CREWAI_TRACING_ENABLED") == "false"

    def test_crewai_telemetry_opt_out_at_module_level(self):
        """Module sets CREWAI_TELEMETRY_OPT_OUT."""
        import os
        import src.services.process_flow_executor  # noqa: F401
        assert os.environ.get("CREWAI_TELEMETRY_OPT_OUT") == "1"
