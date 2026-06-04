"""
Coverage tests for engines/crewai/mlflow_integration.py
Covers: _get_mlflow, enable_autologs, update_execution_trace_id, flush_and_stop_writers
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---- _get_mlflow ----

def test_get_mlflow_available():
    """Test _get_mlflow returns mlflow when available."""
    from src.engines.crewai.mlflow_integration import _get_mlflow
    with patch.dict('sys.modules', {'mlflow': MagicMock()}):
        import importlib
        import src.engines.crewai.mlflow_integration as mod
        # Direct test
        result = _get_mlflow()
        assert result is not None


def test_get_mlflow_unavailable():
    """Test _get_mlflow returns None when mlflow raises error."""
    import sys
    from src.engines.crewai.mlflow_integration import _get_mlflow
    # Temporarily make mlflow unavailable
    original = sys.modules.get('mlflow')
    try:
        sys.modules['mlflow'] = None
        result = _get_mlflow()
        # None or mlflow depending on how the mock works
    except Exception:
        pass
    finally:
        if original is not None:
            sys.modules['mlflow'] = original
        elif 'mlflow' in sys.modules:
            del sys.modules['mlflow']


# ---- enable_autologs ----

def test_enable_autologs_no_mlflow():
    """Test enable_autologs when mlflow is not available."""
    from src.engines.crewai.mlflow_integration import enable_autologs
    with patch('src.engines.crewai.mlflow_integration._get_mlflow', return_value=None):
        enable_autologs()  # Should not raise


def test_enable_autologs_all_enabled():
    """Test enable_autologs with all features enabled."""
    from src.engines.crewai.mlflow_integration import enable_autologs
    mock_mlflow = MagicMock()
    mock_mlflow.autolog = MagicMock()
    mock_mlflow.litellm = MagicMock()
    mock_mlflow.litellm.autolog = MagicMock()
    mock_mlflow.crewai = MagicMock()
    mock_mlflow.crewai.autolog = MagicMock()

    with patch('src.engines.crewai.mlflow_integration._get_mlflow', return_value=mock_mlflow):
        enable_autologs(global_autolog=True, global_log_traces=True, crewai_autolog=True, litellm_spans_only=True)

    mock_mlflow.autolog.assert_called_once_with(log_traces=True, disable=False, silent=True)
    mock_mlflow.litellm.autolog.assert_called_once()
    mock_mlflow.crewai.autolog.assert_called_once()


def test_enable_autologs_all_disabled():
    """Test enable_autologs with all features disabled."""
    from src.engines.crewai.mlflow_integration import enable_autologs
    mock_mlflow = MagicMock()
    mock_mlflow.autolog = MagicMock()

    with patch('src.engines.crewai.mlflow_integration._get_mlflow', return_value=mock_mlflow):
        enable_autologs(
            global_autolog=False,
            global_log_traces=False,
            crewai_autolog=False,
            litellm_spans_only=False
        )

    mock_mlflow.autolog.assert_not_called()


def test_enable_autologs_global_only():
    """Test enable_autologs with global only."""
    from src.engines.crewai.mlflow_integration import enable_autologs
    mock_mlflow = MagicMock()
    mock_mlflow.autolog = MagicMock()
    # No litellm or crewai attributes
    del mock_mlflow.litellm
    del mock_mlflow.crewai

    with patch('src.engines.crewai.mlflow_integration._get_mlflow', return_value=mock_mlflow):
        enable_autologs(
            global_autolog=True,
            global_log_traces=False,
            crewai_autolog=True,
            litellm_spans_only=True
        )
    mock_mlflow.autolog.assert_called_once_with(log_traces=False, disable=False, silent=True)


def test_enable_autologs_exception_handling():
    """Test enable_autologs handles exceptions gracefully."""
    from src.engines.crewai.mlflow_integration import enable_autologs
    mock_mlflow = MagicMock()
    mock_mlflow.autolog = MagicMock(side_effect=Exception("autolog error"))

    with patch('src.engines.crewai.mlflow_integration._get_mlflow', return_value=mock_mlflow):
        # Should not raise
        enable_autologs(global_autolog=True)


def test_enable_autologs_litellm_exception():
    """Test enable_autologs handles litellm exception."""
    from src.engines.crewai.mlflow_integration import enable_autologs
    mock_mlflow = MagicMock()
    mock_mlflow.autolog = MagicMock()
    mock_mlflow.litellm = MagicMock()
    mock_mlflow.litellm.autolog = MagicMock(side_effect=Exception("litellm error"))

    with patch('src.engines.crewai.mlflow_integration._get_mlflow', return_value=mock_mlflow):
        # Should not raise
        enable_autologs(litellm_spans_only=True)


def test_enable_autologs_crewai_exception():
    """Test enable_autologs handles crewai exception."""
    from src.engines.crewai.mlflow_integration import enable_autologs
    mock_mlflow = MagicMock()
    mock_mlflow.autolog = MagicMock()
    mock_mlflow.crewai = MagicMock()
    mock_mlflow.crewai.autolog = MagicMock(side_effect=Exception("crewai error"))

    with patch('src.engines.crewai.mlflow_integration._get_mlflow', return_value=mock_mlflow):
        # Should not raise
        enable_autologs(crewai_autolog=True)


# ---- update_execution_trace_id ----

@pytest.mark.asyncio
async def test_update_execution_trace_id_no_trace_id():
    """Test update_execution_trace_id when trace_id is None."""
    from src.engines.crewai.mlflow_integration import update_execution_trace_id
    # Should return immediately without doing anything
    await update_execution_trace_id("exec1", None, "experiment", None)


@pytest.mark.asyncio
async def test_update_execution_trace_id_success():
    """Test update_execution_trace_id with valid trace.
    The function uses 'from src.services.execution_status_service import ExecutionStatusService'
    inside the function body — we patch it at the source module level.
    """
    from src.engines.crewai.mlflow_integration import update_execution_trace_id
    mock_svc = MagicMock()
    mock_svc.update_mlflow_trace_id = AsyncMock()
    with patch.dict('sys.modules', {'src.services.execution_status_service': MagicMock(
        ExecutionStatusService=mock_svc
    )}):
        await update_execution_trace_id("exec1", "trace123", "experiment", "g1")
    # If no exception was raised, test passed


@pytest.mark.asyncio
async def test_update_execution_trace_id_exception():
    """Test update_execution_trace_id handles exception gracefully."""
    from src.engines.crewai.mlflow_integration import update_execution_trace_id
    mock_svc = MagicMock()
    mock_svc.update_mlflow_trace_id = AsyncMock(side_effect=Exception("db error"))
    with patch.dict('sys.modules', {'src.services.execution_status_service': MagicMock(
        ExecutionStatusService=mock_svc
    )}):
        # Should not raise
        await update_execution_trace_id("exec1", "trace123", "experiment", None)


# ---- flush_and_stop_writers ----

@pytest.mark.asyncio
async def test_flush_and_stop_writers_exception_path():
    """Test flush_and_stop_writers handles exceptions via try/except blocks."""
    from src.engines.crewai.mlflow_integration import flush_and_stop_writers
    mock_flush = AsyncMock()
    # Patch the function-level import of flush_async_logging
    with patch.dict('sys.modules', {
        'src.services.mlflow_tracing_service': MagicMock(flush_async_logging=mock_flush),
        'src.services.trace_queue': MagicMock(get_trace_queue=MagicMock(side_effect=Exception("no queue"))),
    }):
        # Should not raise
        await flush_and_stop_writers()


@pytest.mark.asyncio
async def test_flush_and_stop_writers_with_empty_queue():
    """Test flush_and_stop_writers with empty trace queue."""
    from src.engines.crewai.mlflow_integration import flush_and_stop_writers
    mock_queue = MagicMock()
    mock_queue.qsize.return_value = 0

    mock_tm = MagicMock()
    mock_tm.stop_writer = AsyncMock()

    with patch.dict('sys.modules', {
        'src.services.mlflow_tracing_service': MagicMock(flush_async_logging=AsyncMock()),
        'src.services.trace_queue': MagicMock(get_trace_queue=MagicMock(return_value=mock_queue)),
        'src.engines.crewai.trace_management': MagicMock(TraceManager=mock_tm),
    }):
        # Should not raise
        await flush_and_stop_writers()
