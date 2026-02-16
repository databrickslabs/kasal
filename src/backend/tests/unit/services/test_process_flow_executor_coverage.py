"""Additional tests to reach 100%% coverage on process_flow_executor.py."""
import asyncio
import logging
import os
import sys
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

def _gc(group_id="tg", token="tt"):
    c = MagicMock()
    c.primary_group_id = group_id
    c.group_ids = [group_id]
    c.access_token = token
    c.group_email = "t@e.com"
    return c

def _std():
    return {
        "suppress": MagicMock(return_value=(sys.stdout, sys.stderr, StringIO())),
        "restore": MagicMock(),
        "configure": MagicMock(return_value=MagicMock()),
    }

def _deep_run_extended(
    flow_result=None, flow_exc=False, gc=None,
    trace_init_exc=False, otel_import_err=False, otel_general_err=False,
    crewai_instrumentor_import_err=False,
    db_config_value=None, mlflow_tracing_ready=False, mlflow_error=None,
    mlflow_init_exc=False, mlflow_otel_exc=False,
    flow_config_override=None,
    event_bus_flush_return=True, event_bus_flush_exc=False,
    error_path_mlflow_cleanup_exc=False,
    db_cleanup_exc=False,
    psutil_children_terminate_nsp=False, psutil_children_kill_nsp=False,
    logger_has_handlers=True,
):
    from src.services.process_flow_executor import run_flow_in_process
    p = _std()
    mock_async_logger = MagicMock()
    if logger_has_handlers:
        mock_async_logger.handlers = [MagicMock()]
    else:
        mock_async_logger.handlers = []
    p["configure"].return_value = mock_async_logger
    mock_session = AsyncMock()
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)
    mock_frs = MagicMock()
    if flow_exc:
        async def run_flow_raise(**kw):
            raise RuntimeError("flow_exec_fail")
        mock_frs.run_flow = run_flow_raise
    else:
        async def run_flow_ok(**kw):
            return flow_result or {"status": "COMPLETED", "result": "ok"}
        mock_frs.run_flow = run_flow_ok
    mock_ds_instance = MagicMock()
    mock_ds_cls = MagicMock(return_value=mock_ds_instance)
    if db_config_value is not None:
        async def get_config():
            return db_config_value
        mock_ds_instance.get_databricks_config = get_config
    else:
        async def get_config_none():
            return None
        mock_ds_instance.get_databricks_config = get_config_none
    mock_tm = MagicMock()
    mock_tm.ensure_writer_started = AsyncMock()
    mock_tm.stop_writer = AsyncMock()
    if trace_init_exc:
        mock_tm.ensure_writer_started = AsyncMock(side_effect=RuntimeError("trace_init"))
    mock_event_bus = MagicMock()
    if event_bus_flush_exc:
        mock_event_bus.flush = MagicMock(side_effect=RuntimeError("flush_error"))
    else:
        mock_event_bus.flush = MagicMock(return_value=event_bus_flush_return)
    async def mock_exec_mlflow(kickoff_coro_fn=None, **kw):
        return await kickoff_coro_fn(**kw)
    mock_post_cleanup = AsyncMock()
    if error_path_mlflow_cleanup_exc:
        mock_post_cleanup.side_effect = RuntimeError("mlflow_cleanup_fail")
    mock_uc = MagicMock()
    mock_uc.set_group_context = MagicMock()
    mock_uc.set_user_token = MagicMock()
    verify_gc = MagicMock()
    if gc:
        verify_gc.primary_group_id = getattr(gc, "primary_group_id", None)
    mock_uc.get_group_context = MagicMock(return_value=verify_gc)
    mock_otel_provider = MagicMock()
    mock_otel_provider.get_tracer = MagicMock(return_value=MagicMock())
    crewai_events_mod = MagicMock()
    crewai_events_mod.crewai_event_bus = mock_event_bus
    sys_modules_patches = {"crewai.events": crewai_events_mod}
    mock_mlflow_result = MagicMock()
    mock_mlflow_result.tracing_ready = mlflow_tracing_ready
    mock_mlflow_result.error = mlflow_error
    mock_mlflow_result.experiment_name = "test_exp"
    mock_mlflow_result.otel_exporter_active = False
    async def mock_configure_mlflow(**kw):
        return mock_mlflow_result
    if mlflow_init_exc:
        async def mock_configure_mlflow_exc(**kw):
            raise RuntimeError("mlflow_init_fail")
        configure_mlflow_fn = mock_configure_mlflow_exc
    else:
        configure_mlflow_fn = mock_configure_mlflow
    all_patches = []
    all_patches.append(patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]))
    all_patches.append(patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]))
    all_patches.append(patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]))
    all_patches.append(patch("signal.signal"))
    all_patches.append(patch.dict("sys.modules", sys_modules_patches))
    all_patches.append(patch("src.engines.crewai.trace_management.TraceManager", mock_tm))
    all_patches.append(patch("src.engines.crewai.callbacks.logging_callbacks.AgentTraceEventListener", return_value=MagicMock()))
    all_patches.append(patch("src.engines.crewai.callbacks.logging_callbacks.TaskCompletionEventListener", return_value=MagicMock()))
    all_patches.append(patch("src.utils.user_context.UserContext", mock_uc))
    all_patches.append(patch("src.db.session.safe_async_session", return_value=mock_session_cm))
    all_patches.append(patch("src.db.session.async_session_factory", return_value=mock_session_cm))
    all_patches.append(patch("src.engines.crewai.flow.flow_runner_service.FlowRunnerService", return_value=mock_frs))
    all_patches.append(patch("src.services.databricks_service.DatabricksService", mock_ds_cls))
    all_patches.append(patch("src.services.otel_tracing.mlflow_setup.execute_with_mlflow_trace_async", mock_exec_mlflow))
    all_patches.append(patch("src.services.otel_tracing.mlflow_setup.post_execution_mlflow_cleanup", mock_post_cleanup))
    all_patches.append(patch("src.services.otel_tracing.mlflow_setup.configure_mlflow_in_subprocess", configure_mlflow_fn))
    all_patches.append(patch("src.services.otel_tracing.shutdown_provider", MagicMock()))
    if otel_import_err:
        all_patches.append(patch("src.services.otel_tracing.create_kasal_tracer_provider", side_effect=ImportError("no otel")))
    elif otel_general_err:
        all_patches.append(patch("src.services.otel_tracing.create_kasal_tracer_provider", side_effect=RuntimeError("otel_broken")))
    else:
        all_patches.append(patch("src.services.otel_tracing.create_kasal_tracer_provider", return_value=mock_otel_provider))
    if crewai_instrumentor_import_err:
        all_patches.append(patch.dict("sys.modules", {"openinference.instrumentation.crewai": None}))
    if mlflow_otel_exc:
        all_patches.append(patch("src.services.otel_tracing.mlflow_exporter.KasalMLflowSpanExporter", side_effect=RuntimeError("exporter_fail")))
    if db_cleanup_exc:
        all_patches.append(patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock(side_effect=RuntimeError("db_cleanup_fail"))))
    else:
        all_patches.append(patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()))
    mock_psutil = MagicMock()
    mock_psutil.NoSuchProcess = type("NSP", (Exception,), {})
    mock_psutil.AccessDenied = type("AD", (Exception,), {})
    mock_parent = MagicMock()
    if psutil_children_terminate_nsp:
        mock_child = MagicMock(pid=111)
        mock_child.terminate.side_effect = mock_psutil.NoSuchProcess("gone")
        mock_parent.children.return_value = [mock_child]
        mock_psutil.wait_procs.return_value = ([], [])
    elif psutil_children_kill_nsp:
        mock_child = MagicMock(pid=112)
        mock_child.kill.side_effect = mock_psutil.NoSuchProcess("gone")
        mock_parent.children.return_value = [mock_child]
        mock_psutil.wait_procs.return_value = ([], [mock_child])
    else:
        mock_parent.children.return_value = []
        mock_psutil.wait_procs.return_value = ([], [])
    mock_psutil.Process.return_value = mock_parent
    all_patches.append(patch.dict("sys.modules", {"psutil": mock_psutil}))
    fc = flow_config_override or {"k": "v", "user_token": "tok"}
    ctx_stack = []
    for p_ctx in all_patches:
        p_ctx.__enter__()
        ctx_stack.append(p_ctx)
    try:
        result = run_flow_in_process("deep_ext_exec", fc, group_context=gc)
    finally:
        for p_ctx in reversed(ctx_stack):
            p_ctx.__exit__(None, None, None)
    return result


class TestValidationErrorCatchAllCov:
    def test_validation_error_on_isinstance_failure(self):
        from src.services.process_flow_executor import run_flow_in_process
        class BrokenMeta(type):
            def __instancecheck__(cls, instance):
                raise RuntimeError("isinstance broken")
        class BrokenObj(metaclass=BrokenMeta):
            pass
        result = run_flow_in_process("val_err_test", BrokenObj())
        assert result["status"] == "FAILED"

class TestSignalHandlerChildNSPCov:
    def _get_handler(self):
        from src.services.process_flow_executor import run_flow_in_process
        import signal as signal_mod
        p = _std()
        handlers = {}
        def capture(sig, handler):
            handlers[sig] = handler
        ml = MagicMock()
        ml.run_until_complete.return_value = {"status": "COMPLETED", "result": "ok"}
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]):
         with patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]):
          with patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]):
           with patch("signal.signal", side_effect=capture):
            with patch("asyncio.new_event_loop", return_value=ml):
             with patch("asyncio.set_event_loop"):
              with patch("asyncio.all_tasks", return_value=set()):
               with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=MagicMock(flush=MagicMock(return_value=True)))}):
                with patch("src.engines.crewai.trace_management.TraceManager.stop_writer", new_callable=AsyncMock):
                 with patch("src.services.otel_tracing.shutdown_provider", MagicMock()):
                  with patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()):
                   run_flow_in_process("sig_nsp", {"k": "v"})
        return handlers.get(signal_mod.SIGTERM)
    def test_child_terminate_nosuchprocess(self):
        handler = self._get_handler()
        NSP = type("NoSuchProcess", (Exception,), {})
        mock_child = MagicMock()
        mock_child.terminate.side_effect = NSP("gone")
        mock_child.is_running.return_value = False
        mock_parent = MagicMock()
        mock_parent.children.return_value = [mock_child]
        mp = MagicMock()
        mp.Process.return_value = mock_parent
        mp.NoSuchProcess = NSP
        mp.AccessDenied = type("AD", (Exception,), {})
        mp.wait_procs = MagicMock()
        with patch.dict("sys.modules", {"psutil": mp}):
            with pytest.raises(SystemExit):
                handler(None, None)
    def test_child_kill_nosuchprocess(self):
        handler = self._get_handler()
        NSP = type("NoSuchProcess", (Exception,), {})
        AD = type("AccessDenied", (Exception,), {})
        mock_child = MagicMock()
        mock_child.terminate.return_value = None
        mock_child.is_running.return_value = True
        mock_child.kill.side_effect = NSP("gone")
        mock_parent = MagicMock()
        mock_parent.children.return_value = [mock_child]
        mp = MagicMock()
        mp.Process.return_value = mock_parent
        mp.NoSuchProcess = NSP
        mp.AccessDenied = AD
        mp.wait_procs = MagicMock()
        with patch.dict("sys.modules", {"psutil": mp}):
            with pytest.raises(SystemExit):
                handler(None, None)

class TestOTelBranchesCov:
    def test_crewai_instrumentor_import_error(self):
        r = _deep_run_extended(gc=_gc(), crewai_instrumentor_import_err=True)
        assert r["status"] == "COMPLETED"
    def test_otel_import_error(self):
        r = _deep_run_extended(gc=_gc(), otel_import_err=True)
        assert r["status"] == "COMPLETED"
    def test_otel_general_exception(self):
        r = _deep_run_extended(gc=_gc(), otel_general_err=True)
        assert r["status"] == "COMPLETED"
    def test_otel_logger_handler_routing(self):
        r = _deep_run_extended(gc=_gc(), logger_has_handlers=True)
        assert r["status"] == "COMPLETED"

class TestMLflowBranchesCov:
    def test_mlflow_tracing_ready(self):
        r = _deep_run_extended(gc=_gc(), db_config_value={"host": "t"}, mlflow_tracing_ready=True)
        assert r["status"] == "COMPLETED"
    def test_mlflow_tracing_error(self):
        r = _deep_run_extended(gc=_gc(), db_config_value={"host": "t"}, mlflow_tracing_ready=False, mlflow_error="err")
        assert r["status"] == "COMPLETED"
    def test_mlflow_init_exception(self):
        r = _deep_run_extended(gc=_gc(), db_config_value={"host": "t"}, mlflow_init_exc=True)
        assert r["status"] == "COMPLETED"
    def test_mlflow_otel_exporter_success(self):
        r = _deep_run_extended(gc=_gc(), db_config_value={"host": "t"}, mlflow_tracing_ready=True)
        assert r["status"] == "COMPLETED"
    def test_mlflow_otel_exporter_exception(self):
        r = _deep_run_extended(gc=_gc(), db_config_value={"host": "t"}, mlflow_tracing_ready=True, mlflow_otel_exc=True)
        assert r["status"] == "COMPLETED"

class TestFlowConfigInputsLoggingCov:
    def test_flow_config_with_inputs(self):
        r = _deep_run_extended(gc=_gc(), flow_config_override={"k": "v", "user_token": "tok", "inputs": {"flow_id": "f1", "p": "v"}})
        assert r["status"] == "COMPLETED"

class TestEventBusFlushPostSuccessCov:
    def test_flush_timeout(self):
        r = _deep_run_extended(gc=_gc(), event_bus_flush_return=False)
        assert r["status"] == "COMPLETED"
    def test_flush_exception(self):
        r = _deep_run_extended(gc=_gc(), event_bus_flush_exc=True)
        assert r["status"] == "COMPLETED"

class TestErrorPathFlushAndCleanupCov:
    def test_error_path_flush_exception(self):
        r = _deep_run_extended(gc=_gc(), flow_exc=True, event_bus_flush_exc=True)
        assert r["status"] == "FAILED"
    def test_error_path_mlflow_cleanup_exception(self):
        r = _deep_run_extended(gc=_gc(), flow_exc=True, error_path_mlflow_cleanup_exc=True)
        assert r["status"] == "FAILED"

class TestCleanupEventBusFlushCov:
    def test_cleanup_flush_exception(self):
        from src.services.process_flow_executor import run_flow_in_process
        p = _std()
        ml = MagicMock()
        ml.run_until_complete.return_value = {"status": "COMPLETED", "result": "ok"}
        mock_bus = MagicMock()
        flush_calls = [0]
        def flush_side_effect(timeout=None):
            flush_calls[0] += 1
            if flush_calls[0] >= 2:
                raise RuntimeError("cleanup_flush_err")
            return True
        mock_bus.flush = MagicMock(side_effect=flush_side_effect)
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]):
         with patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]):
          with patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]):
           with patch("signal.signal"):
            with patch("asyncio.new_event_loop", return_value=ml):
             with patch("asyncio.set_event_loop"):
              with patch("asyncio.all_tasks", return_value=set()):
               with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=mock_bus)}):
                with patch("src.engines.crewai.trace_management.TraceManager.stop_writer", new_callable=AsyncMock):
                 with patch("src.services.otel_tracing.shutdown_provider", MagicMock()):
                  with patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()):
                   r = run_flow_in_process("cleanup_flush_exc", {"k": "v"})
        assert r["status"] == "COMPLETED"


class TestOuterCleanupExceptionCov:
    def test_cleanup_exception_caught(self):
        from src.services.process_flow_executor import run_flow_in_process
        p = _std()
        ml = MagicMock()
        call_count = [0]
        def side_effect_run(coro):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"status": "COMPLETED", "result": "ok"}
            raise RuntimeError("cleanup_loop_fail")
        ml.run_until_complete = MagicMock(side_effect=side_effect_run)
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]):
         with patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]):
          with patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]):
           with patch("signal.signal"):
            with patch("asyncio.new_event_loop", return_value=ml):
             with patch("asyncio.set_event_loop"):
              with patch("asyncio.all_tasks", return_value=set()):
               with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=MagicMock(flush=MagicMock(return_value=True)))}):
                with patch("src.services.otel_tracing.shutdown_provider", MagicMock()):
                 with patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()):
                  r = run_flow_in_process("cleanup_exc", {"k": "v"})
        assert r["status"] == "COMPLETED"
    def test_cleanup_exception_logger_also_fails(self):
        from src.services.process_flow_executor import run_flow_in_process
        p = _std()
        mock_logger = MagicMock()
        mock_logger.debug.side_effect = RuntimeError("logger_fail")
        mock_logger.handlers = []
        p["configure"].return_value = mock_logger
        ml = MagicMock()
        call_count = [0]
        def side_effect_run(coro):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"status": "COMPLETED", "result": "ok"}
            raise RuntimeError("cleanup_fail")
        ml.run_until_complete = MagicMock(side_effect=side_effect_run)
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]):
         with patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]):
          with patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]):
           with patch("signal.signal"):
            with patch("asyncio.new_event_loop", return_value=ml):
             with patch("asyncio.set_event_loop"):
              with patch("asyncio.all_tasks", return_value=set()):
               with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=MagicMock(flush=MagicMock(return_value=True)))}):
                with patch("src.services.otel_tracing.shutdown_provider", MagicMock()):
                 with patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()):
                  r = run_flow_in_process("cleanup_logger_exc", {"k": "v"})
        assert r["status"] == "COMPLETED"

class TestStdoutCaptureBarExceptCov:
    def test_stdout_capture_and_logger_both_fail(self):
        from src.services.process_flow_executor import run_flow_in_process
        p = _std()
        bad_capture = MagicMock()
        bad_capture.getvalue.side_effect = RuntimeError("capture_fail")
        p["suppress"].return_value = (sys.stdout, sys.stderr, bad_capture)
        mock_logger = MagicMock()
        mock_logger.error.side_effect = RuntimeError("logger_error_fail")
        mock_logger.info = MagicMock()
        mock_logger.warning = MagicMock()
        mock_logger.debug = MagicMock()
        mock_logger.handlers = []
        p["configure"].return_value = mock_logger
        ml = MagicMock()
        ml.run_until_complete.return_value = {"status": "COMPLETED", "result": "ok"}
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]):
         with patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]):
          with patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]):
           with patch("signal.signal"):
            with patch("asyncio.new_event_loop", return_value=ml):
             with patch("asyncio.set_event_loop"):
              with patch("asyncio.all_tasks", return_value=set()):
               with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=MagicMock(flush=MagicMock(return_value=True)))}):
                with patch("src.engines.crewai.trace_management.TraceManager.stop_writer", new_callable=AsyncMock):
                 with patch("src.services.otel_tracing.shutdown_provider", MagicMock()):
                  with patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()):
                   r = run_flow_in_process("stdout_bare", {"k": "v"})
        assert r["status"] == "COMPLETED"

class TestDBCleanupExceptionCov:
    def test_db_cleanup_exception(self):
        r = _deep_run_extended(gc=_gc(), db_cleanup_exc=True)
        assert r["status"] == "COMPLETED"

class TestFinalPsutilCleanupCov:
    def test_child_terminate_nosuchprocess(self):
        r = _deep_run_extended(gc=_gc(), psutil_children_terminate_nsp=True)
        assert r["status"] == "COMPLETED"
    def test_child_kill_nosuchprocess(self):
        r = _deep_run_extended(gc=_gc(), psutil_children_kill_nsp=True)
        assert r["status"] == "COMPLETED"
    def test_psutil_import_error(self):
        from src.services.process_flow_executor import run_flow_in_process
        p = _std()
        ml = MagicMock()
        ml.run_until_complete.return_value = {"status": "COMPLETED", "result": "ok"}
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]):
         with patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]):
          with patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]):
           with patch("signal.signal"):
            with patch("asyncio.new_event_loop", return_value=ml):
             with patch("asyncio.set_event_loop"):
              with patch("asyncio.all_tasks", return_value=set()):
               with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=MagicMock(flush=MagicMock(return_value=True))), "psutil": None}):
                with patch("src.engines.crewai.trace_management.TraceManager.stop_writer", new_callable=AsyncMock):
                 with patch("src.services.otel_tracing.shutdown_provider", MagicMock()):
                  with patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()):
                   r = run_flow_in_process("psutil_imp_err", {"k": "v"})
        assert r["status"] == "COMPLETED"
    def test_psutil_general_exception(self):
        from src.services.process_flow_executor import run_flow_in_process
        p = _std()
        ml = MagicMock()
        ml.run_until_complete.return_value = {"status": "COMPLETED", "result": "ok"}
        mock_psutil = MagicMock()
        mock_psutil.Process.side_effect = RuntimeError("psutil_fail")
        mock_psutil.NoSuchProcess = type("NSP", (Exception,), {})
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr", p["suppress"]):
         with patch("src.engines.crewai.logging_config.restore_stdout_stderr", p["restore"]):
          with patch("src.engines.crewai.logging_config.configure_subprocess_logging", p["configure"]):
           with patch("signal.signal"):
            with patch("asyncio.new_event_loop", return_value=ml):
             with patch("asyncio.set_event_loop"):
              with patch("asyncio.all_tasks", return_value=set()):
               with patch.dict("sys.modules", {"crewai.events": MagicMock(crewai_event_bus=MagicMock(flush=MagicMock(return_value=True))), "psutil": mock_psutil}):
                with patch("src.engines.crewai.trace_management.TraceManager.stop_writer", new_callable=AsyncMock):
                 with patch("src.services.otel_tracing.shutdown_provider", MagicMock()):
                  with patch("src.services.mlflow_tracing_service.cleanup_async_db_connections", MagicMock()):
                   r = run_flow_in_process("psutil_gen_exc", {"k": "v"})
        assert r["status"] == "COMPLETED"

class TestWrapperFinallyLoggingExceptCov:
    def test_wrapper_finally_log_fails(self):
        from src.services.process_flow_executor import ProcessFlowExecutor
        class FQ:
            def __init__(self):
                self._items = []
            def put(self, item):
                self._items.append(item)
            def get(self):
                return self._items.pop(0) if self._items else None
        rq = FQ()
        with patch("src.services.process_flow_executor.run_flow_in_process", return_value={"status": "COMPLETED"}):
            with patch("logging.shutdown"):
                with patch("logging.getLogger", side_effect=RuntimeError("logger_dead")):
                    with patch("os._exit"):
                        ProcessFlowExecutor._run_flow_wrapper("e1", {}, None, None, rq, FQ())
        assert rq.get()["status"] == "COMPLETED"
