"""Comprehensive unit tests for process_crew_executor.py."""

import asyncio
import os
import time
from contextlib import asynccontextmanager
from queue import Empty
from unittest.mock import AsyncMock, MagicMock, PropertyMock, mock_open, patch

import pytest


class FakeQueue:
    def __init__(self, items=None):
        self._items = list(items or [])
    def empty(self):
        return len(self._items) == 0
    def get_nowait(self):
        if self._items:
            return self._items.pop(0)
        raise Exception("Empty")
    def get(self, block=True, timeout=None):
        if self._items:
            return self._items.pop(0)
        raise Empty()
    def put(self, item):
        self._items.append(item)
    def qsize(self):
        return len(self._items)


class FakeProcess:
    def __init__(self, exitcode=0, alive=False):
        self.pid = 12345
        self.exitcode = exitcode
        self._alive = alive
        self.started = False
        self.terminated = False
        self.killed = False
    def start(self):
        self.started = True
    def join(self, timeout=None):
        return None
    def is_alive(self):
        return self._alive
    def terminate(self):
        self.terminated = True
        self._alive = False
    def kill(self):
        self.killed = True
        self._alive = False


class FakeContext:
    def __init__(self, process):
        self._process = process
        self._queue_count = 0
    def Queue(self):
        self._queue_count += 1
        return FakeQueue([])
    def Process(self, target=None, args=None):
        return self._process


class FakeContextWithResult(FakeContext):
    def __init__(self, process, result_items):
        super().__init__(process)
        self._result_items = result_items
    def Queue(self):
        self._queue_count += 1
        if self._queue_count == 1:
            return FakeQueue(self._result_items)
        return FakeQueue([])


# 1. Module-level hardening (lines 37-74)

class TestModuleLevelHardening:
    def test_crewai_env_vars_set(self):
        assert os.environ.get("CREWAI_TRACING_ENABLED") == "false"
        assert os.environ.get("CREWAI_TELEMETRY_OPT_OUT") == "1"
        assert os.environ.get("CREWAI_ANALYTICS_OPT_OUT") == "1"
        assert os.environ.get("CREWAI_CLOUD_TRACING") == "false"
        assert os.environ.get("CREWAI_CLOUD_TRACING_ENABLED") == "false"
        assert os.environ.get("CREWAI_VERBOSE") == "false"

    def test_input_suppressed(self):
        import builtins
        assert builtins.input("test prompt") == "n"

    def test_input_suppressed_no_prompt(self):
        import builtins
        assert builtins.input() == "n"

    def test_click_patched(self):
        try:
            import click
            assert click.confirm("test") is False
            assert click.prompt("test") == ""
        except ImportError:
            pytest.skip("click not installed")


# 2. run_crew_in_process validation (lines 90-203)

class TestRunCrewInProcessValidation:
    def test_none_config(self):
        from src.services.process_crew_executor import run_crew_in_process
        r = run_crew_in_process("e1", None)
        assert r["status"] == "FAILED"
        assert "None" in r["error"]

    def test_invalid_json(self):
        from src.services.process_crew_executor import run_crew_in_process
        r = run_crew_in_process("e2", "not-json{{{")
        assert r["status"] == "FAILED"

    def test_non_dict(self):
        from src.services.process_crew_executor import run_crew_in_process
        r = run_crew_in_process("e3", [1, 2, 3])
        assert r["status"] == "FAILED"
        assert "dict" in r["error"]

    def test_json_list(self):
        from src.services.process_crew_executor import run_crew_in_process
        r = run_crew_in_process("e4", '[1,2,3]')
        assert r["status"] == "FAILED"

    def test_valid_json(self):
        from src.services.process_crew_executor import run_crew_in_process
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    side_effect=RuntimeError("stop")):
            with pytest.raises(RuntimeError, match="stop"):
                run_crew_in_process("e5", '{"a": 1}')

    def test_dict_config(self):
        from src.services.process_crew_executor import run_crew_in_process
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    side_effect=RuntimeError("stop")):
            with pytest.raises(RuntimeError, match="stop"):
                run_crew_in_process("e6", {"a": 1})


# 3. DATABASE_TYPE env (lines 148-153)

class TestDatabaseTypeEnv:
    def test_set_when_missing(self):
        from src.services.process_crew_executor import run_crew_in_process
        old = os.environ.pop("DATABASE_TYPE", None)
        try:
            ms = MagicMock()
            ms.DATABASE_TYPE = "sqlite"
            with patch("src.config.settings.settings", ms), \
                 patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                       side_effect=RuntimeError("stop")):
                with pytest.raises(RuntimeError, match="stop"):
                    run_crew_in_process("d1", {"k": "v"})
                assert os.environ.get("DATABASE_TYPE") == "sqlite"
        finally:
            if old is not None:
                os.environ["DATABASE_TYPE"] = old
            else:
                os.environ.pop("DATABASE_TYPE", None)

    def test_not_overwritten(self):
        from src.services.process_crew_executor import run_crew_in_process
        old = os.environ.get("DATABASE_TYPE")
        os.environ["DATABASE_TYPE"] = "postgres"
        try:
            with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                       side_effect=RuntimeError("stop")):
                with pytest.raises(RuntimeError, match="stop"):
                    run_crew_in_process("d2", {"k": "v"})
                assert os.environ["DATABASE_TYPE"] == "postgres"
        finally:
            if old is not None:
                os.environ["DATABASE_TYPE"] = old
            else:
                os.environ.pop("DATABASE_TYPE", None)


# 4. run_crew_in_process deeper paths

class TestRunCrewInProcessDeep:
    def _run(self, cfg, inputs=None, gc=None):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.side_effect = RuntimeError("deep-stop")
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"):
                return run_crew_in_process("exec_deep", cfg, inputs, gc)

    def test_agents_tasks(self):
        cfg = {"run_name": "T", "version": "2", "agents": [{"role": "R", "goal": "g", "knowledge_sources": ["s"], "llm": "g4"}], "tasks": [{"description": "d"}]}
        assert self._run(cfg)["status"] == "FAILED"

    def test_string_inputs(self):
        assert self._run({"k": "v"}, inputs="raw")["status"] == "FAILED"

    def test_group_id(self):
        assert self._run({"group_id": "g1", "group_email": "e", "user_token": "t"})["status"] == "FAILED"

    def test_no_group_id(self):
        assert self._run({"agents": []})["status"] == "FAILED"

    def test_captured_stdout(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = "line1\nline2\n"
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            mr = MagicMock()
            mr.raw = "final"
            ml.run_until_complete.return_value = mr
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("asyncio.all_tasks", return_value=set()), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections"), \
                 patch("psutil.Process") as mp:
                mp.return_value.children.return_value = []
                r = run_crew_in_process("es", {"agents": []})
                assert r["status"] == "COMPLETED"
                assert r["result"] == "final"

    def test_result_dict(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.return_value = {"k": "v"}
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("asyncio.all_tasks", return_value=set()), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections"), \
                 patch("psutil.Process") as mp:
                mp.return_value.children.return_value = []
                r = run_crew_in_process("ed", {"agents": []})
                assert r["status"] == "COMPLETED"

    def test_result_conversion_error(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        class Bad:
            def __str__(self):
                raise RuntimeError("no")
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.return_value = Bad()
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("asyncio.all_tasks", return_value=set()), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections"), \
                 patch("psutil.Process") as mp:
                mp.return_value.children.return_value = []
                r = run_crew_in_process("eb", {"agents": []})
                assert r["status"] == "COMPLETED"
                assert "serialized" in r["result"]

    def test_exception_trace_queue(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        tq = MagicMock()
        tq.qsize.side_effect = [1, 1, 0]
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.side_effect = RuntimeError("fail")
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections"), \
                 patch("psutil.Process") as mp, \
                 patch("src.services.trace_queue.get_trace_queue", return_value=tq), \
                 patch("time.sleep"):
                mp.return_value.children.return_value = []
                r = run_crew_in_process("ee", {"agents": []})
                assert r["status"] == "FAILED"

    def test_finally_children(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        ch = MagicMock()
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.return_value = MagicMock(raw="ok")
            pa = MagicMock()
            pa.children.return_value = [ch]
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("asyncio.all_tasks", return_value=set()), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections"), \
                 patch("psutil.Process", return_value=pa), \
                 patch("psutil.wait_procs", return_value=([], [ch])):
                r = run_crew_in_process("ec", {"agents": []})
                assert r["status"] == "COMPLETED"
                ch.terminate.assert_called()

    def test_finally_psutil_import_error(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.return_value = MagicMock(raw="ok")
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("asyncio.all_tasks", return_value=set()), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections"), \
                 patch("psutil.Process", side_effect=ImportError("no")):
                r = run_crew_in_process("en", {"agents": []})
                assert r["status"] == "COMPLETED"

    def test_finally_cleanup_exception(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.return_value = MagicMock(raw="ok")
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("asyncio.all_tasks", return_value=set()), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections"), \
                 patch("psutil.Process", side_effect=Exception("err")):
                r = run_crew_in_process("ef", {"agents": []})
                assert r["status"] == "COMPLETED"

    def test_db_cleanup_error(self):
        from src.services.process_crew_executor import run_crew_in_process
        mc = MagicMock()
        mc.getvalue.return_value = ""
        with patch("src.engines.crewai.logging_config.suppress_stdout_stderr",
                    return_value=(MagicMock(), MagicMock(), mc)), \
             patch("src.engines.crewai.logging_config.restore_stdout_stderr"), \
             patch("src.engines.crewai.logging_config.configure_subprocess_logging",
                    return_value=MagicMock()), \
             patch("src.services.process_crew_executor.signal.signal"), \
             patch.dict(os.environ, {"DATABASE_TYPE": "sqlite"}, clear=False):
            ml = MagicMock()
            ml.run_until_complete.return_value = MagicMock(raw="ok")
            with patch("asyncio.new_event_loop", return_value=ml), \
                 patch("asyncio.set_event_loop"), \
                 patch("asyncio.all_tasks", return_value=set()), \
                 patch("src.services.mlflow_tracing_service.cleanup_async_db_connections",
                       side_effect=Exception("db")), \
                 patch("psutil.Process") as mp:
                mp.return_value.children.return_value = []
                r = run_crew_in_process("eg", {"agents": []})
                assert r["status"] == "COMPLETED"


# 5. _subprocess_initializer

class TestSubprocessInitializer:
    def test_sets_env(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        ProcessCrewExecutor._subprocess_initializer()
        assert os.environ.get("PYTHONUNBUFFERED") == "0"


# 6. _run_crew_wrapper

class TestRunCrewWrapper:
    def test_success(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        rq = FakeQueue()
        with patch("src.services.process_crew_executor.run_crew_in_process",
                    return_value={"status": "COMPLETED"}):
            ProcessCrewExecutor._run_crew_wrapper("e1", {}, None, None, rq, FakeQueue())
            assert rq.get_nowait()["status"] == "COMPLETED"

    def test_exception(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        rq = FakeQueue()
        with patch("src.services.process_crew_executor.run_crew_in_process",
                    side_effect=RuntimeError("boom")):
            ProcessCrewExecutor._run_crew_wrapper("e2", {}, None, None, rq, FakeQueue())
            assert rq.get_nowait()["status"] == "FAILED"


# 7. __init__

class TestInit:
    def test_default(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        assert ProcessCrewExecutor()._max_concurrent == 4

    def test_custom(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        assert ProcessCrewExecutor(max_concurrent=8)._max_concurrent == 8


# 8. run_crew_isolated

class TestRunCrewIsolated:
    @pytest.mark.asyncio
    async def test_completed(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            assert (await e.run_crew_isolated('e1', {}, gc))['status'] == 'COMPLETED'

    @pytest.mark.asyncio
    async def test_sigterm(self):
        fp = FakeProcess(exitcode=-15, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'
            r = await e.run_crew_isolated('e2', {}, gc)
            assert r['status'] == 'STOPPED'
            assert r['exit_code'] == -15

    @pytest.mark.asyncio
    async def test_sigkill(self):
        fp = FakeProcess(exitcode=-9, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'
            assert (await e.run_crew_isolated('e3', {}, gc))['status'] == 'STOPPED'

    @pytest.mark.asyncio
    async def test_failed(self):
        fp = FakeProcess(exitcode=1, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'
            r = await e.run_crew_isolated('e4', {}, gc)
            assert r['status'] == 'FAILED'
            assert r['exit_code'] == 1

    @pytest.mark.asyncio
    async def test_result_queue(self):
        fp = FakeProcess(exitcode=0, alive=False)
        fc = FakeContextWithResult(fp, [{"status": "COMPLETED", "execution_id": "e5"}])
        with patch('src.services.process_crew_executor.mp.get_context', return_value=fc):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'
            r = await e.run_crew_isolated('e5', {}, gc)
            assert r['execution_id'] == 'e5'

    @pytest.mark.asyncio
    async def test_gc_fields(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'tg'; gc.access_token = 'tt'
            cfg = {}
            await e.run_crew_isolated('e6', cfg, gc)
            assert cfg["group_id"] == "tg"
            assert cfg["user_token"] == "tt"

    @pytest.mark.asyncio
    async def test_gc_no_primary(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(spec=[])
            await e.run_crew_isolated('e7', {}, gc)

    @pytest.mark.asyncio
    async def test_no_gc(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            assert (await e.run_crew_isolated('e8', {}, None))['status'] == 'COMPLETED'

    @pytest.mark.asyncio
    async def test_not_dict(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'
            await e.run_crew_isolated('e9', "s", gc)

    @pytest.mark.asyncio
    async def test_empty_token(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = ''
            cfg = {}
            await e.run_crew_isolated('e10', cfg, gc)
            assert "user_token" not in cfg

    @pytest.mark.asyncio
    async def test_env_restore_old(self):
        old = os.environ.get("KASAL_EXECUTION_ID")
        os.environ["KASAL_EXECUTION_ID"] = "old"
        try:
            fp = FakeProcess(exitcode=0, alive=False)
            with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
                from src.services.process_crew_executor import ProcessCrewExecutor
                e = ProcessCrewExecutor(max_concurrent=1)
                gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
                await e.run_crew_isolated('e11', {}, gc)
                assert os.environ.get("KASAL_EXECUTION_ID") == "old"
        finally:
            if old is not None:
                os.environ["KASAL_EXECUTION_ID"] = old
            else:
                os.environ.pop("KASAL_EXECUTION_ID", None)

    @pytest.mark.asyncio
    async def test_env_restore_none(self):
        os.environ.pop("KASAL_EXECUTION_ID", None)
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            await e.run_crew_isolated('e12', {}, gc)
            assert "KASAL_EXECUTION_ID" not in os.environ

    @pytest.mark.asyncio
    async def test_timeout(self):
        fp = FakeProcess(exitcode=None, alive=True)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            async def mwf(coro, timeout=None):
                if hasattr(coro, 'close'): coro.close()
                raise asyncio.TimeoutError()
            with patch('asyncio.wait_for', side_effect=mwf), \
                 patch.object(e, '_relay_task_events', new_callable=AsyncMock), \
                 patch.object(e, '_terminate_orphaned_process', return_value=False):
                assert (await e.run_crew_isolated('e13', {}, gc, timeout=0.1))['status'] == 'TIMEOUT'

    @pytest.mark.asyncio
    async def test_general_exc(self):
        fp = FakeProcess(exitcode=0, alive=False)
        fp.start = MagicMock(side_effect=RuntimeError("fail"))
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            with pytest.raises(RuntimeError, match="fail"):
                await e.run_crew_isolated('e14', {}, gc)

    @pytest.mark.asyncio
    async def test_finally_force_kill(self):
        fp = FakeProcess(exitcode=0, alive=False)
        fc = FakeContext(fp)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=fc):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            ap = FakeProcess(exitcode=None, alive=True)
            ap.terminate = lambda: None
            def st(): e._running_processes['e15'] = ap
            ap.start = st
            fc._process = ap
            await e.run_crew_isolated('e15', {}, gc)

    @pytest.mark.asyncio
    async def test_futures_cleanup(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            e._running_futures['e16'] = MagicMock()
            e._running_executors['e16'] = MagicMock()
            await e.run_crew_isolated('e16', {}, gc)
            assert 'e16' not in e._running_futures
            assert 'e16' not in e._running_executors

    @pytest.mark.asyncio
    async def test_cleanup_error(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            with patch('psutil.process_iter', side_effect=Exception("err")):
                assert (await e.run_crew_isolated('e17', {}, gc))['status'] == 'COMPLETED'

    @pytest.mark.asyncio
    async def test_metrics_completed(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'; gc.access_token = 't'
            await e.run_crew_isolated('e18', {}, gc)
            assert e._metrics["completed_executions"] >= 1

    @pytest.mark.asyncio
    async def test_metrics_stopped(self):
        fp = FakeProcess(exitcode=-15, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'
            await e.run_crew_isolated('e19', {}, gc)
            assert e._metrics["terminated_executions"] >= 1

    @pytest.mark.asyncio
    async def test_metrics_failed(self):
        fp = FakeProcess(exitcode=1, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            gc = MagicMock(); gc.primary_group_id = 'g'
            await e.run_crew_isolated('e20', {}, gc)
            assert e._metrics["failed_executions"] >= 1

    @pytest.mark.asyncio
    async def test_exec_id_fallback(self):
        fp = FakeProcess(exitcode=0, alive=False)
        with patch('src.services.process_crew_executor.mp.get_context', return_value=FakeContext(fp)):
            from src.services.process_crew_executor import ProcessCrewExecutor
            e = ProcessCrewExecutor(max_concurrent=1)
            cfg = {}
            await e.run_crew_isolated('e21', cfg, None)
            assert cfg.get("execution_id") == "e21"


# 9. _relay_task_events

class TestRelayTaskEvents:
    @pytest.mark.asyncio
    async def test_task_started(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        q = FakeQueue([{"event_type": "task_started", "event_source": "c", "event_context": "T", "output": None, "extra_data": {"task_name": "T"}, "created_at": "2024-01-01"}])
        with patch('src.core.sse_manager.sse_manager.broadcast_to_job', new_callable=AsyncMock, return_value=1):
            t = asyncio.create_task(e._relay_task_events(q, "r1"))
            await asyncio.sleep(0.1); t.cancel()
            try: await t
            except asyncio.CancelledError: pass

    @pytest.mark.asyncio
    async def test_task_completed(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        q = FakeQueue([{"event_type": "task_completed", "event_source": "c", "event_context": "T", "output": "r", "extra_data": {}, "trace_metadata": {"k": "v"}}])
        with patch('src.core.sse_manager.sse_manager.broadcast_to_job', new_callable=AsyncMock, return_value=1):
            t = asyncio.create_task(e._relay_task_events(q, "r2"))
            await asyncio.sleep(0.1); t.cancel()
            try: await t
            except asyncio.CancelledError: pass

    @pytest.mark.asyncio
    async def test_non_task(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        q = FakeQueue([{"event_type": "agent_started", "extra_data": {}}])
        with patch('src.core.sse_manager.sse_manager.broadcast_to_job', new_callable=AsyncMock) as m:
            t = asyncio.create_task(e._relay_task_events(q, "r3"))
            await asyncio.sleep(0.1); t.cancel()
            try: await t
            except asyncio.CancelledError: pass
            m.assert_not_called()

    @pytest.mark.asyncio
    async def test_none_data(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        q = FakeQueue([None])
        with patch('src.core.sse_manager.sse_manager.broadcast_to_job', new_callable=AsyncMock) as m:
            t = asyncio.create_task(e._relay_task_events(q, "r4"))
            await asyncio.sleep(0.1); t.cancel()
            try: await t
            except asyncio.CancelledError: pass
            m.assert_not_called()

    @pytest.mark.asyncio
    async def test_queue_error(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        cc = 0
        def bg(block=True, timeout=None):
            nonlocal cc; cc += 1
            if cc <= 1: raise RuntimeError("e")
            raise Empty()
        q = MagicMock(); q.get = bg
        t = asyncio.create_task(e._relay_task_events(q, "r5"))
        await asyncio.sleep(0.3); t.cancel()
        try: await t
        except asyncio.CancelledError: pass

    @pytest.mark.asyncio
    async def test_broadcast_error(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        q = FakeQueue([{"event_type": "task_failed", "event_source": "c", "event_context": "T", "output": "e", "extra_data": {}}])
        with patch('src.core.sse_manager.sse_manager.broadcast_to_job', new_callable=AsyncMock, side_effect=Exception("f")):
            t = asyncio.create_task(e._relay_task_events(q, "r6"))
            await asyncio.sleep(0.1); t.cancel()
            try: await t
            except asyncio.CancelledError: pass


# 10. _process_log_queue

class TestProcessLogQueue:
    @pytest.mark.asyncio
    async def test_no_file(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch('os.path.exists', return_value=False):
            await e._process_log_queue(FakeQueue(), "nf", None)

    @pytest.mark.asyncio
    async def test_no_match(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        me = AsyncMock()
        mc = AsyncMock()
        me.begin.return_value.__aenter__ = AsyncMock(return_value=mc)
        me.begin.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="other\n")), \
             patch('src.db.session.engine', me), \
             patch.dict(os.environ, {"LOG_DIR": "/tmp"}):
            await e._process_log_queue(FakeQueue(), "nm", None)

    @pytest.mark.asyncio
    async def test_match_gc(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mc = AsyncMock()

        @asynccontextmanager
        async def fake_begin():
            yield mc

        me = MagicMock()
        me.begin = fake_begin

        gc = MagicMock(); gc.primary_group_id = "g"; gc.group_email = "e"
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="exec_mat l1\nexec_mat l2\n")), \
             patch('src.db.session.engine', me), \
             patch.dict(os.environ, {"LOG_DIR": "/tmp"}):
            await e._process_log_queue(FakeQueue(), "exec_match", gc)
            assert mc.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_db_error(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', mock_open(read_data="")), \
             patch('src.db.session.engine', side_effect=Exception("db")), \
             patch.dict(os.environ, {"LOG_DIR": "/tmp"}):
            await e._process_log_queue(FakeQueue(), "de", None)

    @pytest.mark.asyncio
    async def test_no_log_dir(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LOG_DIR", None)
            with patch('os.path.exists', return_value=False):
                await e._process_log_queue(FakeQueue(), "nd", None)


# 11. terminate_execution

class TestTerminateExecution:
    @pytest.mark.asyncio
    async def test_live(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(exitcode=None, alive=True)
        e._running_processes['t1'] = p
        assert await e.terminate_execution('t1') is True
        assert p.terminated

    @pytest.mark.asyncio
    async def test_force(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(exitcode=None, alive=True)
        p.terminate = lambda: None
        e._running_processes['t2'] = p
        assert await e.terminate_execution('t2') is True
        assert p.killed

    @pytest.mark.asyncio
    async def test_dead(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(exitcode=0, alive=False)
        e._running_processes['t3'] = p
        assert await e.terminate_execution('t3') is True

    @pytest.mark.asyncio
    async def test_not_tracked(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch.object(e, '_terminate_orphaned_process', return_value=False):
            assert await e.terminate_execution('x') is False

    @pytest.mark.asyncio
    async def test_orphan(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch.object(e, '_terminate_orphaned_process', return_value=True):
            assert await e.terminate_execution('o') is True

    @pytest.mark.asyncio
    async def test_error_psutil(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(exitcode=None, alive=True)
        def te(): raise RuntimeError("e")
        p.terminate = te
        p.join = MagicMock(side_effect=RuntimeError("e"))
        e._running_processes['t4'] = p
        with patch('psutil.Process') as mp:
            mp.return_value.kill = MagicMock()
            assert await e.terminate_execution('t4') is True

    @pytest.mark.asyncio
    async def test_error_all(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(exitcode=None, alive=True)
        def te(): raise RuntimeError("e")
        p.terminate = te
        p.join = MagicMock(side_effect=RuntimeError("e"))
        e._running_processes['t5'] = p
        with patch('psutil.Process', side_effect=Exception("f")), \
             patch.object(e, '_terminate_orphaned_process', return_value=False):
            await e.terminate_execution('t5')
            assert 't5' not in e._running_processes

    @pytest.mark.asyncio
    async def test_metrics(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(exitcode=None, alive=True)
        e._running_processes['t6'] = p
        o = e._metrics["terminated_executions"]
        await e.terminate_execution('t6')
        assert e._metrics["terminated_executions"] == o + 1


# 12. _terminate_orphaned_process

class TestTerminateOrphanedProcess:
    def test_env_match(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock()
        mp.info = {"pid": 1, "name": "python3", "cmdline": []}
        mp.environ.return_value = {"KASAL_EXECUTION_ID": "exec_em"}
        pa = MagicMock(); pa.children.return_value = []; pa.kill = MagicMock()
        with patch('psutil.process_iter', return_value=[mp]), \
             patch('psutil.Process', return_value=pa), \
             patch('psutil.wait_procs'):
            assert e._terminate_orphaned_process("exec_em") is True

    def test_cmdline(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock()
        mp.info = {"pid": 2, "name": "python3", "cmdline": ["python", "exec_cm"]}
        mp.environ.side_effect = psutil.NoSuchProcess(2)
        pa = MagicMock(); pa.children.return_value = []; pa.kill = MagicMock()
        with patch('psutil.process_iter', return_value=[mp]), \
             patch('psutil.Process', return_value=pa), \
             patch('psutil.wait_procs'):
            assert e._terminate_orphaned_process("exec_cm") is True

    def test_children(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock()
        mp.info = {"pid": 3, "name": "python", "cmdline": []}
        mp.environ.return_value = {"KASAL_EXECUTION_ID": "exec_ch"}
        ch = MagicMock(); ch.pid = 4
        pa = MagicMock(); pa.children.return_value = [ch]; pa.kill = MagicMock()
        with patch('psutil.process_iter', return_value=[mp]), \
             patch('psutil.Process', return_value=pa), \
             patch('psutil.wait_procs'):
            assert e._terminate_orphaned_process("exec_ch") is True
            ch.kill.assert_called()

    def test_no_match(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock()
        mp.info = {"pid": 4, "name": "python", "cmdline": []}
        mp.environ.return_value = {"KASAL_EXECUTION_ID": "other"}
        with patch('psutil.process_iter', return_value=[mp]):
            assert e._terminate_orphaned_process("exec_nm") is False

    def test_non_python(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock(); mp.info = {"pid": 5, "name": "nginx", "cmdline": []}
        with patch('psutil.process_iter', return_value=[mp]):
            assert e._terminate_orphaned_process("exec_np") is False

    def test_nsp(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock()
        mp.info.__getitem__ = MagicMock(side_effect=psutil.NoSuchProcess(1))
        with patch('psutil.process_iter', return_value=[mp]):
            assert e._terminate_orphaned_process("exec_nsp") is False

    def test_kill_error(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock()
        mp.info = {"pid": 6, "name": "python", "cmdline": []}
        mp.environ.return_value = {"KASAL_EXECUTION_ID": "exec_ke"}
        pa = MagicMock(); pa.children.return_value = []
        pa.kill.side_effect = psutil.NoSuchProcess(6)
        with patch('psutil.process_iter', return_value=[mp]), \
             patch('psutil.Process', return_value=pa):
            assert e._terminate_orphaned_process("exec_ke") is False

    def test_general(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch('psutil.process_iter', side_effect=RuntimeError("e")):
            assert e._terminate_orphaned_process("exec_ge") is False

    def test_access_denied(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        mp = MagicMock()
        mp.info = {"pid": 9, "name": "python", "cmdline": []}
        mp.environ.side_effect = psutil.AccessDenied(9)
        with patch('psutil.process_iter', return_value=[mp]):
            assert e._terminate_orphaned_process("exec_ad") is False

    def test_import_error(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        import builtins
        orig = builtins.__import__
        def fail(name, *a, **kw):
            if name == 'psutil': raise ImportError("no")
            return orig(name, *a, **kw)
        with patch('builtins.__import__', side_effect=fail):
            assert e._terminate_orphaned_process("exec_ie") is False


# 13. get_metrics

class TestGetMetrics:
    def test_copy(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        m = e.get_metrics()
        m["total_executions"] = 999
        assert e._metrics["total_executions"] == 0

    def test_keys(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        m = e.get_metrics()
        for k in ["total_executions", "active_executions", "completed_executions", "failed_executions", "terminated_executions"]:
            assert k in m


# 14. shutdown

class TestShutdown:
    def test_empty(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        e.shutdown(wait=True)
        assert e._running_processes == {}

    def test_live_wait(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(alive=True); e._running_processes['s1'] = p
        with patch('psutil.Process') as mp:
            mp.return_value.children.return_value = []
            e.shutdown(wait=True)
        assert p.terminated

    def test_no_wait(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(alive=True); e._running_processes['s2'] = p
        with patch('psutil.Process') as mp:
            mp.return_value.children.return_value = []
            e.shutdown(wait=False)
        assert p.terminated

    def test_force_kill(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(alive=True); p.terminate = lambda: None
        e._running_processes['s3'] = p
        with patch('psutil.Process') as mp:
            mp.return_value.children.return_value = []
            e.shutdown(wait=True)
        assert p.killed

    def test_error(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        p = FakeProcess(alive=True)
        def te(): raise RuntimeError("e")
        p.terminate = te
        e._running_processes['s4'] = p
        with patch('psutil.Process') as mp:
            mp.return_value.children.return_value = []
            e.shutdown(wait=True)
        assert e._running_processes == {}

    def test_children(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        ch = MagicMock(); ch.pid = 1
        pa = MagicMock(); pa.children.return_value = [ch]
        with patch('psutil.Process', return_value=pa), \
             patch('psutil.wait_procs', return_value=([ch], [])):
            e.shutdown(wait=True)
        ch.terminate.assert_called()

    def test_children_force(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        ch = MagicMock(); ch.pid = 2
        pa = MagicMock(); pa.children.return_value = [ch]
        with patch('psutil.Process', return_value=pa), \
             patch('psutil.wait_procs', return_value=([], [ch])):
            e.shutdown(wait=True)
        ch.kill.assert_called()

    def test_nsp(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        ch = MagicMock(); ch.terminate.side_effect = psutil.NoSuchProcess(1)
        pa = MagicMock(); pa.children.return_value = [ch]
        with patch('psutil.Process', return_value=pa), \
             patch('psutil.wait_procs', return_value=([], [])):
            e.shutdown(wait=True)

    def test_psutil_error(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch('psutil.Process', side_effect=Exception("e")):
            e.shutdown(wait=True)
        assert e._running_processes == {}


# 15. kill_orphan_crew_processes

class TestKillOrphanCrewProcesses:
    def test_none(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch('psutil.process_iter', return_value=[]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_keyword(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock()
        mp.info = {"pid": 1, "name": "python", "cmdline": ["python", "run_crew_in_process"], "ppid": 2, "create_time": time.time()-120}
        mp.create_time.return_value = time.time()-120
        mp.terminate = MagicMock(); mp.wait = MagicMock()
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 1

    def test_orphaned(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock()
        mp.info = {"pid": 2, "name": "python3", "cmdline": [], "ppid": 1, "create_time": time.time()-120}
        mp.create_time.return_value = time.time()-120
        mp.terminate = MagicMock(); mp.wait = MagicMock()
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 1

    def test_young(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock()
        mp.info = {"pid": 3, "name": "python", "cmdline": ["python", "run_crew_in_process"], "ppid": 2, "create_time": time.time()}
        mp.create_time.return_value = time.time()
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_timeout(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock()
        mp.info = {"pid": 4, "name": "python", "cmdline": ["python", "CrewAI"], "ppid": 2, "create_time": time.time()-120}
        mp.create_time.return_value = time.time()-120
        mp.terminate = MagicMock(); mp.wait.side_effect = psutil.TimeoutExpired(2); mp.kill = MagicMock()
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 1
            mp.kill.assert_called()

    def test_nsp(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock()
        type(mp).info = PropertyMock(side_effect=psutil.NoSuchProcess(1))
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_general(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch('psutil.process_iter', side_effect=RuntimeError("o")):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_non_crew(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock()
        mp.info = {"pid": 5, "name": "python", "cmdline": ["python", "regular.py"], "ppid": 2, "create_time": time.time()-120}
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_all_keywords(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        for kw in ["run_crew_in_process", "CrewAI", "crew.kickoff", "multiprocessing.spawn", "ProcessPoolExecutor"]:
            mp = MagicMock()
            mp.info = {"pid": 6, "name": "python", "cmdline": ["python", kw], "ppid": 2, "create_time": time.time()-120}
            mp.create_time.return_value = time.time()-120; mp.terminate = MagicMock(); mp.wait = MagicMock()
            with patch('psutil.process_iter', return_value=[mp]):
                assert ProcessCrewExecutor.kill_orphan_crew_processes() >= 1

    def test_access_denied(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock(); type(mp).info = PropertyMock(side_effect=psutil.AccessDenied(1))
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_zombie(self):
        import psutil
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock(); type(mp).info = PropertyMock(side_effect=psutil.ZombieProcess(1))
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_none_name(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        mp = MagicMock()
        mp.info = {"pid": 7, "name": None, "cmdline": [], "ppid": 1, "create_time": time.time()-120}
        with patch('psutil.process_iter', return_value=[mp]):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0

    def test_import_error(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        import builtins
        orig = builtins.__import__
        def fail(name, *a, **kw):
            if name == 'psutil': raise ImportError("no")
            return orig(name, *a, **kw)
        with patch('builtins.__import__', side_effect=fail):
            assert ProcessCrewExecutor.kill_orphan_crew_processes() == 0


# 16. Context manager

class TestContextManager:
    def test_enter(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        assert e.__enter__() is e

    def test_exit(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch.object(e, 'shutdown') as m:
            assert e.__exit__(None, None, None) is False
            m.assert_called_once_with(wait=True)

    def test_exit_exc(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        e = ProcessCrewExecutor(max_concurrent=1)
        with patch.object(e, 'shutdown'):
            assert e.__exit__(RuntimeError, RuntimeError("e"), None) is False

    def test_with(self):
        from src.services.process_crew_executor import ProcessCrewExecutor
        with patch.object(ProcessCrewExecutor, 'shutdown'):
            with ProcessCrewExecutor(max_concurrent=1) as e:
                assert e is not None


# 17. ExecutionMode

class TestExecutionMode:
    def test_constants(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.THREAD == "thread"
        assert ExecutionMode.PROCESS == "process"

    def test_require_isolation(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.should_use_process({"require_isolation": True}) is True

    def test_long_duration(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.should_use_process({"expected_duration_minutes": 15}) is True

    def test_experimental(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.should_use_process({"experimental": True}) is True

    def test_default(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.should_use_process({}) is False

    def test_short(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.should_use_process({"expected_duration_minutes": 5}) is False

    def test_boundary_10(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.should_use_process({"expected_duration_minutes": 10}) is False

    def test_boundary_11(self):
        from src.services.process_crew_executor import ExecutionMode
        assert ExecutionMode.should_use_process({"expected_duration_minutes": 11}) is True


# 18. Global instance

class TestGlobalInstance:
    def test_exists(self):
        from src.services.process_crew_executor import process_crew_executor
        assert process_crew_executor is not None

    def test_type(self):
        from src.services.process_crew_executor import ProcessCrewExecutor, process_crew_executor
        assert isinstance(process_crew_executor, ProcessCrewExecutor)

    def test_same(self):
        from src.services.process_crew_executor import process_crew_executor as a
        from src.services.process_crew_executor import process_crew_executor as b
        assert a is b
