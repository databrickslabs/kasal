import pytest
from unittest.mock import patch, Mock
import asyncio

from src.services.process_crew_executor import ProcessCrewExecutor


class FakeQueue:
    def __init__(self, items=None):
        self._items = list(items or [])
    def empty(self):
        return len(self._items) == 0
    def get_nowait(self):
        if self._items:
            return self._items.pop(0)
        raise Exception("Empty")


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
    def Queue(self):
        return FakeQueue([])
    def Process(self, target=None, args=None):
        return self._process


@pytest.mark.asyncio
async def test_run_crew_isolated_completed_no_result(monkeypatch):
    fake_proc = FakeProcess(exitcode=0, alive=False)
    fake_ctx = FakeContext(fake_proc)

    # Patch get_context to return our fake context
    with patch('src.services.process_crew_executor.mp.get_context', return_value=fake_ctx):
        executor = ProcessCrewExecutor(max_concurrent=1)
        group_context = Mock()
        group_context.primary_group_id = 'gid'
        group_context.access_token = 'tok'

        result = await executor.run_crew_isolated(
            execution_id='exec1',
            crew_config={},
            group_context=group_context,
            inputs=None,
            timeout=None,
            debug_tracing_enabled=False,
        )

        assert result['status'] in ('COMPLETED', 'FAILED', 'STOPPED')
        # For exitcode 0 and empty result_queue, it should be COMPLETED
        assert result['status'] == 'COMPLETED'


@pytest.mark.asyncio
async def test_run_crew_isolated_stopped(monkeypatch):
    fake_proc = FakeProcess(exitcode=-15, alive=False)
    fake_ctx = FakeContext(fake_proc)

    with patch('src.services.process_crew_executor.mp.get_context', return_value=fake_ctx):
        executor = ProcessCrewExecutor(max_concurrent=1)
        group_context = Mock()
        group_context.primary_group_id = 'gid'

        result = await executor.run_crew_isolated('exec2', {}, group_context)
        assert result['status'] == 'STOPPED'
        assert 'exit_code' in result


@pytest.mark.asyncio
async def test_run_crew_isolated_failed(monkeypatch):
    fake_proc = FakeProcess(exitcode=1, alive=False)
    fake_ctx = FakeContext(fake_proc)

    with patch('src.services.process_crew_executor.mp.get_context', return_value=fake_ctx):
        executor = ProcessCrewExecutor(max_concurrent=1)
        group_context = Mock()
        group_context.primary_group_id = 'gid'

        result = await executor.run_crew_isolated('exec3', {}, group_context)
        assert result['status'] == 'FAILED'
        assert result['exit_code'] == 1


@pytest.mark.asyncio
async def test_run_crew_isolated_result_queue_path(monkeypatch):
    # Provide a process and a queue with a result
    class FakeContextWithResult(FakeContext):
        def __init__(self, process):
            super().__init__(process)
            self._result_queue = FakeQueue([{"status": "COMPLETED", "execution_id": "exec4"}])
        def Queue(self):
            # First Queue call is result_queue, second is log_queue
            if hasattr(self, '_returned_once'):
                return FakeQueue([])
            self._returned_once = True
            return self._result_queue

    fake_proc = FakeProcess(exitcode=0, alive=False)
    fake_ctx = FakeContextWithResult(fake_proc)

    with patch('src.services.process_crew_executor.mp.get_context', return_value=fake_ctx):
        executor = ProcessCrewExecutor(max_concurrent=1)
        group_context = Mock()
        group_context.primary_group_id = 'gid'

        result = await executor.run_crew_isolated('exec4', {}, group_context)
        assert result['status'] == 'COMPLETED'
        assert result['execution_id'] == 'exec4'


@pytest.mark.asyncio
async def test_terminate_execution_paths():
    executor = ProcessCrewExecutor(max_concurrent=1)

    # Inject a live fake process
    live_proc = FakeProcess(exitcode=None, alive=True)
    executor._running_processes['exec_live'] = live_proc

    terminated = await executor.terminate_execution('exec_live')
    assert terminated is True
    assert live_proc.terminated or live_proc.killed

    # Non-existing execution id
    terminated2 = await executor.terminate_execution('nope')
    assert terminated2 is False


def test_get_metrics_and_shutdown():
    executor = ProcessCrewExecutor(max_concurrent=1)

    # Inject two processes
    p1 = FakeProcess(alive=True)
    p2 = FakeProcess(alive=False)
    executor._running_processes['e1'] = p1
    executor._running_processes['e2'] = p2

    m = executor.get_metrics()
    assert 'total_executions' in m

    executor.shutdown(wait=True)
    # After shutdown, tracking dicts should be empty
    assert executor._running_processes == {}


def test_kill_orphan_crew_processes_noop():
    # Simulate no processes found
    with patch('psutil.process_iter', return_value=[]):
        ProcessCrewExecutor.kill_orphan_crew_processes()

