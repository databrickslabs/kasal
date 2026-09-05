"""The trace writer's release is bounded: a run's terminal status is written
right after it, and one chat run stayed RUNNING forever after answering."""

import asyncio
import time

import pytest

from src.services.chat.run_trace_writer import RunTraceWriter


class _HangingCtx:
    async def __aexit__(self, *_):
        await asyncio.sleep(30)


class _Ctx:
    def __init__(self):
        self.exited = False

    async def __aexit__(self, *_):
        self.exited = True


class TestBoundedClose:
    @pytest.mark.asyncio
    async def test_close_gives_up_on_a_session_exit_that_never_returns(self, caplog):
        writer = RunTraceWriter()
        writer._ctx, writer._session = _HangingCtx(), object()
        started = time.monotonic()
        await writer.close(timeout=0.05)
        assert time.monotonic() - started < 2
        assert writer._ctx is None and writer._session is None
        assert "trace session close timed out" in caplog.text

    @pytest.mark.asyncio
    async def test_close_without_a_timeout_awaits_the_exit(self):
        writer = RunTraceWriter()
        ctx = _Ctx()
        writer._ctx, writer._session = ctx, object()
        await writer.close()
        assert ctx.exited and writer._ctx is None

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        writer = RunTraceWriter()
        await writer.close(timeout=1)
        await writer.close()
