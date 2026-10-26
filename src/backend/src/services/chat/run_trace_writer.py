"""One light-agent run's trace persistence on a single private session.

Every tool/LLM event of a chat answer is a trace row. Writing each on a fresh
isolated session cost a full connection handshake per event on the critical
path; :class:`RunTraceWriter` opens ONE private session lazily and reuses it
until the run releases it, serialising the writes so concurrent handler tasks
never interleave on the async connection.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class RunTraceWriter:
    """Persists one light-agent run's trace events on a single private session.

    Perf: the previous per-event ``get_isolated_db_session()`` opened a fresh
    connection (full TCP+TLS+auth on Lakebase NullPool) for EVERY tool/LLM
    event — ~10-20 handshakes per chat answer, all on the critical path since
    completion awaits the persists. This writer opens ONE private session
    lazily on the first persist and reuses it until ``close()``.

    Concurrency: handlers schedule persists onto the main loop via
    ``run_coroutine_threadsafe``, so several (tool_usage, <tool>_run, llm_call,
    llm_response, response_run) run as concurrent tasks. Interleaving their DB
    work corrupts the async connection's greenlet state (the symptom is
    ``MissingGreenlet`` mid-run), so the internal lock makes writes strictly
    one-at-a-time — which is also what makes single-session reuse safe.

    The parent ExecutionHistory row is verified once by the first successful
    write; later writes skip that SELECT and carry the resolved ``run_id``
    forward. A failed write rolls back and drops the session so the next
    persist reopens a fresh connection.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._ctx: Any = None
        self._session: Any = None
        self._verified = False
        self._run_id: Optional[int] = None

    async def _get_session(self) -> Any:
        if self._session is None:
            from src.db.session import get_isolated_db_session

            self._ctx = get_isolated_db_session()
            self._session = await self._ctx.__aenter__()
        return self._session

    async def close(self, timeout: Optional[float] = None) -> None:
        """Release the private session. Idempotent; never raises.

        ``timeout`` bounds the release. The run's terminal status is written
        right after this call, and a session exit that never returns would
        leave the run marked RUNNING forever with its answer already on
        screen — which is how one chat run stayed RUNNING after answering.
        On timeout the session is abandoned (the driver drops the connection
        when it is collected) and the run goes on to its status write.
        """
        ctx = self._ctx
        self._ctx = None
        self._session = None
        if ctx is None:
            return
        try:
            exit_ = ctx.__aexit__(None, None, None)
            if timeout is None:
                await exit_
            else:
                await asyncio.wait_for(exit_, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(
                f"[light_agent] trace session close timed out after {timeout}s; "
                "abandoning it so the run can finish"
            )
        except Exception as close_err:  # noqa: BLE001
            logger.debug(f"[light_agent] trace session close skipped: {close_err}")

    async def persist(self, trace_data: Dict[str, Any]) -> None:
        """Write one trace event. Never raises — a lost trace must not fail the run."""
        try:
            from src.services.trace import ExecutionTraceService

            async with self._lock:
                session = await self._get_session()
                try:
                    if self._run_id is not None:
                        trace_data.setdefault("run_id", self._run_id)
                    item = await ExecutionTraceService(session).create_trace(
                        trace_data,
                        verify_execution_exists=not self._verified,
                    )
                    await session.commit()
                    self._verified = True
                    if self._run_id is None:
                        self._run_id = getattr(item, "run_id", None)
                except Exception:
                    # The connection may be poisoned — roll back and drop it so
                    # the next persist reopens a fresh one.
                    try:
                        await session.rollback()
                    except Exception:  # noqa: BLE001
                        pass
                    await self.close()
                    raise
            logger.debug(
                f"[light_agent] trace persisted: job_id={trace_data.get('job_id')} "
                f"event_type={trace_data.get('event_type')}"
            )
        except Exception as persist_err:  # noqa: BLE001
            logger.warning(
                f"[light_agent] trace persist FAILED "
                f"(event_type={trace_data.get('event_type')}): {persist_err}",
                exc_info=True,
            )
