"""Kasal-backed CrewAI flow-state persistence.

CrewAI's default ``@persist`` writes flow state to a stray SQLite file
(``~/Library/Application Support/.../flow_states.db``) that is NOT Kasal's database
and is lost on restart in ephemeral/production environments (Databricks Apps +
Lakebase). This implementation routes save/load through Kasal's own database so
checkpoints land in SQLite (dev) or Lakebase/Postgres (prod), survive restarts,
and are queryable by the app.

Bridging note: CrewAI's :class:`FlowPersistence` API is *synchronous* and its hooks
run inside the flow's already-running event loop, while Kasal's DB access is async
(and, for Lakebase, loop-/token-bound). We therefore run each async DB operation on
a short-lived dedicated thread + event loop. This avoids ``asyncio.run`` deadlocks
(we're already inside a running loop) and lets Kasal's swappable session factory
build a loop-correct, Lakebase-aware engine for that thread — the same mechanism
crew threads already rely on.
"""
import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable, Optional

from crewai.flow.persistence.base import FlowPersistence
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Module-level guard so we only attempt the (lock-prone) CREATE TABLE once per
# process, lazily, and only if a write actually hits a missing table. The table
# is normally created by create_all at app startup, so this rarely fires.
_table_ensured = False
_table_lock = threading.Lock()


def _run_async(coro_factory: Callable[[], Any]) -> Any:
    """Run an async coroutine to completion on a fresh thread + event loop.

    ``coro_factory`` must be a zero-arg callable that *creates* the coroutine, so the
    coroutine is bound to the new loop (not the caller's). Called from CrewAI's sync
    persistence hooks, which execute inside the flow's running loop — so a dedicated
    thread is required to avoid deadlock.
    """
    box: dict = {}

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            box["value"] = loop.run_until_complete(coro_factory())
        except BaseException as exc:  # noqa: BLE001 - re-raised on the caller thread
            box["error"] = exc
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if "error" in box:
        raise box["error"]
    return box.get("value")


class KasalFlowPersistence(FlowPersistence):
    """``FlowPersistence`` backed by Kasal's database (SQLite dev / Lakebase prod)."""

    persistence_type: str = "KasalFlowPersistence"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # NOTE: we deliberately do NOT run CREATE TABLE here. The flow_states table
        # is created by create_all at app startup (the model is registered in
        # src/models/__init__.py). Running DDL on every flow build takes a SQLite
        # EXCLUSIVE lock and contends with the app's single shared write connection
        # (StaticPool) — a cause of "database is locked". init_db() is kept for the
        # FlowPersistence contract and as a lazy self-heal if a write ever finds the
        # table missing (see _execute_with_retry).

    def init_db(self) -> None:
        """Idempotently ensure the flow_states table exists in the active database.

        Lazy safety net only — normally the table already exists (create_all at
        startup), so this is not run on construction.
        """
        global _table_ensured

        async def _init() -> None:
            from src.db.session import async_session_factory
            from src.models.flow_state import FlowState

            async with async_session_factory() as session:
                conn = await session.connection()
                await conn.run_sync(
                    lambda sync_conn: FlowState.__table__.create(sync_conn, checkfirst=True)
                )
                await session.commit()

        with _table_lock:
            _run_async(_init)
            _table_ensured = True

    def _execute_with_retry(self, coro_factory: Callable[[], Any], *, attempts: int = 5) -> Any:
        """Run a DB coroutine via the thread bridge, tolerating SQLite contention.

        Mirrors the app's retry_db_operation philosophy: a busy SQLite file ("database
        is locked") is retried with exponential backoff (busy_timeout already makes
        writers wait); a missing table self-heals once via init_db(). Non-DB errors
        propagate immediately.
        """
        delay = 0.2
        last_error: Optional[BaseException] = None
        for _ in range(attempts):
            try:
                return _run_async(coro_factory)
            except Exception as exc:  # noqa: BLE001
                message = str(exc).lower()
                last_error = exc
                if "no such table" in message and not _table_ensured:
                    try:
                        self.init_db()
                    except Exception as init_err:  # noqa: BLE001
                        logger.warning(f"[KasalFlowPersistence] lazy init_db failed: {init_err}")
                    continue
                if "database is locked" in message or "database is busy" in message:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise
        if last_error is not None:
            raise last_error

    def save_state(
        self, flow_uuid: str, method_name: str, state_data: "dict[str, Any] | BaseModel"
    ) -> None:
        """Persist a flow-state snapshot to Kasal's DB after a method completes."""
        if isinstance(state_data, BaseModel):
            state_dict = state_data.model_dump()
        else:
            state_dict = dict(state_data) if state_data else {}

        # default=str ensures persistence never crashes on a stray non-JSON value
        # (defense-in-depth; CrewOutput values are already serialized upstream).
        state_json = json.dumps(state_dict, default=str)

        async def _save() -> None:
            # The session is the unit-of-work boundary; DB access goes through the
            # repository (clean architecture), and the caller commits.
            from src.db.session import async_session_factory
            from src.repositories.flow_state_repository import FlowStateRepository

            async with async_session_factory() as session:
                repo = FlowStateRepository(session)
                await repo.add_state(flow_uuid, method_name, state_json)
                await session.commit()

        self._execute_with_retry(_save)
        logger.debug(
            f"[KasalFlowPersistence] Saved flow state for {flow_uuid} after '{method_name}'"
        )

    def load_state(self, flow_uuid: str) -> Optional["dict[str, Any]"]:
        """Load the most recent persisted state for a flow UUID (for resume)."""

        async def _load() -> Optional[str]:
            from src.db.session import async_session_factory
            from src.repositories.flow_state_repository import FlowStateRepository

            async with async_session_factory() as session:
                repo = FlowStateRepository(session)
                return await repo.get_latest_state_json(flow_uuid)

        state_json = self._execute_with_retry(_load)
        if not state_json:
            return None
        try:
            return json.loads(state_json)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(
                f"[KasalFlowPersistence] Could not parse persisted state for {flow_uuid}: {e}"
            )
            return None
