"""Durable per-conversation state for the exported app.

A tiny key-value store keyed by ``(conversation_id, key)`` that backs the
previously in-memory stores — conversation history, cancel flags, live
progress, and A2UI surfaces — so they survive an app restart and stay correct
if the platform ever runs more than one process. Databricks Apps restart on
every deploy/config change/crash; before this module, a restart mid-turn made
Stop silently no-op, left the A2UI poll returning "idle" forever, and dropped
the multi-turn conversation context.

Backend selection (once, on first use):
  1. Lakebase Postgres — when the app has a Lakebase instance attached
     (``LAKEBASE_INSTANCE_NAME``, set by the Kasal deploy) or standard ``PG*``
     env vars are present. Auth uses the app's own identity via the Databricks
     SDK (``generate_database_credential``); the short-lived token is refreshed
     by reconnecting on failure.
  2. SQLite — a local file (``AGENT_STATE_SQLITE``, default
     ``.agent_state.sqlite`` at the project root). Local dev, or Lakebase not
     attached/reachable. Survives process restarts but not container rebuilds.
  3. In-memory dict — last resort; the original pre-persistence behavior.

Every operation is best-effort and NEVER raises: on a backend error the call
logs and degrades, and the calling modules keep their own in-memory caches, so
the app falls back to the old in-process behavior instead of failing a turn.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

_TABLE = "kasal_app_state"
# Rows older than this are pruned outright (history included). Ephemeral keys
# (cancel/progress/a2ui) additionally pass max_age on read for their 600s TTL.
_ROW_TTL_SECONDS = int(os.environ.get("AGENT_STATE_TTL_SECONDS", str(7 * 24 * 3600)))
_PRUNE_EVERY_WRITES = 200

_lock = threading.RLock()
_backend: Optional["_Backend"] = None
_writes_since_prune = 0


class _Backend:
    name = "base"

    def get(self, cid: str, key: str) -> Optional[Tuple[str, float]]:
        raise NotImplementedError

    def set(self, cid: str, key: str, value: str, now: float) -> None:
        raise NotImplementedError

    def delete(self, cid: str, key: str) -> None:
        raise NotImplementedError

    def prune(self, cutoff: float) -> None:
        raise NotImplementedError


class _MemoryBackend(_Backend):
    name = "memory"

    def __init__(self) -> None:
        self._data: Dict[Tuple[str, str], Tuple[str, float]] = {}

    def get(self, cid: str, key: str) -> Optional[Tuple[str, float]]:
        return self._data.get((cid, key))

    def set(self, cid: str, key: str, value: str, now: float) -> None:
        self._data[(cid, key)] = (value, now)

    def delete(self, cid: str, key: str) -> None:
        self._data.pop((cid, key), None)

    def prune(self, cutoff: float) -> None:
        stale = [k for k, (_, ts) in self._data.items() if ts < cutoff]
        for k in stale:
            self._data.pop(k, None)


class _SQLiteBackend(_Backend):
    name = "sqlite"

    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {_TABLE} ("
            "conversation_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT, "
            "updated_at REAL NOT NULL, PRIMARY KEY (conversation_id, key))"
        )
        self._conn.commit()

    def get(self, cid: str, key: str) -> Optional[Tuple[str, float]]:
        row = self._conn.execute(
            f"SELECT value, updated_at FROM {_TABLE} "
            "WHERE conversation_id = ? AND key = ?",
            (cid, key),
        ).fetchone()
        return (row[0], row[1]) if row else None

    def set(self, cid: str, key: str, value: str, now: float) -> None:
        self._conn.execute(
            f"INSERT OR REPLACE INTO {_TABLE} "
            "(conversation_id, key, value, updated_at) VALUES (?, ?, ?, ?)",
            (cid, key, value, now),
        )
        self._conn.commit()

    def delete(self, cid: str, key: str) -> None:
        self._conn.execute(
            f"DELETE FROM {_TABLE} WHERE conversation_id = ? AND key = ?", (cid, key)
        )
        self._conn.commit()

    def prune(self, cutoff: float) -> None:
        self._conn.execute(f"DELETE FROM {_TABLE} WHERE updated_at < ?", (cutoff,))
        self._conn.commit()


class _PostgresBackend(_Backend):
    """Lakebase Postgres. Credentials are short-lived OAuth tokens minted with
    the app's identity, so every statement retries ONCE through a reconnect —
    that refresh path is what keeps the store working past token expiry (~1h).
    """

    name = "lakebase"
    # Lakebase is Postgres 15+: non-owners get "permission denied for schema
    # public" (42501), while the app's CAN_CONNECT_AND_CREATE database resource
    # grants DATABASE-level CREATE — i.e. the app SP may create SCHEMAS, just
    # not objects in `public`. So the store lives in its own schema.
    _SCHEMA = "agent_server"

    def __init__(self) -> None:
        self._conn = None
        self._connect()
        self._table = f"{self._SCHEMA}.{_TABLE}"
        try:
            self._execute(f"CREATE SCHEMA IF NOT EXISTS {self._SCHEMA}")
            self._execute(self._create_table_sql(self._table))
        except Exception:
            # e.g. the schema exists but is owned by another role: try public
            # (works when an admin ran scripts/grant_lakebase_permissions.py);
            # if that also fails, the caller falls back to SQLite.
            self._table = _TABLE
            self._execute(self._create_table_sql(self._table))

    @staticmethod
    def _create_table_sql(table: str) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "conversation_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT, "
            "updated_at DOUBLE PRECISION NOT NULL, "
            "PRIMARY KEY (conversation_id, key))"
        )

    def _connect(self) -> None:
        # pg8000: pure-Python, BSD-licensed Postgres driver (same one Kasal
        # itself ships) — deliberately NOT psycopg, which is LGPL.
        import ssl

        import pg8000.dbapi

        host = os.environ.get("PGHOST")
        port = os.environ.get("PGPORT", "5432")
        dbname = (
            os.environ.get("PGDATABASE")
            or os.environ.get("LAKEBASE_DATABASE_NAME")
            or "databricks_postgres"
        )
        user = os.environ.get("PGUSER")
        password = os.environ.get("PGPASSWORD")
        instance = os.environ.get("LAKEBASE_INSTANCE_NAME")

        if not host or not password:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            if not host:
                if not instance:
                    raise RuntimeError("no PGHOST and no LAKEBASE_INSTANCE_NAME")
                host = w.database.get_database_instance(name=instance).read_write_dns
            if not user:
                # In Databricks Apps the PG role is the app SP's client id;
                # locally it's the developer's user name.
                user = os.environ.get("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name
            if not password:
                cred = w.database.generate_database_credential(
                    request_id=str(uuid.uuid4()),
                    instance_names=[instance] if instance else None,
                )
                password = cred.token
        if not user:
            raise RuntimeError("no PGUSER/DATABRICKS_CLIENT_ID for Lakebase auth")

        old = self._conn
        conn = pg8000.dbapi.connect(
            host=host,
            port=int(port),
            database=dbname,
            user=user,
            password=password,
            ssl_context=ssl.create_default_context(),
            timeout=10,
        )
        conn.autocommit = True
        self._conn = conn
        if old is not None:
            try:
                old.close()
            except Exception:  # noqa: BLE001
                pass

    def _execute(self, sql: str, params: tuple = (), fetch: bool = False):
        # One retry through a reconnect: covers expired OAuth tokens and
        # dropped connections without leaking the failure to the caller.
        for attempt in (1, 2):
            try:
                cur = self._conn.cursor()
                try:
                    cur.execute(sql, params)
                    return cur.fetchone() if fetch else None
                finally:
                    cur.close()
            except Exception:
                if attempt == 2:
                    raise
                self._connect()

    def get(self, cid: str, key: str) -> Optional[Tuple[str, float]]:
        row = self._execute(
            f"SELECT value, updated_at FROM {self._table} "
            "WHERE conversation_id = %s AND key = %s",
            (cid, key),
            fetch=True,
        )
        return (row[0], row[1]) if row else None

    def set(self, cid: str, key: str, value: str, now: float) -> None:
        self._execute(
            f"INSERT INTO {self._table} (conversation_id, key, value, updated_at) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (conversation_id, key) "
            "DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            (cid, key, value, now),
        )

    def delete(self, cid: str, key: str) -> None:
        self._execute(
            f"DELETE FROM {self._table} WHERE conversation_id = %s AND key = %s",
            (cid, key),
        )

    def prune(self, cutoff: float) -> None:
        self._execute(f"DELETE FROM {self._table} WHERE updated_at < %s", (cutoff,))


def _sqlite_path() -> str:
    return os.environ.get(
        "AGENT_STATE_SQLITE",
        str(Path(__file__).parent.parent / ".agent_state.sqlite"),
    )


def _pick_backend() -> _Backend:
    if os.environ.get("LAKEBASE_INSTANCE_NAME") or os.environ.get("PGHOST"):
        try:
            backend = _PostgresBackend()
            print("[state_store] Conversation state -> Lakebase Postgres.")
            return backend
        except Exception as exc:  # noqa: BLE001
            print(
                f"[state_store] Lakebase unavailable ({exc}); falling back to SQLite."
            )
    try:
        path = _sqlite_path()
        backend = _SQLiteBackend(path)
        print(f"[state_store] Conversation state -> SQLite ({path}).")
        return backend
    except Exception as exc:  # noqa: BLE001
        print(f"[state_store] SQLite unavailable ({exc}); state is in-memory only.")
        return _MemoryBackend()


def _get_backend() -> _Backend:
    global _backend
    with _lock:
        if _backend is None:
            _backend = _pick_backend()
        return _backend


def backend_name() -> str:
    return _get_backend().name


def get_text(cid: Optional[str], key: str, max_age: Optional[float] = None) -> Optional[str]:
    """The stored value, or None when absent/expired. Never raises."""
    if not cid:
        return None
    try:
        with _lock:
            item = _get_backend().get(str(cid), key)
        if item is None:
            return None
        value, ts = item
        if max_age is not None and (time.time() - ts) > max_age:
            return None
        return value
    except Exception as exc:  # noqa: BLE001
        print(f"[state_store] get({key}) failed: {exc}")
        return None


def set_text(cid: Optional[str], key: str, value: str) -> None:
    """Store a value (best-effort; occasionally prunes expired rows)."""
    if not cid:
        return
    global _writes_since_prune
    try:
        now = time.time()
        with _lock:
            backend = _get_backend()
            backend.set(str(cid), key, value, now)
            _writes_since_prune += 1
            if _writes_since_prune >= _PRUNE_EVERY_WRITES:
                _writes_since_prune = 0
                backend.prune(now - _ROW_TTL_SECONDS)
    except Exception as exc:  # noqa: BLE001
        print(f"[state_store] set({key}) failed: {exc}")


def delete(cid: Optional[str], key: str) -> None:
    if not cid:
        return
    try:
        with _lock:
            _get_backend().delete(str(cid), key)
    except Exception as exc:  # noqa: BLE001
        print(f"[state_store] delete({key}) failed: {exc}")


def get_json(cid: Optional[str], key: str, max_age: Optional[float] = None) -> Any:
    raw = get_text(cid, key, max_age=max_age)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def set_json(cid: Optional[str], key: str, value: Any) -> None:
    try:
        raw = json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError) as exc:
        print(f"[state_store] set_json({key}) not serializable: {exc}")
        return
    set_text(cid, key, raw)


def _reset_for_tests() -> None:
    """Drop the chosen backend so the next call re-selects (simulates restart)."""
    global _backend, _writes_since_prune
    with _lock:
        _backend = None
        _writes_since_prune = 0
