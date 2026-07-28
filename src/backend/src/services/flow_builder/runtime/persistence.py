"""Flow state persistence.

Authored module; surface validated against the kasal_engine datamodel.
Kasal subclasses FlowPersistence (KasalFlowPersistence, DB-backed) and also
uses the default backend via ``@persist()`` — SQLiteFlowPersistence here,
stored under db_storage_path().
"""

import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.utils.storage_paths import db_storage_path

logger = logging.getLogger(__name__)


class PendingFeedbackContext(BaseModel):
    """Context stored while a flow is paused for human feedback."""

    model_config = ConfigDict(extra="allow")

    method_name: str | None = None
    prompt: str | None = None


class FlowPersistence(ABC):
    persistence_type: str = "custom"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

    @abstractmethod
    def init_db(self) -> None:
        """Create the backing store if needed."""

    @abstractmethod
    def save_state(
        self,
        flow_uuid: str,
        method_name: str,
        state_data: dict[str, Any] | BaseModel,
    ) -> None:
        """Persist the flow state after a method completed."""

    @abstractmethod
    def load_state(self, flow_uuid: str) -> dict[str, Any] | None:
        """Load the latest persisted state for a flow, or None."""

    def save_pending_feedback(
        self,
        flow_uuid: str,
        context: PendingFeedbackContext,
        state_data: dict[str, Any] | BaseModel,
    ) -> None:
        raise NotImplementedError(
            f"{type(self).__name__} does not support pending feedback."
        )

    def load_pending_feedback(
        self, flow_uuid: str
    ) -> tuple[dict[str, Any], PendingFeedbackContext] | None:
        return None

    def clear_pending_feedback(self, flow_uuid: str) -> None:
        return None


def _state_to_json(state_data: dict[str, Any] | BaseModel) -> str:
    if isinstance(state_data, BaseModel):
        return state_data.model_dump_json()
    return json.dumps(state_data, default=str)


class SQLiteFlowPersistence(FlowPersistence):
    persistence_type = "sqlite"

    def __init__(self, db_path: str | None = None) -> None:
        base = Path(db_path) if db_path else Path(db_storage_path()) / "flow_states.db"
        base.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base)
        self.init_db()

    def init_db(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS flow_states (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       flow_uuid TEXT NOT NULL,
                       method_name TEXT NOT NULL,
                       state_json TEXT NOT NULL,
                       created_at TEXT NOT NULL
                   )"""
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_flow_states_uuid "
                "ON flow_states(flow_uuid)"
            )

    def save_state(
        self,
        flow_uuid: str,
        method_name: str,
        state_data: dict[str, Any] | BaseModel,
    ) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO flow_states (flow_uuid, method_name, state_json, created_at) "
                "VALUES (?, ?, ?, ?)",
                (
                    flow_uuid,
                    method_name,
                    _state_to_json(state_data),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def load_state(self, flow_uuid: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT state_json FROM flow_states WHERE flow_uuid = ? "
                "ORDER BY id DESC LIMIT 1",
                (flow_uuid,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])


def persist(persistence: FlowPersistence | None = None) -> Any:
    """Attach persistence to a Flow class (or a single flow method).

    Class use (kasal's pattern): every completed method saves state; kickoff
    with ``{"id": ...}`` restores. Method use marks just that method — the
    engine saves after every method anyway, so it degrades gracefully.
    """

    def decorator(target: Any) -> Any:
        backend = persistence or SQLiteFlowPersistence()
        if isinstance(target, type):
            target._persistence_instance = backend
            return target
        target.__persist_backend__ = backend
        return target

    return decorator
