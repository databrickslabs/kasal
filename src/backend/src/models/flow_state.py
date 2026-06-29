from datetime import datetime

from sqlalchemy import Column, DateTime, Index, Integer, String, Text

from src.db.base import Base


class FlowState(Base):
    """Flow state checkpoints for CrewAI's @persist, stored in Kasal's own database.

    CrewAI's default persistence writes flow state to a stray SQLite file outside
    Kasal's DB (lost on restart in ephemeral/prod and never in Lakebase). This table
    keeps that state inside Kasal's database instead — SQLite in dev, Lakebase/Postgres
    in prod — so checkpoints survive restarts and are queryable by the app.

    The table is append-only: each method completion writes a new row. The most recent
    row for a given ``flow_uuid`` is the current checkpoint used to resume.
    """

    __tablename__ = "flow_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # CrewAI flow state UUID (the @persist state "id" used to resume a run).
    flow_uuid = Column(String(36), nullable=False, index=True)
    # Name of the flow method that had just completed when this state was saved.
    method_name = Column(String(255), nullable=False)
    # JSON-serialized flow state dict.
    state_json = Column(Text, nullable=False)
    # Timezone-naive UTC to match the TIMESTAMP WITHOUT TIME ZONE column (asyncpg
    # rejects binding a tz-aware datetime to a naive Postgres/Lakebase column), and
    # to stay consistent with flow_execution / execution_history.
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_flow_states_uuid_created", "flow_uuid", "created_at"),
    )
