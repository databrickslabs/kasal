from datetime import datetime
from uuid import uuid4

from sqlalchemy import Column, DateTime, Index, String, Text

from src.db.base import Base


def generate_uuid():
    return str(uuid4())


class CrewFeedback(Base):
    """
    Thumbs up/down feedback on a cataloged crew.

    Captured from chat mode after a crew runs; thumbs-down carries a comment
    explaining what went wrong. Surfaced in the Agent Builder catalog so the
    AI engineer can review per-crew sentiment and the down-vote reasons.
    """

    __tablename__ = "crew_feedback"

    id = Column(String, primary_key=True, default=generate_uuid)
    crew_id = Column(String, nullable=False, index=True)
    rating = Column(String(8), nullable=False)  # 'up' | 'down'
    comment = Column(Text, nullable=True)  # required by the UI for 'down'
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Multi-group fields (REQUIRED for all models)
    group_id = Column(String(100), index=True, nullable=True)
    group_email = Column(String(255), nullable=True)

    __table_args__ = (
        Index("idx_crew_feedback_crew_created", "crew_id", "created_at"),
    )
