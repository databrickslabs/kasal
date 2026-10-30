"""Workspace opt-in to the decision model (provider: Jev).

Credentials never enter execution payloads.
"""

from sqlalchemy import Boolean, String, false
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class DecisionConfig(Base):
    """One row per workspace that has touched the decision-model switch.

    ``group_id`` is deliberately NOT a foreign key to ``groups.id``: personal
    workspaces (``user_<email>``) have no ``groups`` row, so the key made the
    setting impossible to save there. It is the same plain workspace id that
    ``apikey.group_id`` uses for the JEV_API_KEY this row pairs with.
    """

    __tablename__ = "decision_config"

    group_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
