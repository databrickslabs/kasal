from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class Publication(Base):
    """A crew OR FLOW deliberately exposed to callers outside this Kasal instance.

    ONE record, N protocols. The alternative — an ``mcp_published`` flag beside
    an ``a2a_published`` flag — splits ``description`` and ``input_schema`` into
    two copies that drift, and one quietly becomes wrong while each surface still
    looks fine on its own. The MCP tool list and the A2A card's ``skills[]`` are
    two projections of ONE row, so they cannot advertise different capabilities.

    ``entity_type`` is what makes flows publishable on equal terms. A flow is a
    capability an external agent can invoke exactly as a crew is; only the
    execution path differs, and that difference belongs in the invocation layer,
    not in a second publication table with its own copy of description, schema
    and group scoping.

    ``description`` is load-bearing: it is the only thing a calling agent matches
    on, in either protocol (MCP tool description, A2A ``AgentSkill.description``).
    A vague one means the capability is never selected.

    Nothing is published by default. A row here is the record of someone
    deciding, for a specific crew or flow, in a specific group.
    """

    __tablename__ = "publications"
    __table_args__ = (
        # An entity is published once; the protocol list lives inside the row.
        UniqueConstraint("entity_type", "entity_id", name="uq_publication_entity"),
        # An external name is how a caller addresses the capability, so it must
        # be unambiguous WITHIN a group — and across TYPES too: a crew and a flow
        # published under the same name would be one ambiguous tool.
        UniqueConstraint("external_name", "group_id", name="uq_publication_name_group"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    #: "crew" | "flow" — which execution path this capability runs on.
    entity_type: Mapped[str] = mapped_column(
        String(16), nullable=False, default="crew", index=True
    )
    #: The crew id or flow id. Stored as a string because the two use different
    #: id types and this column addresses both.
    entity_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    #: Which external surfaces expose it, e.g. ``["mcp", "a2a"]``. An empty list
    #: keeps the name/description/schema someone wrote while exposing nothing, so
    #: toggling a protocol off does not destroy the publication.
    protocols: Mapped[Any] = mapped_column(JSON, nullable=False, default=list)

    #: The MCP tool name / A2A skill id. Stable contract — external clients pin it.
    external_name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)

    #: JSON Schema for declared inputs. Without one, a per-capability tool is
    #: barely more useful than the generic start_crew.
    input_schema: Mapped[Any] = mapped_column(JSON, nullable=True)

    #: Whether this capability HOLDS A CONVERSATION: follow-up turns go back to
    #: it with the recent transcript instead of being re-matched from scratch.
    #: Set by the publisher. A flow whose state declares `conversational` holds
    #: one whether or not this is set; for a crew this flag is the only way.
    conversational: Mapped[bool] = mapped_column(Boolean, nullable=True, default=False)

    #: Tenant isolation. NOT nullable: an unscoped publication is reachable from
    #: outside with no group to filter by, which is the leak this layer exists to
    #: prevent.
    group_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_by_email: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def __repr__(self) -> str:
        return (
            f"<Publication {self.entity_type}={self.entity_id} "
            f"name={self.external_name} protocols={self.protocols}>"
        )


#: The class was CrewPublication while only crews were publishable. Kept so the
#: rename does not have to land in every import at once.
CrewPublication = Publication
