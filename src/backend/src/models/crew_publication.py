from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Integer, String, UniqueConstraint

from src.db.base import Base


class CrewPublication(Base):
    """A crew deliberately exposed to callers OUTSIDE this Kasal instance.

    ONE record, N protocols. The alternative — an ``mcp_published`` flag beside
    an ``a2a_published`` flag — splits ``description`` and ``input_schema`` into
    two copies that drift, and one quietly becomes wrong while each surface still
    looks fine on its own. The MCP tool list and the A2A card's ``skills[]`` are
    two projections of ONE row, so they cannot advertise different capabilities.

    ``description`` is load-bearing: it is the only thing a calling agent matches
    on, in either protocol (MCP tool description, A2A ``AgentSkill.description``).
    A vague one means the crew is never selected.

    Nothing is published by default. A row here is the record of someone
    deciding, for a specific crew, in a specific group.
    """

    __tablename__ = "crew_publications"
    __table_args__ = (
        # A crew is published once; the protocol list lives inside the row.
        UniqueConstraint("crew_id", name="uq_crew_publication_crew"),
        # An external name is how a caller addresses the capability, so it must
        # be unambiguous WITHIN a group. Across groups it may repeat — two
        # tenants may both publish "analyse_powerbi_model".
        UniqueConstraint(
            "external_name", "group_id", name="uq_crew_publication_name_group"
        ),
    )

    id = Column(Integer, primary_key=True)
    crew_id = Column(String, nullable=False, index=True)

    #: Which external surfaces expose this crew, e.g. ``["mcp", "a2a"]``.
    #: Empty list == published record exists but nothing is exposed (the same as
    #: unpublished, and allowed, so a caller can toggle protocols without losing
    #: the name/description/schema they wrote).
    protocols = Column(JSON, nullable=False, default=list)

    #: The MCP tool name / A2A skill id. Stable contract — external clients pin it.
    external_name = Column(String, nullable=False)
    description = Column(String, nullable=False)

    #: JSON Schema for the crew's declared inputs. Without one, an MCP Layer-2
    #: tool is barely more useful than the generic start_crew.
    input_schema = Column(JSON, nullable=True)

    #: Tenant isolation. NOT nullable: an unscoped publication is reachable from
    #: outside with no group to filter by, which is the leak this whole layer
    #: exists to prevent.
    group_id = Column(String, nullable=False, index=True)
    created_by_email = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self) -> str:
        return (
            f"<CrewPublication crew_id={self.crew_id} "
            f"name={self.external_name} protocols={self.protocols}>"
        )
