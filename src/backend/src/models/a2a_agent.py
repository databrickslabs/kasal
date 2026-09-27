from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class A2AAgent(Base):
    """A remote agent Kasal can call over A2A.

    The outbound half of the A2A surface. ``a2a_push_configs`` is someone else
    subscribing to Kasal's work; this is Kasal delegating work to someone else's
    agent, and the two never share a row.

    Shaped after ``MCPServer`` on purpose — same group scoping, same
    ``enabled``/``global_enabled`` pair, same encrypted credential — because to
    an operator these are the same kind of thing: an external capability you
    attach to agents. A second, differently-shaped configuration model for the
    second protocol is how the two surfaces start behaving differently for no
    reason a user could explain.
    """

    __tablename__ = "a2a_agents"
    __table_args__ = (
        UniqueConstraint("name", "group_id", name="uq_a2aagent_name_group"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    #: The name agents refer to this remote by. Unique per workspace, not
    #: globally: two tenants naming their own remote "Researcher" is normal.
    name: Mapped[str] = mapped_column(String, nullable=False)
    #: Where the Agent Card lives. Either the card URL itself or the agent's
    #: base URL — the client resolves /.well-known/agent.json from the latter,
    #: because "paste the agent's URL" is what an operator will actually do.
    card_url: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    #: "obo" forwards the CALLING user's Databricks token, which keeps the
    #: identity model consistent with the rest of Kasal: work at the far end
    #: runs as the person who asked for it, not as the workspace.
    auth_type: Mapped[str] = mapped_column(
        String, default="obo", nullable=True
    )  # "obo" | "api_key" | "none"
    encrypted_api_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=True)
    #: Available to every agent without being listed per-agent, matching what
    #: the same flag means for an MCP server.
    global_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)

    group_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_by_email: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    timeout_seconds: Mapped[int] = mapped_column(Integer, default=300, nullable=True)

    #: The last card Kasal fetched, cached so the tool description and the UI
    #: can name the remote's skills without a network round-trip per render.
    #: A cache, never a source of truth — a call always re-reads the card when
    #: it needs to resolve a skill.
    cached_card: Mapped[Any] = mapped_column(JSON, nullable=True)
    card_fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    #: Why the last card fetch failed, so a misconfigured remote is visible in
    #: the UI instead of only showing up as a tool that silently does nothing.
    last_error: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
