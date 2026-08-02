"""Schemas for crew publication — the contract for every surface it feeds.

Two of those surfaces are external (MCP, A2A) and one is internal (``chat``, the
"Use existing" router). The record is the same either way; only ``protocols``
says who may reach it.

Validation lives here rather than in the service so a malformed publication is
rejected at the edge, before anything reaches the repository.
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

#: The surfaces a crew can be published to. Adding one here is the only place a
#: new protocol needs registering for publication purposes.
#:
#: ``chat`` is INTERNAL — it makes a capability routable from a ChatMode prompt in
#: "Use existing" mode and exposes nothing outside the workspace. That is why this
#: is no longer called ``ExternalProtocol``: a literal named External listing an
#: internal surface encodes a falsehood in the type system.
PublicationProtocol = Literal["mcp", "a2a", "chat"]

#: What kind of thing is being published. Crews and flows are equal citizens
#: externally — a caller invokes a capability and does not care which execution
#: path runs it.
PublishableEntity = Literal["crew", "flow"]

#: MCP tool names and A2A skill ids share this shape in practice: lowercase,
#: digits, underscores. Kept deliberately narrow — it is a wire identifier that
#: external clients pin, not a display name.
_EXTERNAL_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class CrewPublicationBase(BaseModel):
    external_name: str = Field(
        ...,
        description="The MCP tool name / A2A skill id. Stable contract.",
    )
    description: str = Field(
        ...,
        min_length=1,
        max_length=1024,
        description=(
            "What this crew does and WHEN to use it. The only thing a calling "
            "agent matches on, in either protocol."
        ),
    )
    protocols: List[PublicationProtocol] = Field(
        default_factory=list,
        description=(
            "Which surfaces may reach this crew. 'mcp' and 'a2a' expose it "
            "outside the workspace; 'chat' only makes it routable from a "
            "ChatMode prompt in 'Use existing' mode."
        ),
    )
    input_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "JSON Schema for the crew's declared inputs, including which are "
            "'required'. Authored at publish time — the placeholder syntax "
            "carries no optionality, so nothing downstream can infer it."
        ),
    )

    @field_validator("external_name")
    @classmethod
    def validate_external_name(cls, v: str) -> str:
        if not _EXTERNAL_NAME_RE.match(v):
            raise ValueError(
                "external_name must be lowercase letters, digits and underscores, "
                "start with a letter, and be at most 64 characters "
                f"(got {v!r})"
            )
        return v

    @field_validator("protocols")
    @classmethod
    def validate_protocols_unique(
        cls, v: List[PublicationProtocol]
    ) -> List[PublicationProtocol]:
        if len(set(v)) != len(v):
            raise ValueError(f"protocols must not repeat (got {v!r})")
        return v


class CrewPublicationCreate(CrewPublicationBase):
    """Request body for publishing a crew."""


class CrewPublicationUpdate(BaseModel):
    """Partial update. Every field optional; omitted fields are left alone."""

    external_name: Optional[str] = None
    description: Optional[str] = Field(default=None, min_length=1, max_length=1024)
    protocols: Optional[List[PublicationProtocol]] = None
    input_schema: Optional[Dict[str, Any]] = None

    @field_validator("external_name")
    @classmethod
    def validate_external_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not _EXTERNAL_NAME_RE.match(v):
            raise ValueError(
                "external_name must be lowercase letters, digits and underscores, "
                "start with a letter, and be at most 64 characters "
                f"(got {v!r})"
            )
        return v


class CrewPublicationResponse(CrewPublicationBase):
    """A publication as returned by the API."""

    id: int
    entity_type: PublishableEntity = "crew"
    entity_id: str
    group_id: str
    created_by_email: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class PublishedCapability(BaseModel):
    """A published crew as the ADAPTERS see it — protocol-neutral.

    Both the MCP tool list and the A2A card's ``skills[]`` render from this, so
    they cannot advertise different capabilities. Deliberately does NOT carry
    ``group_id``: by the time an adapter holds one of these the group filter has
    already been applied, and re-exposing it invites a caller-supplied override.
    """

    entity_type: PublishableEntity = "crew"
    entity_id: str
    name: str
    description: str
    #: Which teamspace published it. A caller identified only by email sees every
    #: teamspace they belong to, so "which one does this tool belong to" is a
    #: question the surfaces have to be able to answer — and it is what
    #: disambiguates two teamspaces publishing the same name.
    teamspace: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = None
    #: Whether this capability holds a CONVERSATION rather than answering once.
    #: Only a flow can, and only one that declares `state.conversational`. The
    #: chat router needs it to know that a follow-up belongs to the capability
    #: that answered the previous turn instead of being re-matched from scratch;
    #: the other adapters ignore it.
    conversational: bool = False

    @property
    def crew_id(self) -> str:
        """Back-compat for readers written when only crews were publishable."""
        return self.entity_id
