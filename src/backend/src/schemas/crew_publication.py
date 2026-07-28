"""Schemas for crew publication — the external-facing contract.

Validation lives here rather than in the service so a malformed publication is
rejected at the edge, before anything reaches the repository.
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

#: The surfaces a crew can be exposed over. Adding one here is the only place a
#: new protocol needs registering for publication purposes.
ExternalProtocol = Literal["mcp", "a2a"]

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
    protocols: List[ExternalProtocol] = Field(
        default_factory=list,
        description="Which external surfaces expose this crew.",
    )
    input_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description="JSON Schema for the crew's declared inputs.",
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
        cls, v: List[ExternalProtocol]
    ) -> List[ExternalProtocol]:
        if len(set(v)) != len(v):
            raise ValueError(f"protocols must not repeat (got {v!r})")
        return v


class CrewPublicationCreate(CrewPublicationBase):
    """Request body for publishing a crew."""


class CrewPublicationUpdate(BaseModel):
    """Partial update. Every field optional; omitted fields are left alone."""

    external_name: Optional[str] = None
    description: Optional[str] = Field(default=None, min_length=1, max_length=1024)
    protocols: Optional[List[ExternalProtocol]] = None
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
    crew_id: str
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

    crew_id: str
    name: str
    description: str
    input_schema: Optional[Dict[str, Any]] = None
