"""Schemas for the remote-A2A-agent registry.

Kasal vocabulary, not A2A's — these describe a CONFIGURATION row, not a wire
message. The spec's camelCase types live in ``schemas/a2a.py`` and stop at the
protocol boundary.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

#: "obo" forwards the calling user's Databricks token, which keeps a delegated
#: call running as the person who asked for it.
AUTH_TYPES = ("obo", "api_key", "none")


class A2AAgentBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    card_url: str = Field(
        ...,
        description=(
            "The agent's URL or its Agent Card URL. Either works — the card is "
            "resolved from /.well-known/agent.json when a base URL is given."
        ),
    )
    description: Optional[str] = None
    auth_type: str = "obo"
    enabled: bool = True
    global_enabled: bool = False
    timeout_seconds: int = Field(default=300, ge=1, le=3600)


class A2AAgentCreate(A2AAgentBase):
    #: Write-only. Stored encrypted and never returned.
    api_key: Optional[str] = None


class A2AAgentUpdate(BaseModel):
    name: Optional[str] = None
    card_url: Optional[str] = None
    description: Optional[str] = None
    auth_type: Optional[str] = None
    enabled: Optional[bool] = None
    global_enabled: Optional[bool] = None
    timeout_seconds: Optional[int] = Field(default=None, ge=1, le=3600)
    api_key: Optional[str] = None


class A2AAgentResponse(A2AAgentBase):
    """What the API returns.

    No ``api_key``, encrypted or otherwise: whoever set it has it, and a
    configuration listing is not a credential store to read back from.
    ``has_api_key`` is what the UI actually needs.
    """

    id: int
    has_api_key: bool = False
    skills: List[Dict[str, Any]] = Field(default_factory=list)
    card_fetched_at: Optional[datetime] = None
    last_error: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class A2AAgentListResponse(BaseModel):
    agents: List[A2AAgentResponse] = Field(default_factory=list)
    count: int = 0


class A2AConnectionTest(BaseModel):
    """The outcome of actually fetching the card.

    ``message`` carries Kasal's own description of the failure, never the
    remote's response body — that is untrusted input to a server-side request.
    """

    connected: bool
    message: str
    agent_name: Optional[str] = None
    skills: List[Dict[str, Any]] = Field(default_factory=list)
