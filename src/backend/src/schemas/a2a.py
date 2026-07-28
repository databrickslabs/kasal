"""A2A wire schemas.

Field names mirror the A2A specification EXACTLY — ``camelCase``,
``AgentCard``, ``skills``, ``TASK_STATE_*`` — and are deliberately not renamed
into Kasal vocabulary. These types are the public contract: an external agent
parses them against the spec, not against this codebase. Translating names here
would mean every A2A client needs a Kasal-specific adapter, which is the whole
thing the standard exists to avoid.

Kasal's own vocabulary stops at the edge of this module. Everything inbound is
converted to the canonical external state in ``services/external/state.py``
before it reaches any logic.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

#: The spec's task states. The canonical internal vocabulary
#: (``ExternalTaskState``) uses the lowercase short forms; these are the wire
#: constants, and the adapter maps between them in one place.
A2ATaskState = Literal[
    "TASK_STATE_SUBMITTED",
    "TASK_STATE_WORKING",
    "TASK_STATE_INPUT_REQUIRED",
    "TASK_STATE_AUTH_REQUIRED",
    "TASK_STATE_COMPLETED",
    "TASK_STATE_FAILED",
    "TASK_STATE_CANCELED",
    "TASK_STATE_REJECTED",
]


class Part(BaseModel):
    """Exactly one of text / data / url, per the spec."""

    kind: Literal["text", "data", "url"]
    text: Optional[str] = None
    data: Optional[Any] = None
    url: Optional[str] = None


class Artifact(BaseModel):
    """A task output composed of parts."""

    artifactId: Optional[str] = None
    name: Optional[str] = None
    parts: List[Part] = Field(default_factory=list)


class Message(BaseModel):
    """A message on a task. ``role`` is the spec's, not a chat role."""

    role: Literal["user", "agent"] = "user"
    parts: List[Part] = Field(default_factory=list)
    messageId: Optional[str] = None
    taskId: Optional[str] = None


class TaskStatus(BaseModel):
    state: A2ATaskState
    #: When a task is INPUT_REQUIRED this carries the question, which is how an
    #: A2A caller discovers what a paused run wants without a second call.
    message: Optional[Message] = None
    timestamp: Optional[str] = None


class Task(BaseModel):
    id: str
    contextId: Optional[str] = None
    status: TaskStatus
    artifacts: List[Artifact] = Field(default_factory=list)


class AgentProvider(BaseModel):
    organization: str
    url: Optional[str] = None


class AgentSkill(BaseModel):
    """One capability the agent offers.

    Projected from ``CrewPublication`` — the SAME rows the MCP tool list renders
    from, so the two surfaces cannot advertise different capabilities.
    """

    id: str
    name: str
    description: str
    tags: List[str] = Field(default_factory=list)
    inputModes: List[str] = Field(default_factory=lambda: ["text"])
    outputModes: List[str] = Field(default_factory=lambda: ["text"])
    #: JSON Schema for the skill's inputs, when the crew declares one.
    inputSchema: Optional[Dict[str, Any]] = None


class AgentCapabilities(BaseModel):
    """What the agent can do.

    A capability flag is a PROMISE. Advertising streaming that does not work is
    worse than advertising ``false``, so these are flipped only once the
    behaviour is real — ``pushNotifications`` stays false until delivery exists.
    """

    streaming: bool = False
    pushNotifications: bool = False
    stateTransitionHistory: bool = False


class AgentInterface(BaseModel):
    url: str
    transport: str = "HTTP+JSON"


class SecurityScheme(BaseModel):
    """How a caller authenticates.

    Kasal runs external work on the caller's own Databricks token (OBO), so the
    card advertises an OAuth2/bearer scheme and the caller is expected to
    present one. A caller that does not gets TASK_STATE_AUTH_REQUIRED.
    """

    type: str = "http"
    scheme: str = "bearer"
    description: Optional[str] = None


class AgentCard(BaseModel):
    """The discovery document, served at /.well-known/agent.json."""

    protocolVersion: str
    name: str
    description: str
    version: str
    provider: Optional[AgentProvider] = None
    capabilities: AgentCapabilities = Field(default_factory=AgentCapabilities)
    securitySchemes: Dict[str, SecurityScheme] = Field(default_factory=dict)
    security: List[Dict[str, List[str]]] = Field(default_factory=list)
    interfaces: List[AgentInterface] = Field(default_factory=list)
    defaultInputModes: List[str] = Field(default_factory=lambda: ["text"])
    defaultOutputModes: List[str] = Field(default_factory=lambda: ["text"])
    skills: List[AgentSkill] = Field(default_factory=list)


class SendMessageRequest(BaseModel):
    message: Message
    #: Which published capability to run. A2A callers pick a skill from the card.
    skillId: Optional[str] = None
    taskId: Optional[str] = None
