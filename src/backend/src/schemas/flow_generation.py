"""A constrained plan: the model chooses crew IDs and logic, never executable code."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.flow import Edge, Node
from src.schemas.flow_output import FlowOutputContract
from src.utils.model_config import DEFAULT_ENGINE_MODEL


class FlowGenerationRequest(BaseModel):
    session_id: str | None = Field(default=None, max_length=255)
    prompt: str = Field(min_length=1, max_length=12000, pattern=r"\S")
    model: str = Field(default=DEFAULT_ENGINE_MODEL, min_length=1, max_length=255)
    tools: list[str] = Field(default_factory=list, max_length=100)
    mcp_servers: list[str] = Field(default_factory=list, max_length=24)
    current_crew_ids: list[str] = Field(default_factory=list, max_length=24)


class RouteCondition(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    field: str = Field(
        max_length=240,
        pattern=r"^[A-Za-z][A-Za-z0-9_]*(?:\[\])?(?:\.[A-Za-z][A-Za-z0-9_]*(?:\[\])?)*$",
    )
    operator: Literal["==", "!=", ">", ">=", "<", "<=", "contains"]
    value: str | float | bool


class RouteConditionGroup(BaseModel):
    model_config = ConfigDict(extra="forbid")
    subject: str = Field(
        default="",
        max_length=160,
        pattern=r"^(?:[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)*)?$",
    )
    terms: list[RouteCondition] = Field(min_length=1, max_length=12)
    connector: Literal["AND", "OR"] = "AND"


class CrewLink(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str
    target: str
    join: Literal["ALL", "ANY"] = "ALL"
    condition: RouteCondition | None = None
    condition_groups: list[RouteConditionGroup] = Field(
        default_factory=list, max_length=8
    )
    otherwise: bool = False


class CrewFlowPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    explanation: str = Field(min_length=1, max_length=6000)
    crew_ids: list[str] = Field(default_factory=list, max_length=24)
    output_contracts: list[FlowOutputContract] = Field(
        default_factory=list, max_length=24
    )
    links: list[CrewLink] = Field(default_factory=list, max_length=64)
    missing_capabilities: list[str] = Field(default_factory=list, max_length=20)


class FlowGenerationResponse(BaseModel):
    name: str
    message: str
    nodes: list[Node] = Field(default_factory=list)
    edges: list[Edge] = Field(default_factory=list)
    missing_capabilities: list[str] = Field(default_factory=list)
