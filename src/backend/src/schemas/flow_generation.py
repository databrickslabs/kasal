"""A constrained plan: the model chooses crew IDs and logic, never executable code."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.flow import Edge, Node


class FlowGenerationRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=12000, pattern=r"\S")
    model: str = Field(default="databricks-gpt-5-3-codex", min_length=1, max_length=255)
    current_crew_ids: list[str] = Field(default_factory=list, max_length=24)


class RouteCondition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    field: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_]{0,79}$")
    operator: Literal["==", "!=", ">", ">=", "<", "<=", "contains"]
    value: str | float | bool


class CrewLink(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source: str
    target: str
    join: Literal["ALL", "ANY"] = "ALL"
    condition: RouteCondition | None = None
    otherwise: bool = False


class CrewFlowPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    explanation: str = Field(min_length=1, max_length=6000)
    crew_ids: list[str] = Field(default_factory=list, max_length=24)
    links: list[CrewLink] = Field(default_factory=list, max_length=64)
    missing_capabilities: list[str] = Field(default_factory=list, max_length=20)


class FlowGenerationResponse(BaseModel):
    name: str
    message: str
    nodes: list[Node] = Field(default_factory=list)
    edges: list[Edge] = Field(default_factory=list)
    missing_capabilities: list[str] = Field(default_factory=list)
