"""Workspace opt-in only. API keys use the existing API key endpoints."""

from pydantic import BaseModel, ConfigDict, Field


class DecisionConfigUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool


class DecisionConfigResponse(BaseModel):
    enabled: bool = False
    api_key_configured: bool = False


class DecisionRecommendationRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=12000)


class DecisionRecommendationResponse(BaseModel):
    model: str | None = None
    effort: str | None = None
