"""Workspace opt-in only. API keys use the existing API key endpoints."""

from pydantic import BaseModel, ConfigDict, Field


class DecisionConfigUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool


class DecisionConfigResponse(BaseModel):
    enabled: bool = False
    api_key_configured: bool = False
    # Enabled, keyed and a provider URL set: the chat model selector offers Auto.
    available: bool = False
    # The deployment's connection ("jev", "openrouter" or "databricks") and the
    # workspace key it needs (JEV_API_KEY or OPENROUTER_API_KEY; None for
    # Databricks, which uses the workspace's own Databricks auth).
    # ``api_key_configured`` is about that credential: for Databricks, whether
    # the workspace's host and token resolve.
    connection: str = "jev"
    api_key_name: str | None = "JEV_API_KEY"


class DecisionRecommendationRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=12000)


class DecisionRecommendationResponse(BaseModel):
    model: str | None = None
    effort: str | None = None
