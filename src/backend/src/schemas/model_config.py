from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ModelConfigBase(BaseModel):
    """Base schema with common model configuration attributes."""

    key: str = Field(..., description="Unique identifier for the model")
    name: str = Field(..., description="Display name of the model")
    provider: Optional[str] = Field(
        None, description="Provider of the model (e.g., 'openai', 'anthropic')"
    )
    temperature: Optional[float] = Field(
        None, description="Temperature setting for generation"
    )
    context_window: Optional[int] = Field(
        None, description="Maximum context window size in tokens"
    )
    max_output_tokens: Optional[int] = Field(
        None, description="Maximum output tokens allowed"
    )
    params: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "Sampling parameters sent with every request to this model. "
            "OpenAI-standard names go top level (top_p, frequency_penalty, "
            "presence_penalty, stop); a provider-only knob goes under "
            "extra_body (e.g. {'extra_body': {'repetition_penalty': 1.05}}), "
            "because the OpenAI SDK strips unknown top-level kwargs."
        ),
    )
    unsupported_params: Optional[List[str]] = Field(
        None,
        description=(
            "Parameter names this endpoint rejects, e.g. ['temperature']. "
            "Filtered out before the request is built — there is no drop_params "
            "safety net on this path, so sending one is a 400."
        ),
    )
    extended_thinking: Optional[bool] = Field(
        False, description="Whether extended thinking is enabled"
    )
    enabled: Optional[bool] = Field(True, description="Whether the model is enabled")


class ModelConfigCreate(ModelConfigBase):
    """Schema for creating a new model configuration."""

    pass


class ModelConfigUpdate(ModelConfigBase):
    """Schema for updating an existing model configuration."""

    pass


class ModelConfigResponse(ModelConfigBase):
    """Schema for model configuration responses."""

    id: int
    created_at: datetime
    updated_at: datetime
    # Derived, never stored: whether this model accepts a native reasoning
    # budget. The UI needs it to avoid offering a Reasoning Effort control that
    # the engine will silently discard — the allow-list lives in
    # src/utils/model_config.py and is computed here so the UI and the engine
    # can never disagree about which models it applies to.
    supports_reasoning_effort: bool = Field(
        False, description="Model accepts a native reasoning-effort budget"
    )

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="after")
    def _derive_reasoning_support(self) -> "ModelConfigResponse":
        from src.utils.model_config import model_supports_reasoning_effort

        self.supports_reasoning_effort = model_supports_reasoning_effort(self.key)
        return self


class ModelToggleUpdate(BaseModel):
    """Schema for toggling model enabled status."""

    enabled: bool = Field(..., description="New enabled status")


class ModelListResponse(BaseModel):
    """Schema for list of model configurations."""

    models: List[ModelConfigResponse]
    count: int
    # The server's default model, so clients stop hardcoding one. Every backend
    # default (models/agent.py, schemas/agent.py, schemas/crew.py, the engine
    # paths) derives from the SAME constant, and shipping it here means a UI
    # placeholder cannot drift from what the backend would actually pick.
    # Overridable per deployment via DEFAULT_LLM_MODEL.
    default_model: str = Field(
        default_factory=lambda: _default_engine_model(),
        description="Model the server falls back to when none is specified",
    )


def _default_engine_model() -> str:
    # Imported lazily: src.utils.model_config imports SQLAlchemy, and schemas are
    # loaded early enough that a module-level import here risks a cycle.
    from src.utils.model_config import DEFAULT_ENGINE_MODEL

    return DEFAULT_ENGINE_MODEL
