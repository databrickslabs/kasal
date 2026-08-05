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
    thinking_budget_tokens: Optional[int] = Field(
        None,
        description=(
            "Thinking token budget for MANUAL-mode Anthropic models "
            "(Claude 4.1-4.6). Ignored by adaptive models, which reject a "
            "budget. Null uses the Kasal default when extended_thinking is on."
        ),
    )
    reasoning_effort: Optional[str] = Field(
        None,
        description=(
            "Thinking depth for ADAPTIVE-mode Anthropic models (Claude "
            "4.7+/5/Fable): low | medium | high. Ignored by manual models, whose "
            "depth is the budget. Null uses the endpoint's own default."
        ),
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
    # Derived for the same reason: which Anthropic thinking control this model
    # takes is a property of the model, and the two are mutually exclusive —
    # sending a budget to an adaptive model, or `enabled` to one that requires
    # `adaptive`, is a hard 400 on a real run. Computed from the transport's own
    # model lists so the UI cannot offer a control the request would be rejected
    # for, and cannot drift from the code that builds the request.
    #   "manual"   -> show a Thinking Budget (tokens) field
    #   "adaptive" -> show a Thinking Effort field
    #   None       -> show neither; this model has no thinking surface
    thinking_mode: Optional[str] = Field(
        None, description="Anthropic thinking surface: 'manual', 'adaptive' or null"
    )
    # The effort values THIS model accepts, in increasing depth. Shipped rather
    # than hardcoded in the frontend because there are FIVE distinct scales
    # across the catalogue and a wrong value is a 400, not a warning: Anthropic
    # adaptive takes low..max; gpt-5 takes minimal..high but rejects "none";
    # gpt-5-1 takes "none" but rejects "minimal"; the 5-2/5-4/5-6 line adds
    # "xhigh"; Gemini takes only low/medium/high. Empty = no effort control.
    allowed_efforts: List[str] = Field(
        default_factory=list, description="Effort values this model accepts"
    )
    # Sampling parameters this endpoint REJECTS. The UI must hide a control for
    # each: the catalogue declared nothing for any model, which is why Edit Model
    # offered `temperature` on claude-opus-5 — a model that answers it with a 400.
    refused_params: List[str] = Field(
        default_factory=list, description="Sampling parameters this model rejects"
    )
    # Whether the model's thinking TEXT can be displayed at all. False does not
    # mean it does not reason: every gpt-5* reasons and bills for it, and simply
    # never returns the trace over chat completions.
    returns_thinking_text: bool = Field(
        False, description="Thinking text is retrievable for this model"
    )

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="after")
    def _derive_reasoning_support(self) -> "ModelConfigResponse":
        from src.core.llm.model_capabilities import model_capability
        from src.core.llm.transport.completion import thinking_mode
        from src.utils.model_config import model_supports_reasoning_effort

        self.supports_reasoning_effort = model_supports_reasoning_effort(self.key)
        # Keyed off `name` (the SERVED model) with the catalogue key as a
        # fallback: the key is a Kasal alias and can differ from what the
        # endpoint is actually running (e.g. databricks-glm-5-2 -> system.ai.*).
        self.thinking_mode = thinking_mode(self.name) or thinking_mode(self.key)
        capability = model_capability(self.name) or model_capability(self.key)
        if capability:
            self.allowed_efforts = list(capability.efforts)
            self.refused_params = list(capability.refuses)
            self.returns_thinking_text = capability.returns_text
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
