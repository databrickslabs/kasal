from datetime import datetime
from typing import Any, Optional

from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class ModelConfig(Base):
    """
    ModelConfig model for storing LLM configurations.
    Enhanced with group isolation for multi-tenant deployments.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(
        String, nullable=False
    )  # Removed unique=True to allow same key for different groups
    name: Mapped[str] = mapped_column(String, nullable=False)
    provider: Mapped[Optional[str]] = mapped_column(String)
    temperature: Mapped[Optional[float]] = mapped_column(Float)
    context_window: Mapped[Optional[int]] = mapped_column(Integer)
    max_output_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    extended_thinking: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )
    #: Thinking depth, for the two Anthropic modes. Which one applies is a
    #: property of the model, not a choice — `transport.thinking_mode()` decides,
    #: and sending the wrong one is a 400:
    #:   * "manual" models (Claude 4.1–4.6) use `thinking_budget_tokens`
    #:   * "adaptive" models (Claude 4.7+/5/Fable) use `reasoning_effort`
    #: Both NULL means "on with the endpoint's own default" once
    #: `extended_thinking` is set. An agent may override either per run.
    thinking_budget_tokens: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    reasoning_effort: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=True)

    #: Sampling parameters sent with every request to this model.
    #:
    #: Until this existed, the only two knobs Kasal could express were
    #: ``temperature`` and ``max_output_tokens`` — so the transport's ``top_p``,
    #: ``frequency_penalty``, ``presence_penalty`` and ``stop`` fields were
    #: declared, forwarded on every request, and had ZERO assignment sites in
    #: the codebase. Anything else needed code, per model, in a handler.
    #:
    #: Keys are sent as-is, so an OpenAI-standard name goes top level
    #: (``{"top_p": 0.8}``) and a provider-only knob goes under ``extra_body``
    #: (``{"extra_body": {"repetition_penalty": 1.05, "top_k": 20}}``) — the
    #: OpenAI SDK strips unknown top-level kwargs client-side, so vLLM's extra
    #: samplers are reachable no other way.
    #:
    #: Empty by default, and deliberately so: every value here changes what the
    #: model does, and a default applied to models nobody tested it on is how
    #: you fix one task and break another. Measured, not assumed —
    #: ``frequency_penalty=0.3`` cured a repeating list and simultaneously turned
    #: a 12-row markdown table from 681 characters into 9679 and a truncation.
    params: Mapped[Any] = mapped_column(JSON, nullable=True)

    #: Parameter names this endpoint REFUSES, e.g. ``["temperature", "stop"]``.
    #:
    #: Replaces asking the model's NAME whether it accepts something. The
    #: substring tests this supersedes lived in three files and disagreed:
    #: ``model_rejects_temperature`` (utils/model_config.py),
    #: ``supports_stop_words`` (the transport) and two separate ``is_gpt5``
    #: checks in the manager. There is no litellm ``drop_params`` net on this
    #: path — a param that is set IS sent — so being wrong here is a 400, and
    #: the answer belongs beside the model it describes.
    unsupported_params: Mapped[Any] = mapped_column(JSON, nullable=True)

    # Multi-tenant fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    created_by_email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Creator email for audit

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
