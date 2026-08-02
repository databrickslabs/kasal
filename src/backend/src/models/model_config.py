from datetime import datetime, timezone

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, Integer, String

from src.db.base import Base


class ModelConfig(Base):
    """
    ModelConfig model for storing LLM configurations.
    Enhanced with group isolation for multi-tenant deployments.
    """

    id = Column(Integer, primary_key=True)
    key = Column(
        String, nullable=False
    )  # Removed unique=True to allow same key for different groups
    name = Column(String, nullable=False)
    provider = Column(String)
    temperature = Column(Float)
    context_window = Column(Integer)
    max_output_tokens = Column(Integer)
    extended_thinking = Column(Boolean, default=False)
    enabled = Column(Boolean, default=True)

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
    params = Column(JSON, nullable=True)

    #: Parameter names this endpoint REFUSES, e.g. ``["temperature", "stop"]``.
    #:
    #: Replaces asking the model's NAME whether it accepts something. The
    #: substring tests this supersedes lived in three files and disagreed:
    #: ``model_rejects_temperature`` (utils/model_config.py),
    #: ``supports_stop_words`` (the transport) and two separate ``is_gpt5``
    #: checks in the manager. There is no litellm ``drop_params`` net on this
    #: path — a param that is set IS sent — so being wrong here is a 400, and
    #: the answer belongs beside the model it describes.
    unsupported_params = Column(JSON, nullable=True)

    # Multi-tenant fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
