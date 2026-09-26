from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base
from src.utils.model_config import DEFAULT_ENGINE_MODEL


def generate_uuid():
    return str(uuid4())


class Agent(Base):
    """
    Agent model representing an AI agent in the system.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "agents"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_uuid)
    name: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[str] = mapped_column(String, nullable=False)
    goal: Mapped[str] = mapped_column(String, nullable=False)
    backstory: Mapped[Optional[str]] = mapped_column(String)

    # Multi-group fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    created_by_email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Creator email for audit

    # Core configuration
    llm: Mapped[str] = mapped_column(
        String, default=DEFAULT_ENGINE_MODEL, nullable=True
    )
    temperature: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )  # Optional temperature override (0-100, will be converted to 0.0-1.0)
    #: Per-agent overrides of the model's thinking settings. NULL inherits the
    #: model row, the same contract as `temperature`. Which of the two applies is
    #: the MODEL's property, not a choice here — a budget belongs to Claude
    #: 4.1–4.6 and an effort level to 4.7+/5/Fable and the GPT-5/Gemini families,
    #: and `core.llm.model_capabilities` is what decides. Storing both means a
    #: model swap cannot invalidate a saved agent: the transport simply sends
    #: whichever the new model accepts.
    thinking_budget_tokens: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    reasoning_effort: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    execution_effort: Mapped[Any] = mapped_column(JSON, nullable=True)
    #: Per-agent max OUTPUT tokens. NULL inherits the model row's
    #: `max_output_tokens`, the same contract as the overrides above. Applied to
    #: the agent's own LLM by kernel/agent_builder._apply_output_cap_override on
    #: whichever field that model takes (`max_tokens`, or `max_completion_tokens`
    #: for the GPT-5 family). Reasoning tokens count against it.
    max_tokens: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tools: Mapped[Any] = mapped_column(JSON, default=list, nullable=False)
    #: Agent Skills attached to this agent, BY NAME. Names rather than ids
    #: because a skill's name is its identity in the format — it must match the
    #: folder it exports to — so a name survives an export/import round trip and
    #: keeps working when a workspace overrides a builtin with its own version.
    skills: Mapped[Any] = mapped_column(JSON, default=list, nullable=True)
    tool_configs: Mapped[Any] = mapped_column(
        JSON, default=dict, nullable=True
    )  # User-specific tool configuration overrides
    function_calling_llm: Mapped[Optional[str]] = mapped_column(String)

    # Execution settings
    max_iter: Mapped[int] = mapped_column(Integer, default=25, nullable=True)
    max_rpm: Mapped[Optional[int]] = mapped_column(Integer)
    max_execution_time: Mapped[Optional[int]] = mapped_column(Integer)
    verbose: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)
    allow_delegation: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )
    cache: Mapped[bool] = mapped_column(Boolean, default=True, nullable=True)

    # Memory settings
    memory: Mapped[bool] = mapped_column(Boolean, default=True, nullable=True)
    embedder_config: Mapped[Any] = mapped_column(JSON, nullable=True)

    # Templates
    system_template: Mapped[Optional[str]] = mapped_column(String)
    prompt_template: Mapped[Optional[str]] = mapped_column(String)
    response_template: Mapped[Optional[str]] = mapped_column(String)

    # Code execution settings
    allow_code_execution: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=True
    )
    code_execution_mode: Mapped[str] = mapped_column(
        String, default="safe", nullable=True
    )

    # Additional settings
    max_retry_limit: Mapped[int] = mapped_column(Integer, default=2, nullable=True)
    use_system_prompt: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )
    respect_context_window: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )

    # Knowledge sources
    knowledge_sources: Mapped[Any] = mapped_column(JSON, default=list, nullable=True)

    # Date awareness settings (CrewAI 1.9+)
    inject_date: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=True
    )  # Injects current date into agent's context (enabled by default)
    date_format: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Custom date format (e.g., '%B %d, %Y')

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def __init__(self, **kwargs):
        super(Agent, self).__init__(**kwargs)
        if self.tools is None:
            self.tools = []
        if self.knowledge_sources is None:
            self.knowledge_sources = []
