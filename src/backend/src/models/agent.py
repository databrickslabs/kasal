from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String

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

    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False)
    role = Column(String, nullable=False)
    goal = Column(String, nullable=False)
    backstory = Column(String)

    # Multi-group fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), nullable=True)  # Creator email for audit

    # Core configuration
    llm = Column(String, default=DEFAULT_ENGINE_MODEL)
    temperature = Column(
        Integer, nullable=True
    )  # Optional temperature override (0-100, will be converted to 0.0-1.0)
    #: Per-agent overrides of the model's thinking settings. NULL inherits the
    #: model row, the same contract as `temperature`. Which of the two applies is
    #: the MODEL's property, not a choice here — a budget belongs to Claude
    #: 4.1–4.6 and an effort level to 4.7+/5/Fable and the GPT-5/Gemini families,
    #: and `core.llm.model_capabilities` is what decides. Storing both means a
    #: model swap cannot invalidate a saved agent: the transport simply sends
    #: whichever the new model accepts.
    thinking_budget_tokens = Column(Integer, nullable=True)
    reasoning_effort = Column(String, nullable=True)
    #: Per-agent max OUTPUT tokens. NULL inherits the model row's
    #: `max_output_tokens`, the same contract as the overrides above. Applied to
    #: the agent's own LLM by kernel/agent_builder._apply_output_cap_override on
    #: whichever field that model takes (`max_tokens`, or `max_completion_tokens`
    #: for the GPT-5 family). Reasoning tokens count against it.
    max_tokens = Column(Integer, nullable=True)
    tools = Column(JSON, default=list, nullable=False)
    #: Agent Skills attached to this agent, BY NAME. Names rather than ids
    #: because a skill's name is its identity in the format — it must match the
    #: folder it exports to — so a name survives an export/import round trip and
    #: keeps working when a workspace overrides a builtin with its own version.
    skills = Column(JSON, default=list, nullable=True)
    tool_configs = Column(
        JSON, default=dict, nullable=True
    )  # User-specific tool configuration overrides
    function_calling_llm = Column(String)

    # Execution settings
    max_iter = Column(Integer, default=25)
    max_rpm = Column(Integer)
    max_execution_time = Column(Integer)
    verbose = Column(Boolean, default=False)
    allow_delegation = Column(Boolean, default=False)
    cache = Column(Boolean, default=True)

    # Memory settings
    memory = Column(Boolean, default=True)
    embedder_config = Column(JSON)

    # Templates
    system_template = Column(String)
    prompt_template = Column(String)
    response_template = Column(String)

    # Code execution settings
    allow_code_execution = Column(Boolean, default=False)
    code_execution_mode = Column(String, default="safe")

    # Additional settings
    max_retry_limit = Column(Integer, default=2)
    use_system_prompt = Column(Boolean, default=True)
    respect_context_window = Column(Boolean, default=True)

    # Knowledge sources
    knowledge_sources = Column(JSON, default=list)

    # Date awareness settings (CrewAI 1.9+)
    inject_date = Column(
        Boolean, default=True
    )  # Injects current date into agent's context (enabled by default)
    date_format = Column(
        String, nullable=True
    )  # Custom date format (e.g., '%B %d, %Y')

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __init__(self, **kwargs):
        super(Agent, self).__init__(**kwargs)
        if self.tools is None:
            self.tools = []
        if self.knowledge_sources is None:
            self.knowledge_sources = []
