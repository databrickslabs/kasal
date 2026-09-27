from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


def generate_uuid() -> str:
    return str(uuid4())


class Task(Base):
    """
    Task model representing a task in the system.
    Enhanced with group isolation for multi-group deployments.
    """

    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_uuid)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    agent_id: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("agents.id"), nullable=True
    )
    expected_output: Mapped[str] = mapped_column(String, nullable=False)
    tools: Mapped[Any] = mapped_column(JSON, default=list, nullable=False)
    tool_configs: Mapped[Any] = mapped_column(
        JSON, default=dict, nullable=True
    )  # User-specific tool configuration overrides
    async_execution: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)
    context: Mapped[Any] = mapped_column(JSON, default=list, nullable=True)
    config: Mapped[Any] = mapped_column(JSON, default=dict, nullable=True)

    # Multi-group fields
    group_id: Mapped[Optional[str]] = mapped_column(
        String(100), index=True, nullable=True
    )  # Group isolation
    created_by_email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )  # Creator email for audit

    # Output configuration
    output_json: Mapped[Optional[str]] = mapped_column(String)
    output_pydantic: Mapped[Optional[str]] = mapped_column(String)
    output_file: Mapped[Optional[str]] = mapped_column(String)
    output: Mapped[Any] = mapped_column(JSON, nullable=True)
    markdown: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)

    # Advanced configuration
    callback: Mapped[Optional[str]] = mapped_column(String)
    callback_config: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # Configuration for callbacks like DatabricksVolume
    human_input: Mapped[bool] = mapped_column(Boolean, default=False, nullable=True)
    converter_cls: Mapped[Optional[str]] = mapped_column(String)
    guardrail: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Code-based guardrail (function name)
    llm_guardrail: Mapped[Any] = mapped_column(
        JSON, nullable=True
    )  # LLM-based guardrail configuration

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )

    def __init__(self, **kwargs: Any) -> None:
        # Store the explicitly provided kwargs before calling super
        explicit_kwargs = set(kwargs.keys())

        # Extract condition if present (it's not a column, but should be in config)
        condition = kwargs.pop("condition", None)

        super(Task, self).__init__(**kwargs)
        if self.id is None:
            self.id = generate_uuid()
        if self.tools is None:
            self.tools = []
        if self.context is None:
            self.context = []
        if self.config is None:
            self.config = {}
        if self.async_execution is None:
            self.async_execution = False
        if self.markdown is None:
            self.markdown = False
        if self.human_input is None:
            self.human_input = False
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()

        # Ensure synchronization between config and dedicated fields
        # If output_pydantic is in config, update the dedicated field
        if (
            self.config
            and "output_pydantic" in self.config
            and self.config["output_pydantic"]
        ):
            self.output_pydantic = self.config["output_pydantic"]
        # If output_pydantic is set as a field but not in config, add it to config
        elif self.output_pydantic and (not self.config.get("output_pydantic")):
            self.config["output_pydantic"] = self.output_pydantic

        # Same for other config values that have dedicated fields
        if self.config and "output_json" in self.config and self.config["output_json"]:
            self.output_json = self.config["output_json"]
        elif self.output_json and (not self.config.get("output_json")):
            self.config["output_json"] = self.output_json

        if self.config and "output_file" in self.config and self.config["output_file"]:
            self.output_file = self.config["output_file"]
        elif self.output_file and (not self.config.get("output_file")):
            self.config["output_file"] = self.output_file

        if self.config and "callback" in self.config and self.config["callback"]:
            self.callback = self.config["callback"]
        elif self.callback and (not self.config.get("callback")):
            self.config["callback"] = self.callback

        # Synchronize markdown field
        if (
            self.config
            and "markdown" in self.config
            and self.config["markdown"] is not None
        ):
            self.markdown = self.config["markdown"]
        elif "markdown" in explicit_kwargs and (self.config.get("markdown") is None):
            # Only add to config if markdown was explicitly provided
            self.config["markdown"] = self.markdown

        # Synchronize guardrail field (code-based guardrail)
        if self.config and "guardrail" in self.config and self.config["guardrail"]:
            self.guardrail = self.config["guardrail"]
        elif self.guardrail and (not self.config.get("guardrail")):
            self.config["guardrail"] = self.guardrail

        # Synchronize llm_guardrail field (LLM-based guardrail)
        # config → column: If llm_guardrail is explicitly in config, sync to column
        if self.config and "llm_guardrail" in self.config:
            self.llm_guardrail = self.config["llm_guardrail"]
        # Note: We do NOT sync column → config here. The column stores the
        # LLM-generated suggestion (set during crew generation). The config
        # stores the user's explicit choice (set via the UI toggle).
        # This ensures guardrails are disabled by default after generation.

        # Ensure condition is properly structured in config if present
        if condition is not None:
            # Note: self.config is guaranteed to be a dict at this point due to line 66
            self.config["condition"] = {
                "type": condition.get("type"),
                "parameters": condition.get("parameters", {}),
                "dependent_task": condition.get("dependent_task"),
            }
