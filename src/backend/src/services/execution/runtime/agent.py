"""BaseAgent and Agent.

Authored module; surface validated against the kasal_engine datamodel.
The agent executes through the shared executor (single LLM turn that drives
tool execution via available_functions, crewAI 1.x style) and emits
AgentExecutionCompleted / LiteAgentExecution* events on the engine bus.
Members excluded from the datamodel surface (include=false) — knowledge,
checkpoints, code execution, adapters — are not implemented.
"""

import asyncio
import hashlib
import logging
import uuid
from collections.abc import Callable
from typing import Any, Literal

from pydantic import UUID4, BaseModel, ConfigDict, Field

from src.services.execution.events.bus import event_bus
from src.services.execution.events.types import (
    AgentExecutionCompletedEvent,
    AgentExecutionStartedEvent,
    LiteAgentExecutionCompletedEvent,
    LiteAgentExecutionErrorEvent,
    LiteAgentExecutionStartedEvent,
)
from src.services.tools.base import BaseTool
from .executor import build_messages, run_agent, structured_from_raw, json_schema_instruction
from .types import LiteAgentOutput, PlanningConfig

logger = logging.getLogger(__name__)


class BaseAgent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: UUID4 = Field(default_factory=uuid.uuid4, frozen=True)
    role: str = Field(description="Role of the agent")
    goal: str = Field(description="Objective of the agent")
    backstory: str = Field(description="Backstory of the agent")
    config: dict[str, Any] | None = Field(default=None, exclude=True)
    cache: bool = True
    verbose: bool = False
    max_rpm: int | None = None
    allow_delegation: bool = False
    tools: list[BaseTool] | None = Field(default_factory=list)
    max_iter: int = 25
    crew: Any | None = Field(default=None, exclude=True)
    max_tokens: int | None = None
    callbacks: list[Any] = Field(default_factory=list)
    tools_results: list[dict[str, Any]] = Field(default_factory=list)

    @property
    def key(self) -> str:
        source = f"{self.role}|{self.goal}|{self.backstory}"
        return hashlib.md5(source.encode(), usedforsecurity=False).hexdigest()


class Agent(BaseAgent):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    llm: Any | None = Field(
        default=None, description="Language model that will run the agent."
    )
    function_calling_llm: Any | None = Field(
        default=None,
        description="Deprecated; accepted for compatibility and unused.",
    )
    max_execution_time: int | None = None
    step_callback: Callable[..., Any] | None = None
    use_system_prompt: bool | None = True
    system_template: str | None = None
    prompt_template: str | None = None
    response_template: str | None = None
    allow_code_execution: bool | None = Field(
        default=False,
        description="Deprecated in crewAI and unsupported in the engine; kasal hardcodes False.",
    )
    code_execution_mode: Literal["safe", "unsafe"] = "safe"
    respect_context_window: bool = True
    max_retry_limit: int = 2
    max_context_window_size: int | None = None
    inject_date: bool = False
    date_format: str = "%Y-%m-%d"
    planning: bool = False
    planning_config: PlanningConfig | None = None
    reasoning: bool = Field(
        default=False,
        description="Deprecated alias for planning; kasal passes planning_config instead.",
    )
    embedder: dict[str, Any] | None = None
    guardrail: Any | None = None
    guardrail_max_retries: int = 3

    def execute_task(
        self,
        task: Any,
        context: str | None = None,
        tools: list[BaseTool] | None = None,
    ) -> str:
        """Execute a task and return the raw text output."""
        prompt = task.prompt()
        if context:
            prompt += f"\n\nThis is the context you're working with:\n{context}"

        effective_tools = tools
        if effective_tools is None:
            effective_tools = task.tools or self.tools

        event_bus.emit(
            self,
            AgentExecutionStartedEvent(
                agent=self, task=task, tools=effective_tools, task_prompt=prompt
            ),
        )
        raw = run_agent(self, prompt, effective_tools, task=task)
        event_bus.emit(
            self, AgentExecutionCompletedEvent(agent=self, task=task, output=raw)
        )
        return raw

    def create_agent_executor(
        self, tools: list[BaseTool] | None = None, task: Any = None
    ) -> None:
        """Compatibility no-op: the engine builds execution state per call."""

    def kickoff(
        self,
        messages: str | list[dict[str, str]],
        response_format: type[Any] | None = None,
        input_files: dict[str, Any] | None = None,
        from_checkpoint: Any | None = None,
    ) -> LiteAgentOutput:
        """Standalone (lite) execution: run this agent directly on messages."""
        agent_info = {
            "id": str(self.id),
            "role": self.role,
            "goal": self.goal,
            "backstory": self.backstory,
        }
        if isinstance(messages, str):
            user_content = messages
            chat: list[dict[str, str]] = []
        else:
            chat = list(messages)
            user_content = chat[-1]["content"] if chat else ""

        event_bus.emit(
            self,
            LiteAgentExecutionStartedEvent(
                agent_info=agent_info, tools=self.tools, messages=messages
            ),
        )
        try:
            if response_format is not None:
                user_content += json_schema_instruction(response_format)
            built = build_messages(self, user_content)
            if chat:
                built = [built[0], *chat[:-1], {"role": "user", "content": user_content}]
            raw = run_agent(self, user_content, self.tools, messages=built)
            structured = (
                structured_from_raw(response_format, raw)
                if response_format is not None
                else None
            )
        except Exception as e:
            event_bus.emit(
                self,
                LiteAgentExecutionErrorEvent(agent_info=agent_info, error=str(e)),
            )
            raise
        event_bus.emit(
            self,
            LiteAgentExecutionCompletedEvent(agent_info=agent_info, output=raw),
        )
        return LiteAgentOutput(
            raw=raw, pydantic=structured, agent_role=self.role, messages=built
        )

    async def kickoff_async(
        self,
        messages: str | list[dict[str, str]],
        response_format: type[Any] | None = None,
        input_files: dict[str, Any] | None = None,
        from_checkpoint: Any | None = None,
    ) -> LiteAgentOutput:
        return await asyncio.to_thread(
            self.kickoff, messages, response_format, input_files, from_checkpoint
        )

    def message(self, content: str, **kwargs: Any) -> str:
        """Send a single message through a temporary one-task crew."""
        from .crew import Crew
        from .task import Task

        task = Task(
            description=content,
            expected_output="Respond to the user's message appropriately.",
            agent=self,
        )
        crew = Crew(agents=[self], tasks=[task], verbose=self.verbose)
        return crew.kickoff().raw
