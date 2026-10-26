"""A2AAgentTool — delegate work to a remote A2A agent.

The outbound half, as an agent sees it. A Kasal agent calls this the way it
calls any other tool; underneath, the work runs on someone else's agent and
comes back as text.

Two decisions worth stating, because both were the alternative to something
more elaborate:

- **The tool polls; the protocol client does not.** How long a delegation may
  take is a property of the delegating crew, and the remote's own budget is
  none of Kasal's business. So the wait lives here, bounded by the timeout the
  operator configured on the remote.
- **A remote that pauses for input returns its question as the tool result**,
  along with its task id, rather than raising an approval into Kasal's HITL
  machinery. The calling agent is already an agent: handing it the question and
  letting it answer with ``task_id`` is a loop it can drive itself, and it does
  not stall a crew behind a human who never asked to be involved. Kasal's own
  HITL still fires normally when the LOCAL run needs approval to make the call.
"""

import logging
import time
from typing import Any, List, Optional

from pydantic import BaseModel, Field

from src.services.external.state import ExternalTaskState, is_terminal

from .async_bridge import run_async_with_context
from .base import BaseTool

logger = logging.getLogger(__name__)

#: How often to ask the remote for progress. A remote crew takes minutes, so a
#: tighter interval buys nothing and costs a request per second per delegation.
POLL_INTERVAL_SECONDS = 3.0


class A2AAgentToolSchema(BaseModel):
    request: str = Field(
        ...,
        description="What you want the remote agent to do, in plain language.",
    )
    skill_id: Optional[str] = Field(
        default=None,
        description=(
            "Which of the remote's skills to use. Omit to let the remote choose."
        ),
    )
    task_id: Optional[str] = Field(
        default=None,
        description=(
            "Only when continuing a remote task that asked you a question: pass "
            "the task id it returned, and put your answer in 'request'."
        ),
    )


class A2AAgentTool(BaseTool):
    """Calls one configured remote agent.

    Bound to a single remote at construction rather than taking an agent name as
    an argument: the tool's description then names the actual skills available,
    which is what the calling model selects on. A generic "call any agent" tool
    would make the model guess a name it has never seen.
    """

    name: str = "Delegate to remote agent"
    description: str = "Delegate a task to a remote A2A agent."
    args_schema: type[BaseModel] = A2AAgentToolSchema

    agent_name: str = ""
    interface_url: str = ""
    api_key: Optional[str] = None
    user_token: Optional[str] = None
    timeout_seconds: int = 300
    skills: List[dict] = Field(default_factory=list)

    def __init__(
        self,
        agent_name: str = "",
        interface_url: str = "",
        api_key: Optional[str] = None,
        user_token: Optional[str] = None,
        timeout_seconds: int = 300,
        skills: Optional[List[dict]] = None,
        description: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.agent_name = agent_name
        self.interface_url = interface_url
        self.api_key = api_key
        self.user_token = user_token
        self.timeout_seconds = timeout_seconds or 300
        self.skills = skills or []
        if agent_name:
            self.name = f"Delegate to {agent_name}"
        self.description = description or self._describe()

    def _describe(self) -> str:
        base = (
            f"Delegate a task to '{self.agent_name}', a remote agent, and wait for "
            "its answer."
        )
        if not self.skills:
            return base
        listed = "; ".join(
            f"{s.get('id')}: {s.get('description') or s.get('name')}"
            for s in self.skills[:10]
        )
        return f"{base} Available skills — {listed}."

    def _run(self, **kwargs: Any) -> str:
        request = kwargs.get("request")
        if not request:
            return "Error: describe what the remote agent should do."
        if not self.interface_url:
            return f"Error: remote agent '{self.agent_name}' is not configured."

        try:
            return run_async_with_context(
                self._delegate(
                    str(request),
                    kwargs.get("skill_id"),
                    kwargs.get("task_id"),
                ),
                timeout=self.timeout_seconds + 30,
            )
        except Exception as exc:  # noqa: BLE001
            # Returned, not raised: a failed delegation is information the
            # calling agent can act on (try another skill, do it itself), and a
            # raise aborts the whole task instead.
            logger.warning("A2A delegation to %s failed: %s", self.agent_name, exc)
            return f"Error delegating to '{self.agent_name}': {exc}"

    async def _delegate(
        self, request: str, skill_id: Optional[str], task_id: Optional[str]
    ) -> str:
        from src.services.a2a.a2a_client import client as a2a_client

        task = await a2a_client.send_message(
            self.interface_url,
            request,
            skill_id=skill_id,
            task_id=task_id,
            api_key=self.api_key,
            token=self.user_token,
        )
        remote_task_id = task.get("id")
        state = a2a_client.from_wire_state((task.get("status") or {}).get("state"))

        # A task waiting on a human, or on credentials Kasal does not have, will
        # not move by being polled — waiting out the timeout on either is pure
        # delay before the same answer.
        _STOP = (ExternalTaskState.INPUT_REQUIRED, ExternalTaskState.AUTH_REQUIRED)

        deadline = time.monotonic() + self.timeout_seconds
        while not is_terminal(state) and state not in _STOP:
            if time.monotonic() > deadline:
                return (
                    f"'{self.agent_name}' is still working after "
                    f"{self.timeout_seconds}s (task {remote_task_id}). It was not "
                    "cancelled — ask again with this task_id to check on it."
                )
            import asyncio

            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            task = await a2a_client.get_task(
                self.interface_url,
                str(remote_task_id),
                api_key=self.api_key,
                token=self.user_token,
            )
            state = a2a_client.from_wire_state((task.get("status") or {}).get("state"))

        text = a2a_client.text_of_task(task)

        if state == ExternalTaskState.INPUT_REQUIRED:
            question = text or "(it did not say what it needs)"
            return (
                f"'{self.agent_name}' needs more information before it can finish "
                f"(task_id: {remote_task_id}). It asked: {question}\n\n"
                "Answer by calling this tool again with that task_id and your "
                "answer as the request."
            )
        if state == ExternalTaskState.AUTH_REQUIRED:
            return (
                f"'{self.agent_name}' rejected the credentials Kasal presented. "
                "A workspace admin needs to fix its configuration."
            )
        if state in (ExternalTaskState.FAILED, ExternalTaskState.REJECTED):
            return f"'{self.agent_name}' could not complete the task. {text}".strip()
        if state == ExternalTaskState.CANCELED:
            return f"'{self.agent_name}' cancelled the task."

        return text or f"'{self.agent_name}' finished but returned no output."
