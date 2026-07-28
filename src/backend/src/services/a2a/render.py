"""Translation between Kasal's canonical external shapes and A2A's wire types.

The ONLY place A2A names appear alongside Kasal ones. Everything else in the
adapter works in canonical terms, so there is one table to check when the spec
moves — not a scattering of string literals.
"""

from typing import Optional

from src.schemas.a2a import Artifact, Message, Part, Task, TaskStatus
from src.services.external.artifacts import Artifact as CanonicalArtifact
from src.services.external.invocation import InvocationResult
from src.services.external.state import ExternalTaskState

#: canonical -> wire. The canonical vocabulary IS A2A's, just in short form
#: (see services/external/state.py for why it was adopted for both protocols),
#: so this is a naming map and nothing more.
_STATE_TO_WIRE = {
    ExternalTaskState.SUBMITTED: "TASK_STATE_SUBMITTED",
    ExternalTaskState.WORKING: "TASK_STATE_WORKING",
    ExternalTaskState.INPUT_REQUIRED: "TASK_STATE_INPUT_REQUIRED",
    ExternalTaskState.AUTH_REQUIRED: "TASK_STATE_AUTH_REQUIRED",
    ExternalTaskState.COMPLETED: "TASK_STATE_COMPLETED",
    ExternalTaskState.FAILED: "TASK_STATE_FAILED",
    ExternalTaskState.CANCELED: "TASK_STATE_CANCELED",
    ExternalTaskState.REJECTED: "TASK_STATE_REJECTED",
}


def to_wire_state(state: ExternalTaskState) -> str:
    """Canonical state -> the spec's constant."""
    return _STATE_TO_WIRE[state]


def to_parts(artifact: CanonicalArtifact) -> list:
    """Canonical parts -> A2A ``Part``s.

    The kinds already match — the canonical artifact borrowed A2A's vocabulary
    because MCP had none to lend — so this only reshapes the field layout.
    """
    parts = []
    for part in artifact.parts:
        if part.kind == "text":
            parts.append(Part(kind="text", text=str(part.content)))
        elif part.kind == "url":
            parts.append(Part(kind="url", url=str(part.content)))
        else:
            parts.append(Part(kind="data", data=part.content))
    return parts


def to_task(
    result: InvocationResult,
    canonical_artifact: Optional[CanonicalArtifact] = None,
    prompt: Optional[str] = None,
) -> Task:
    """An invocation result -> the ``Task`` an A2A caller polls.

    ``prompt`` is the pending question when a run has paused. It rides in
    ``status.message`` rather than in an artifact because that is where the spec
    puts a task's current message, and it is what makes INPUT_REQUIRED
    actionable: the caller sees WHAT is being asked without a second request.
    """
    status = TaskStatus(state=to_wire_state(result.state))

    if prompt:
        status.message = Message(
            role="agent", parts=[Part(kind="text", text=prompt)], taskId=result.run_id
        )
    elif result.error:
        status.message = Message(
            role="agent",
            parts=[Part(kind="text", text=result.error)],
            taskId=result.run_id,
        )

    artifacts = []
    if canonical_artifact is not None and canonical_artifact.parts:
        artifacts.append(
            Artifact(
                artifactId=f"{result.run_id}-output", parts=to_parts(canonical_artifact)
            )
        )

    return Task(id=result.run_id, status=status, artifacts=artifacts)


def text_of(message: Message) -> str:
    """The text a caller sent, joined.

    A2A messages are multi-part; a crew takes one prompt. Joining is lossy for
    binary parts, which is honest — those are not something a crew input can
    carry today, and silently dropping them would look like the text was all
    that was sent.
    """
    return "\n".join(p.text for p in message.parts if p.kind == "text" and p.text)
