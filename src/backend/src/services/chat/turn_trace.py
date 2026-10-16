"""One chat kickoff inside an MLflow root trace, in the teamspace's own experiment.

Chat runs in the server process, beside every other teamspace's turns, so its
MLflow setup is the PARENT-process one the dispatcher and the builder use
(``mlflow_parent_setup.configure_parent_mlflow_tracing``): memoized per
teamspace, its OAuth and ``set_experiment`` work on a worker thread, and no
process-wide root-trace autolog. The crew subprocess's setup, which chat used
to run every turn, is written for a process that holds a single run: it makes
blocking calls and changes process-wide MLflow state.

Where the trace goes:

- On Databricks Apps the parent setup binds the teamspace's trace destination
  to the turn's own context, so concurrent turns each write to their own.
- Elsewhere the binding is process-wide. The root opens only while it is this
  teamspace's and no other setup is in flight, checked in the same synchronous
  step that opens it (a trace's experiment is fixed when its root starts).
  Otherwise the turn runs untraced rather than in another teamspace's
  experiment.

The trace carries the root span — the prompt sent, the answer, the chat session
— plus any span opened beneath it; the per-call LLM spans the global autolog
used to add are gone with it.

Fully best-effort: an MLflow problem falls back to a plain kickoff, and
``kickoff_async`` runs exactly once (a real kickoff error propagates).
"""

import logging
from typing import Optional, Protocol

from src.core.databricks_app import is_databricks_app
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

_PROMPT_CAP = 20000

Prompt = str | list[dict[str, str]]


class _Agent(Protocol):
    async def kickoff_async(self, messages: Prompt) -> object:
        """Run the agent's turn."""


async def _tracing_ready(group_context: Optional[GroupContext]) -> bool:
    from src.db.session import routed_scoped_session
    from src.services.mlflow.mlflow_parent_setup import (
        configure_parent_mlflow_tracing,
    )

    async with routed_scoped_session() as session:
        return await configure_parent_mlflow_tracing(
            session, group_context, label="Chat"
        )


def _destination_is_ours(group_context: Optional[GroupContext]) -> bool:
    if is_databricks_app():
        return True  # bound to this turn's context by the setup
    from src.services.mlflow.mlflow_parent_setup import owns_process_binding

    return owns_process_binding(group_context)


def _trace_inputs(
    kickoff_prompt: Prompt, run_name: Optional[str], trace_context: str
) -> dict[str, object]:
    # The ACTUAL prompt sent to the model (conversation preamble + the current
    # message), not the one-line task label. Capped so a long conversation
    # does not bloat the trace.
    prompt = kickoff_prompt if isinstance(kickoff_prompt, str) else str(kickoff_prompt)
    if len(prompt) > _PROMPT_CAP:
        prompt = prompt[:_PROMPT_CAP] + "…[truncated]"
    return {"run_name": run_name or trace_context, "prompt": prompt}


def _tag_session(config: object, group_context: Optional[GroupContext]) -> None:
    """CHAT-ONLY: group a conversation's turns into one MLflow session.

    ``session_id`` rides on the config or inside its inputs depending on the
    entry path; read both. Crew/flow deliberately do not set this.
    """
    session_id = getattr(config, "session_id", None) or (
        getattr(config, "inputs", None) or {}
    ).get("session_id")
    if not session_id:
        return
    try:
        import mlflow

        metadata = {"mlflow.trace.session": str(session_id)}
        user = getattr(group_context, "group_email", None)
        if user:
            metadata["mlflow.trace.user"] = str(user)
        mlflow.update_current_trace(metadata=metadata)
    except Exception:  # noqa: BLE001 — a missing session tag must not fail the turn
        logger.debug("[light_agent] mlflow session tag skipped", exc_info=True)


async def kickoff_with_mlflow_trace(
    agent: _Agent,
    kickoff_prompt: Prompt,
    config: object,
    execution_id: str,
    trace_context: str,
    group_context: Optional[GroupContext],
) -> object:
    """Run ``agent.kickoff_async`` inside this teamspace's MLflow root trace."""

    async def _do() -> object:
        # Cooperative stop: POST /executions/{id}/stop sets this event and the
        # transport round loop checks it before every LLM round, so Stop
        # reaches an in-process chat run mid-flight. contextvars flow through
        # kickoff_async's asyncio.to_thread.
        from src.core.execution_stop import (
            bind_stop_event,
            discard_stop_event,
            reset_stop_event,
            stop_event_for,
        )

        stop_token = bind_stop_event(stop_event_for(execution_id))
        try:
            return await agent.kickoff_async(kickoff_prompt)
        finally:
            reset_stop_event(stop_token)
            discard_stop_event(execution_id)

    try:
        from src.services.mlflow.tracing import start_root_trace
        from src.services.otel_tracing.mlflow_setup import (
            extract_trace_outputs,
            set_trace_attributes,
        )

        ready = await _tracing_ready(group_context)
    except Exception as setup_err:  # noqa: BLE001 — tracing is best-effort
        logger.warning(
            "[light_agent] MLflow setup failed; running without trace: %s",
            setup_err,
            exc_info=True,
        )
        return await _do()
    if not ready:
        logger.info("[light_agent] MLflow tracing not ready — chat kickoff untraced")
        return await _do()

    inputs_cfg = getattr(config, "inputs", None) or {}
    run_name = inputs_cfg.get("run_name")
    flow_config = {
        "model": getattr(config, "model", None),
        "execution_type": "agent",
        "run_name": run_name,
    }
    inputs = _trace_inputs(kickoff_prompt, run_name, trace_context)
    # No await between this check and the root opening (see module docstring).
    if not _destination_is_ours(group_context):
        logger.info(
            "[light_agent] another teamspace holds the process-wide MLflow "
            "binding — chat kickoff untraced"
        )
        return await _do()
    with start_root_trace(f"chat_kickoff:{run_name or trace_context}", inputs) as root:
        _tag_session(config, group_context)
        try:
            set_trace_attributes(root, flow_config, logger, run_name=run_name)
        except Exception:  # noqa: BLE001 — tracing is best-effort
            logger.debug("[light_agent] trace attributes skipped", exc_info=True)
        result = await _do()
        try:
            outputs = extract_trace_outputs(result, logger)
            if outputs and root is not None and hasattr(root, "set_outputs"):
                root.set_outputs(outputs)
        except Exception:  # noqa: BLE001 — tracing is best-effort
            logger.debug("[light_agent] trace outputs skipped", exc_info=True)
        return result
