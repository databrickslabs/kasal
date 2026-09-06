"""Chat kickoff, shared preambles, and graceful budget exhaustion."""

import asyncio
import time
from types import SimpleNamespace

from src.core.llm.effort import resolve_effort
from src.core.llm.transport.exceptions import ExecutionBudgetExceededError
from src.core.llm.transport.request_deadline import run_deadline


async def kickoff_chat_turn(
    service,
    agent,
    config,
    execution_id,
    trace_context,
    group_context,
    group_id,
    prompt,
    agent_spec,
    conversation_preamble,
    _agent_memory,
    _log,
):
    raw = (getattr(config, "inputs", None) or {}).get(
        "execution_effort"
    ) or agent_spec.get("execution_effort")
    if raw:
        budget = resolve_effort(raw)
        seconds = min(
            budget["run_max_seconds"],
            getattr(agent, "max_execution_time", None) or budget["max_execution_time"],
        )
        # Chat has no Crew.kickoff: stamp the same fixed point on its lone agent.
        deadline = time.monotonic() + seconds
        object.__setattr__(agent, "run_deadline", deadline)
        object.__setattr__(agent, "_kasal_run_deadline", deadline)
    try:
        with run_deadline(seconds if raw else None):
            return await _kickoff(
                service,
                agent,
                config,
                execution_id,
                trace_context,
                group_context,
                group_id,
                prompt,
                agent_spec,
                conversation_preamble,
                _agent_memory,
                _log,
            )
    except ExecutionBudgetExceededError as error:
        if not raw:
            raise
        _log(f"Effort limit reached: {error}")
        partial = getattr(error, "partial", "") or ""
        note = "Execution limit reached. These are the results available so far; increase Effort to allow more work."
        return SimpleNamespace(
            raw=f"{partial}\n\n{note}".strip(), budget_exhausted=True
        )


async def _kickoff(
    service,
    agent,
    config,
    execution_id,
    trace_context,
    group_context,
    group_id,
    prompt,
    agent_spec,
    conversation_preamble,
    _agent_memory,
    _log,
):
    # ── Memory recall — Kasal's engine Agent does not consult
    # memory itself, so recall here and prepend a capped context
    # block. One embedding + one search (no LLM calls);
    # best-effort. The CrewAI engine's agent is the exception:
    # there the attach step really does set ``agent.memory``
    # (a CrewAI Agent has the field; Kasal's pydantic Agent
    # rejects it), and its kickoff recalls on its own — running
    # the preamble too re-read the same store moments later
    # (two "Memory Read" rows in the trace) and injected the
    # same context twice.
    memory_block = ""
    _agent_recalls_itself = (
        _agent_memory is not None and getattr(agent, "memory", None) is _agent_memory
    )
    if _agent_recalls_itself:
        _log(
            "Memory recall left to the agent (CrewAI engine consults memory during kickoff)"
        )
    if _agent_memory is not None and not _agent_recalls_itself:
        from src.services.memory.run.recall import build_memory_preamble

        memory_block = await asyncio.to_thread(
            build_memory_preamble, _agent_memory, prompt
        )
        if memory_block:
            _log("Memory recall: context block injected")
    # Attaching a file binds the knowledge tool and scopes it to
    # that file, but nothing told the agent the file EXISTS — so
    # it answered "please share or upload the report" holding the
    # tool that would have read it. Last of the preamble parts,
    # closest to the user's message.
    from src.services.chat.attachment_hint import (
        build_attachment_hint,
    )

    attachment_hint = build_attachment_hint(agent_spec)
    if attachment_hint:
        _log("Attached files noted for the agent")
    preamble_parts = [
        part
        for part in (
            memory_block,
            conversation_preamble,
            attachment_hint,
        )
        if part
    ]
    kickoff_prompt = (
        "\n\n".join(preamble_parts) + f"\n\nCurrent message:\n{prompt}"
        if preamble_parts
        else prompt
    )
    return await service._kickoff_with_mlflow_trace(
        agent,
        kickoff_prompt,
        config,
        execution_id,
        trace_context,
        group_context,
        group_id,
    )
