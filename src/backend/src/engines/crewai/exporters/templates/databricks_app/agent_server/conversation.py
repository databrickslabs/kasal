"""Generic conversation layer for a deployed Kasal crew.

Every app Kasal deploys ships this same layer; only the injected crew specifics
(purpose, required input, LLM, crew runner) differ. It wraps the synthesized crew
in a conversational, info-gathering assistant — it chats with the user, explains
what the crew can do, gathers/clarifies the input the crew needs, and only runs
the crew once it has enough. Multi-turn history is kept per conversation id.

Each turn is a single deterministic pass: classify -> (gather|run) -> reply.
(Implemented with plain agent calls rather than a CrewAI Flow to avoid the Flow
event bus re-entering / looping inside the server.)
"""

from typing import Any, Callable, Dict, List, Optional

import mlflow
from crewai import Agent

from agent_server import cancel, progress

# Injected once by agent.configure_conversation() — keeps this layer crew-agnostic.
_CFG: Dict[str, Any] = {
    "name": "agent",
    "input_key": "topic",
    "purpose": "",
    "llm_factory": None,  # Callable[[], LLM]
    "crew_runner": None,  # Callable[[str], str] — runs the full crew (research/deep)
    "chat_runner": None,  # Callable[[str], str] — single LiteAgent answer (chat mode)
}

# Per-conversation history keyed by the AgentServer conversation id (in-process;
# resets on restart — fine for a chat session).
_HISTORY: Dict[str, List[dict]] = {}


def configure(
    *,
    name: str,
    input_key: str,
    purpose: str,
    llm_factory: Callable[[], Any],
    crew_runner: Callable[[str], str],
    chat_runner: Optional[Callable[[str], str]] = None,
) -> None:
    _CFG.update(
        name=name,
        input_key=input_key,
        purpose=purpose,
        llm_factory=llm_factory,
        crew_runner=crew_runner,
        chat_runner=chat_runner,
    )


def _agent(role: str, goal: str, backstory: str) -> Agent:
    return Agent(
        role=role,
        goal=goal,
        backstory=backstory,
        llm=_CFG["llm_factory"](),
        verbose=False,
    )


def _classify(message: str, history: List[dict]) -> str:
    """Route the latest message: RUN (in-scope + ready), DECLINE (out of scope), or GATHER."""
    agent = _agent(
        role=f"{_CFG['name']} intake assistant",
        goal="Decide whether to run the crew, decline, or gather more information.",
        backstory=(
            "You gather the input the crew needs before running it, answer questions "
            "about what it can do, and turn away requests outside the crew's purpose."
        ),
    )
    task = (
        f"The crew's purpose: {_CFG['purpose']}\n"
        f"It needs an input called '{_CFG['input_key']}'.\n"
        f"Conversation so far: {history}\n"
        f"Latest user message: '{message}'\n\n"
        "Classify the latest message into exactly ONE word:\n"
        "- DECLINE: it asks for something OUTSIDE the crew's purpose/domain (a "
        "different subject, or a task unrelated to that purpose).\n"
        "- RUN: it is IN scope AND gives a CLEAR, unambiguous input the crew can act on.\n"
        "- GATHER: it is in scope but a greeting, a capability question, or is missing "
        "the input / ambiguous / could mean several things.\n"
        "Answer with exactly one of: DECLINE, RUN, GATHER."
    )
    out = (agent.kickoff(task).raw or "").strip().upper()
    if "DECLINE" in out:
        return "DECLINE"
    return "RUN" if "RUN" in out else "GATHER"


def _gather(message: str, history: List[dict]) -> str:
    """Answer / ask one concise clarifying question (limited to the crew's purpose)."""
    agent = _agent(
        role=f"{_CFG['name']} assistant",
        goal=f"Help the user and gather what's needed to run: {_CFG['purpose']}",
        backstory=(
            "You are a helpful assistant limited to this crew's purpose. You explain "
            "what you can do, and when the request is unclear or missing details you "
            "ask one concise, specific clarifying question rather than guessing. You "
            "NEVER produce, generate, draft, or preview the deliverable itself while "
            "gathering — that only happens after the user confirms what they want. "
            "You do not attempt tasks outside the crew's scope."
        ),
    )
    return (
        agent.kickoff(
            f"Crew purpose: {_CFG['purpose']}\n"
            f"Needed input: '{_CFG['input_key']}'\n"
            f"Conversation: {history}\n"
            f"User: '{message}'\n"
            "The request does not yet give a clear, specific input to run on. Reply "
            "with ONE short, specific clarifying question (you may briefly note what "
            "you can do first). Do NOT generate, draft, or output any part of the "
            "deliverable (e.g. quiz questions, slides, a report) in this reply — only "
            "ask. The crew runs on the NEXT turn once the user answers."
        ).raw
        or ""
    )


def _decline(message: str, history: List[dict]) -> str:
    """Politely turn away an out-of-scope request and redirect to what the crew does."""
    agent = _agent(
        role=f"{_CFG['name']} assistant",
        goal=f"Politely decline out-of-scope requests and redirect to: {_CFG['purpose']}",
        backstory=(
            "You are strictly limited to this crew's purpose. When a request falls "
            "outside it, you briefly and politely say you can't help with that, then "
            "offer a concrete example of what you CAN do. You never attempt the "
            "out-of-scope task."
        ),
    )
    return (
        agent.kickoff(
            f"Crew purpose: {_CFG['purpose']}\n"
            f"User: '{message}'\n"
            "This request is OUTSIDE the crew's purpose. In 1-2 sentences, politely "
            "decline and give one concrete example of what you can help with instead. "
            "Do NOT attempt the requested task or produce any of its content."
        ).raw
        or ""
    )


def _run(message: str, history: List[dict]) -> str:
    """Run the crew (hierarchical supervisor) with recent context for continuity."""
    recent = history[-6:]
    context = "\n".join(f"{m['role']}: {m['content']}" for m in recent)
    request = f"{context}\nuser: {message}".strip() if context else message
    response = _CFG["crew_runner"](request) or ""
    # The supervisor returns 'CLARIFY: <question>' when it needs more info; show the
    # question to the user (their reply resumes the goal next turn).
    if response.strip().upper().startswith("CLARIFY:"):
        response = response.split(":", 1)[1].strip()
    return response


def _chat(message: str, history: List[dict]) -> str:
    """Chat mode: answer with a single LiteAgent (no crew), with recent context."""
    recent = history[-6:]
    context = "\n".join(f"{m['role']}: {m['content']}" for m in recent)
    request = f"{context}\nuser: {message}".strip() if context else message
    return _CFG["chat_runner"](request) or ""


def _tag_trace_session(session_id: Optional[str], user_id: Optional[str]) -> None:
    """Tag the active trace with the standard MLflow session/user metadata so the
    Databricks Traces UI groups a multi-turn chat into one session (and filters by
    user). Keys ``mlflow.trace.session`` / ``mlflow.trace.user`` work on all
    MLflow 3.x. Must run inside an active ``@mlflow.trace`` span; best-effort.
    """
    metadata = {}
    if session_id:
        metadata["mlflow.trace.session"] = str(session_id)
    if user_id:
        metadata["mlflow.trace.user"] = str(user_id)
    if not metadata:
        return
    try:
        mlflow.update_current_trace(metadata=metadata)
    except Exception as exc:  # noqa: BLE001 — never fail a turn over telemetry
        print(f"update_current_trace(session) failed: {exc}")


@mlflow.trace(name="conversation_turn")
def respond(
    message: str,
    conversation_id: Optional[str] = None,
    user_id: Optional[str] = None,
    mode: str = "research",
) -> str:
    """Run one conversational turn and return the assistant's reply.

    ``mode`` selects the depth:
      chat     -> a single LiteAgent answer (no crew, no intake classification);
      research -> classify, then gather more info or run the (fast) crew;
      deep     -> same flow as research but the crew uses deep tools + reasoning.

    Traced with ``@mlflow.trace`` so every turn (including pure gather/clarify
    turns that don't run the crew) is written to the experiment; nested crew/LLM
    calls attach as child spans via ``mlflow.crewai.autolog()``. The conversation
    id doubles as the MLflow session id so the whole chat groups together in the
    Traces UI.
    """
    _tag_trace_session(conversation_id, user_id)
    # Bind this thread to the conversation so the CrewAI event-bus listener
    # (crew_progress) can report live, ephemeral "doing X" status for this turn.
    progress.set_current(conversation_id)
    try:
        history = list(_HISTORY.get(conversation_id, [])) if conversation_id else []
        is_chat = mode == "chat" and _CFG.get("chat_runner") is not None
        try:
            if is_chat:
                # Quick single-agent answer; no crew intake/gather pipeline.
                response = _chat(message, history)
            else:
                decision = _classify(message, history)
                if decision == "RUN":
                    response = _run(message, history)
                elif decision == "DECLINE":
                    response = _decline(message, history)
                else:
                    response = _gather(message, history)
        except cancel.CrewCancelled:
            raise  # user Stop / timeout — abort the turn, don't retry
        except Exception as exc:  # noqa: BLE001
            # Never fail the request — fall back to the same-mode runner directly.
            print(f"Conversation layer error ({exc}); answering directly.")
            runner = _CFG["chat_runner"] if is_chat else _CFG["crew_runner"]
            try:
                response = runner(message)
            except cancel.CrewCancelled:
                raise
            except Exception as exc2:  # noqa: BLE001
                response = f"Sorry, something went wrong: {exc2}"

        if conversation_id is not None:
            history.append({"role": "user", "content": message})
            history.append({"role": "assistant", "content": response})
            _HISTORY[conversation_id] = history[-20:]
        return response
    except cancel.CrewCancelled:
        # Aborted mid-turn: return a clean message; don't pollute history.
        return "Stopped."
    finally:
        # Drop any cancel flag for this turn and clear the thread-local; keep the
        # last status in the store so the UI can show it through the (brief) A2UI
        # compose step. The handler clears it.
        cancel.clear(conversation_id)
        progress.clear_current()
