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

# Injected once by agent.configure_conversation() — keeps this layer crew-agnostic.
_CFG: Dict[str, Any] = {
    "name": "agent",
    "input_key": "topic",
    "purpose": "",
    "llm_factory": None,  # Callable[[], LLM]
    "crew_runner": None,  # Callable[[str], str] — takes the user's request text
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
) -> None:
    _CFG.update(
        name=name,
        input_key=input_key,
        purpose=purpose,
        llm_factory=llm_factory,
        crew_runner=crew_runner,
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
    """RUN if the conversation clearly gives the crew its input, else GATHER."""
    agent = _agent(
        role=f"{_CFG['name']} intake assistant",
        goal="Decide whether there is enough information to run the crew.",
        backstory=(
            "You gather the input the crew needs before running it, and answer "
            "questions about what it can do."
        ),
    )
    task = (
        f"The crew's purpose: {_CFG['purpose']}\n"
        f"It needs an input called '{_CFG['input_key']}'.\n"
        f"Conversation so far: {history}\n"
        f"Latest user message: '{message}'\n\n"
        "Answer with exactly RUN only if the conversation gives a CLEAR and "
        "UNAMBIGUOUS input the crew can act on. If the request is a greeting, a "
        "question about capabilities, missing the input, or AMBIGUOUS / could mean "
        "several things, answer with exactly GATHER."
    )
    out = (agent.kickoff(task).raw or "").strip().upper()
    return "RUN" if "RUN" in out else "GATHER"


def _gather(message: str, history: List[dict]) -> str:
    """Answer / ask one concise clarifying question (limited to the crew's purpose)."""
    agent = _agent(
        role=f"{_CFG['name']} assistant",
        goal=f"Help the user and gather what's needed to run: {_CFG['purpose']}",
        backstory=(
            "You are a helpful assistant limited to this crew's purpose. You explain "
            "what you can do, and when the request is unclear or missing details you "
            "ask one concise, specific clarifying question rather than guessing. "
            "You do not attempt tasks outside the crew's scope."
        ),
    )
    return (
        agent.kickoff(
            f"Crew purpose: {_CFG['purpose']}\n"
            f"Needed input: '{_CFG['input_key']}'\n"
            f"Conversation: {history}\n"
            f"User: '{message}'\n"
            "Reply helpfully. If the input is missing OR ambiguous, ask one specific "
            "clarifying question so the user can tell you exactly what they want."
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


@mlflow.trace(name="conversation_turn")
def respond(message: str, conversation_id: Optional[str] = None) -> str:
    """Run one conversational turn and return the assistant's reply.

    Traced with ``@mlflow.trace`` so every turn (including pure gather/clarify
    turns that don't run the crew) is written to the experiment; nested crew/LLM
    calls attach as child spans via ``mlflow.crewai.autolog()``.
    """
    history = list(_HISTORY.get(conversation_id, [])) if conversation_id else []
    try:
        decision = _classify(message, history)
        response = _run(message, history) if decision == "RUN" else _gather(message, history)
    except Exception as exc:  # noqa: BLE001
        # Never fail the request — fall back to running the crew directly.
        print(f"Conversation layer error ({exc}); running crew directly.")
        try:
            response = _CFG["crew_runner"](message)
        except Exception as exc2:  # noqa: BLE001
            response = f"Sorry, something went wrong: {exc2}"

    if conversation_id is not None:
        history.append({"role": "user", "content": message})
        history.append({"role": "assistant", "content": response})
        _HISTORY[conversation_id] = history[-20:]
    return response
