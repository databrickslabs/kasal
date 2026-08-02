"""Not paying twice for work a conversation has already done.

A conversational flow re-runs its graph every turn. That is correct for the
crew that ANSWERS the turn — the question changed — and wasteful for everything
upstream of it: three crews spent minutes gathering material on turn 1, and
turn 2 asked a follow-up about that same material and gathered it all again.

The outputs were never lost. Each crew already writes its result into state
under its own name (``state['agentic ai frameworks']``), and a conversational
flow restores that state at the start of every turn. So by the time turn 2
begins, the answers are sitting in memory and the flow runs the crews anyway,
because nothing tells it not to.

What decides
============

**The crews that ANSWER always run.** Those are the terminal crews — nothing
listens to them, so their output is the turn's answer — and the crew this turn
SELECTED, which may be an intermediate one when the caller asked for an
intermediate artefact. Reusing either returns turn 1's answer to turn 2's
question, the failure this whole feature exists to avoid. Everything else is
material, and material is reusable.

Only the terminal half was enforced at first, and the gap was not theoretical:
a flow whose selected crew fed four listeners counted as material, so narrowing
chose it and reuse skipped it, and the turn ran nothing. See
``crews_that_answer``.

**An edited crew always runs.** Reuse is keyed on a content hash of the crew —
its tasks, its agents' roles and goals, and the model — the same identity the
resume path uses. Change what a crew does and its stored output stops matching,
so it re-runs instead of silently replaying an answer produced by a different
crew. Without that check, editing a crew mid-conversation would be ignored.

**A refresh overrides everything.** ``refresh_outputs`` on the run config makes
the turn ignore every stored output. Some follow-up genuinely means "go and look
again", and no rule inferred from state can tell that turn from the others.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

#: Where a crew's content hash is remembered, beside its output. A dict rather
#: than one key per crew, so the state's channel list stays readable.
#:
#: The name must NOT start with an underscore. Typed state is a pydantic model,
#: and pydantic v2 treats a leading-underscore name as a PRIVATE attribute: the
#: assignment succeeds, ``getattr`` returns the value, and ``model_dump()``
#: leaves it out. State is persisted from ``model_dump()``, so the channel was
#: written on every turn, read back correctly in-process, and vanished the
#: moment the turn ended. Every later turn then found an output with no recorded
#: identity, refused to trust it, and re-ran the crew — reuse could never fire
#: even once, and nothing failed.
IDENTITY_CHANNEL = "kasal_crew_identities"


def terminal_crew_names(flow_config: Optional[Dict[str, Any]]) -> Set[str]:
    """Crews nothing else listens to — the ones that answer the turn.

    Delegates rather than re-deriving. This and outcome selection were computing
    the same property by two routes, which is a drift waiting to happen: the day
    they disagree, a crew is both "the answer to this turn" and "safe to reuse",
    and the turn returns the previous answer.
    """
    from src.services.flow_builder.conversation.outcomes import terminal_crews

    return terminal_crews(flow_config)


def selected_outcome(flow: Any) -> Optional[str]:
    """Which outcome THIS turn asked for, read from the FLOW, not from state.

    ``_narrow_to_outcome`` runs before ``kickoff_async``, and kickoff restores
    the checkpoint over the state — ``_restore_state`` writes back every stored
    channel, which is what makes a crew's output survive and also means the
    ``last_outcome`` channel is the PREVIOUS turn's value for the whole of this
    one. Reading the selection from there answered a different question than the
    one being asked: on a turn that selected the website crew it returned the
    frameworks crew, so reuse protected the wrong one and re-ran material it
    already had.
    """
    text = str(getattr(flow, "_kasal_selected_outcome", "") or "").strip()
    return text or None


def crews_that_answer(flow_config: Optional[Dict[str, Any]], flow: Any) -> Set[str]:
    """Crews whose output IS this turn's answer, so they must always run.

    Terminal crews, PLUS the outcome this turn selected — and the second half is
    the one that was missing.

    Observed: a conversational flow answered turn 1 and then answered nothing at
    all, for every turn after, while the trace showed a completed run. Selection
    picked the right crew every time ("this turn produces 'Agentic AI
    Frameworks', confidence 0.95") and reuse then skipped it ("Reusing … not
    running it again"), so the run executed nothing — one LLM call, and that was
    the surface composer. The answer in the database was turn 1's, carried
    forward, and the chat received only activity cards.

    The cause is that the two features were built against different definitions
    of "the crew that answers". Reuse protected only crews nothing listens to;
    that crew fed four listeners, so it counted as MATERIAL and material is
    reusable. But narrowing had just chosen it as the thing to produce — and a
    turn may legitimately ask for an intermediate artefact, which is exactly
    what happened. Both rules were satisfied and nothing ran.

    So the question is asked once, here, with the turn's choice included. Reuse
    stays what it is for — not paying twice for the material upstream of the
    answer — and can no longer swallow the answer itself.
    """
    protected = terminal_crew_names(flow_config)
    selected = selected_outcome(flow)
    return protected | {selected} if selected else protected


def record_identity(state: Any, crew_name: str, identity: Optional[str]) -> None:
    """Remember which crew produced the output just stored."""
    if not identity or not crew_name or state is None:
        return
    try:
        identities = dict(_identities(state))
        identities[crew_name] = identity
        state[IDENTITY_CHANNEL] = identities
    except Exception as exc:  # noqa: BLE001 — bookkeeping, never fatal
        logger.debug("[flow-reuse] could not record identity: %s", exc)


def _identities(state: Any) -> Dict[str, str]:
    try:
        stored = state[IDENTITY_CHANNEL] if IDENTITY_CHANNEL in state else None
    except Exception:  # noqa: BLE001
        stored = None
    return dict(stored) if isinstance(stored, dict) else {}


def reusable_output(
    state: Any,
    crew_name: str,
    identity: Optional[str],
    terminal: Set[str],
    refresh: bool = False,
) -> Optional[Any]:
    """This crew's stored answer, when it may be reused. Otherwise None.

    Returns None — meaning "run the crew" — in every doubtful case: no state, a
    terminal crew, a refresh, nothing stored, or a stored output whose crew has
    since been edited. Running again is only expensive; reusing wrongly is an
    answer to a question nobody asked.
    """
    if refresh or not crew_name or state is None:
        return None
    if crew_name in terminal:
        return None

    try:
        stored = state[crew_name] if crew_name in state else None
    except Exception:  # noqa: BLE001
        return None
    if stored is None:
        return None

    known = _identities(state).get(crew_name)
    if identity and known and known != identity:
        logger.info(
            "[flow-reuse] '%s' has changed since its stored output; re-running",
            crew_name,
        )
        return None
    if identity and not known:
        # Written before identities were recorded. Re-run rather than trust an
        # output whose provenance cannot be checked.
        return None

    return stored


def emit_reused(crew_name: str, output: Any) -> None:
    """Put a reused crew on the trace, marked as restored rather than run.

    Reuses the event a resumed flow already emits: the timeline shows the whole
    flow WITHOUT claiming the crew executed, which is the difference between a
    fast turn and a turn that appears to have done work it did not.
    """
    try:
        from src.core.events import event_bus
        from src.core.events.types import CrewCheckpointRestoredEvent

        event_bus.emit(
            None, CrewCheckpointRestoredEvent(crew_name=crew_name, output=output)
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("[flow-reuse] could not emit reuse event: %s", exc)


def reuse_enabled(state_config: Any, refresh: bool = False) -> bool:
    """Whether this run may reuse anything at all.

    Only a conversational flow. A one-shot run has no previous turn to reuse
    from, and a flow resumed from a crash already has its own skip machinery.
    """
    if refresh or not isinstance(state_config, dict):
        return False
    return bool(state_config.get("conversational"))


def describe(reused: List[str], ran: List[str]) -> str:
    """One log line a person can read to see what a turn actually cost."""
    return (
        f"reused {len(reused)} crew(s) {sorted(reused)}; "
        f"ran {len(ran)} crew(s) {sorted(ran)}"
    )
