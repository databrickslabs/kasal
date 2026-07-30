"""What makes a flow crew "the same crew" as the one in a checkpoint.

Resuming a flow replays the stored output of every crew before the resume
point. Without a check on what those crews now ARE, editing one of them is
silently ignored: the crew is skipped, its old output is fed downstream, and
the run looks like it worked. That is the failure this module exists to stop.

The identity is a content hash of everything about a crew that can change its
output — its tasks and the agents running them. It is computed from runtime
``Task`` objects, which is what makes it reliable: the SAME objects and the
SAME function are used when recording a completed crew and when deciding
whether a crew may be skipped on resume, so the two sides cannot drift apart
by computing "the same" hash from two different representations.

Crews (Agent Builder) have had this since the beginning — a task carries
``Task.key`` and the runtime refuses a checkpoint whose keys do not match.
Flow crews stored only the crew's NAME, which does not change when you rewrite
what the crew does.
"""

import hashlib
import logging
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

# Joined with a byte that cannot appear in a role, goal or model name, so
# "a" + "bc" and "ab" + "c" cannot collide into the same identity.
_SEP = "\x00"


def _agent_fingerprint(agent: Any) -> str:
    """Everything about an agent that changes what it produces.

    ``Agent.key`` covers role/goal/backstory but NOT the model — and swapping
    the model is one of the most common edits when tuning a crew, so relying on
    ``key`` alone would treat a re-modelled agent as unchanged.
    """
    if agent is None:
        return ""

    parts = [str(getattr(agent, "key", "") or "")]

    llm = getattr(agent, "llm", None)
    model = getattr(llm, "model", None) if llm is not None else None
    if model is None:
        # Some paths carry the model as a plain string rather than an LLM.
        model = llm if isinstance(llm, str) else None
    parts.append(str(model or ""))

    return "|".join(parts)


def compute_crew_identity(
    crew_name: Optional[str], tasks: Optional[Iterable[Any]]
) -> Optional[str]:
    """Content hash of a flow crew, or None when it cannot be computed.

    Args:
        crew_name: The crew's name — part of the identity because renaming a
            crew changes which stored output it matches.
        tasks: The crew's runtime ``Task`` objects, IN ORDER. Order matters:
            the same tasks run in a different sequence produce different work.

    Returns:
        A hex digest, or None if there are no tasks to hash. None means "cannot
        verify", and callers must treat that as *not* a match rather than as a
        pass — an identity that silently degrades to "everything matches" would
        be worse than having none at all.
    """
    task_list = list(tasks or [])
    if not task_list:
        return None

    parts = [str(crew_name or "")]
    for task in task_list:
        key = getattr(task, "key", None)
        if not key:
            # A task with no content key makes the whole crew unverifiable;
            # better to say so than to hash around the hole.
            logger.debug(
                "Crew %r has a task with no content key — identity unavailable",
                crew_name,
            )
            return None
        parts.append(str(key))
        parts.append(_agent_fingerprint(getattr(task, "agent", None)))

    return hashlib.md5(_SEP.join(parts).encode(), usedforsecurity=False).hexdigest()


# Outcomes of comparing a crew against its checkpoint. Three, not two: "cannot
# verify" is genuinely different from "changed", and collapsing them either
# re-runs work needlessly or hides a real edit.
MATCH = "match"  # verified identical — safe to skip
CHANGED = "changed"  # verified different — must re-run
UNVERIFIED = "unverified"  # no identity on one side — skipped, but say so


def verify_crew_identity(current: Optional[str], stored: Optional[str]) -> str:
    """Compare a crew against the one that produced a checkpoint.

    ``UNVERIFIED`` still permits the skip. Every checkpoint written before
    identities existed — and every trace-derived one, which can never have them
    — has no stored value, and refusing those would make existing checkpoints
    worthless overnight for the exact workflow this protects. So old
    checkpoints keep behaving as they always did; only the guarantee is absent,
    and the caller is told.
    """
    if not current or not stored:
        return UNVERIFIED
    return MATCH if current == stored else CHANGED
