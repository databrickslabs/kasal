"""What makes a flow crew "the same crew" as the one in a checkpoint.

Resuming a flow replays the stored output of every crew before the resume
point. Without a check on what those crews now ARE, editing one of them is
silently ignored: the crew is skipped, its old output is fed downstream, and
the run looks like it worked. That is the failure this module exists to stop.

The hash itself now lives in ``services/execution/runtime/identity.py``, shared
with the crew path — what remains here is the flow's VERDICT vocabulary
(match / changed / unverified), which is a flow-builder decision about whether
a crew may be skipped, not a hashing concern.

It is computed from runtime ``Task`` objects, which is what makes it reliable:
the SAME objects and the SAME function are used when recording a completed crew
and when deciding whether a crew may be skipped on resume, so the two sides
cannot drift apart by computing "the same" hash from two different
representations. That is also why the move happened — once crews needed the
identity too, a second implementation would have been a third representation.

Flow crews originally stored only the crew's NAME, which does not change when
you rewrite what the crew does.
"""

import logging
from typing import Any, Iterable, Optional

from src.services.execution.runtime.identity import agent_fingerprint, crew_identity

logger = logging.getLogger(__name__)

# Re-exported so a flow call site does not have to know where the hash moved.
__all__ = [
    "CHANGED",
    "MATCH",
    "UNVERIFIED",
    "agent_fingerprint",
    "compute_crew_identity",
    "verify_crew_identity",
]


def compute_crew_identity(
    crew_name: Optional[str], tasks: Optional[Iterable[Any]]
) -> Optional[str]:
    """Content hash of a flow crew, or None when it cannot be computed.

    Thin alias over ``runtime.identity.crew_identity``. The hash moved into the
    runtime once CREWS needed the same one — the recorder, the flow's skip
    decision and the crew runtime's own prefix check must agree by
    construction, and three modules computing "the same" hash is how they stop
    agreeing.

    Args:
        crew_name: The crew's name — part of the identity because renaming a
            crew changes which stored output it matches.
        tasks: The crew's runtime ``Task`` objects, IN ORDER.

    Returns:
        A hex digest, or None if it cannot be taken. None means "cannot
        verify", and callers must treat that as *not* a match rather than as a
        pass.
    """
    return crew_identity(crew_name, tasks)


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
