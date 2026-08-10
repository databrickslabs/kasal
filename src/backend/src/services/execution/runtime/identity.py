"""What makes a unit of work "the same unit" as the one in a checkpoint.

A resume replays stored output for work it believes has not changed. Deciding
that needs a content hash of everything about the work that could change what
it produces — and, critically, the SAME hash computed the same way on both
sides, when the checkpoint is written and when it is read back. Two hashes over
two representations of "the same" task drift, and the failure mode is silent:
stale output fed downstream while the run looks fine.

This lives in ``runtime/`` rather than beside the checkpointing code because
only the runtime holds the built ``Task`` and ``Agent`` objects. The checkpoint
recorder (``agent_builder/checkpoint_adapter.py``), the flow builder's
skip decision (``flow_builder/checkpoint_identity.py``) and the crew's own
prefix validation (``crew.py::_load_checkpoint``) all call in here, so all
three agree by construction.

It stays pure — no session, no repository, no ``GroupContext`` — like the rest
of ``runtime/``. It hashes objects it is handed and nothing else.

**What is covered, and why that set.** ``Task.key`` alone (description and
expected output) was the original identity, and it misses the two edits people
most often make while tuning: swapping the agent's model, and changing which
tools a task may use. Both change the output; neither moved the hash, so both
were silently replayed from a checkpoint.

The set is bounded from the other side too. It covers what an agent can DO —
role, model, tools — and not the prose describing who it is. See
:func:`agent_fingerprint`: hashing goal and backstory made regenerating a crew
invalidate every checkpoint it had, because regeneration re-words them while
the work stays the same. An identity that dies on every save protects nothing.
"""

import hashlib
import logging
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

# Joined with a byte that cannot appear in a role, goal, model or tool name, so
# "a" + "bc" and "ab" + "c" cannot collide into the same identity.
_SEP = "\x00"


def _tool_names(holder: Any) -> str:
    """The tool names available to ``holder``, order-insensitively.

    Sorted because tool ORDER does not change what a task can do, while the
    SET does — leaving them unsorted would invalidate checkpoints every time
    an unrelated reordering happened upstream.
    """
    tools = getattr(holder, "tools", None) or []
    names = []
    for tool in tools:
        name = getattr(tool, "name", None)
        names.append(str(name if name is not None else tool))
    return ",".join(sorted(names))


def agent_fingerprint(agent: Any) -> str:
    """What about an agent invalidates a checkpoint, and no more.

    Role, model and tools — deliberately NOT goal or backstory, though
    ``Agent.key`` hashes all three.

    Including them was measurably too strict. Regenerating a crew re-words
    goals and backstories while keeping the same roles and work ("Specialized
    linguistic *executor*" becoming "Specialized linguistic *agent*"), and that
    invalidated every task the agent ran — a resume that could have replayed
    five completed tasks re-ran all of them. Prose about who the agent IS moves
    constantly; what it can DO is role, model and tools.

    The trade is real and one-directional: a re-worded goal that genuinely
    changes behaviour is now replayed rather than re-run. Rewind explicitly
    from the dialog when that is what happened. It is the better failure — a
    checkpoint that survives ordinary editing is worth having, and one that
    dies on every save is not.
    """
    if agent is None:
        return ""

    parts = [str(getattr(agent, "role", "") or "")]

    llm = getattr(agent, "llm", None)
    model = getattr(llm, "model", None) if llm is not None else None
    if model is None:
        # Some paths carry the model as a plain string rather than an LLM.
        model = llm if isinstance(llm, str) else None
    parts.append(str(model or ""))
    parts.append(_tool_names(agent))

    return "|".join(parts)


def task_identity(task: Any) -> Optional[str]:
    """Content hash of one task, or None when it cannot be computed.

    Returns:
        A hex digest, or None when the task has no content key. None means
        "cannot verify", and callers must treat that as *not* a match rather
        than as a pass — an identity that silently degrades to "everything
        matches" is worse than having none at all.
    """
    key = getattr(task, "key", None)
    if not key:
        logger.debug("Task %r has no content key — identity unavailable", task)
        return None

    parts = [
        str(key),
        agent_fingerprint(getattr(task, "agent", None)),
        # The task's own tool restriction, which narrows the agent's set.
        _tool_names(task),
    ]
    return hashlib.md5(_SEP.join(parts).encode(), usedforsecurity=False).hexdigest()


def crew_identity(
    crew_name: Optional[str], tasks: Optional[Iterable[Any]]
) -> Optional[str]:
    """Content hash of a whole crew, or None when it cannot be computed.

    Args:
        crew_name: Part of the identity because renaming a crew changes which
            stored output it matches.
        tasks: The crew's ``Task`` objects, IN ORDER. Order matters: the same
            tasks run in a different sequence produce different work.

    Returns:
        A hex digest, or None when there are no tasks or any task is
        unverifiable — one task without a content key makes the whole crew
        unverifiable, which is better said than hashed around.
    """
    task_list = list(tasks or [])
    if not task_list:
        return None

    parts = [str(crew_name or "")]
    for task in task_list:
        identity = task_identity(task)
        if identity is None:
            logger.debug(
                "Crew %r has a task with no content key — identity unavailable",
                crew_name,
            )
            return None
        parts.append(identity)

    return hashlib.md5(_SEP.join(parts).encode(), usedforsecurity=False).hexdigest()


def content_key(description: Any, expected_output: Any) -> str:
    """The text half of a task's identity, from raw strings.

    Deliberately takes strings rather than a task, because its whole purpose is
    to be computable from BOTH sides of a comparison the full identity cannot
    span: a built runtime ``Task`` inside a run, and a saved definition read out
    of the database by a reader that is not running anything.

    That is why the checkpoint stores it alongside ``identity``. ``identity``
    hashes tool objects and a resolved LLM, which only exist once a crew has
    been built — so a UI asking "will task 4 be restored?" cannot compute it,
    and comparing a hash of tool NAMES against a hash of tool IDs would answer
    "changed" for everything.

    It is strictly weaker: re-modelling or re-tooling a task leaves the content
    key alone. Run time still catches those through ``identity``, so a reader
    using this must present it as "at least these will re-run", never as a
    guarantee of what survives.

    Matches ``Task.key`` exactly (``runtime/task.py``) — the same two fields in
    the same order with the same separator. Kept in step by
    ``test_content_key_matches_task_key``.
    """
    source = f"{description}|{expected_output}"
    return hashlib.md5(source.encode(), usedforsecurity=False).hexdigest()


def crew_content_key(
    crew_name: Optional[str], tasks: Optional[Iterable[Any]]
) -> Optional[str]:
    """The text half of a whole crew's identity — the flow-unit counterpart.

    Same weakness and same purpose as :func:`content_key`: a reader holding the
    saved flow definition can recompute this, and cannot recompute
    :func:`crew_identity`.
    """
    task_list = list(tasks or [])
    if not task_list:
        return None

    parts = [str(crew_name or "")]
    for task in task_list:
        key = getattr(task, "key", None)
        if not key:
            return None
        parts.append(str(key))

    return hashlib.md5(_SEP.join(parts).encode(), usedforsecurity=False).hexdigest()


def legacy_task_identity(task: Any) -> Optional[str]:
    """The identity this module replaced: ``Task.key``, and nothing else.

    Checkpoints written before tools and model joined the hash stored this
    value. Comparing them against the current identity would mismatch every
    one of them and re-run work that has not changed — making existing
    checkpoints worthless the moment this shipped, for exactly the workflow
    they exist to protect. So a stored identity matching EITHER form is
    accepted, and the digests cannot be confused for one another: they are
    hashes of different inputs, so a legacy match is a legacy value.
    """
    key = getattr(task, "key", None)
    return str(key) if key else None
