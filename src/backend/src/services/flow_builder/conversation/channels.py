"""How a write to a flow-state channel merges with what is already there.

A flow state without reducers can only overwrite. That is fine for
``topic`` — the newest value IS the value — and useless for anything that has
to accumulate: a conversation's ``messages``, findings collected by successive
crews, a counter. Overwriting is why a multi-turn flow keeps only the newest
turn.

So each declared channel carries a merge policy, and every writer goes through
it. The four here cover everything the existing flows do; the set is
deliberately small, because a reducer is evaluated on state a user cannot see
and an exotic one is a debugging problem nobody wants.

``replace`` is the default, which is what makes this additive: a flow that
declares no reducers behaves exactly as it does today.
"""

import logging
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

REPLACE = "replace"
APPEND = "append"
MERGE = "merge"
ADD = "add"


def _replace(current: Any, incoming: Any) -> Any:
    """The newest write wins. The default, and right for every scalar."""
    return incoming


def _append(current: Any, incoming: Any) -> Any:
    """Concatenate onto a list.

    A non-list incoming value is appended as one item rather than rejected —
    ``state["messages"] = one_message`` is the natural thing to write from a
    node, and failing it would push every author to remember the brackets.
    """
    base = list(current) if isinstance(current, (list, tuple)) else []
    if isinstance(incoming, (list, tuple)):
        base.extend(incoming)
    elif incoming is not None:
        base.append(incoming)
    return base


def _merge(current: Any, incoming: Any) -> Any:
    """Shallow dict merge, incoming keys winning.

    Shallow on purpose: a deep merge has to decide what happens to lists nested
    inside dicts, and every answer to that surprises somebody.
    """
    if not isinstance(incoming, dict):
        # Nothing sensible to merge — treat it as a replacement rather than
        # dropping the write.
        return incoming
    base = dict(current) if isinstance(current, dict) else {}
    base.update(incoming)
    return base


def _add(current: Any, incoming: Any) -> Any:
    """Numeric sum, for counters."""
    try:
        return (current or 0) + (incoming or 0)
    except TypeError:
        logger.warning(
            "[flow-state] channel with reducer 'add' got non-numeric %r; replacing",
            type(incoming).__name__,
        )
        return incoming


REDUCERS: Dict[str, Callable[[Any, Any], Any]] = {
    REPLACE: _replace,
    APPEND: _append,
    MERGE: _merge,
    ADD: _add,
}


def normalize_reducer(name: Any) -> str:
    """The reducer a declaration names, or ``replace`` when it names nothing usable.

    An unknown name falls back rather than raising: a schema is authored data,
    and a typo in it must not make the flow unrunnable. It is logged, because a
    silently-ignored reducer would look exactly like a broken one.
    """
    if not name:
        return REPLACE
    key = str(name).strip().lower()
    if key in REDUCERS:
        return key
    logger.warning(
        "[flow-state] unknown reducer %r; using '%s'. Known: %s",
        name,
        REPLACE,
        sorted(REDUCERS),
    )
    return REPLACE


def apply_reducer(reducer: str, current: Any, incoming: Any) -> Any:
    """Merge one write into one channel."""
    return REDUCERS.get(reducer, _replace)(current, incoming)
