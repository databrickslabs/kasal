"""Which Kasal process this is: the server, or a crew/flow run subprocess.

Several behaviours differ inside the spawned run interpreter — it writes traces
and status straight to the database instead of relaying through the parent,
streams tokens back over the result queue, and has no SSE clients of its own.
That used to be signalled with ``CREW_SUBPROCESS_MODE`` / ``FLOW_SUBPROCESS_MODE``
environment variables the child set on itself: a process-global string anyone
could set, that leaked across tests calling the entry points in-process, and
that every grandchild inherited whether or not it was a run.

This is the same fact as an explicit flag. The child marks itself first thing
(``subprocess_bootstrap.prepare_child_environment``); nothing else writes it.
"""

from typing import Optional

#: None in the server; "crew" or "flow" inside a run subprocess.
_ROLE: Optional[str] = None


def mark_run_subprocess(kind: str) -> None:
    """Record that this interpreter is a ``kind`` ("crew" / "flow") run."""
    global _ROLE
    _ROLE = kind


def reset() -> None:
    """Back to "the server" (tests that drive a child entry point in-process)."""
    global _ROLE
    _ROLE = None


def in_run_subprocess() -> bool:
    """True inside a crew OR flow run subprocess."""
    return _ROLE is not None


def in_flow_subprocess() -> bool:
    return _ROLE == "flow"
