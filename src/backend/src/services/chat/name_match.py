"""Resolve a saved crew or flow by the name a user typed in chat.

The run / load / delete slash commands all resolve a name the same way, so the
rule lives here once: a case-insensitive substring match, exact names preferred,
and duplicates of one name resolved to the most recently updated copy.
"""

from datetime import datetime
from typing import List, Optional, Protocol, Sequence, Tuple, TypeVar


class Named(Protocol):
    """A saved catalog item: a crew or a flow."""

    @property
    def name(self) -> Optional[str]: ...

    @property
    def created_at(self) -> datetime: ...

    @property
    def updated_at(self) -> Optional[datetime]: ...


N = TypeVar("N", bound=Named)


def _lower_name(item: Named) -> str:
    # A crew's name column is nullable; an unnamed row matches nothing typed.
    return (item.name or "").lower()


def match_by_name(items: Sequence[N], name: str) -> Tuple[Optional[N], List[N]]:
    """Return ``(chosen, matches)`` for ``name``.

    ``chosen`` is the single match or, when every match carries the same name
    (duplicates), the most recently updated one — so ``len(matches) > 1`` with a
    ``chosen`` means "most recent of several". ``chosen`` is None when nothing
    matches (``matches`` empty) or the matches are ambiguous.
    """
    wanted = name.lower()
    matches = [item for item in items if wanted in _lower_name(item)]
    # Prioritize exact name matches to avoid infinite loops when multiple items
    # share the same name.
    exact_matches = [item for item in matches if _lower_name(item) == wanted]
    if exact_matches:
        matches = exact_matches
    if len(matches) == 1:
        return matches[0], matches
    if len(matches) > 1 and len({_lower_name(item) for item in matches}) == 1:
        newest = sorted(
            matches,
            key=lambda item: item.updated_at or item.created_at,
            reverse=True,
        )[0]
        return newest, matches
    return None, matches
