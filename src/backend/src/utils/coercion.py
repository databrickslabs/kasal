"""Turning configuration values into the types code can rely on.

Configuration reaches this codebase from three places that all lie: a JSON
column, an environment variable, and a text field somebody typed into. Every
consumer ends up writing the same guard, and the tree has seven copies of it —
``tool_policies``, ``scrape_website``, ``search_guard``, ``sweep``,
``powerbi_field_parameters_calculation_groups_tool``, ``flow_eval_context`` and
(until this module) ``llm/manager``. Five of those seven do not catch
``OverflowError``, so a value of ``Infinity`` raises out of them.

Kept deliberately small and dependency-free: ``utils`` sits below services, so
anything here must be importable from anywhere without dragging a session, a
model or a client along with it.
"""

from typing import Any, Optional


def positive_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """``value`` as an int greater than zero, else ``default``.

    Rejects, rather than raises, on everything configuration actually contains:
    ``None``, an empty string, prose, a negative or zero count, and infinity.

    ``OverflowError`` is the one worth naming. JSON has no infinity, but
    Python's decoder accepts the literal ``Infinity`` and hands back a float
    that ``int()`` refuses — a case that reached two separate call sites here
    within a day of each other, each time as a crash in a code path whose whole
    job was to tolerate bad input.
    """
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if number > 0 else default
