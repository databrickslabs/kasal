"""Honest parts of a Power BI Q&A answer, shared by Tools 72 and 80.

* ``attempt_lines``: the retry history, saying the run failed when no attempt
  succeeded (never "successful on attempt N" by position);
* ``unapplied_filter_line``: the report filters the answer does NOT respect;
* ``prompt_filter_lines``: the active-filter lines for the LLM prompt, plus
  the filters left out of it.
"""

from __future__ import annotations

from .report_filters import filter_to_dax, unapplied_reason


def attempt_lines(attempts: list[dict]) -> list[str]:
    """Retry history; empty for a single successful attempt."""
    if not attempts or (len(attempts) == 1 and attempts[0].get("success")):
        return []
    winner = next((a for a in attempts if a.get("success")), None)
    if winner:
        head = f"**Attempts**: {len(attempts)} (succeeded on attempt {winner['attempt']})\n"
    else:
        head = (
            f"**Attempts**: {len(attempts)}, all failed: the run did not produce "
            "an answer\n"
        )
    lines = [head, "\n### Retry History\n"]
    for att in attempts:
        if att.get("success"):
            lines.append(f"**Attempt {att['attempt']}**: ✅ Success")
            continue
        lines.append(f"**Attempt {att['attempt']}**: ❌ Failed")
        if att.get("error"):
            lines.append(f"  - Error: {str(att['error'])[:100]}...")
    lines.append("")
    return lines


def unapplied_filter_line(unapplied: list[tuple[str, str]]) -> list[str]:
    """``**Report filters not applied**: col (reason), ...`` or nothing."""
    if not unapplied:
        return []
    items = ", ".join(f"{col} ({reason})" for col, reason in unapplied)
    return [
        f"**Report filters not applied**: {items}. The numbers above do not "
        "respect these filters.\n"
    ]


def prompt_filter_lines(
    filters: dict, skipped: list[str] | None = None
) -> tuple[list[str], list[tuple[str, str]]]:
    """``(prompt lines, unapplied)`` for the active filters.

    ``skipped``: filter keys left out because their table is not in the model
    context. They are reported as not applied together with filters that have
    no exact value.
    """
    lines: list[str] = []
    unapplied = [(key, "table not in the model context") for key in skipped or []]
    for name, value in filters.items():
        key = name if "[" in name else f"CorrectTable[{name}]"
        dax_filter = filter_to_dax(key, value)
        if not dax_filter:
            unapplied.append((name, unapplied_reason(value)))
        elif "[" in name:
            lines.append(f"  {dax_filter}")
        else:  # unqualified key: the LLM has to find the right table
            lines.append(f"  [{name}] — use {dax_filter}")
    return lines, unapplied
