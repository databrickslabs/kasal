"""Pre-clean DAX text before pattern matching (comments, BLANK(), slicer scaffolding).

``preclean_dax`` is the first step of ``DaxTranslator.translate``: it strips
comments, normalises ``BLANK()`` and drops the date-picker ``var`` scaffolding
that the ratio measures carry. Pure string transform.
"""

from __future__ import annotations

import re


def preclean_dax(dax: str) -> str:
    """PROP-6: strip DAX comments and normalize BLANK() before translation.

    - Remove ``//`` and ``--`` line comments (DAX allows both); left in, an
      inline ``-- Denominator`` leaks into the emitted SQL and comments out the
      rest of the line.
    - Remove ``/* ... */`` block comments.
    - Replace standalone ``BLANK()`` with ``NULL`` (emitted literally it is not
      valid Spark SQL).
    """
    if not dax:
        return dax
    dax = re.sub(r"/\*.*?\*/", " ", dax, flags=re.DOTALL)
    cleaned_lines = []
    for line in dax.split("\n"):
        line = re.sub(r"//.*$", "", line)
        line = re.sub(r"--.*$", "", line)
        cleaned_lines.append(line)
    dax = "\n".join(cleaned_lines)
    # BLANK() → NULL, but ONLY when it's part of a larger expression. A measure
    # that is *only* BLANK() is a display placeholder — leave it for the
    # quick-reject matcher to drop (converting it to bare NULL would hide it).
    if dax.strip().upper() not in ("BLANK()", "BLANK ()"):
        dax = re.sub(r"\bBLANK\s*\(\s*\)", "NULL", dax, flags=re.IGNORECASE)

    # Strip slicer-scalar date-window scaffolding vars. Many real-world ratio
    # measures prefix the real formula with:
    #     var std = CALCULATE([F_Start_date])
    #     var etd = CALCULATE([F_End_date])
    # These reference dynamic date-picker measures ([F_Start_date] etc.) that
    # are NOT translatable and NOT used by the ratio aggregation — but left in,
    # they poison the LLM translation (leftover [F_Start_date] trips the
    # DAX-construct validator) and block the ~36 ratios underneath. Drop any
    # `var <n> = ...` line whose body references only these window pickers,
    # AND drop bare references to them elsewhere. Only touches measures that
    # actually contain the window-picker names, so normal DAX is untouched.
    if re.search(r"F_Start_date|F_End_date|PY_Start_date|PY_End_date", dax, re.I):
        _picker = re.compile(r"F_Start_date|F_End_date|PY_Start_date|PY_End_date", re.I)
        kept = []
        dropped_vars: set[str] = set()
        for line in dax.split("\n"):
            vm = re.match(r"\s*var\s+(\w+)\s*=\s*(.+)$", line, re.IGNORECASE)
            if (
                vm
                and _picker.search(vm.group(2))
                and not re.search(
                    r"\b(SUM|SUMX|COUNT|AVERAGE|MIN|MAX)\s*\(", vm.group(2), re.I
                )
            ):
                dropped_vars.add(vm.group(1))  # e.g. std, etd
                continue  # drop the scaffolding var line
            kept.append(line)
        dax = "\n".join(kept)
        # remove now-dangling references to the dropped vars (bare tokens)
        for v in dropped_vars:
            dax = re.sub(rf"(?<![\w.])\b{re.escape(v)}\b(?![\w.(])", "", dax)
    return dax
