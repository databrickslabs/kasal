"""DAX variable-chain and DIVIDE argument helpers (pure string transforms).

``resolve_var_chain`` inlines simple scalar VAR assignments into the RETURN
expression; ``extract_divide_args`` locates the first DIVIDE(...) call and its
numerator/denominator.
"""

from __future__ import annotations

import re


def resolve_var_chain(dax: str) -> str:
    """Resolve chained VAR assignments up to 5 levels deep.

    For example:
        var a = SUM(t[x])
        var b = a + 1
        return b
    becomes:
        var b = SUM(t[x]) + 1
        return b

    Only resolves simple scalar assignments (not CALCULATE/SUMX blocks).
    """
    # Normalize single-line multi-var
    dax = re.sub(r"\s+(?=\bvar\s)", "\n", dax, flags=re.IGNORECASE)
    dax = re.sub(r"\s+(?=\breturn\s)", "\n", dax, flags=re.IGNORECASE)
    var_map: dict[str, str] = {}
    lines = dax.split("\n")
    for line in lines:
        m = re.match(r"\s*var\s+(\w+)\s*=\s*(.+)", line.strip(), re.IGNORECASE)
        if m:
            var_name = m.group(1)
            var_expr = m.group(2).strip()
            if var_expr.count("(") != var_expr.count(")"):
                continue
            if re.search(
                r"\b(CALCULATE|SUMX|FILTER|COUNTX|AVERAGEX)\s*\(",
                var_expr,
                re.IGNORECASE,
            ):
                continue
            for prev_var in sorted(var_map.keys(), key=len, reverse=True):
                var_expr = re.sub(
                    rf"\b{re.escape(prev_var)}\b", var_map[prev_var], var_expr
                )
            var_map[var_name] = var_expr
    if not var_map:
        return dax
    result_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.lower().startswith("return "):
            ret_expr = stripped[7:].strip()
            for var_name in sorted(var_map.keys(), key=len, reverse=True):
                ret_expr = re.sub(
                    rf"\b{re.escape(var_name)}\b", var_map[var_name], ret_expr
                )
            result_lines.append(f"return {ret_expr}")
        else:
            result_lines.append(line)
    return "\n".join(result_lines)


def extract_divide_args(raw: str) -> tuple[int, int, str, str] | None:
    """Return (start, end, numerator, denominator) of the first DIVIDE(...) in *raw*."""
    m = re.search(r"DIVIDE\s*\(", raw, re.IGNORECASE)
    if not m:
        return None
    div_start = m.start()
    inner_start = m.end()
    depth = 1
    pos = inner_start
    comma_positions: list[int] = []
    while pos < len(raw) and depth > 0:
        ch = raw[pos]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                break
        elif ch == "," and depth == 1:
            comma_positions.append(pos)
        pos += 1
    if len(comma_positions) >= 1:
        num = raw[inner_start : comma_positions[0]]
        if len(comma_positions) >= 2:
            den = raw[comma_positions[0] + 1 : comma_positions[1]]
        else:
            den = raw[comma_positions[0] + 1 : pos]
        div_end = pos + 1  # past the closing paren
        return div_start, div_end, num.strip(), den.strip()
    return None
