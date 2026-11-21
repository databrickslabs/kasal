"""YAML scalar formatting helpers shared by the metric-view YAML emitters.

Pure string functions: they decide how a value is written into the emitted
YAML (bare, quoted, or a block scalar) and never inspect a spec.
"""

from __future__ import annotations

import re

_PLAIN_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PBI_AUTO_DATE = re.compile(
    r"^(local_?date_?table|date_?table_?template)_", re.IGNORECASE
)


def uc_yaml_name(name: str) -> str:
    """A ``catalog.schema.table`` reference as metric-view YAML needs it: bare when
    every part is a plain identifier, each other part back-quoted (a hyphenated
    catalog such as ``acme-dev``). Anything that is not a plain dotted name (SQL,
    an already quoted name, a placeholder) is returned unchanged."""
    if not name or "`" in name or any(c in name for c in " ()\n{}"):
        return name
    parts = name.split(".")
    if len(parts) < 2 or any(not p for p in parts):
        return name
    if all(_PLAIN_PART.match(p) for p in parts):
        return name
    if not all(re.match(r"^[A-Za-z0-9_-]+$", p) for p in parts):
        return name
    return ".".join(p if _PLAIN_PART.match(p) else f"`{p}`" for p in parts)


def is_pbi_auto_date_table(name: str) -> bool:
    """Power BI's internal auto date tables (``LocalDateTable_<guid>``,
    ``DateTableTemplate_<guid>``, in any casing or snake form). They exist only
    inside the model, so a join to one can never deploy."""
    return bool(_PBI_AUTO_DATE.match(name or ""))


# Every character YAML reads as a line break. Model text (a measure name, DAX) holding one
# outside "\n" would end the comment or block scalar it is written in and start a new key.
_LINE_BREAKS = re.compile("\r\n|[\n\r\x0b\x0c\x85\u2028\u2029]")


def yaml_lines(text: str) -> list[str]:
    """``text`` split at every line break YAML knows, for one comment or block line each."""
    return _LINE_BREAKS.split(text or "")


def one_line(text: str) -> str:
    """``text`` on one line: its line breaks become spaces."""
    return _LINE_BREAKS.sub(" ", text or "")


def expr_value(expr: str, indent: int = 4) -> str:
    """An expression as the value of an ``expr:`` key. One that spans several lines is a
    block scalar: written as it is, its continuation lines keep their own indentation (an
    ``END`` at column 0), and the YAML no longer parses."""
    return _yaml_scalar(expr, indent) if _LINE_BREAKS.search(expr) else expr


def _yaml_scalar(value: str, indent: int = 0) -> str:
    """Format a string value for safe YAML emission.

    - Plain text with no special chars -> bare scalar
    - Single-line with YAML-special chars -> double-quoted
    - Multi-line -> block scalar (|-)
    """
    if not value:
        return "''"
    value = _LINE_BREAKS.sub("\n", value)
    if "\n" in value:
        prefix = " " * indent
        block_lines = value.split("\n")
        return "|-\n" + "\n".join(
            f"{prefix}  {item_value}" for item_value in block_lines
        )
    if any(
        c in value
        for c in ("{", "}", ":", "#", "'", "[", "]", "*", "&", "!", "%", "@", "`")
    ):
        # Prefer single quotes for values with backticks (no escaping needed)
        if "`" in value and "'" not in value:
            return f"'{value}'"
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def _yaml_needs_quoting(val: str) -> bool:
    """Check if a YAML scalar value needs quoting."""
    if not val:
        return True
    # Characters that require quoting in YAML
    if any(
        c in val
        for c in (":", "#", "{", "}", "[", "]", "&", "*", "!", "|", ">", "%", "@")
    ):
        return True
    # Leading/trailing whitespace
    if val != val.strip():
        return True
    return False


def _yaml_val(val: str) -> str:
    """Format a YAML scalar value, quoting only when necessary."""
    if _yaml_needs_quoting(val):
        escaped = val.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return val
