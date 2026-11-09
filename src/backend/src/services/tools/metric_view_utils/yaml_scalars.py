"""YAML scalar formatting helpers shared by the metric-view YAML emitters.

Pure string functions: they decide how a value is written into the emitted
YAML (bare, quoted, or a block scalar) and never inspect a spec.
"""

from __future__ import annotations


def _yaml_scalar(value: str, indent: int = 0) -> str:
    """Format a string value for safe YAML emission.

    - Plain text with no special chars -> bare scalar
    - Single-line with YAML-special chars -> double-quoted
    - Multi-line -> block scalar (|-)
    """
    if not value:
        return "''"
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
