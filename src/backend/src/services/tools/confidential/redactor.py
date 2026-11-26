"""Redact a tool result so that no fractional value reaches the agent.

One entry point, ``redact_output``, handles every shape a tool returns:

- a rendered markdown table: column names, row count and the inferred type
  of each column are kept; fractional cells become ``PLACEHOLDER``;
- a JSON string or a structure: keys and categorical values are kept, fractional
  numbers become ``PLACEHOLDER``;
- free text (a Genie answer, a summary): a line that carries a number is
  replaced by ``NOTICE``, because a sentence with a number in it is a value;
  text without numbers passes through;
- an error message: every numeric literal is replaced by ``<n>``, the text is
  kept so the agent can still tell what went wrong.
"""

import json
import re
from typing import Dict, List, Sequence, Set

from src.services.tools.confidential.cells import (
    PLACEHOLDER,
    CellKind,
    classify_text,
    classify_value,
)

#: Replaces a line of free text that carried numbers.
NOTICE = "[answer withheld: it contained numeric values (confidential values mode)]"

#: What an error message's numbers become.
_ERROR_NUMBER = "<n>"
_NUMBER = re.compile(r"\d+(?:[.,]\d+)*(?:[eE][-+]?\d+)?")
_DIGIT = re.compile(r"\d")
_TABLE_SEPARATOR = re.compile(r"\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)*\|?")
#: A prose line that names a failure. It is scrubbed rather than replaced, so
#: the agent still learns why a call failed ("Genie error: column <n> missing").
_ERROR_WORD = re.compile(
    r"\b(error|failed|exception|traceback|timed out|blocked)\b", re.IGNORECASE
)
_ERROR_START = re.compile(
    r"\s*(error\b|dax execution failed|failed\b|exception\b|traceback\b|"
    r"blocked\b|http error|query timed out|tool call blocked)",
    re.IGNORECASE,
)

_KIND_LABEL: Dict[str, str] = {
    "text": "text",
    "int": "int",
    "date": "date",
    "bool": "bool",
    "fractional": "fractional, withheld",
}


def scrub_error_text(text: str) -> str:
    """An error message with every numeric literal replaced by ``<n>``."""
    return _NUMBER.sub(_ERROR_NUMBER, text)


def is_error_text(text: str) -> bool:
    """True when the first line of a string reads as an error message."""
    first_line = text.strip().split("\n", 1)[0][:200]
    return _ERROR_START.match(first_line) is not None


def redact_output(output: object) -> object:
    """The output with every fractional value withheld; see the module docstring."""
    if isinstance(output, str):
        return redact_text(output)
    if isinstance(output, (dict, list, tuple)):
        return _redact_structure(output)
    if (
        output is None
        or isinstance(output, (bool, int))
        or hasattr(output, "isoformat")
    ):
        return output
    if isinstance(output, float):
        return PLACEHOLDER
    # Every other object (a DataFrame, a Decimal): judge its printed form.
    return redact_text(str(output))


_NOT_JSON = object()


def redact_text(text: str) -> str:
    """Redact a string result: error text, JSON, or a table-and-prose mix."""
    if is_error_text(text):
        return scrub_error_text(text)
    stripped = text.strip()
    if stripped[:1] in ("{", "["):
        parsed = _parse_json(stripped)
        if parsed is not _NOT_JSON:
            return json.dumps(_redact_structure(parsed), indent=2, ensure_ascii=False)
    return _redact_lines(text)


def _parse_json(text: str) -> object:
    try:
        return json.loads(text)
    except ValueError:
        return _NOT_JSON


def _redact_structure(value: object) -> object:
    """Recurse through dicts and lists; withhold fractional scalars and keys."""
    if isinstance(value, dict):
        return {
            _redact_key(key): _redact_structure(item) for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact_structure(item) for item in value]
    if classify_value(value) == "fractional":
        return PLACEHOLDER
    return value


def _redact_key(key: object) -> object:
    if isinstance(key, str) and classify_text(key) == "fractional":
        return PLACEHOLDER
    return key


def _redact_lines(text: str) -> str:
    """Tables are redacted cell by cell; prose lines with digits become NOTICE."""
    lines = text.splitlines()
    out: List[str] = []
    index = 0
    while index < len(lines):
        if lines[index].lstrip().startswith("|"):
            end = index
            while end < len(lines) and lines[end].lstrip().startswith("|"):
                end += 1
            out.extend(_redact_table(lines[index:end]))
            index = end
            continue
        line = lines[index]
        if _ERROR_WORD.search(line):
            out.append(scrub_error_text(line))
        elif _DIGIT.search(line):
            if not out or out[-1] != NOTICE:
                out.append(NOTICE)
        else:
            out.append(line)
        index += 1
    return "\n".join(out)


def _redact_table(block: Sequence[str]) -> List[str]:
    """A markdown table block, with fractional cells withheld and a summary line."""
    has_separator = len(block) > 1 and _is_separator(block[1])
    header = _split_row(block[0])
    body_start = 2 if has_separator else 1
    body = [_split_row(line) for line in block[body_start:]]

    redacted_body = [[_redact_cell(cell) for cell in row] for row in body]
    rendered = [_render_row([_clean_header(cell) for cell in header])]
    if has_separator:
        rendered.append(block[1])
    rendered.extend(_render_row(row) for row in redacted_body)

    summary = _column_summary(header, body)
    rendered.append(f"_{len(body)} rows. Columns: {summary}._")
    return rendered


def _is_separator(line: str) -> bool:
    return _TABLE_SEPARATOR.fullmatch(line.strip()) is not None


def _split_row(line: str) -> List[str]:
    inner = line.strip()
    if inner.startswith("|"):
        inner = inner[1:]
    if inner.endswith("|"):
        inner = inner[:-1]
    return [cell.strip() for cell in inner.split("|")]


def _render_row(cells: Sequence[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def _redact_cell(cell: str) -> str:
    return PLACEHOLDER if classify_text(cell) == "fractional" else cell


def _clean_header(cell: str) -> str:
    """Power BI returns ``[Table].[Column]``; keep the column name only."""
    return cell.split("].[")[-1].rstrip("]") if "].[" in cell else cell


def _column_summary(header: Sequence[str], body: Sequence[Sequence[str]]) -> str:
    parts: List[str] = []
    for position, name in enumerate(header):
        kinds: Set[CellKind] = set()
        for row in body:
            if position < len(row):
                kinds.add(classify_text(row[position]))
        parts.append(f"{_clean_header(name)} ({_describe(kinds)})")
    return ", ".join(parts) if parts else "none"


def _describe(kinds: Set[CellKind]) -> str:
    kinds = kinds - {"null"}
    if "fractional" in kinds:
        return _KIND_LABEL["fractional"]
    if not kinds:
        return "empty"
    if len(kinds) == 1:
        return _KIND_LABEL[next(iter(kinds))]
    return "mixed"


__all__ = [
    "NOTICE",
    "is_error_text",
    "redact_output",
    "redact_text",
    "scrub_error_text",
]
