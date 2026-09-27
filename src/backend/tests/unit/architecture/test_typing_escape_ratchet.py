"""Typing escape hatches, enforced as a per-file count ratchet.

mypy runs strict over ``src/`` with an empty allowance (``check_types.py``), but
a zero there is only as honest as the escapes it lets through. Three ways to
make an error disappear without fixing it are counted here, per file, against
``typing_escape_baseline.json`` (keys ``path::kind``):

* ``type-ignore`` — a ``# type: ignore[...]`` comment. mypy already rejects one
  that silences nothing (``warn_unused_ignores``) or names no error code
  (``ignore-without-code``); this stops the live ones multiplying.
* ``cast``        — a ``cast(...)`` / ``typing.cast(...)`` call: an assertion
  mypy takes on trust and never checks.
* ``Any``         — a reference to ``typing.Any`` (annotations, aliases,
  ``Dict[str, Any]`` …; imports are not counted). ``Any`` switches type checking
  off for everything that touches the value.

A file may not gain any of them and a new file starts at zero. Prefer a precise
type, a ``TypedDict``/model, a ``Protocol`` or an ``isinstance`` narrowing. When
the counts drop, lock the gain in::

    uv run python tests/unit/architecture/test_typing_escape_ratchet.py --update

Scope matches mypy's: ``src/`` minus the export templates (``[tool.mypy]
exclude``). Quoted (string) annotations are not parsed, so they are not counted.
"""

from __future__ import annotations

import ast
import io
import pathlib
import re
import sys
import tokenize
from collections import Counter

try:
    from . import _ratchet
except ImportError:  # run as a script for --update
    import _ratchet  # type: ignore[no-redef]

BACKEND = pathlib.Path(__file__).resolve().parents[3]
SRC = BACKEND / "src"
BASELINE = pathlib.Path(__file__).with_name("typing_escape_baseline.json")
EXCLUDED_PARTS = ("/export/templates/",)

_TYPE_IGNORE = re.compile(r"#\s*type:\s*ignore\b")


def _is_typing_name(node: ast.AST, name: str) -> bool:
    """``name`` or ``typing.name`` / ``t.name``-style module access."""
    if isinstance(node, ast.Name):
        return node.id == name
    return (
        isinstance(node, ast.Attribute)
        and node.attr == name
        and isinstance(node.value, ast.Name)
        and node.value.id in {"typing", "t", "typing_extensions"}
    )


def _type_ignores(source: str) -> int:
    tokens = tokenize.generate_tokens(io.StringIO(source).readline)
    return sum(
        1
        for token in tokens
        if token.type == tokenize.COMMENT and _TYPE_IGNORE.search(token.string)
    )


def counts_for(source: str) -> Counter[str]:
    """``{kind: count}`` for one module's source."""
    counts: Counter[str] = Counter()
    counts["type-ignore"] = _type_ignores(source)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and _is_typing_name(node.func, "cast"):
            counts["cast"] += 1
        elif _is_typing_name(node, "Any") and not isinstance(
            getattr(node, "ctx", None), ast.Store
        ):
            counts["Any"] += 1
    return counts


def current_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in sorted(SRC.rglob("*.py")):
        relative = path.relative_to(BACKEND).as_posix()
        if "__pycache__" in path.parts or any(
            p in f"/{relative}" for p in EXCLUDED_PARTS
        ):
            continue
        for kind, count in counts_for(path.read_text()).items():
            if count:
                counts[f"{relative}::{kind}"] = count
    return counts


def test_no_file_gains_a_typing_escape():
    grown = _ratchet.grown(_ratchet.load(BASELINE), current_counts())
    assert not grown, (
        "More type: ignore / cast( / Any than recorded (file::kind: allowed -> "
        "now; 0 means new):\n  "
        + "\n  ".join(grown)
        + "\n\nType it precisely instead: a real annotation, a TypedDict or model, "
        "a Protocol, or an isinstance narrowing. Never add to the baseline."
    )


def test_the_baseline_only_shrinks():
    stale = _ratchet.stale(_ratchet.load(BASELINE), current_counts())
    assert not stale, (
        "Fewer typing escapes than recorded — lock the gain in by lowering the "
        "baseline:\n  "
        + "\n  ".join(stale)
        + "\n\n  uv run python tests/unit/architecture/test_typing_escape_ratchet.py "
        "--update"
    )


def test_counts_what_it_claims():
    source = (
        "from typing import Any, cast\n"
        "import typing\n"
        "x: Any = cast(int, 1)  # type: ignore[assignment]\n"
        "y: typing.Any = typing.cast(str, 'a')\n"
        "s = '# type: ignore'\n"
    )
    assert counts_for(source) == Counter({"type-ignore": 1, "cast": 2, "Any": 2})


if __name__ == "__main__":
    if "--update" not in sys.argv:
        sys.exit("usage: test_typing_escape_ratchet.py --update")
    _ratchet.update(BASELINE, current_counts())
