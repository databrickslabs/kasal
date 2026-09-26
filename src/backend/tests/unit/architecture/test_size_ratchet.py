"""File- and function-size ratchets (root CLAUDE.md, "File Size Limits").

Files — ``.py`` under ``src/backend/src``, ``.ts``/``.tsx`` under
``src/frontend/src`` (tests included; same limits for source and tests):

* a file NOT in the baseline may not exceed the 800-line target;
* a baselined file at or under the 1500-line ceiling may grow up to 1500;
* a baselined file OVER the ceiling may not grow at all, and when it shrinks the
  baseline must be lowered to lock the gain in.

Functions — Python functions and methods in ``src/backend/src`` (decorators
excluded, nested functions counted on their own as well): none may exceed 200
lines unless baselined, and a baselined one may only shrink. The key is
``path::Qualified.name``, so renaming or moving a long function counts as a new
one; split it instead.

Baselines: ``file_size_baseline.json`` / ``function_size_baseline.json`` next to
this file. Lower them (they can only go down) with::

    uv run python tests/unit/architecture/test_size_ratchet.py --update
"""

from __future__ import annotations

import ast
import pathlib
import sys
from collections.abc import Iterator

try:
    from . import _ratchet
except ImportError:  # run as a script for --update
    import _ratchet  # type: ignore[no-redef]

BACKEND = pathlib.Path(__file__).resolve().parents[3]
REPO = BACKEND.parents[1]
FILE_BASELINE = pathlib.Path(__file__).with_name("file_size_baseline.json")
FUNCTION_BASELINE = pathlib.Path(__file__).with_name("function_size_baseline.json")

FILE_TARGET = 800
FILE_CEILING = 1500
FUNCTION_LIMIT = 200

_ROOTS = (
    (REPO / "src" / "backend" / "src", (".py",)),
    (REPO / "src" / "frontend" / "src", (".ts", ".tsx")),
)


def _files() -> Iterator[pathlib.Path]:
    for root, suffixes in _ROOTS:
        for path in root.rglob("*"):
            if (
                path.suffix in suffixes
                and path.is_file()
                and "node_modules" not in path.parts
                and "__pycache__" not in path.parts
            ):
                yield path


def _lines(path: pathlib.Path) -> int:
    return path.read_bytes().count(b"\n")


def file_sizes() -> dict[str, int]:
    """Every file over the target, as ``{repo-relative path: lines}``."""
    sizes = {}
    for path in _files():
        lines = _lines(path)
        if lines > FILE_TARGET:
            sizes[path.relative_to(REPO).as_posix()] = lines
    return sizes


def _functions(tree: ast.AST) -> Iterator[tuple[str, int]]:
    def visit(node: ast.AST, prefix: str) -> Iterator[tuple[str, int]]:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = f"{prefix}{child.name}"
                yield name, (child.end_lineno or child.lineno) - child.lineno + 1
                yield from visit(child, f"{name}.")
            elif isinstance(child, ast.ClassDef):
                yield from visit(child, f"{prefix}{child.name}.")
            else:
                yield from visit(child, prefix)

    yield from visit(tree, "")


def function_sizes() -> dict[str, int]:
    """Every function over the limit, as ``{path::qualname: lines}``.

    A qualname defined more than once in a module (conditional definitions)
    keeps its largest size.
    """
    sizes: dict[str, int] = {}
    for path in sorted((REPO / "src" / "backend" / "src").rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relative = path.relative_to(REPO).as_posix()
        for name, lines in _functions(ast.parse(path.read_text())):
            if lines > FUNCTION_LIMIT:
                key = f"{relative}::{name}"
                sizes[key] = max(lines, sizes.get(key, 0))
    return sizes


def _allowed_file_size(baseline: dict[str, int], path: str) -> int:
    recorded = baseline.get(path)
    if recorded is None:
        return FILE_TARGET
    return max(recorded, FILE_CEILING)


def test_no_file_grows_past_its_limit():
    baseline = _ratchet.load(FILE_BASELINE)
    over = [
        f"{path}: {lines} lines (limit {_allowed_file_size(baseline, path)})"
        for path, lines in sorted(file_sizes().items())
        if lines > _allowed_file_size(baseline, path)
    ]
    assert not over, (
        "Files over their size limit:\n  " + "\n  ".join(over) + "\n\nA new file "
        f"may not exceed {FILE_TARGET} lines, an existing one may not pass "
        f"{FILE_CEILING}, and one already past {FILE_CEILING} may not grow. "
        "Extract the new code into a sibling module along a real seam (root "
        "CLAUDE.md, 'File Size Limits'); never add to the baseline."
    )


def test_the_file_baseline_only_shrinks():
    baseline = _ratchet.load(FILE_BASELINE)
    current = file_sizes()
    # Over the ceiling: every line gained must be locked in. At or under it:
    # the entry only goes once the file drops back under the target.
    stale = [
        f"{path}: {recorded} -> {current.get(path, 0)}"
        for path, recorded in sorted(baseline.items())
        if path not in current or (recorded > FILE_CEILING and current[path] < recorded)
    ]
    assert not stale, (
        "Files smaller than recorded — lower the baseline:\n  "
        + "\n  ".join(stale)
        + "\n\n  uv run python tests/unit/architecture/test_size_ratchet.py --update"
    )


def test_no_function_grows_past_its_limit():
    grown = _ratchet.grown(_ratchet.load(FUNCTION_BASELINE), function_sizes())
    assert not grown, (
        f"Functions over {FUNCTION_LIMIT} lines, or longer than recorded "
        "(path::function: allowed -> now; 0 means new):\n  "
        + "\n  ".join(grown)
        + "\n\nSplit the function into named steps. Never add to the baseline."
    )


def test_the_function_baseline_only_shrinks():
    stale = _ratchet.stale(_ratchet.load(FUNCTION_BASELINE), function_sizes())
    assert not stale, (
        "Functions shorter than recorded (or gone) — lower the baseline:\n  "
        + "\n  ".join(stale)
        + "\n\n  uv run python tests/unit/architecture/test_size_ratchet.py --update"
    )


def _update() -> None:
    _ratchet.update(FILE_BASELINE, file_sizes())
    _ratchet.update(FUNCTION_BASELINE, function_sizes())


if __name__ == "__main__":
    if "--update" not in sys.argv:
        sys.exit("usage: test_size_ratchet.py --update")
    _update()
