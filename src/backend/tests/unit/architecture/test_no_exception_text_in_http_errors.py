"""A router never sends an exception's text to the client.

``raise HTTPException(status_code=500, detail=str(e))`` returns whatever the
exception said: SQL fragments, file paths, hostnames, upstream response bodies,
sometimes a token echoed back by an SDK. It also makes the message the API
contract, so wording changes in a library become breaking changes.

Log the exception (``logger.exception(...)``) and return a fixed message. For an
error the CLIENT caused, raise a ``src.core.exceptions`` type from the service
with a message written for the client.

Flagged, inside an ``except ... as e`` block, when an ``HTTPException`` (positional
or ``detail=``) carries ``str(e)``, ``repr(e)``, ``e`` itself, or an f-string /
``.format`` / ``%`` / ``+`` expression that interpolates ``e``.

Existing sites are counted per file in ``http_exception_text_baseline.json``,
which may only shrink. When this landed the 15 plain 500s were fixed; what is left
are 4xx responses that pass a ``ValueError``'s text through as the client message
(``flows_router`` 422, ``kpi_conversion_router`` 2x 400). Those want a
``BadRequestError`` raised by the service with a message written for the client::

    uv run python tests/unit/architecture/test_no_exception_text_in_http_errors.py --update
"""

from __future__ import annotations

import ast
import pathlib
import sys
from collections import Counter

try:
    from . import _ratchet
except ImportError:  # run as a script for --update
    import _ratchet  # type: ignore[no-redef]

BACKEND = pathlib.Path(__file__).resolve().parents[3]
API = BACKEND / "src" / "api"
BASELINE = pathlib.Path(__file__).with_name("http_exception_text_baseline.json")


def _mentions(node: ast.AST, names: set[str]) -> bool:
    return any(isinstance(n, ast.Name) and n.id in names for n in ast.walk(node))


def _is_http_exception(call: ast.Call) -> bool:
    func = call.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    return name == "HTTPException"


def _detail(call: ast.Call) -> ast.AST | None:
    for keyword in call.keywords:
        if keyword.arg == "detail":
            return keyword.value
    return call.args[1] if len(call.args) > 1 else None


def _leaks(tree: ast.AST) -> int:
    hits = 0
    for handler in ast.walk(tree):
        if not isinstance(handler, ast.ExceptHandler) or not handler.name:
            continue
        names = {handler.name}
        for statement in handler.body:
            for node in ast.walk(statement):
                if not isinstance(node, ast.Call) or not _is_http_exception(node):
                    continue
                detail = _detail(node)
                if detail is not None and _mentions(detail, names):
                    hits += 1
    return hits


def current_counts() -> dict[str, int]:
    counts: Counter[str] = Counter()
    for path in sorted(API.rglob("*.py")):
        hits = _leaks(ast.parse(path.read_text()))
        if hits:
            counts[path.relative_to(BACKEND).as_posix()] = hits
    return dict(counts)


def test_the_detector_sees_the_shapes_it_claims():
    source = """
try:
    pass
except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
except ValueError as e:
    raise HTTPException(400, f"bad: {e}")
except KeyError as err:
    raise fastapi.HTTPException(status_code=404, detail="missing " + repr(err))
except OSError as e:
    logger.exception("disk")
    raise HTTPException(status_code=500, detail="Internal error")
"""
    assert _leaks(ast.parse(source)) == 3


def test_no_router_gains_an_exception_text_leak():
    grown = _ratchet.grown(_ratchet.load(BASELINE), current_counts())
    assert not grown, (
        "HTTPException detail built from the caught exception "
        "(file: allowed -> now):\n  " + "\n  ".join(grown) + "\n\nLog it with "
        "logger.exception(...) and return a fixed message; for a client error, "
        "raise a src.core.exceptions type from the service instead."
    )


def test_the_baseline_only_shrinks():
    stale = _ratchet.stale(_ratchet.load(BASELINE), current_counts())
    assert not stale, (
        "Fewer leaks than recorded — lower the baseline:\n  "
        + "\n  ".join(stale)
        + "\n\n  uv run python "
        "tests/unit/architecture/test_no_exception_text_in_http_errors.py --update"
    )


if __name__ == "__main__":
    if "--update" not in sys.argv:
        sys.exit("usage: test_no_exception_text_in_http_errors.py --update")
    _ratchet.update(BASELINE, current_counts())
