"""The composer must stay importable by an app that has no ``src`` package.

``compose.py`` is vendored VERBATIM into every exported Databricks App — copied
into ``agent_server/a2ui/`` by the exporter — and that app ships no Kasal source
at all. One ``from src.…`` import here produces an export that fails on startup,
and nothing in the live app would notice, because live Kasal has ``src`` on the
path.

The constraint used to be carried by the directory name: the module lived in
``src/shared/``, and a docstring explained what "shared" meant. That is a
convention, and a convention cannot fail a build. This can.
"""

import ast
import pathlib

import pytest

_A2UI = pathlib.Path(__file__).parents[4] / "src" / "services" / "a2ui"
COMPOSER = _A2UI / "compose.py"
#: ``stream.py`` is vendored alongside it (compose imports it for the skeleton),
#: so it lives under exactly the same constraint.
VENDORED = (COMPOSER, _A2UI / "stream.py")

#: Everything the composer is allowed to import. Standard library only — the LLM
#: is injected by the caller as an ``llm_call`` callable, never imported.
ALLOWED_ROOTS = {
    "__future__",
    "abc",
    "collections",
    "copy",
    "dataclasses",
    "datetime",
    "enum",
    "functools",
    "hashlib",
    "itertools",
    "json",
    "logging",
    "math",
    "os",
    "pathlib",
    "random",
    "re",
    "string",
    "textwrap",
    "time",
    "typing",
    "uuid",
}


def _imported_roots(path: pathlib.Path):
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative import — inside the vendored package, fine
                continue
            if node.module:
                yield node.module.split(".")[0]


@pytest.mark.parametrize("path", VENDORED, ids=lambda p: p.name)
def test_the_vendored_module_exists(path):
    assert path.exists(), path


@pytest.mark.parametrize("path", VENDORED, ids=lambda p: p.name)
def test_it_imports_nothing_from_kasal(path):
    """A ``src.`` import here ships a broken export, silently."""
    offenders = sorted({r for r in _imported_roots(path) if r == "src"})

    assert (
        not offenders
    ), f"{path.name} is vendored into exported apps, which have no 'src' package"


@pytest.mark.parametrize("path", VENDORED, ids=lambda p: p.name)
def test_it_imports_only_the_standard_library(path):
    """Third-party imports break the export too — the app's requirements.txt is
    generated from the template, not from what this file happens to need."""
    used = set(_imported_roots(path))
    unexpected = sorted(used - ALLOWED_ROOTS)

    assert not unexpected, (
        f"{path.name} may only import the stdlib; found {unexpected}. "
        "If one of these is genuinely stdlib, add it to ALLOWED_ROOTS."
    )
