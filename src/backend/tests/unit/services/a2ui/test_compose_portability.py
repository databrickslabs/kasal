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

COMPOSER = (
    pathlib.Path(__file__).parents[4] / "src" / "services" / "a2ui" / "compose.py"
)

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


def test_the_composer_exists_where_the_exporter_looks_for_it():
    assert COMPOSER.exists(), COMPOSER


def test_the_composer_imports_nothing_from_kasal():
    """A ``src.`` import here ships a broken export, silently."""
    offenders = sorted({r for r in _imported_roots(COMPOSER) if r == "src"})

    assert (
        not offenders
    ), "compose.py is vendored into exported apps, which have no 'src' package"


def test_the_composer_imports_only_the_standard_library():
    """Third-party imports break the export too — the app's requirements.txt is
    generated from the template, not from what this file happens to need."""
    used = set(_imported_roots(COMPOSER))
    unexpected = sorted(used - ALLOWED_ROOTS)

    assert not unexpected, (
        f"compose.py may only import the stdlib; found {unexpected}. "
        "If one of these is genuinely stdlib, add it to ALLOWED_ROOTS."
    )
