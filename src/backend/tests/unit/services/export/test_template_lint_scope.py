"""The template tree's linter exclusions must be exactly the token-bearing files.

The whole ``src/services/export/templates/`` tree used to be skipped by black,
isort and ruff — 3,000+ lines of shipped code that no quality gate ever saw.
Two reasons were given: ``{{TOKEN}}`` placeholders make some files invalid
Python, and the files imported packages (crewai) absent from this env.

crewai is gone. And the first reason was never quite right: ``{{X}}`` PARSES
(it is a set containing a set), and a token inside a string literal is just a
string. What actually defeats a linter is narrower — a token standing in for
CODE, which ruff reports as an undefined name and black reformats around.

So the criterion these tests enforce is that narrower one, derived from the
files themselves. It keeps the list honest in both directions:

- a NEW template file is linted by default (nobody has to remember to add it);
- a file that GAINS a token is caught here, with a clear message, rather than
  by a confusing parse error from black in CI;
- a file that LOSES its tokens stops being exempt.

Without this, "narrow the exclusion" quietly becomes "the exclusion drifted back
to the whole tree", which is where it started.
"""

import re
import tomllib

import pytest

from src.services.export.databricks_app_exporter import TEMPLATE_DIR

# Our placeholders are uppercase {{TOKEN}}. GitHub Actions ${{ vars.X }} lives in
# deploy.yml, not in *.py, so this is unambiguous here.
_TOKEN_RE = re.compile(r"\{\{[A-Z_]+\}\}")

PYPROJECT = TEMPLATE_DIR.parents[3].parent / "pyproject.toml"


def _python_files():
    return sorted(
        p for p in TEMPLATE_DIR.parent.rglob("*.py") if "__pycache__" not in p.parts
    )


def _tokens_in_code_position(source: str) -> bool:
    """True when a ``{{TOKEN}}`` stands in for code rather than sitting in a string.

    ``service_name = os.environ.get("OTEL_SERVICE_NAME", "{{APP_NAME}}")`` is
    ordinary Python and lints fine. ``MODEL_OVERRIDE = {{MODEL_OVERRIDE}}`` is
    not: ruff reports MODEL_OVERRIDE as undefined and black reformats the set
    literal it thinks it sees.

    Implemented by removing every string constant's contents from consideration:
    whatever tokens remain are code.
    """
    import ast

    found = _TOKEN_RE.findall(source)
    if not found:
        return False
    in_strings = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            in_strings.extend(_TOKEN_RE.findall(node.value))
    # More occurrences in the file than are accounted for inside strings.
    from collections import Counter

    return bool(Counter(found) - Counter(in_strings))


def _token_bearing():
    """Template .py files a linter cannot process — the exemptions we allow."""
    return {
        p.name
        for p in _python_files()
        if _tokens_in_code_position(p.read_text(encoding="utf-8"))
    }


@pytest.fixture(scope="module")
def config():
    assert PYPROJECT.is_file(), f"backend pyproject not found at {PYPROJECT}"
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def _excluded_names(patterns):
    """The bare filenames named by a list of exclusion patterns."""
    return {p.rsplit("/", 1)[-1] for p in patterns if p.endswith(".py")}


class TestExclusionsMatchReality:
    def test_black_excludes_exactly_the_token_bearing_files(self, config):
        pattern = config["tool"]["black"]["extend-exclude"]
        expected = _token_bearing()
        listed = set(re.findall(r"\(([^)]*)\)", pattern)[0].split("|"))
        assert {f"{n}.py" for n in listed} == expected, (
            "black's extend-exclude is out of step with the files that actually "
            f"carry {{{{TOKEN}}}} placeholders.\n  excluded: {sorted(listed)}\n"
            f"  token-bearing: {sorted(expected)}"
        )

    def test_ruff_excludes_exactly_the_token_bearing_files(self, config):
        listed = _excluded_names(config["tool"]["ruff"]["exclude"])
        assert listed == _token_bearing()

    def test_isort_excludes_exactly_the_token_bearing_files(self, config):
        listed = _excluded_names(config["tool"]["isort"]["extend_skip_glob"])
        assert listed == _token_bearing()

    def test_mypy_still_skips_the_whole_tree_and_that_is_deliberate(self, config):
        """The app imports ``agent_server.*``, a package that exists only in a
        rendered export, so mypy cannot resolve it from the repo. Formatting is
        checkable here; type-checking is not."""
        assert "src/services/export/templates/" in config["tool"]["mypy"]["exclude"]

    def test_most_of_the_tree_is_actually_linted_now(self):
        """Guards against a 'narrowing' that excludes everything by another
        route: the point of the change is that the majority is covered."""
        total = len(_python_files())
        excluded = len(_token_bearing())
        assert (
            total - excluded >= 20
        ), f"only {total - excluded} of {total} template files are linted"


class TestTheExcludedFilesReallyNeedIt:
    def test_a_token_inside_a_string_does_not_earn_an_exemption(self):
        """``otel.py`` carries {{APP_NAME}} inside a string literal and is linted
        like anything else. This is the case the original whole-tree exclusion
        got wrong: 'contains a token' is not the same as 'a linter cannot read
        it', and conflating them cost 22 files their coverage."""
        assert not _tokens_in_code_position(
            'x = os.environ.get("NAME", "{{APP_NAME}}")\n'
        )
        assert _tokens_in_code_position("MODEL_OVERRIDE = {{MODEL_OVERRIDE}}\n")
        assert "otel.py" not in _token_bearing()

    def test_every_template_file_parses(self):
        """All of them do — including the excluded ones. Their problem is
        undefined names and reformatting, not syntax."""
        import ast

        for path in _python_files():
            try:
                ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError as exc:  # pragma: no cover - failure detail
                pytest.fail(f"{path} is not valid Python: {exc}")
