"""Scope guard: judges run on demand; Kasal never touches the scorer registry.

``register()``/``start()`` of a scorer, ``list_scorers``, ``delete_scorer``
and scheduled scorers are MONITORING: on Databricks they create an
experiment-level job the app's service principal usually may not create, and
they are out of scope until an explicit, opt-in monitoring step. Kasal's
judges live in the MLflow Prompt Registry instead.

A source scan (no src import), over the code that builds or runs judges.
"""

import ast
import pathlib

_SRC = pathlib.Path(__file__).resolve().parents[5] / "src"
_SCANNED = [
    _SRC / "services" / "prompt_optimization",
    _SRC / "services" / "mlflow",
]
_FORBIDDEN_CALLS = {"register", "list_scorers", "delete_scorer", "get_scorer"}
_FORBIDDEN_NAMES = {"ScorerSamplingConfig", "list_scorers", "delete_scorer"}


def _offences(path: pathlib.Path) -> list[str]:
    found = []
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else getattr(func, "id", "")
            )
            if name in _FORBIDDEN_CALLS:
                found.append(f"{path.name}:{node.lineno} calls {name}()")
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in _FORBIDDEN_NAMES:
                    found.append(f"{path.name}:{node.lineno} imports {alias.name}")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if "scheduled-scorers" in node.value:
                found.append(f"{path.name}:{node.lineno} scheduled-scorers API")
    return found


def test_no_scorer_registration_or_monitoring():
    files = [p for root in _SCANNED for p in root.rglob("*.py")]
    assert any(p.parent.name == "builtin_judges" for p in files), "scan is empty"
    offences = [o for path in files for o in _offences(path)]
    assert not offences, "\n".join(offences)


def test_the_guard_catches_what_it_claims(tmp_path):
    sample = tmp_path / "bad.py"
    sample.write_text(
        "from mlflow.genai.scorers import list_scorers\n"
        "Safety().register(name='s')\n"
        "delete_scorer(name='s')\n"
    )
    assert len(_offences(sample)) == 3
