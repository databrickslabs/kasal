"""Environment variables are read in three places, and nowhere new.

A Databricks App sets only what its deployment injects: the platform facts
(``DATABRICKS_HOST``, the app service principal, ``PG*``) and the ``app.yaml``
bindings. A setting read from any other variable is a setting nobody can change
in Apps — which is how dozens of knobs (memory retention, A2UI, chat history,
event triggers, …) ended up fixed at their defaults in production. Those moved to
Configuration; see ``services/settings/engine_settings.py`` and the section each
belongs to. API keys come from ``ApiKeysService`` and never from the environment.

The three modules that MAY read the environment:

* ``config/settings.py`` — the server's own settings;
* ``config/logging.py`` — logging, configured before anything else exists;
* ``core/databricks_app.py`` — the Databricks Apps platform contract.

Everything else is a ratchet against ``env_read_baseline.json``, keyed
``path::VARIABLE`` (``<dynamic>`` when the name is not a literal). Those are the
reads that remain — platform facts and process plumbing (``DATABRICKS_HOST``,
``LOG_DIR``, ``CREW_SUBPROCESS_MODE``, …) spread across modules. A file may not
gain one, a new file starts at zero, and moving one into the three modules
lowers the baseline::

    uv run python tests/unit/architecture/test_env_reads_stay_in_config.py --update

Export templates are excluded: an exported app is its own single-tenant
Databricks App and reads its own ``app.yaml``.
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
SRC = BACKEND / "src"
BASELINE = pathlib.Path(__file__).with_name("env_read_baseline.json")

ALLOWED = {
    "src/config/settings.py",
    "src/config/logging.py",
    "src/core/databricks_app.py",
}
EXCLUDED_PARTS = ("/export/templates/",)

_GETTERS = {"os.getenv", "getenv", "os.environ.get", "environ.get"}
_ENVIRON = {"os.environ", "environ"}


def _name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return "<dynamic>"


def reads_in(tree: ast.AST) -> list[str]:
    """Variable names read from the environment in ``tree``."""
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and ast.unparse(node.func) in _GETTERS:
            found.append(_name(node.args[0] if node.args else None))
        elif (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, ast.Load)
            and ast.unparse(node.value) in _ENVIRON
        ):
            found.append(_name(node.slice))
        elif (
            isinstance(node, ast.Compare)
            and len(node.ops) == 1
            and isinstance(node.ops[0], (ast.In, ast.NotIn))
            and ast.unparse(node.comparators[0]) in _ENVIRON
        ):
            found.append(_name(node.left))
    return found


def current_counts() -> dict[str, int]:
    counts: Counter[str] = Counter()
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(BACKEND).as_posix()
        if rel in ALLOWED or any(part in f"/{rel}" for part in EXCLUDED_PARTS):
            continue
        for name in reads_in(ast.parse(path.read_text(), filename=rel)):
            counts[f"{rel}::{name}"] += 1
    return dict(counts)


def test_no_new_environment_reads():
    grown = _ratchet.grown(_ratchet.load(BASELINE), current_counts())
    assert not grown, (
        "New environment reads outside config/settings.py, config/logging.py and "
        "core/databricks_app.py (path::VARIABLE: allowed -> now):\n  "
        + "\n  ".join(grown)
        + "\n\nA setting belongs in Configuration (engine_settings for a server-wide "
        "one, the owning section's config for a per-workspace one). An API key comes "
        "from ApiKeysService. A genuine platform fact goes in core/databricks_app.py."
    )


def test_the_baseline_only_shrinks():
    stale = _ratchet.stale(_ratchet.load(BASELINE), current_counts())
    assert not stale, (
        "Fewer environment reads than recorded — lock the gain in:\n  "
        + "\n  ".join(stale)
        + "\n\n  uv run python tests/unit/architecture/"
        "test_env_reads_stay_in_config.py --update"
    )


def test_the_retired_settings_are_not_read_anywhere():
    """The variables that moved to Configuration must not come back."""
    retired = {
        "JEV_API_BASE",
        "KASAL_AGENT_MAX_EXECUTION_TIME",
        "KASAL_LLM_MAX_CONCURRENCY",
        "KNOWLEDGE_MIN_SCORE",
        "KNOWLEDGE_MAX_SEARCHES",
        "KNOWLEDGE_TTL_DAYS",
        "LAKEBASE_KNOWLEDGE_ROLE",
        "DAX_LLM_BATCH_SIZE",
        "SCRAPE_WEBSITE_MAX_CHARS",
        "SCRAPE_WEBSITE_MAX_FETCH_BYTES",
        "EMBEDDING_BATCH_SIZE",
        "EMBEDDING_TIMEOUT_SECONDS",
        "EMBEDDING_HTTP_TIMEOUT_SECONDS",
    }
    prefixes = (
        "KASAL_BUDGET_",
        "KASAL_MEMORY_SWEEP",
        "KASAL_MEMORY_FORGETTING",
        "KASAL_MEMORY_RECALL",
        "KASAL_MEMORY_WRITE",
        "KASAL_EVENT_TRIGGERS_",
        "A2UI_",
        "CHAT_",
        "WORKFLOW_RECIPE_",
    )
    back = sorted(
        key
        for key in current_counts()
        if (name := key.split("::")[1]) in retired or name.startswith(prefixes)
    )
    assert not back, "Retired env settings read again:\n  " + "\n  ".join(back)


if __name__ == "__main__":
    if "--update" not in sys.argv:
        sys.exit("usage: test_env_reads_stay_in_config.py --update")
    _ratchet.update(BASELINE, current_counts())
