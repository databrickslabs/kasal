"""No workspace credential is ever WRITTEN into the process environment.

One Kasal server serves many workspaces, and ``os.environ`` is shared by all of
them — and inherited by every child it spawns. A key written there for
workspace A is read by workspace B's next request (this happened: ToolFactory
pre-loaded SERPER/PERPLEXITY/OPENAI keys into the env, the tools read the env
before the database, and B searched with A's key). So credentials are passed
explicitly: third-party keys come from ``ApiKeysService`` for the caller's
group, Databricks auth from ``get_auth_context``.

What this checks, over ``src/`` (export templates excluded — an exported app is
single-tenant and reads its own env by design):

* ``os.environ[NAME] = ...``, ``os.environ.setdefault(NAME, ...)``,
  ``os.putenv(NAME, ...)``, ``os.environ.update({NAME: ...})``, a dict literal
  passed as ``env=`` and ``<something>env[NAME] = ...`` — where ``NAME`` is a
  credential-shaped string (``*_API_KEY``, ``*_TOKEN``, ``*_SECRET``,
  ``*PASSWORD``);
* a write whose key is NOT a literal, because a computed key can be anything.
  Each surviving one is listed in ``_DYNAMIC_WRITES`` with why it is safe.

What it does NOT check, deliberately: READS. The Databricks Apps platform injects
its own facts, the app service principal included, and Kasal must keep reading
them from the environment (``PLATFORM_INJECTED`` below). Writing one of them is
still flagged when the name is credential-shaped.
"""

from __future__ import annotations

import ast
import pathlib
import re

BACKEND = pathlib.Path(__file__).resolve().parents[3]
SRC = BACKEND / "src"

CREDENTIAL_NAME = re.compile(r"(_API_KEY|_TOKEN|_SECRET|PASSWORD)$")

#: Injected by the Databricks Apps platform or bound in app.yaml. READING these
#: is required (the app SP authenticates from DATABRICKS_CLIENT_ID/SECRET); this
#: test never flags a read. Listed so that line is explicit, not implied.
PLATFORM_INJECTED = frozenset(
    {
        "DATABRICKS_APP_NAME",
        "DATABRICKS_APP_PORT",
        "DATABRICKS_APP_URL",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_HOST",
        "DATABRICKS_WORKSPACE_ID",
        "PGAPPNAME",
        "PGDATABASE",
        "PGHOST",
        "PGPORT",
        "PGSSLMODE",
        "PGUSER",
        "KASAL_DEFAULT_MODEL",
        "KASAL_LAKEBASE_RESOURCE",
        "KASAL_OUTPUT_VOLUME",
        "KASAL_SQL_WAREHOUSE_ID",
        "MLFLOW_TRACING_SQL_WAREHOUSE_ID",
    }
)

#: Credential-shaped literal writes that are genuinely needed, keyed by
#: ``(path, unparsed target)``.
_ALLOWED_LITERAL_WRITES = {
    # MLflow reads Databricks auth ONLY from the environment. sp_auth._pinned is
    # the single, scoped window that may write a token there: windows for
    # different credentials are mutually exclusive, so a concurrent request
    # never sees another's token, and the original env is restored on exit.
    ("services/mlflow/sp_auth.py", "os.environ['DATABRICKS_TOKEN']"),
}

#: Writes with a computed key, and why each is not a credential.
_DYNAMIC_WRITES = {
    # DatabricksURLUtils.AI_GATEWAY_ENV_VAR == "DATABRICKS_ENABLE_AI_GATEWAY", a flag.
    ("utils/databricks_auth.py", "os.environ[DatabricksURLUtils.AI_GATEWAY_ENV_VAR]"),
    (
        "services/databricks/workspace/service.py",
        "os.environ[DatabricksURLUtils.AI_GATEWAY_ENV_VAR]",
    ),
    # Restores SWAP_KEYS to what they were before the auth window opened.
    ("services/mlflow/sp_auth.py", "os.environ[k]"),
    # Restores / sets _URL_ENV_KEYS (endpoint URLs) around an evaluation.
    ("services/mlflow/evaluation_runner.py", "os.environ[key]"),
    # Restores MLFLOW_EXPERIMENT_NAME / MLFLOW_EXPERIMENT_ID after GEPA.
    ("services/prompt_optimization/crew_runner.py", "os.environ[k]"),
    ("services/prompt_optimization/template_runner.py", "os.environ[k]"),
    # harness_choice.subprocess_env(): {"KASAL_HARNESS": <engine name>}.
    (
        "services/execution/engine_service.py",
        "os.environ.update(subprocess_env(engine))",
    ),
    ("services/execution/harnesses/selection.py", "os.environ[HARNESS_ENV_VAR]"),
    # _PRIVACY_ENV: CrewAI telemetry opt-outs.
    (
        "services/execution/harnesses/crewai/availability.py",
        "os.environ.setdefault(key, value)",
    ),
}


def _is_environ(node: ast.AST) -> bool:
    return isinstance(node, ast.Attribute) and node.attr == "environ"


def _is_env_name(node: ast.AST) -> bool:
    """``os.environ`` or a local mapping that will become an env (``env``, ``child_env``)."""
    if _is_environ(node):
        return True
    return isinstance(node, ast.Name) and node.id.lower().endswith("env")


def _literal(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _dict_credential_keys(node: ast.AST) -> list[str]:
    if not isinstance(node, ast.Dict):
        return []
    keys = [_literal(k) for k in node.keys if k is not None]
    return [k for k in keys if k and CREDENTIAL_NAME.search(k)]


def violations(tree: ast.AST, rel: str) -> tuple[list[str], list[str]]:
    """``(credential writes, unlisted dynamic writes)`` in one module."""
    creds: list[str] = []
    dynamic: list[str] = []

    def literal_write(key: str, target: str, lineno: int) -> None:
        if CREDENTIAL_NAME.search(key) and (rel, target) not in _ALLOWED_LITERAL_WRITES:
            creds.append(f"{rel}:{lineno}: {target}")

    def dynamic_write(target: str, lineno: int) -> None:
        if (rel, target) not in _DYNAMIC_WRITES:
            dynamic.append(f"{rel}:{lineno}: {target}")

    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Subscript) and _is_env_name(target.value):
                    key = _literal(target.slice)
                    text = ast.unparse(target)
                    if key is not None:
                        literal_write(key, text, node.lineno)
                    elif _is_environ(target.value):
                        dynamic_write(text, node.lineno)
        elif isinstance(node, ast.Call):
            func = node.func
            text = ast.unparse(node)
            is_putenv = isinstance(func, ast.Attribute) and func.attr == "putenv"
            is_env_call = (
                isinstance(func, ast.Attribute)
                and func.attr in ("setdefault", "update")
                and _is_environ(func.value)
            )
            if is_putenv or (is_env_call and func.attr == "setdefault"):
                key = _literal(node.args[0]) if node.args else None
                if key is not None:
                    literal_write(key, text, node.lineno)
                else:
                    dynamic_write(text, node.lineno)
            elif is_env_call:  # update
                arg = node.args[0] if node.args else None
                if isinstance(arg, ast.Dict):
                    for key in _dict_credential_keys(arg):
                        literal_write(key, text, node.lineno)
                    if any(k is None or _literal(k) is None for k in arg.keys):
                        dynamic_write(text, node.lineno)
                else:
                    dynamic_write(text, node.lineno)
            for kw in node.keywords:
                if kw.arg == "env":
                    for key in _dict_credential_keys(kw.value):
                        creds.append(f"{rel}:{node.lineno}: env= dict sets {key}")
    return creds, dynamic


def _scan() -> tuple[list[str], list[str]]:
    creds: list[str] = []
    dynamic: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        if rel.startswith("services/export/templates/"):
            continue
        c, d = violations(ast.parse(path.read_text()), rel)
        creds += c
        dynamic += d
    return creds, dynamic


def test_no_credential_is_written_into_the_environment():
    creds, _ = _scan()
    assert not creds, (
        "A credential is written into os.environ, which every workspace shares. "
        "Pass it explicitly (ApiKeysService / get_auth_context) instead:\n  "
        + "\n  ".join(creds)
    )


def test_every_computed_env_write_is_accounted_for():
    _, dynamic = _scan()
    assert not dynamic, (
        "os.environ is written with a computed key. Prove it can never be a "
        "credential and add it to _DYNAMIC_WRITES with the reason:\n  "
        + "\n  ".join(dynamic)
    )


def test_the_allow_lists_are_not_stale():
    """A listed exception that no longer exists must be removed."""
    seen_literal: set[tuple[str, str]] = set()
    seen_dynamic: set[tuple[str, str]] = set()
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        for node in ast.walk(ast.parse(path.read_text())):
            text = (
                ast.unparse(node) if isinstance(node, (ast.Subscript, ast.Call)) else ""
            )
            if (rel, text) in _ALLOWED_LITERAL_WRITES:
                seen_literal.add((rel, text))
            if (rel, text) in _DYNAMIC_WRITES:
                seen_dynamic.add((rel, text))
    assert _ALLOWED_LITERAL_WRITES <= seen_literal, (
        _ALLOWED_LITERAL_WRITES - seen_literal
    )
    assert _DYNAMIC_WRITES <= seen_dynamic, _DYNAMIC_WRITES - seen_dynamic


def test_the_checker_catches_every_write_shape_and_ignores_reads():
    code = """
import os
os.environ["SERPER_API_KEY"] = k
os.environ.setdefault("OPENAI_API_KEY", k)
os.putenv("POWERBI_PASSWORD", k)
os.environ.update({"DATABRICKS_TOKEN": k})
child_env["MY_SECRET"] = k
subprocess.run(cmd, env={"PATH": p, "X_API_KEY": k})
os.environ[name] = k
# Reads are never flagged — including the platform's own credentials.
a = os.environ.get("DATABRICKS_CLIENT_SECRET")
b = os.environ["DATABRICKS_CLIENT_ID"]
c = os.getenv("DATABRICKS_TOKEN")
# A non-credential literal write is fine.
os.environ["CREW_SUBPROCESS_MODE"] = "true"
"""
    creds, dynamic = violations(ast.parse(code), "example.py")
    assert len(creds) == 6, creds
    assert dynamic == ["example.py:9: os.environ[name]"], dynamic
    for name in PLATFORM_INJECTED:
        assert not violations(ast.parse(f"x = os.environ.get({name!r})"), "e.py")[0]
