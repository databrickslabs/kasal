"""Credential-bearing tool-config columns are SensitiveJSON, and only that.

The column type is the one place these values are encrypted (on write) and
decrypted (on load). A column switched back to plain JSON would store new
secrets in plaintext with every test still green, and a service that encrypts
or decrypts these values itself either double-handles them or writes plaintext
back through the session it holds.

Imports are plain ``import`` statements: this check covers several modules, so
it lives with the other architecture checks, not beside one of them.
"""

import ast
import pathlib

import src.db.all_models  # noqa: F401  registers every table on Base
import src.db.base as db_base
import src.db.sensitive_json as sensitive_json

#: table -> columns that hold tool credentials.
ENCRYPTED = {
    "agents": {"tool_configs"},
    "crews": {"tool_configs", "nodes"},
    # tasks.config nests a second copy of the callback config (its PAT).
    "tasks": {"tool_configs", "callback_config", "config"},
    "tools": {"config"},
    "group_tools": {"config"},
}

SERVICES = pathlib.Path(sensitive_json.__file__).resolve().parents[1] / "services"
HELPERS = {
    "encrypt_sensitive_fields",
    "decrypt_sensitive_fields",
    "encrypt_sensitive_json",
    "decrypt_sensitive_json",
}


def test_tool_config_columns_use_sensitive_json():
    tables = db_base.Base.metadata.tables
    wrong = [
        f"{table}.{column}: {type(tables[table].c[column].type).__name__}"
        for table, columns in ENCRYPTED.items()
        for column in sorted(columns)
        if not isinstance(tables[table].c[column].type, sensitive_json.SensitiveJSON)
    ]
    assert not wrong, f"must be SensitiveJSON: {wrong}"


def test_no_service_encrypts_or_decrypts_tool_configs_itself():
    offenders = []
    for path in sorted(SERVICES.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            names = []
            if isinstance(node, ast.ImportFrom):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.Attribute):
                names = [node.attr]
            if HELPERS.intersection(names):
                offenders.append(f"{path.relative_to(SERVICES)}:{node.lineno}")
    assert not offenders, (
        "the SensitiveJSON column type owns tool-config encryption; "
        f"remove these calls: {offenders}"
    )
