"""``setup_access``: the access objects behind ``value_access`` / ``object_access``.

Creates, idempotently, in ``access_schema`` (catalog.schema): the mapping table
``row_access_map`` and the two row-filter functions, with the bodies of the
reference access notebook (only the schema is a parameter, so a customer can
repoint the functions at a rules table of their own). Optionally grants EXECUTE
and SELECT to reader groups, and merges mapping rows.

Dry-run by default: the statements are returned and nothing runs. Executing is
``confirm=true``; statements run in order and the first failure stops the run.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.services.tools.metric_view_utils.dax_condition_sql import _sql_str_literal
from src.services.tools.ucmv_source_layer_utils.inputs import parse_json_list
from src.services.tools.ucmv_source_layer_utils.names import is_identifier, quote_fqn
from src.services.tools.ucmv_source_layer_utils.sql_types import SqlError, SqlFn

MAP_TABLE = "row_access_map"
OBJECT_FUNCTION = "object_access"
VALUE_FUNCTION = "value_access"
_MAX_TEXT = 255


@dataclass(frozen=True)
class AccessRow:
    """One mapping row: ``group_name`` may read ``value`` of ``field`` (``*`` = all)."""

    group_name: str
    field: str
    value: str
    reason: str | None = None


def _text(value: object) -> str | None:
    return None if value is None else str(value).strip()


def parse_access_rows(raw: object) -> list[AccessRow]:
    """``[{group_name, field, value, reason}]``; a repeated key keeps the last row."""
    rows: dict[tuple[str, str, str], AccessRow] = {}
    for index, item in enumerate(parse_json_list(raw, "access_rows")):
        label = f"access_rows entry #{index + 1}"
        group = _text(item.get("group_name")) or ""
        field = _text(item.get("field")) or ""
        value = _text(item.get("value")) or ""
        if not group or len(group) > _MAX_TEXT or any(c in group for c in "`\r\n"):
            raise ValueError(
                f"{label}: group_name must be an account group name "
                "(1-255 characters, no backquote or line break)"
            )
        if not is_identifier(field):
            raise ValueError(
                f"{label}: field must be a plain identifier, the rule type (e.g. company_code)"
            )
        if not value or len(value) > _MAX_TEXT:
            raise ValueError(
                f"{label}: value is required (1-255 characters; * = every value)"
            )
        reason = _text(item.get("reason")) or None
        rows[(group, field.lower(), value)] = AccessRow(group, field, value, reason)
    return list(rows.values())


def _grant_group(group: str) -> str:
    return "`" + group + "`"


def _merge_sql(table: str, rows: list[AccessRow]) -> str:
    values = ",\n    ".join(
        "("
        + ", ".join(
            [
                _sql_str_literal(row.group_name),
                _sql_str_literal(row.field),
                _sql_str_literal(row.value),
                (
                    _sql_str_literal(row.reason)
                    if row.reason is not None
                    else "CAST(NULL AS STRING)"
                ),
            ]
        )
        + ")"
        for row in rows
    )
    return (
        f"MERGE INTO {table} AS t\n"
        "USING (SELECT * FROM VALUES\n    "
        f"{values}\n"
        "  AS src(group_name, field, value, reason)) AS src\n"
        "ON t.group_name = src.group_name AND lower(t.field) = lower(src.field) "
        "AND t.value = src.value\n"
        "WHEN MATCHED THEN UPDATE SET t.reason = src.reason\n"
        "WHEN NOT MATCHED THEN INSERT (group_name, field, value, reason) "
        "VALUES (src.group_name, src.field, src.value, src.reason)"
    )


def access_setup_statements(
    access_schema: str,
    reader_groups: list[str],
    access_rows: list[AccessRow],
) -> list[str]:
    """Every statement of the setup, in execution order. Each one is idempotent."""
    schema = quote_fqn(access_schema)
    table = quote_fqn(f"{access_schema}.{MAP_TABLE}")
    object_fn = quote_fqn(f"{access_schema}.{OBJECT_FUNCTION}")
    value_fn = quote_fqn(f"{access_schema}.{VALUE_FUNCTION}")
    statements = [
        "CREATE SCHEMA IF NOT EXISTS "
        f"{schema} COMMENT "
        + _sql_str_literal(
            "Objects shared by the report schemas: row-filter functions, their mapping table."
        ),
        f"""CREATE TABLE IF NOT EXISTS {table} (
  group_name STRING NOT NULL COMMENT 'Account group (as in is_account_group_member).',
  field      STRING NOT NULL COMMENT 'Rule type, the first argument of value_access, e.g. company_code or country.',
  value      STRING NOT NULL COMMENT 'Allowed value of that field; * = every value.',
  reason     STRING          COMMENT 'Why the group has the value (e.g. the Power BI role it mirrors).'
) COMMENT 'Row-level security for value_access: which account group may read which values. One row per group, field and value.'""",
        f"""CREATE OR REPLACE FUNCTION {object_fn}(allowed_groups STRING)
  RETURNS BOOLEAN
  COMMENT 'Row filter for views without row-level security: the reader must be in one of the ;-separated account groups (PERMISSION_DENIED otherwise), then sees every row. An empty list allows everybody.'
  RETURN CASE
    WHEN trim(coalesce(allowed_groups, '')) = '' THEN true
    WHEN EXISTS (SELECT 1 FROM (SELECT trim(g) AS g FROM (SELECT explode(split(allowed_groups, ';')) AS g)) WHERE g <> '' AND is_account_group_member(g))
    THEN true
    ELSE raise_error('PERMISSION_DENIED: the reader is in none of the groups allowed to read the sources of this view.')
  END""",
        f"""CREATE OR REPLACE FUNCTION {value_fn}(access_field STRING, row_value STRING)
  RETURNS BOOLEAN
  COMMENT 'Row filter: the reader sees a row if one of their account groups has the row value (or *) for this field in row_access_map.'
  RETURN EXISTS (
    SELECT 1 FROM {table} m
    WHERE lower(m.field) = lower(access_field)
      AND (m.value = '*' OR m.value = trim(CAST(row_value AS STRING)))
      AND is_account_group_member(m.group_name))""",
    ]
    for group in dict.fromkeys(reader_groups):
        grantee = _grant_group(group)
        statements += [
            f"GRANT EXECUTE ON FUNCTION {object_fn} TO {grantee}",
            f"GRANT EXECUTE ON FUNCTION {value_fn} TO {grantee}",
            f"GRANT SELECT ON TABLE {table} TO {grantee}",
        ]
    if access_rows:
        statements.append(_merge_sql(table, access_rows))
    return statements


def setup_plan(statements: list[str]) -> dict[str, object]:
    """Dry-run result: the statements, nothing executed."""
    return {
        "mode": "dry_run",
        "statements": statements,
        "hint": "nothing was executed; set confirm=true to run these statements in order",
    }


def run_setup(statements: list[str], sql: SqlFn) -> dict[str, object]:
    """Execute the statements in order; the first failure stops the run."""
    steps: list[dict[str, object]] = []
    for statement in statements:
        label = " ".join(statement.split())[:80]
        try:
            sql(statement)
        except SqlError as exc:
            steps.append({"statement": label, "status": "failed", "error": str(exc)})
            break
        steps.append({"statement": label, "status": "applied"})
    return {
        "mode": "executed",
        "steps": steps,
        "all_applied": len(steps) == len(statements)
        and all(s["status"] == "applied" for s in steps),
    }
