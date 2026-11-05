"""Direct grants on a metric view.

``CREATE OR REPLACE VIEW`` drops a view's direct grants (grants inherited from the schema or
catalog survive). The deployer reads the direct grants before a redeploy and re-applies them
after a successful one. ``information_schema.table_privileges`` exposes ``GRANTEE``,
``PRIVILEGE_TYPE`` and ``INHERITED_FROM`` (checked against the Databricks docs). Direct grants
are the rows with ``INHERITED_FROM = 'NONE'``; that literal is the brief's assumption and is
not spelled out in the docs page, so a fresh workspace should be spot-checked once.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .sql_helpers import SqlRunner, error_text, quote_name, result_rows, string_literal

_DIRECT_GRANTS_SQL = (
    "SELECT grantee, privilege_type FROM {catalog}.information_schema.table_privileges "
    "WHERE table_schema = {schema} AND table_name = {view} AND inherited_from = 'NONE'"
)
_PRIVILEGE = re.compile(r"^[A-Z][A-Z_]*$")
_OWNERSHIP = "OWNERSHIP"


class GrantReadError(RuntimeError):
    """The direct grants of a view could not be read (the caller must not redeploy)."""

    def __init__(self, message: str, http_status: object = None) -> None:
        super().__init__(message)
        self.http_status = http_status


@dataclass(frozen=True)
class DirectGrant:
    grantee: str
    privilege: str


def read_direct_grants(
    run_sql: SqlRunner, catalog: str, schema: str, view: str
) -> tuple[list[DirectGrant], int]:
    """Direct grants of ``catalog.schema.view`` as ``(grants, skipped)``.

    ``skipped`` counts rows that cannot be re-applied (OWNERSHIP, malformed rows). An empty
    result (view absent, or no direct grants) is a success with no grants. Raises
    ``GrantReadError`` when the statement itself fails."""
    outcome = run_sql(
        _DIRECT_GRANTS_SQL.format(
            catalog=quote_name(catalog),
            schema=string_literal(schema),
            view=string_literal(view),
        )
    )
    if not outcome.get("success"):
        raise GrantReadError(error_text(outcome), outcome.get("http_status"))
    grants: list[DirectGrant] = []
    skipped = 0
    for row in result_rows(outcome.get("data")):
        if len(row) < 2 or not isinstance(row[0], str) or not isinstance(row[1], str):
            skipped += 1
            continue
        privilege = row[1].strip()
        if privilege == _OWNERSHIP or not _PRIVILEGE.match(privilege):
            skipped += 1
            continue
        grants.append(DirectGrant(grantee=row[0], privilege=privilege))
    return sorted(set(grants), key=lambda g: (g.grantee, g.privilege)), skipped


def grant_statement(view_fqn: str, grant: DirectGrant) -> str:
    return f"GRANT {grant.privilege} ON TABLE {view_fqn} TO {quote_name(grant.grantee)}"


def restore_grants(
    run_sql: SqlRunner,
    view_fqn: str,
    catalog: str,
    schema: str,
    view: str,
    before: list[DirectGrant],
) -> dict[str, object]:
    """Re-apply ``before`` after a redeploy and compare the grant set with a fresh read.

    ``preserved`` is true only when every GRANT succeeded and the post-deploy direct grants
    equal the pre-deploy set (same count and same members)."""
    failures: list[str] = []
    for grant in before:
        outcome = run_sql(grant_statement(view_fqn, grant))
        if not outcome.get("success"):
            failures.append(
                f"{grant.privilege} to {grant.grantee}: {error_text(outcome)}"
            )
    try:
        after, _ = read_direct_grants(run_sql, catalog, schema, view)
    except GrantReadError as error:
        failures.append(f"re-read after redeploy failed: {error}")
        return {
            "before": len(before),
            "after": None,
            "preserved": False,
            "failures": failures,
        }
    preserved = not failures and set(after) == set(before)
    return {
        "before": len(before),
        "after": len(after),
        "preserved": preserved,
        "failures": failures,
    }
