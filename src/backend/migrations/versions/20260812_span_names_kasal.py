"""execution_trace.span_name: CrewAI.* -> kasal.*

Revision ID: 20260812_span_kasal
Revises: 20260810_exec_crew_id
Create Date: 2026-08-12

The engine stopped being a vendored copy of crewAI a long time ago — there is no
``src/engines/`` and no CrewAIInstrumentor — but twelve span names still carried
the old library's prefix, so every tool row in the timeline read
``CrewAI.tool.execute``.

Renaming them in code alone would have meant keeping a ``CrewAI.*`` alias table
around forever to read old rows, which is not a rename. So the rows are renamed
too: after this runs, nothing in the database or the codebase says CrewAI.

Only the twelve names this codebase ever emitted are touched, and only the
``CrewAI.`` prefix is rewritten — ``kasal.*`` spans (the event bridge's own,
which were always named that way) are not matched by the filter.

Note that Alembic does not run at startup in this project (``init_db`` uses
``create_all`` plus ``run_schema_self_heal``), so a deployed database needs
``alembic upgrade head`` for the old rows to be rewritten. Until then those rows
simply do not map to an event type — they are history, not live behaviour.
"""

from alembic import op
from sqlalchemy import text

revision = "20260812_span_kasal"
down_revision = "20260810_exec_crew_id"
branch_labels = None
depends_on = None

#: The names this codebase emitted under the vendored library, without prefix.
_RENAMED = (
    "crew.kickoff",
    "crew.complete",
    "task.execute",
    "task.complete",
    "task.fail",
    "agent.execute",
    "agent.complete",
    "tool.execute",
    "tool.complete",
    "tool.error",
    "llm.call",
    "llm.complete",
)


def _rewrite(from_prefix: str, to_prefix: str) -> None:
    """Move the listed spans from one prefix to the other.

    Name by name rather than a prefix wildcard: a blanket
    ``replace(span_name, 'kasal.', 'CrewAI.')`` on the way back down would
    rename the forty-odd spans that were always ``kasal.*``, inventing history
    that never existed.
    """
    connection = op.get_bind()
    for suffix in _RENAMED:
        connection.execute(
            text("UPDATE execution_trace SET span_name = :new WHERE span_name = :old"),
            {"new": f"{to_prefix}{suffix}", "old": f"{from_prefix}{suffix}"},
        )


def upgrade() -> None:
    _rewrite("CrewAI.", "kasal.")


def downgrade() -> None:
    _rewrite("kasal.", "CrewAI.")
