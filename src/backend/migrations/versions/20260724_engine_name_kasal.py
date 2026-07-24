"""Rename engine_name 'crewai' to 'kasal' (native engine rebrand)

The kasal engine replaced crewAI; engineconfig rows keyed engine_name='crewai'
must follow so lookups keyed on 'kasal' find them. The same heal also runs
idempotently at app startup (src/db/session.py::_heal_engine_config_names)
because deployed apps self-heal their schema instead of running alembic.

Revision ID: 20260724_engine_name_kasal
Revises: 20260720_powerbi_extraction
Create Date: 2026-07-24 22:30:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20260724_engine_name_kasal'
down_revision = '20260720_powerbi_extraction'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "UPDATE engineconfig SET engine_name = 'kasal' WHERE engine_name = 'crewai'"
    )


def downgrade():
    op.execute(
        "UPDATE engineconfig SET engine_name = 'crewai' WHERE engine_name = 'kasal'"
    )
