"""ui_config — per-component toggles instead of hand-written catalog JSON

Revision ID: 20260806_uicfg_toggles
Revises: 20260802_mc_params
Create Date: 2026-08-06

Restricting which components agents may emit had exactly two usable settings —
"full" and "minimal" — and anything in between meant `catalog_type="custom"` and
pasting the whole catalog as JSON into a textarea. That is a poor trade for
"turn off Quiz": it is easy to get wrong, and being a frozen snapshot it does not
pick up components added to A2UI afterwards, so a workspace silently loses access
to every new component until someone re-pastes it.

`disabled_components` supports a `catalog_type="select"` mode that stores the
EXCLUSIONS instead. Everything is enabled by default, the admin ticks off what
they do not want, and the set therefore GROWS with the product rather than
freezing. `custom` stays for anyone already using it.

Nullable, empty by default: an existing row keeps whatever catalog_type it has
and behaves exactly as before this migration.

Note that Alembic does not run at startup in this project — ``init_db`` uses
``create_all`` plus ``run_schema_self_heal``. ``_ensure_ui_config_columns`` in
``src/db/session.py`` is what actually heals a deployed database; this migration
keeps the declared schema honest for anyone who does run the chain.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260806_uicfg_toggles"
down_revision = "20260802_mc_params"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ui_config",
        sa.Column("disabled_components", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("ui_config", "disabled_components")
