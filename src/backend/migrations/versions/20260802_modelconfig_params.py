"""modelconfig — somewhere to declare what gets sent with a request

Revision ID: 20260802_mc_params
Revises: 20260801_flow_group
Create Date: 2026-08-02

The transport has always declared ``top_p``, ``frequency_penalty``,
``presence_penalty``, ``stop`` and an ``additional_params`` escape hatch, and has
always forwarded every one of them that is set. Nothing set any of them: the
model catalogue could express exactly two knobs, ``temperature`` and
``max_output_tokens``, so influencing anything else meant editing Python inside a
provider handler. That is what made every sampling fix per-model — and it is why
a degenerate-output incident had no configuration-level answer at all.

``params`` is that declaration. Keys are sent as written, so an OpenAI-standard
name goes top level (``{"top_p": 0.8}``) and a provider-only knob goes under
``extra_body`` (``{"extra_body": {"repetition_penalty": 1.05, "top_k": 20}}``) —
the OpenAI SDK strips unknown top-level kwargs client-side, so vLLM's extra
samplers are reachable no other way.

``unsupported_params`` is the other half. There is no litellm ``drop_params``
net on this path: what is set IS sent, and a stray ``frequency_penalty`` is a
400 on OpenAI's reasoning models. That question used to be answered by matching
substrings of the model NAME in three separate files, which disagreed with each
other; it now lives beside the model it describes.

Both nullable, both empty by default. A model that declares neither sends
exactly what it sent before this migration — deliberately, because a sampling
default applied to models nobody measured it on is how you fix one workload and
break another. Measured: ``frequency_penalty=0.3`` cured a repeating 25-item
list AND turned a 12-row markdown table from 681 characters into 9679 and a
truncation, because a table legitimately repeats its separator row.

Note that Alembic does not run at startup in this project — ``init_db`` uses
``create_all`` plus ``run_schema_self_heal``. ``_ensure_modelconfig_columns``
there is what actually heals a deployed database; this migration keeps the
declared schema honest for anyone who does run the chain.
"""

import sqlalchemy as sa
from alembic import op

revision = "20260802_mc_params"
down_revision = "20260801_flow_group"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("modelconfig") as batch:
        batch.add_column(sa.Column("params", sa.JSON(), nullable=True))
        batch.add_column(sa.Column("unsupported_params", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("modelconfig") as batch:
        batch.drop_column("unsupported_params")
        batch.drop_column("params")
