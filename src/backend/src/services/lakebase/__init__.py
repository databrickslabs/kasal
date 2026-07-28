"""
Lakebase: the managed Postgres the app can hot-swap onto.

Provisioning, connection/auth, schema and migrations, per-group permissions,
plus the export/import of database contents to Databricks volumes
(``management``).

Subprocess note: a spawned interpreter must re-activate Lakebase itself
(``db.database_router.activate_lakebase_in_subprocess``) — it is not inherited
from the parent's hot-swap.
"""
