"""Refuse the Lakebase operations that a Databricks App installation owns.

With the native ``lakebase`` resource bound, the installation chooses the
database and Kasal already runs on it, in schema ``kasal``. A manual connection
would point Kasal somewhere else, and connect-and-migrate with "recreate" runs
``DROP SCHEMA kasal CASCADE`` on the very schema the app is using. Both are
refused with a 400 before any connection is made.

The check is ``LakebaseAppResource.from_env()``, the one ``save_config`` has
always used, so every entry point agrees on what "installation-managed" means.
"""

from typing import Optional

from src.core.databricks_app import LakebaseAppResource
from src.core.exceptions import BadRequestError

MANAGED_BY_INSTALLATION = (
    "Lakebase is managed through the Databricks App installation resources"
)

MIGRATION_REFUSED = (
    "Migrating to Lakebase is disabled on this installation: Kasal already runs "
    "on the database attached as the app's 'lakebase' resource, and a migration "
    "would drop and replace its 'kasal' schema. To bring data from an older "
    "install, migrate it before upgrading; see 'Upgrading from v1.3.0' in the docs."
)

SCHEMA_DROP_REFUSED = (
    "Refusing to drop the 'kasal' schema: it belongs to the database attached as "
    "this app's 'lakebase' resource."
)


def is_installation_managed() -> bool:
    """Whether the Databricks App installation binds the Lakebase database."""
    return LakebaseAppResource.from_env() is not None


def refuse_when_installation_managed(
    message: Optional[str] = None,
) -> None:
    """Raise BadRequestError (400) when the installation binds the database."""
    if is_installation_managed():
        raise BadRequestError(message or MANAGED_BY_INSTALLATION)
