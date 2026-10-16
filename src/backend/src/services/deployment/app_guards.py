"""Guards for the one-click Databricks Apps deploy.

Two things the deploy must not do on a user's say-so:

* **Put an unchecked name into SQL.** The deploy grants the app's service
  principal the Unity Catalog privileges its trace tables need, on a catalog
  and schema the user typed or the workspace configured, and runs that as the
  teamspace's PAT owner. A name reaches the GRANT only after it passes the
  rules below, and then only backtick-quoted with any backtick doubled.
* **Replace an app it did not create.** Every app this flow creates carries a
  description naming the crew that deployed it. An existing app with that
  name is redeployed only when it came from the same crew, or from a Kasal
  deploy made before the crew id was recorded.
"""

import logging
import re
from typing import List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound

from src.core.exceptions import BadRequestError, ConflictError

logger = logging.getLogger(__name__)

# Catalog and schema names: letters, digits, underscores and hyphens, up to 255
# characters. Unity Catalog accepts more inside backticks, but nothing a deploy
# needs, and these names are also written into app.yaml and table names.
_OBJECT_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,254}")
# The grantee: the app's service principal application id (a UUID). Allow the
# characters of a user or service principal name, but no quotes or spaces.
_PRINCIPAL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._@+-]{0,254}")

KASAL_DESCRIPTION = "Deployed from Kasal"


def validate_uc_name(value: str, kind: str) -> str:
    """Return ``value`` when it is a valid catalog or schema name, else refuse."""
    if not _OBJECT_NAME.fullmatch(value or ""):
        raise BadRequestError(
            f"Invalid Unity Catalog {kind} name. Use letters, digits, "
            "underscores and hyphens (at most 255 characters)."
        )
    return value


def validate_principal(value: str) -> str:
    """Return ``value`` when it is a valid grantee name, else refuse."""
    if not _PRINCIPAL.fullmatch(value or ""):
        raise BadRequestError("Invalid Unity Catalog principal name.")
    return value


def quote_uc_ident(name: str) -> str:
    """Backtick-quote one identifier, doubling any backtick inside it."""
    return "`" + name.replace("`", "``") + "`"


def trace_grant_statements(catalog: str, schema: str, principal: str) -> List[str]:
    """The GRANTs that let ``principal`` create and write its trace tables."""
    cat = quote_uc_ident(validate_uc_name(catalog, "catalog"))
    sch = quote_uc_ident(validate_uc_name(schema, "schema"))
    who = quote_uc_ident(validate_principal(principal))
    return [
        f"GRANT USE CATALOG ON CATALOG {cat} TO {who}",
        "GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA "
        f"{cat}.{sch} TO {who}",
    ]


def app_description(crew_id: str) -> str:
    """The description that records which crew deployed an app."""
    return f"{KASAL_DESCRIPTION} (crew {crew_id})" if crew_id else KASAL_DESCRIPTION


def overwrite_refusal(
    description: Optional[str], crew_id: str, app_name: str
) -> Optional[str]:
    """None when ``crew_id`` may redeploy over the existing app, else why not."""
    text = description if isinstance(description, str) else ""
    if text in (app_description(crew_id), KASAL_DESCRIPTION):
        return None
    if text.startswith(KASAL_DESCRIPTION):
        return (
            f"App '{app_name}' was deployed from another crew. "
            "Choose another app name."
        )
    return (
        f"App '{app_name}' already exists and was not deployed from Kasal. "
        "Choose another app name."
    )


def refuse_foreign_app(client: WorkspaceClient, app_name: str, crew_id: str) -> None:
    """Raise ConflictError when ``app_name`` exists and this crew did not deploy it.

    Blocking (one SDK call): run it off the event loop. An app that does not
    exist, or that this PAT cannot read, is left to the deploy step, where
    creating over an unreadable app fails with an error.
    """
    try:
        existing = client.apps.get(name=app_name)
    except NotFound:
        return
    except DatabricksError as exc:
        logger.info("Could not read app %s before deploy: %s", app_name, exc)
        return
    reason = overwrite_refusal(existing.description, crew_id, app_name)
    if reason:
        raise ConflictError(reason)
