"""Turn the raw UC prompt-registry PERMISSION_DENIED into an actionable message.

The MLflow Prompt Registry stores a prompt as a Unity Catalog FUNCTION, which
needs CREATE FUNCTION + EXECUTE + MANAGE on the target schema. The trap: UC's
``GRANT ALL PRIVILEGES`` deliberately EXCLUDES ``MANAGE`` (privilege-escalation
guard), so an app service principal with ``ALL PRIVILEGES`` still gets a 403 and
``SHOW GRANTS`` hides the gap. Rather than surface a raw stack trace, translate
it into the exact GRANT to run — resolved to the catalog/schema actually in use.
"""

from __future__ import annotations


def is_permission_denied(exc: Exception) -> bool:
    text = str(exc)
    return "PERMISSION_DENIED" in text or "Permission denied" in text


def prompt_registry_grant_hint(prompt_name: str) -> str:
    """Actionable fix for a UC prompt-registry permission denial.

    ``prompt_name`` is the three-level ``catalog.schema.prompt`` name, so the
    catalog and schema in the message are the real ones.
    """
    parts = prompt_name.split(".")
    schema = ".".join(parts[:2]) if len(parts) >= 2 else "<catalog>.<schema>"
    catalog = parts[0] if parts else "<catalog>"
    return (
        f"The app's service principal cannot write to the MLflow prompt registry "
        f"in Unity Catalog schema '{schema}'. This is almost always a missing "
        f"MANAGE privilege: GRANT ALL PRIVILEGES does NOT include MANAGE, which "
        f"the prompt registry requires. As a catalog admin, grant the app SP "
        f"(the client_id shown in the error): "
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `<app-sp-application-id>`; "
        f"GRANT USE SCHEMA, CREATE FUNCTION, EXECUTE, MANAGE ON SCHEMA {schema} "
        f"TO `<app-sp-application-id>`;  (or ALTER SCHEMA {schema} OWNER TO the "
        f"SP). See docs: prompt-optimization-setup."
    )
