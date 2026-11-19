"""Power BI / Fabric / Entra ids as URL path segments.

Workspace, dataset, report and tenant ids reach these calls from tool arguments (an agent
fills them in), from configuration and from request bodies, and every call carries a bearer
token or a credential. An id holding ``/``, ``?``, ``#``, ``%`` or ``..`` re-targets the
request: ``../../groups/<other>/datasets/<other>`` runs the query in another workspace. Only
the characters of a GUID or a tenant domain pass.

Pure functions: no I/O.
"""

from __future__ import annotations

import re

_PATH_ID = re.compile(r"[A-Za-z0-9-]{1,64}(?:\.[A-Za-z0-9-]{1,64}){0,8}")
FABRIC_API = "https://api.fabric.microsoft.com/v1"
POWERBI_API = "https://api.powerbi.com/v1.0/myorg"


def path_id(value: object, name: str = "id") -> str:
    """``value`` when it can only be one URL path segment; raises ``ValueError`` otherwise."""
    if not isinstance(value, str) or not _PATH_ID.fullmatch(value):
        raise ValueError(f"{name} is not a valid Power BI / Entra id: {value!r}")
    return value


def execute_queries_url(dataset_id: object, workspace_id: object = None) -> str:
    """The executeQueries endpoint of a dataset, in a workspace or in "My workspace"."""
    group = f"/groups/{path_id(workspace_id, 'workspace_id')}" if workspace_id else ""
    return f"{POWERBI_API}{group}/datasets/{path_id(dataset_id, 'dataset_id')}/executeQueries"


def semantic_model_definition_url(workspace_id: object, dataset_id: object) -> str:
    return (
        f"{FABRIC_API}/workspaces/{path_id(workspace_id, 'workspace_id')}"
        f"/semanticModels/{path_id(dataset_id, 'dataset_id')}/getDefinition?format=TMDL"
    )


def report_definition_url(workspace_id: object, report_id: object) -> str:
    return (
        f"{FABRIC_API}/workspaces/{path_id(workspace_id, 'workspace_id')}"
        f"/reports/{path_id(report_id, 'report_id')}/getDefinition"
    )


def token_url(tenant_id: object) -> str:
    """The Entra token endpoint of a tenant (the request carries the secret or password)."""
    return (
        f"https://login.microsoftonline.com/{path_id(tenant_id, 'tenant_id')}"
        "/oauth2/v2.0/token"
    )
