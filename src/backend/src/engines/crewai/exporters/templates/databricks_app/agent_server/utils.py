"""Databricks integration helpers for the agent server.

These are framework-agnostic: ``get_user_workspace_client`` builds a
WorkspaceClient from the forwarded user token (OBO) so tools/MCP run as the
requesting user, and ``build_mcp_url`` turns a relative Databricks-managed MCP
path into a fully-qualified URL.
"""

import logging
from typing import Optional

from databricks.sdk import WorkspaceClient
from mlflow.genai.agent_server import get_request_headers
from mlflow.types.responses import ResponsesAgentRequest


def get_session_id(request: ResponsesAgentRequest) -> Optional[str]:
    if request.context and request.context.conversation_id:
        return request.context.conversation_id
    if request.custom_inputs and isinstance(request.custom_inputs, dict):
        return request.custom_inputs.get("session_id")
    return None


def get_databricks_host(workspace_client: Optional[WorkspaceClient] = None) -> Optional[str]:
    workspace_client = workspace_client or WorkspaceClient()
    try:
        return workspace_client.config.host
    except Exception as e:  # noqa: BLE001
        logging.exception(f"Error getting databricks host from env: {e}")
        return None


def build_mcp_url(path: str, workspace_client: Optional[WorkspaceClient] = None) -> str:
    if not path.startswith("/"):
        return path
    hostname = get_databricks_host(workspace_client)
    return f"{hostname}{path}"


def get_user_workspace_client() -> WorkspaceClient:
    """Authenticate as the requesting user via the forwarded OBO access token,
    falling back to the app's service principal when no user token is available.

    Databricks Apps injects ``x-forwarded-access-token`` on each request; the
    MLflow AgentServer surfaces it through ``get_request_headers()``. That token
    is only present on the request thread — when work runs off it (e.g. a crew
    kickoff in ``asyncio.to_thread``) the header is absent, so we must NOT build a
    ``WorkspaceClient(token=None, auth_type="pat")`` (which has no valid auth and
    breaks MCP/Genie calls). Instead fall back to the default client, which uses
    the app's service-principal OAuth (``DATABRICKS_CLIENT_ID``/``SECRET`` that
    Databricks Apps injects).
    """
    try:
        token = get_request_headers().get("x-forwarded-access-token")
    except Exception:  # noqa: BLE001 - off the request thread / no context
        token = None
    if token:
        return WorkspaceClient(token=token, auth_type="pat")
    return WorkspaceClient()
