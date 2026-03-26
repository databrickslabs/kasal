"""
Power BI DAX Executor Tool for CrewAI

Executes a pre-configured DAX query directly against a Power BI semantic model.
Unlike the DAX Generator, this tool does not use an LLM — it takes a DAX EVALUATE
statement that the user has already written and executes it via the Execute Queries API.

Author: Kasal Team
Date: 2026
"""

import asyncio
import contextvars
import logging
from typing import Any, Dict, Optional, Type
from concurrent.futures import ThreadPoolExecutor

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr
import httpx

from src.engines.crewai.tools.custom.powerbi_auth_utils import get_powerbi_access_token

logger = logging.getLogger(__name__)

_EXECUTOR = ThreadPoolExecutor(max_workers=5)


def _run_async_in_sync_context(coro):
    """
    Safely run an async coroutine from a synchronous context.
    Handles nested event loop scenarios (e.g., FastAPI).
    Propagates contextvars (like execution_id) to worker threads.
    """
    try:
        asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        future = _EXECUTOR.submit(ctx.run, asyncio.run, coro)
        return future.result()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class PowerBIDaxExecutorSchema(BaseModel):
    """Input schema for PowerBIDaxExecutorTool."""

    workspace_id: Optional[str] = Field(
        None,
        description="[Power BI] Workspace ID (GUID) containing the semantic model."
    )
    dataset_id: Optional[str] = Field(
        None,
        description="[Power BI] Dataset/Semantic Model ID (GUID) to execute the DAX query against."
    )
    dax_query: Optional[str] = Field(
        None,
        description="[Required] The DAX EVALUATE statement to execute against the Power BI semantic model."
    )

    # Auth fields
    auth_method: Optional[str] = Field(
        None,
        description="Authentication method: 'service_principal', 'service_account', or 'user_oauth'."
    )
    tenant_id: Optional[str] = Field(None, description="Azure AD tenant ID.")
    client_id: Optional[str] = Field(None, description="Azure AD application/client ID.")
    client_secret: Optional[str] = Field(None, description="Client secret for service principal auth.")
    username: Optional[str] = Field(None, description="Username/UPN for service account auth.")
    password: Optional[str] = Field(None, description="Password for service account auth.")
    access_token: Optional[str] = Field(None, description="Pre-obtained OAuth access token.")

    # Output options
    output_format: Optional[str] = Field(
        "markdown",
        description="Output format: 'markdown' (default), 'json', or 'table'."
    )
    max_rows: Optional[int] = Field(
        1000,
        description="Maximum number of rows to return (default 1000)."
    )


class PowerBIDaxExecutorTool(BaseTool):
    """
    Executes a pre-configured DAX query directly against a Power BI semantic model.

    Takes a DAX EVALUATE statement the user has already written and executes it
    via the Power BI Execute Queries API. No LLM is involved.
    Supports Service Principal, Service Account, and User OAuth authentication.
    """

    name: str = "Power BI DAX Executor"
    description: str = (
        "Executes a pre-configured DAX EVALUATE statement directly against a Power BI semantic model "
        "via the Execute Queries API. Accepts workspace ID, dataset ID, authentication credentials, "
        "and a DAX EVALUATE statement. Returns results as a formatted markdown table or JSON. "
        "No LLM required — use when you already have a working DAX query and want to run it against Power BI."
    )
    args_schema: Type[BaseModel] = PowerBIDaxExecutorSchema

    _default_config: Dict[str, Any] = PrivateAttr(default_factory=dict)

    def __init__(self, **kwargs):
        config_keys = [
            "workspace_id", "dataset_id", "dax_query",
            "auth_method", "tenant_id", "client_id", "client_secret",
            "username", "password", "access_token",
            "output_format", "max_rows",
        ]
        default_config = {k: kwargs.pop(k, None) for k in config_keys}
        super().__init__(**kwargs)
        self._default_config = {k: v for k, v in default_config.items() if v is not None}

    def _run(self, **kwargs) -> str:
        """Synchronous entry point — delegates to async _execute."""
        return _run_async_in_sync_context(self._execute(kwargs))

    async def _execute(self, kwargs: Dict[str, Any]) -> str:
        """Async execution: merge config, auth, execute DAX, format output."""
        # Merge runtime kwargs with defaults (runtime kwargs take priority)
        config = {**self._default_config, **{k: v for k, v in kwargs.items() if v is not None}}

        workspace_id = config.get("workspace_id", "")
        dataset_id = config.get("dataset_id", "")
        dax_query = config.get("dax_query", "")
        output_format = config.get("output_format", "markdown")
        max_rows = int(config.get("max_rows", 1000))

        if not workspace_id:
            return "Error: workspace_id is required."
        if not dataset_id:
            return "Error: dataset_id is required."
        if not dax_query:
            return "Error: dax_query is required."

        # Authenticate
        try:
            access_token = await get_powerbi_access_token(
                tenant_id=config.get("tenant_id"),
                client_id=config.get("client_id"),
                client_secret=config.get("client_secret"),
                access_token=config.get("access_token"),
                username=config.get("username"),
                password=config.get("password"),
                auth_method=config.get("auth_method"),
            )
        except Exception as e:
            return f"Error obtaining Power BI access token: {e}"

        # Execute DAX
        result = await self._execute_dax_query(workspace_id, dataset_id, access_token, dax_query)

        if not result["success"]:
            return f"DAX execution failed: {result.get('error', 'Unknown error')}"

        rows = result["data"]
        columns = result["columns"]

        if not rows:
            return "DAX query executed successfully but returned no rows."

        # Cap at max_rows
        if len(rows) > max_rows:
            rows = rows[:max_rows]
            truncated = True
        else:
            truncated = False

        # Format output
        output = self._format_output(rows, columns, output_format)

        if truncated:
            output += f"\n\n_Results truncated to {max_rows} rows._"

        return output

    async def _execute_dax_query(
        self, workspace_id: str, dataset_id: str, access_token: str, dax_query: str
    ) -> Dict[str, Any]:
        """Execute DAX query via Power BI Execute Queries API."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        payload = {"queries": [{"query": dax_query}], "serializerSettings": {"includeNulls": True}}
        result: Dict[str, Any] = {"success": False, "data": [], "row_count": 0, "columns": [], "error": None}

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    result["error"] = data["error"].get("message", str(data["error"]))
                    return result
                tables = data.get("results", [{}])[0].get("tables", [])
                if tables:
                    rows = tables[0].get("rows", [])
                    result["data"] = rows
                    result["row_count"] = len(rows)
                    result["success"] = True
                    if rows:
                        result["columns"] = list(rows[0].keys())
                return result
            except httpx.HTTPStatusError as e:
                result["error"] = f"HTTP {e.response.status_code}: {e.response.text}"
                return result
            except Exception as e:
                result["error"] = str(e)
                return result

    def _format_output(self, rows: list, columns: list, output_format: str) -> str:
        """Format query results as markdown table, JSON, or plain table."""
        import json as _json

        if output_format == "json":
            return _json.dumps(rows, indent=2, default=str)

        # Markdown or table (both produce a markdown table)
        if not columns:
            return "No columns returned."

        # Clean column names (Power BI returns "[TableName].[ColumnName]" style)
        clean_cols = [c.split("].[")[-1].rstrip("]") if "].[" in c else c for c in columns]

        header = "| " + " | ".join(clean_cols) + " |"
        separator = "| " + " | ".join(["---"] * len(clean_cols)) + " |"
        data_rows = []
        for row in rows:
            cells = [str(row.get(col, "")) for col in columns]
            data_rows.append("| " + " | ".join(cells) + " |")

        return "\n".join([header, separator] + data_rows)
