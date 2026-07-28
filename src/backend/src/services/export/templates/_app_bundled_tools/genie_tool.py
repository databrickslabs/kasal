"""Self-contained GenieTool for an exported Kasal Databricks App.

Talks to the Databricks Genie Conversations API through the Databricks SDK
(``WorkspaceClient.genie``), which handles the start-conversation /
poll-until-complete loop. Authentication prefers the requesting user's
forwarded OBO token (so Genie runs as the user and respects their table grants)
and falls back to the app's service principal.

This module is intentionally standalone — it has no Kasal (``src.*``)
dependencies — so it runs unchanged inside the deployed app. The Genie space is
taken from the ``GENIE_SPACE_ID`` env var or the ``space_id`` baked in by Kasal
at export time.
"""

import os
from typing import Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr


class _GenieInput(BaseModel):
    question: str = Field(
        ...,
        description="A natural-language question about the data in the Genie space.",
    )


class GenieTool(BaseTool):
    """Ask a Databricks Genie space a natural-language question about its data."""

    name: str = "GenieTool"
    description: str = (
        "Ask a natural-language question about the data in a Databricks Genie "
        "space. Genie translates the question to SQL, runs it against the "
        "configured tables, and returns the answer plus the result table."
    )
    args_schema: Type[BaseModel] = _GenieInput

    _space_id: Optional[str] = PrivateAttr(default=None)
    _max_result_rows: int = PrivateAttr(default=200)
    _conversation_id: Optional[str] = PrivateAttr(default=None)

    def __init__(
        self,
        space_id: Optional[str] = None,
        max_result_rows: int = 200,
        result_as_answer: bool = False,
        **_kwargs,
    ):
        super().__init__(result_as_answer=result_as_answer)
        space = space_id or os.environ.get("GENIE_SPACE_ID") or None
        if isinstance(space, list):
            space = space[0] if space else None
        self._space_id = space
        self._max_result_rows = max_result_rows or 200

    def _client(self):
        """Workspace client as the requesting user (OBO) if possible, else the app SP."""
        try:
            from agent_server.utils import get_user_workspace_client

            return get_user_workspace_client()
        except Exception:  # noqa: BLE001
            from databricks.sdk import WorkspaceClient

            return WorkspaceClient()

    def _run(self, question: str) -> str:
        if not self._space_id:
            return (
                "GenieTool is not configured: set the GENIE_SPACE_ID environment "
                "variable (or pass space_id) to a Databricks Genie space id."
            )
        try:
            w = self._client()
            if self._conversation_id:
                msg = w.genie.create_message_and_wait(
                    self._space_id, self._conversation_id, question
                )
            else:
                msg = w.genie.start_conversation_and_wait(self._space_id, question)
                self._conversation_id = getattr(msg, "conversation_id", None)
            return self._format(w, msg)
        except Exception as exc:  # noqa: BLE001
            return f"Genie query failed: {exc}"

    def _format(self, w, msg) -> str:
        parts = []
        message_id = getattr(msg, "message_id", None) or getattr(msg, "id", None)
        for att in getattr(msg, "attachments", None) or []:
            text = getattr(att, "text", None)
            if text is not None and getattr(text, "content", None):
                parts.append(str(text.content))
            query = getattr(att, "query", None)
            if query is not None:
                if getattr(query, "description", None):
                    parts.append(f"What the query does:\n{query.description}")
                if getattr(query, "query", None):
                    parts.append(f"SQL:\n{query.query}")
                try:
                    res = w.genie.get_message_attachment_query_result(
                        msg.space_id,
                        msg.conversation_id,
                        message_id,
                        att.attachment_id,
                    )
                    table = self._render(res)
                    if table:
                        parts.append(table)
                except Exception as exc:  # noqa: BLE001
                    parts.append(f"(Could not fetch the query result: {exc})")
        answer = "\n\n".join(p for p in parts if p)
        return answer or (getattr(msg, "content", None) or "Genie returned no answer.")

    def _render(self, res) -> str:
        sr = getattr(res, "statement_response", None)
        if sr is None:
            return ""
        manifest = getattr(sr, "manifest", None)
        result = getattr(sr, "result", None)
        cols = []
        schema = getattr(manifest, "schema", None) if manifest else None
        if schema is not None and getattr(schema, "columns", None):
            cols = [getattr(c, "name", "") for c in schema.columns]
        rows = getattr(result, "data_array", None) or [] if result is not None else []
        if not rows:
            return ""
        rows = rows[: self._max_result_rows]
        ncols = len(cols) or len(rows[0])
        header_cells = cols or [f"col{i}" for i in range(ncols)]

        def _cell(value) -> str:
            return str("" if value is None else value).replace("|", "\\|").replace("\n", " ")

        header = "| " + " | ".join(header_cells) + " |"
        sep = "| " + " | ".join("---" for _ in header_cells) + " |"
        body = "\n".join("| " + " | ".join(_cell(c) for c in r) + " |" for r in rows)
        return "Query Results:\n" + "\n".join([header, sep, body])
