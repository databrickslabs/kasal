"""Gmail tool — reads the user's Gmail through the Unity Catalog connections proxy.

Calls ``{workspace}/api/2.0/unity-catalog/connections/{connection}/proxy/...``
with the run's OBO user token. Databricks injects the per-user Google OAuth
credential stored on the system-managed connection (default:
``system_ai_agent_gmail``) — this code never sees Google tokens. This is the
documented "UC connections proxy" pattern for agent tools; SQL
``http_request()`` is explicitly blocked for these per-user connections.

SECURITY — OBO ONLY, PERSONAL WORKSPACE ONLY: the proxy resolves the Google
credential FROM the calling Databricks identity, so the credential MUST belong
to the requesting user.

1. Only the on-behalf-of (OBO) user token is genuinely per-user in Kasal — the
   auth chain's "PAT" is a GROUP-SHARED token (loaded by group_id) or an
   SPN-derived env token, both of which map every caller in a group to ONE
   mailbox. So this tool uses the request's OBO token and nothing else.

2. The tool runs ONLY in the caller's PERSONAL workspace (the group whose id
   is generate_individual_group_id(user_email)). In a SHARED workspace a crew
   — and its emitted email content — is visible to other members, so reading
   one member's inbox there would leak personal mail to the group. Outside the
   personal workspace the tool refuses to run.

Requirements for the calling user:
- ``USE CONNECTION`` on the connection, and
- a one-time Google authorization on the connection page
  (Catalog → Connections → the connection → login).

Proxy limitation (verified live): query strings are folded into the forwarded
path and the upstream 404s, so NO endpoint here uses query parameters — the
message list is fetched plain (newest first) and filtered client-side.
"""
import asyncio
import base64
import logging
import re
from typing import Any, Dict, List, Optional, Type

import aiohttp
from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr, field_validator

from src.utils.telemetry import get_user_agent_header, KasalProduct

logger = logging.getLogger(__name__)

# The Gmail list endpoint returns 100 ids per page; reading each message is a
# separate proxy round-trip, so cap how many full messages one call may fetch.
_MAX_RESULTS_CAP = 25

# Gmail message ids are URL-safe hex/base64 tokens. The id is LLM-supplied and
# lands in the proxy URL path — without this gate a crafted value like
# "../../../api/2.0/…" would path-traverse OUT of the Gmail proxy and replay
# the caller's Databricks token against an arbitrary workspace API.
_MESSAGE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class GmailToolInput(BaseModel):
    """Input schema for the Gmail tool."""

    action: str = Field(
        "search",
        description=(
            "'search' lists the most recent emails (newest first) with sender, "
            "subject, date and a snippet; 'read' fetches ONE email's full body. "
        ),
    )
    message_id: Optional[str] = Field(
        None,
        description="The Gmail message id to read (required when action='read').",
    )
    max_results: int = Field(
        10,
        description=f"How many recent emails 'search' returns (1-{_MAX_RESULTS_CAP}).",
    )
    days: Optional[int] = Field(
        None,
        description=(
            "Optionally restrict 'search' to emails received in the last N days "
            "(e.g. 1 = today's emails)."
        ),
    )

    @field_validator("action", mode="before")
    @classmethod
    def normalize_action(cls, value: Any) -> str:
        action = str(value or "search").strip().lower()
        return action if action in ("search", "read") else "search"


def _decode_base64url(data: str) -> str:
    """Decode Gmail's base64url message bodies (padding-tolerant)."""
    try:
        padded = data + "=" * (-len(data) % 4)
        return base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _extract_body_text(payload: Dict[str, Any]) -> str:
    """The first text part of a Gmail message payload, walking MIME parts."""
    if not isinstance(payload, dict):
        return ""
    mime_type = str(payload.get("mimeType", ""))
    body_data = (payload.get("body") or {}).get("data")
    if mime_type.startswith("text/plain") and body_data:
        return _decode_base64url(body_data)
    for part in payload.get("parts") or []:
        text = _extract_body_text(part)
        if text:
            return text
    # No text/plain anywhere — fall back to the first text/* part (e.g. html).
    if mime_type.startswith("text/") and body_data:
        return _decode_base64url(body_data)
    return ""


def _header_map(payload: Dict[str, Any]) -> Dict[str, str]:
    """Gmail's header list as a case-insensitive-keyed dict (lowercased)."""
    return {
        str(h.get("name", "")).lower(): str(h.get("value", ""))
        for h in (payload.get("headers") or [])
        if isinstance(h, dict)
    }


class GmailTool(BaseTool):
    name: str = "Gmail"
    description: str = (
        "Read the user's Gmail inbox. action='search' lists the most recent "
        "emails (sender, subject, date, snippet and the message_id); "
        "action='read' with a message_id returns one email's full content. "
        "Results are read-only and scoped to the user running the crew."
    )
    aliases: List[str] = ["GmailTool", "Email", "Inbox"]
    args_schema: Type[BaseModel] = GmailToolInput

    _connection_name: str = PrivateAttr(default="system_ai_agent_gmail")
    _timeout: int = PrivateAttr(default=60)
    _tool_id: Optional[int] = PrivateAttr(default=None)
    _user_token: Optional[str] = PrivateAttr(default=None)
    _group_id: Optional[str] = PrivateAttr(default=None)
    _user_email: Optional[str] = PrivateAttr(default=None)

    def __init__(
        self,
        tool_config: Optional[dict] = None,
        tool_id: Optional[int] = None,
        user_token: Optional[str] = None,
        group_id: Optional[str] = None,
        user_email: Optional[str] = None,
        result_as_answer: bool = False,
    ):
        super().__init__(result_as_answer=result_as_answer)
        tool_config = tool_config or {}
        if tool_id is not None:
            self._tool_id = tool_id
        if user_token:
            self._user_token = user_token
        if group_id:
            self._group_id = group_id
        if user_email:
            self._user_email = user_email
        if tool_config.get("connection_name"):
            self._connection_name = str(tool_config["connection_name"])
        if tool_config.get("timeout"):
            self._timeout = int(tool_config["timeout"])
        logger.info(
            f"GmailTool configured: connection={self._connection_name}, "
            f"timeout={self._timeout}s, has_user_token={bool(self._user_token)}, "
            f"group_id={self._group_id}"
        )

    def _is_personal_workspace(self) -> bool:
        """True only when the active group IS the caller's personal workspace.

        Email must never be read in a shared workspace, where crews and their
        output are visible to other members.
        """
        if not self._group_id or not self._user_email:
            return False
        try:
            from src.utils.user_context import GroupContext

            personal = GroupContext.generate_individual_group_id(self._user_email)
        except Exception:
            return False
        return self._group_id.lower() == personal.lower()

    # ------------------------------------------------------------------
    # Auth + proxy plumbing
    # ------------------------------------------------------------------

    _AUTH_REQUIRED_MESSAGE = (
        "Gmail requires YOUR own Databricks identity (an on-behalf-of user "
        "token). It deliberately does NOT fall back to shared credentials: "
        "the Google account is resolved from the calling identity, and Kasal's "
        "PAT/service-principal credentials are group-shared, so a fallback "
        "would expose one user's mailbox to everyone in the group. Run this on "
        "the deployed app, where each request carries your identity."
    )

    _SHARED_WORKSPACE_MESSAGE = (
        "Gmail is available only in your personal workspace. In a shared "
        "workspace, crews and their output are visible to other members, so "
        "reading your inbox there would expose your personal mail to the "
        "group. Switch to your personal workspace to use Gmail."
    )

    async def _get_auth(self) -> Optional[Dict[str, str]]:
        """OBO-ONLY headers. No PAT/SPN fallback — Kasal's PAT is group-shared,
        which would break per-user mailbox isolation (see module docstring)."""
        if not self._user_token:
            return None
        try:
            from src.utils.databricks_auth import get_auth_context

            # Pass ONLY the user token. get_auth_context still runs its chain,
            # so verify it actually authenticated as OBO with this token and
            # refuse anything else (a group PAT / SPN it may have fallen back
            # to maps every caller to a single mailbox).
            auth = await get_auth_context(user_token=self._user_token)
            if not auth:
                return None
            method = str(getattr(auth, "auth_method", "")).lower()
            if method != "obo":
                logger.error(
                    f"[GmailTool] Refusing '{method}' credentials — Gmail runs "
                    "ONLY with the per-user OBO token; PAT/SPN are group-shared "
                    "and would map every caller to one mailbox."
                )
                return None
            headers = auth.get_headers()
            headers.update(get_user_agent_header(KasalProduct.MCP))
            self._workspace_url = getattr(auth, "workspace_url", None)
            return headers
        except Exception as e:
            logger.error(f"[GmailTool] Error building auth headers: {e}")
            return None

    def _proxy_base(self, workspace_url: str) -> str:
        return (
            f"{workspace_url.rstrip('/')}/api/2.0/unity-catalog/connections/"
            f"{self._connection_name}/proxy"
        )

    async def _proxy_get(
        self, session: aiohttp.ClientSession, base: str, path: str, headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """GET a Gmail API path through the UC connections proxy."""
        import gzip
        import json

        url = f"{base}{path}"
        # Ask for an uncompressed body — and still sniff for gzip below: the
        # proxy can relay Google's compressed payload while DROPPING the
        # Content-Encoding header, so aiohttp won't decompress it and a naive
        # .json() dies with "can't decode byte 0x8b".
        request_headers = {**headers, "Accept-Encoding": "identity"}
        async with session.get(
            url, headers=request_headers, timeout=aiohttp.ClientTimeout(total=self._timeout)
        ) as resp:
            raw = await resp.read()
            if raw[:2] == b"\x1f\x8b":  # gzip magic
                try:
                    raw = gzip.decompress(raw)
                except Exception:
                    pass
            text = raw.decode("utf-8", errors="replace")
            if resp.status != 200:
                raise RuntimeError(
                    f"Gmail proxy returned HTTP {resp.status} for {path}: {text[:300]}"
                )
            return json.loads(text)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    async def _search(self, max_results: int, days: Optional[int]) -> str:
        if not self._is_personal_workspace():
            return self._SHARED_WORKSPACE_MESSAGE
        if not self._user_token:
            return self._AUTH_REQUIRED_MESSAGE
        headers = await self._get_auth()
        if not headers:
            return self._AUTH_REQUIRED_MESSAGE
        workspace_url = getattr(self, "_workspace_url", None)
        if not workspace_url:
            return "Error: could not resolve the Databricks workspace URL."

        base = self._proxy_base(workspace_url)
        max_results = max(1, min(int(max_results or 10), _MAX_RESULTS_CAP))
        cutoff_ms: Optional[int] = None
        if days and days > 0:
            import time

            cutoff_ms = int((time.time() - days * 86400) * 1000)

        async with aiohttp.ClientSession() as session:
            listing = await self._proxy_get(
                session, base, "/gmail/v1/users/me/messages", headers
            )
            ids = [m.get("id") for m in (listing.get("messages") or []) if m.get("id")]
            if not ids:
                return "No emails found in the inbox."

            lines: List[str] = []
            for message_id in ids[:max_results]:
                detail = await self._proxy_get(
                    session, base, f"/gmail/v1/users/me/messages/{message_id}", headers
                )
                if cutoff_ms is not None:
                    try:
                        if int(detail.get("internalDate", 0)) < cutoff_ms:
                            # The list is newest-first — everything after this
                            # is older than the window.
                            break
                    except (TypeError, ValueError):
                        pass
                hdrs = _header_map(detail.get("payload") or {})
                lines.append(
                    f"- message_id: {message_id}\n"
                    f"  from: {hdrs.get('from', '(unknown)')}\n"
                    f"  subject: {hdrs.get('subject', '(no subject)')}\n"
                    f"  date: {hdrs.get('date', '')}\n"
                    f"  snippet: {detail.get('snippet', '')}"
                )

        if not lines:
            window = f" in the last {days} day(s)" if days else ""
            return f"No emails found{window}."
        header_line = f"Most recent emails ({len(lines)}):"
        footer = "\nUse action='read' with a message_id for an email's full content."
        return header_line + "\n" + "\n".join(lines) + footer

    async def _read(self, message_id: str) -> str:
        if not self._is_personal_workspace():
            return self._SHARED_WORKSPACE_MESSAGE
        if not self._user_token:
            return self._AUTH_REQUIRED_MESSAGE
        if not _MESSAGE_ID_PATTERN.fullmatch(message_id or ""):
            return (
                "Error: invalid message_id. Use an id exactly as returned by "
                "action='search'."
            )
        headers = await self._get_auth()
        if not headers:
            return self._AUTH_REQUIRED_MESSAGE
        workspace_url = getattr(self, "_workspace_url", None)
        if not workspace_url:
            return "Error: could not resolve the Databricks workspace URL."

        base = self._proxy_base(workspace_url)
        async with aiohttp.ClientSession() as session:
            detail = await self._proxy_get(
                session, base, f"/gmail/v1/users/me/messages/{message_id}", headers
            )

        payload = detail.get("payload") or {}
        hdrs = _header_map(payload)
        body = _extract_body_text(payload) or detail.get("snippet", "")
        if len(body) > 8000:
            body = body[:8000] + "\n…[truncated]"
        return (
            f"From: {hdrs.get('from', '(unknown)')}\n"
            f"To: {hdrs.get('to', '')}\n"
            f"Subject: {hdrs.get('subject', '(no subject)')}\n"
            f"Date: {hdrs.get('date', '')}\n\n"
            f"{body}"
        )

    # ------------------------------------------------------------------
    # CrewAI entry points
    # ------------------------------------------------------------------

    async def _run_async(
        self,
        action: str = "search",
        message_id: Optional[str] = None,
        max_results: int = 10,
        days: Optional[int] = None,
    ) -> str:
        try:
            if action == "read":
                if not message_id:
                    return (
                        "Error: action='read' needs a message_id. Use "
                        "action='search' first to list emails with their ids."
                    )
                return await self._read(message_id)
            return await self._search(max_results, days)
        except Exception as e:
            logger.error(f"[GmailTool] Error: {e}")
            message = str(e)
            if "login" in message.lower() or "UNAUTHENTICATED" in message:
                return (
                    f"Gmail access is not authorized yet: {message}\n"
                    "Open the connection page in Databricks (Catalog → "
                    f"Connections → {self._connection_name}) and complete the "
                    "Google login, then retry."
                )
            return f"Error reading Gmail: {message}"

    def _run(
        self,
        action: str = "search",
        message_id: Optional[str] = None,
        max_results: int = 10,
        days: Optional[int] = None,
    ) -> str:
        """Synchronous wrapper handling CrewAI's threaded event loops."""
        import concurrent.futures

        def run_in_new_loop() -> str:
            return asyncio.run(self._run_async(action, message_id, max_results, days))

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self._run_async(action, message_id, max_results, days))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            return executor.submit(run_in_new_loop).result()
