"""
Dashboard Repository Layer

Handles all communication with the Databricks Lakeview (AI/BI Dashboard) REST API.

Auth strategy (avoids the slow SPN token-refresh path):
  Token priority:  OBO user_token → PAT from DB (via ApiKeysService) → env var
  URL priority:    already-loaded _databricks_auth host → SDK Config host → None
"""
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from src.utils.telemetry import get_user_agent_header, KasalProduct

logger = logging.getLogger(__name__)


class DashboardRepository:
    """
    Repository for interacting with the Databricks Lakeview REST API.

    Endpoint base: /api/2.0/lakeview/dashboards
    """

    def __init__(self, user_token: Optional[str] = None):
        """
        Args:
            user_token: Optional OBO user token forwarded from the HTTP request.
        """
        self._user_token = user_token
        self._host: Optional[str] = None
        self._auth_headers: Optional[Dict[str, str]] = None
        self._client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(retries=2),
            timeout=30.0,
        )

    async def _resolve_host(self) -> Optional[str]:
        """
        Resolve workspace URL without making any blocking network / OAuth calls.
        Uses only already-loaded state or config-file reads.
        """
        # 1. Already-loaded internal config (set by _load_config earlier in process lifetime)
        try:
            from src.utils.databricks_auth import _databricks_auth
            if _databricks_auth._workspace_host:
                return _databricks_auth._workspace_host.rstrip("/")
        except Exception:
            pass

        # 2. SDK Config — reads ~/.databrickscfg / env vars, no network call
        try:
            from databricks.sdk.config import Config
            sdk_cfg = Config()
            if sdk_cfg.host:
                return sdk_cfg.host.rstrip("/")
        except Exception:
            pass

        # 3. Environment variable
        env_host = os.environ.get("DATABRICKS_HOST", "")
        if env_host:
            return env_host.rstrip("/")

        return None

    async def _resolve_auth_headers(self) -> Dict[str, str]:
        """Authorization header for a Lakeview call, or {} if none could be built.

        Delegates to ``databricks_auth``, which owns the OBO -> PAT -> SPN chain.
        This used to reimplement that chain's first two steps by calling
        ApiKeysService directly — a repository reaching into a service, and a
        second copy of an auth priority order that had already drifted (it had no
        SPN step, so a workspace authenticating by service principal got an
        unauthenticated request and a 401 nobody could explain).
        """
        from src.utils.databricks_auth import get_databricks_auth_headers

        headers, error = await get_databricks_auth_headers(user_token=self._user_token)
        if error or not headers:
            logger.warning(f"[DashboardRepo] no auth headers: {error or 'none returned'}")
            return {}
        return {k: v for k, v in headers.items() if k.lower() == "authorization"}

    async def _get_headers(self) -> Dict[str, str]:
        if self._auth_headers is None:
            self._auth_headers = await self._resolve_auth_headers()
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        headers.update(self._auth_headers)
        if "Authorization" not in headers:
            logger.warning("[DashboardRepo] No auth token — request may return 401")
        headers.update(get_user_agent_header(KasalProduct.DASHBOARD))
        return headers

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    async def get_dashboard(self, dashboard_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a Lakeview dashboard by ID. Returns None if not found."""
        base_url = await self._get_base_url()
        headers = await self._get_headers()
        resp = await self._client.get(f"{base_url}/dashboards/{dashboard_id}", headers=headers)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def list_dashboards(self, page_size: int = 50) -> List[Dict[str, Any]]:
        """
        List Lakeview dashboards (single page only to avoid rate limits).
        Returns up to page_size dashboards.
        """
        base_url = await self._get_base_url()
        headers = await self._get_headers()
        url = f"{base_url}/dashboards"
        params: Dict[str, Any] = {"page_size": min(page_size, 200)}

        resp = await self._client.get(url, headers=headers, params=params)
        if resp.status_code == 429:
            logger.warning("[DashboardRepo] Rate limited by Databricks, returning empty list")
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("dashboards", [])

    async def close(self) -> None:
        await self._client.aclose()
