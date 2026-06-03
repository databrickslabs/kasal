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
        self._pat: Optional[str] = None
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

    async def _resolve_pat(self) -> Optional[str]:
        """
        Retrieve a PAT token without triggering SPN token refresh.
        Priority: OBO user_token → PAT from DB (ApiKeysService) → env var.
        """
        if self._user_token:
            return self._user_token

        # PAT from DB — same as get_auth_context() Priority 2
        try:
            from src.services.api_keys_service import ApiKeysService
            from src.db.session import async_session_factory
            from src.utils.user_context import UserContext

            group_id: Optional[str] = None
            try:
                ctx = UserContext.get_group_context()
                if ctx and hasattr(ctx, "primary_group_id"):
                    group_id = ctx.primary_group_id
            except Exception:
                pass

            if group_id:
                async with async_session_factory() as session:
                    svc = ApiKeysService(session, group_id=group_id)
                    for key_name in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
                        try:
                            api_key = await svc.find_by_name(key_name)
                            if api_key and api_key.encrypted_value:
                                from src.utils.encryption_utils import EncryptionUtils
                                pat = EncryptionUtils.decrypt_value(api_key.encrypted_value)
                                if pat:
                                    logger.debug(f"[DashboardRepo] PAT from DB ({key_name})")
                                    return pat
                        except Exception as e:
                            logger.debug(f"[DashboardRepo] PAT DB lookup {key_name}: {e}")
        except Exception as e:
            logger.debug(f"[DashboardRepo] PAT DB lookup failed: {e}")

        # Env var PAT
        for env_key in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
            val = os.environ.get(env_key, "")
            if val:
                logger.debug(f"[DashboardRepo] PAT from env ({env_key})")
                return val

        return None

    async def _get_base_url(self) -> str:
        if not self._host:
            self._host = await self._resolve_host()
        if not self._host:
            raise RuntimeError(
                "No Databricks workspace URL configured. "
                "Set DATABRICKS_HOST or configure a Databricks profile."
            )
        host = self._host
        if not host.startswith("https://"):
            host = f"https://{host}"
        return f"{host}/api/2.0/lakeview"

    async def _get_headers(self) -> Dict[str, str]:
        if not self._pat:
            self._pat = await self._resolve_pat()
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self._pat:
            headers["Authorization"] = f"Bearer {self._pat}"
        else:
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
