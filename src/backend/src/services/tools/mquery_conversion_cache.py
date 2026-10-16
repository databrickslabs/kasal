"""Same-day cache for the M-Query Conversion Pipeline tool.

A cached conversion is served back only to a caller it belongs to:

* **The run's own teamspace.** Rows are stored and read under the run's primary
  group, taken from its ``GroupContext`` (``ToolSessionProvider.run_group_ids``),
  never from tool input. A call with no group context neither reads nor writes.
* **The same settings.** The key is the Power BI workspace and dataset plus a
  hash of every non-secret setting (target catalog and schema, LLM use and
  model, scan and output options, validation endpoint, principal ids), so a
  re-run with other settings converts afresh instead of returning stale SQL.
* **A caller who can still read the source.** A hit is returned only after the
  caller's credentials have listed the workspace's datasets through the same
  Power BI admin API the scan uses, and found the dataset when one is named.
  Any failure there is a miss; the fresh run then reports Power BI's own error.
"""

import hashlib
import json
import logging
from typing import Dict, Mapping, Optional
from urllib.parse import quote

from src.services.tools.tool_session_provider import ToolSessionProvider

logger = logging.getLogger(__name__)

# Credentials are checked, never hashed into a stored key.
_SECRET_SETTINGS = frozenset(
    {
        "access_token",
        "client_secret",
        "password",
        "llm_token",
        "databricks_pat",
        "exec_access_token",
        "exec_client_secret",
    }
)
# Outputs that report a failure rather than a conversion; never kept for the day.
_NOT_A_RESULT = ("Error", "No semantic models")


def cache_group() -> Optional[str]:
    """The run's own teamspace, or None when there is no run context."""
    groups = ToolSessionProvider.run_group_ids()
    return groups[0] if groups else None


def cache_key(
    workspace_id: str, dataset_id: Optional[str], settings: Mapping[str, object]
) -> str:
    """``<workspace>__<dataset or all>__<hash of the non-secret settings>``."""
    material = {k: v for k, v in settings.items() if k not in _SECRET_SETTINGS}
    blob = json.dumps(material, sort_keys=True, default=str).encode()
    digest = hashlib.sha256(blob).hexdigest()[:16]
    return f"{workspace_id}__{dataset_id or 'all'}__{digest}"


async def caller_can_read(
    auth_config: Mapping[str, object], workspace_id: str, dataset_id: Optional[str]
) -> bool:
    """Whether these credentials can list the workspace's datasets right now."""
    import httpx

    from src.services.converters.formats.mquery.scanner import PowerBIAdminScanner
    from src.services.tools.powerbi_auth_utils import (
        get_powerbi_access_token_from_config,
    )

    url = (
        f"{PowerBIAdminScanner.API_BASE}/admin/groups/"
        f"{quote(str(workspace_id), safe='')}/datasets"
    )
    try:
        token = await get_powerbi_access_token_from_config(dict(auth_config))
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        if resp.status_code != 200:
            return False
        if not dataset_id:
            return True
        return any(d.get("id") == dataset_id for d in resp.json().get("value", []))
    except Exception as exc:  # noqa: BLE001 — unverified access is a cache miss
        logger.info("[Cache] Power BI access check failed: %s", type(exc).__name__)
        return False


async def get_cached_output(
    key: str,
    workspace_id: str,
    dataset_id: Optional[str],
    auth_config: Mapping[str, object],
) -> Optional[str]:
    """Today's cached output for this group and key, if the caller may read it."""
    group_id = cache_group()
    if not group_id:
        return None
    async with ToolSessionProvider.cache_service() as svc:
        cached = await svc.get_cached_metadata(
            group_id=group_id, dataset_id=key, workspace_id=workspace_id
        )
    if not (cached and "formatted_output" in cached):
        return None
    if not await caller_can_read(auth_config, workspace_id, dataset_id):
        logger.info("[Cache] Not serving the cached conversion: access not verified")
        return None
    return str(cached["formatted_output"])


async def save_cached_output(
    key: str, workspace_id: str, metadata: Dict[str, object]
) -> None:
    """Store a successful conversion under the run's group; skip failures."""
    group_id = cache_group()
    output = str(metadata.get("formatted_output") or "")
    if not group_id or not output or output.startswith(_NOT_A_RESULT):
        return
    async with ToolSessionProvider.cache_service() as svc:
        await svc.save_metadata(
            group_id=group_id,
            dataset_id=key,
            workspace_id=workspace_id,
            metadata=metadata,
        )
