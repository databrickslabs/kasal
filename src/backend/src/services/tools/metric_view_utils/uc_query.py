"""UC warehouse query helper — read-only SQL against a Databricks SQL warehouse.

Shared by the config-generator's optional enrichment pass (P2 filter-set value
resolution, P3 cross-fact grain/union probes). It is deliberately small and
read-oriented:

  * auth via the standard AuthContext chain (OBO → PAT → SPN), same as the
    metric-view deployer;
  * an SSRF allowlist enforced *inside* this module (callers cannot bypass it);
  * ``run_query`` returns ``{success, columns, rows}`` (unlike the deployer's
    DDL-only ``_execute_sql_sync`` which returns no rows);
  * ``resolve_workspace_and_warehouse`` auto-picks a warehouse when only a host
    is known.

All functions are async and carry the Kasal User-Agent (telemetry rule in
`src/backend/CLAUDE.md`). Callers on sync paths must bridge via
``async_bridge.run_async_with_context`` so group_id / OBO token propagate.
"""
from __future__ import annotations

import asyncio
import logging
import re
import urllib.parse
from typing import Any, Optional

logger = logging.getLogger(__name__)

class UCQueryError(Exception):
    """Raised for auth/host/execution failures the caller should log + skip on."""


def _host_allowed(workspace_url: str) -> bool:
    """SSRF check before attaching a Databricks credential to an outbound call.

    SEC #5: uses the shared ``url_security.is_trusted_databricks_host`` (single
    source of truth) instead of a local suffix allowlist, so this can't drift from
    the deployer / the rest of the codebase. Fails closed if the helper is
    unavailable rather than trusting an unknown host.
    """
    try:
        from src.utils.url_security import is_trusted_databricks_host
        return is_trusted_databricks_host(workspace_url)
    except Exception:  # noqa: BLE001 — no shared helper → do not trust the host
        return False


async def _auth_headers(host_override: Optional[str] = None) -> tuple[str, dict]:
    """Return ``(workspace_url, headers)`` via the AuthContext chain (OBO→PAT→SPN),
    with the Kasal User-Agent attached. Raises ``UCQueryError`` on failure or an
    untrusted host.
    """
    from src.utils.databricks_auth import get_auth_context
    # Fetch the OBO token from the execution context (get_auth_context SKIPS OBO
    # when user_token is None). Tools run inside crew/flow execution where the
    # engine populates UserContext; fall back to the group context's forwarded
    # token. Mirrors how GenieTool / AgentBricksTool obtain OBO in tool_factory.
    user_token = None
    try:
        from src.utils.user_context import UserContext
        user_token = UserContext.get_user_token()
        if not user_token:
            gc = UserContext.get_group_context()
            user_token = getattr(gc, 'access_token', None) if gc else None
    except Exception:  # noqa: BLE001 — context may be absent (e.g. unit tests)
        user_token = None
    auth = await get_auth_context(user_token=user_token)
    if auth is None:
        raise UCQueryError('authentication failed (no AuthContext)')
    if host_override:
        url = host_override.strip().rstrip('/')
        if not url.startswith('https://'):
            url = f'https://{url}'
        auth.workspace_url = url
    workspace_url = (auth.workspace_url or '').rstrip('/')
    if not workspace_url:
        raise UCQueryError('workspace_url not configured')
    if not _host_allowed(workspace_url):
        raise UCQueryError(f'untrusted host: {urllib.parse.urlparse(workspace_url).hostname}')
    headers = auth.get_headers()
    from src.utils.telemetry import get_user_agent_header, KasalProduct
    headers.update(get_user_agent_header(KasalProduct.POWERBI))
    headers.setdefault('Content-Type', 'application/json')
    return workspace_url, headers


async def resolve_workspace_and_warehouse(
    warehouse_id: Optional[str] = None,
    host_override: Optional[str] = None,
) -> tuple[str, str, dict]:
    """Resolve ``(workspace_url, warehouse_id, headers)``.

    ``warehouse_id`` may be a bare id or a full SQL-endpoint URL (the ``/warehouses/
    <id>`` segment is parsed out). When no id is given, the best warehouse is
    auto-picked: prefer RUNNING, else the first available. Raises ``UCQueryError``.
    """
    # A full endpoint URL can carry its own host + id.
    endpoint_host = None
    if warehouse_id and '://' in warehouse_id:
        parsed = urllib.parse.urlparse(warehouse_id)
        endpoint_host = f'{parsed.scheme}://{parsed.netloc}'
        m = re.search(r'/warehouses/([a-f0-9]+)', parsed.path)
        warehouse_id = m.group(1) if m else None

    workspace_url, headers = await _auth_headers(host_override or endpoint_host)
    if warehouse_id:
        return workspace_url, warehouse_id, headers

    # Auto-pick a warehouse.
    import httpx as _httpx
    async with _httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(f'{workspace_url}/api/2.0/sql/warehouses', headers=headers)
        resp.raise_for_status()
        warehouses = resp.json().get('warehouses', [])
    if not warehouses:
        raise UCQueryError('no SQL warehouses found in workspace')
    running = [w for w in warehouses if w.get('state') == 'RUNNING']
    picked = running[0]['id'] if running else warehouses[0]['id']
    return workspace_url, picked, headers


async def run_query(
    sql: str,
    warehouse_id: Optional[str] = None,
    host_override: Optional[str] = None,
    _resolved: Optional[tuple[str, str, dict]] = None,
) -> dict[str, Any]:
    """Execute read-only ``sql`` and return ``{success, columns, rows}`` (or
    ``{success: False, error}``). Never raises — failures are returned so a caller
    can log + skip. Pass ``_resolved`` to reuse an earlier
    ``resolve_workspace_and_warehouse`` result across many queries.
    """
    import httpx as _httpx
    try:
        if _resolved is None:
            _resolved = await resolve_workspace_and_warehouse(warehouse_id, host_override)
        workspace_url, wid, headers = _resolved
    except UCQueryError as e:
        return {'success': False, 'error': str(e)}
    except Exception as e:  # noqa: BLE001 — surface as a skip reason, don't crash config-gen
        return {'success': False, 'error': f'warehouse resolve failed: {e}'}

    url = f'{workspace_url}/api/2.0/sql/statements'
    payload = {
        'statement': sql, 'warehouse_id': wid,
        'wait_timeout': '50s', 'on_wait_timeout': 'CONTINUE',
    }
    try:
        async with _httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            sid = data.get('statement_id')
            state = data.get('status', {}).get('state', '')
            polls = 0
            while state in ('RUNNING', 'PENDING') and polls < 30:
                await asyncio.sleep(2)
                pr = await client.get(f'{url}/{sid}', headers=headers)
                data = pr.json()
                state = data.get('status', {}).get('state', '')
                polls += 1
            if state == 'SUCCEEDED':
                result = data.get('result', {})
                manifest = data.get('manifest', {})
                cols = [c['name'] for c in manifest.get('schema', {}).get('columns', [])]
                return {'success': True, 'columns': cols,
                        'rows': result.get('data_array', []) or []}
            err = data.get('status', {}).get('error', {})
            return {'success': False, 'error': err.get('message', f'state: {state}')}
    except Exception as e:  # noqa: BLE001
        return {'success': False, 'error': str(e)}


def _quote_ident(name: str) -> str:
    """Back-quote a possibly-qualified identifier for Spark SQL (``a.b.c`` →
    ```a`.`b`.`c``). Rejects anything that isn't a plain dotted identifier so a
    hostile table/column name can't inject SQL."""
    parts = name.split('.')
    for p in parts:
        if not re.fullmatch(r'[A-Za-z_][\w]*', p):
            raise UCQueryError(f'unsafe identifier: {name!r}')
    return '.'.join(f'`{p}`' for p in parts)


async def select_distinct(
    table: str,
    value_col: str,
    flag_col: Optional[str] = None,
    flag_value: int = 1,
    limit: int = 1000,
    warehouse_id: Optional[str] = None,
    host_override: Optional[str] = None,
    _resolved: Optional[tuple[str, str, dict]] = None,
) -> dict[str, Any]:
    """``SELECT DISTINCT <value_col> FROM <table> [WHERE <value_col> IS NOT NULL
    AND <flag_col> = <flag_value>]`` → adds a flat ``values`` list to the
    ``run_query`` result.

    SECURITY: the predicate is built from STRUCTURED params, not a caller-supplied
    WHERE string. ``table``/``value_col``/``flag_col`` are each identifier-validated
    + back-quoted via ``_quote_ident`` (they derive from PBI-scan metadata, i.e.
    attacker-influenceable), and ``flag_value`` is int-coerced. This closes the
    injection where an unvalidated flag-column name reached the WHERE clause.
    Returns ``{success: False, error}`` on failure — never raises.
    """
    try:
        tbl = _quote_ident(table)
        col = _quote_ident(value_col)
        flag = _quote_ident(flag_col) if flag_col else None
    except UCQueryError as e:
        return {'success': False, 'error': str(e)}
    sql = f'SELECT DISTINCT {col} FROM {tbl}'
    if flag is not None:
        sql += f' WHERE {col} IS NOT NULL AND {flag} = {int(flag_value)}'
    sql += f' ORDER BY {col} LIMIT {int(limit)}'
    res = await run_query(sql, warehouse_id, host_override, _resolved=_resolved)
    if res.get('success'):
        res['values'] = [row[0] for row in res.get('rows', []) if row and row[0] is not None]
    return res
