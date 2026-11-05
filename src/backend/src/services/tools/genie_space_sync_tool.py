"""Genie Space Sync — a Genie space as code: extract, deploy, and check.

WHY: a Genie space is configuration (instructions, example SQL, snippets, column
settings, benchmarks) that drifts from the metric views it queries. Exporting it to
YAML gives a reviewable backup and a deployable definition; checking it against the
deployed views catches broken SQL, columns that no longer exist, and double-scaled
percentages before users do.

Actions:
  extract  space_id -> the five YAML files (header, config, data_sources,
           instructions, benchmarks). Read-only; the output is the backup artifact.
  deploy   space_files -> create or update the space. A dry run unless
           confirm_deploy is true.
  check    space_id (live) or space_files (offline) -> verdicts: SQL and snippet row
           counts, column settings, percentage scaling, prose identifiers, and
           reconciliation mappings against the deployed metric views.

Logic lives in ``genie_space_sync_utils/``; this class wires real I/O into it. The
Genie REST calls and the SQL Statement API are the same ones the Genie generator
and the drift monitor use.
"""

import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Optional, Type

import requests
from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.base import BaseTool
from src.services.tools.databricks_tool_utils import resolve_tool_auth
from src.services.tools.genie_space_sync_utils.checks_mapping import parse_mapping_set
from src.services.tools.genie_space_sync_utils.checks_report import build_report
from src.services.tools.genie_space_sync_utils.deploy_plan import (
    build_payload,
    resolve_target,
)
from src.services.tools.genie_space_sync_utils.genie_api import (
    GenieApiError,
    GenieHttp,
    create_space,
    get_space,
    requests_http,
    space_url,
    update_space,
)
from src.services.tools.genie_space_sync_utils.ids import ensure_ids
from src.services.tools.genie_space_sync_utils.space_files import (
    SpaceFilesError,
    definition_title,
    files_to_definition,
    parse_space_files,
    serialized_to_files,
    summarize_definition,
)
from src.services.tools.genie_space_sync_utils.sql_results import SqlFn
from src.utils.telemetry import KasalProduct, get_user_agent_header
from src.utils.url_security import is_trusted_databricks_host

logger = logging.getLogger(__name__)

_ACTIONS = ("extract", "deploy", "check")
_SPACE_ID = re.compile(r"[0-9a-f]{32}")
#: Settings a tool instance may carry as defaults. confirm_deploy is deliberately
#: absent: applying a change is always a per-call decision.
_CONFIG_KEYS = (
    "databricks_host",
    "warehouse_id",
    "check_sql",
    "reconciliation_mappings",
)


class GenieSpaceSyncSchema(BaseModel):
    """Input schema for GenieSpaceSyncTool."""

    action: str = Field(
        ...,
        description=(
            "extract (space -> YAML files), deploy (YAML files -> space; dry run unless "
            "confirm_deploy), or check (space or files vs the deployed metric views)."
        ),
    )
    space_id: Optional[str] = Field(
        None, description="32-character Genie space ID (extract, check, deploy target)."
    )
    space_files: Optional[str] = Field(
        None,
        description=(
            "JSON object of file name -> YAML text (header.yaml, config.yaml, "
            "data_sources.yaml, instructions.yaml, benchmarks.yaml). The output of "
            "extract; the input of deploy; an offline input of check."
        ),
    )
    warehouse_id: Optional[str] = Field(
        None, description="SQL warehouse for deploy (required) and the SQL checks."
    )
    confirm_deploy: Optional[bool] = Field(
        False,
        description="deploy only: apply the change. Without it deploy returns a dry-run plan.",
    )
    check_sql: Optional[bool] = Field(
        True,
        description="check only: run example, benchmark and snippet SQL as row counts.",
    )
    reconciliation_mappings: Optional[str] = Field(
        None,
        description=(
            "check only: JSON object of mapping file name -> reconciliation mapping YAML; "
            "each is checked against its deployed metric view."
        ),
    )
    databricks_host: Optional[str] = Field(
        None,
        description="Workspace URL override; must be the credential's own workspace.",
    )


@dataclass(frozen=True)
class _Session:
    host: str
    http: GenieHttp
    sql: SqlFn


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _as_bool(value: object, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return default


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return {str(key): item for key, item in value.items()}


def _space_id(value: object) -> str:
    text = _text(value)
    if not _SPACE_ID.fullmatch(text):
        raise ValueError("space_id must be a 32-character lowercase hex Genie space ID")
    return text


def _serialized(space: dict[str, object]) -> dict[str, object]:
    raw = space.get("serialized_space")
    if not isinstance(raw, str):
        raise ValueError(
            "the space response has no serialized_space; the caller needs EDIT permission"
        )
    parsed: object = json.loads(raw)
    return _as_dict(parsed)


def _header_of(space_id: str, space: dict[str, object]) -> dict[str, object]:
    return {
        "space_id": space_id,
        "title": _text(space.get("title")),
        "description": _text(space.get("description")),
    }


def _sql_runner(host: str, headers: dict[str, str], warehouse_id: str) -> SqlFn:
    def _run(statement: str) -> dict[str, object]:
        if not warehouse_id:
            return {
                "success": False,
                "error": "warehouse_id is required for SQL checks",
            }
        from src.services.tools.metric_view_deployer_tool import MetricViewDeployerTool

        try:
            result = MetricViewDeployerTool._execute_sql_sync(
                statement, host, warehouse_id, headers
            )
        except requests.RequestException as exc:
            return {
                "success": False,
                "error": f"SQL request failed: {type(exc).__name__}",
            }
        return dict(result)

    return _run


def _open_session(host_override: str, warehouse_id: str) -> _Session:
    auth = resolve_tool_auth(host_override or None)
    if auth is None:
        raise RuntimeError("Databricks authentication failed")
    host = (auth.workspace_url or "").rstrip("/")
    if not host or not is_trusted_databricks_host(host):
        raise RuntimeError(
            "workspace URL is not configured or not a trusted Databricks host"
        )
    headers = {**auth.get_headers(), **get_user_agent_header(KasalProduct.GENIE)}
    return _Session(
        host=host,
        http=requests_http(host, headers),
        sql=_sql_runner(host, headers, warehouse_id),
    )


def _extract(get: Callable[[str], object], session: _Session) -> dict[str, object]:
    space_id = _space_id(get("space_id"))
    space = get_space(session.http, space_id)
    if space is None:
        raise ValueError(f"space {space_id} was not found or is not visible")
    serialized = _serialized(space)
    header = _header_of(space_id, space)
    return {
        "action": "extract",
        "space_id": space_id,
        "title": header["title"],
        "files": serialized_to_files(serialized, header),
        "counts": summarize_definition(serialized),
    }


def _deploy(get: Callable[[str], object], session: _Session) -> dict[str, object]:
    raw_files = get("space_files")
    if not raw_files:
        raise SpaceFilesError("deploy needs space_files (the output of extract)")
    header, serialized = files_to_definition(parse_space_files(raw_files))
    title = definition_title(header)
    if not title:
        raise SpaceFilesError("header.yaml needs a title")
    warehouse_id = _text(get("warehouse_id"))
    if not warehouse_id:
        raise ValueError("warehouse_id is required for deploy")
    header_space_id = _text(header.get("space_id"))
    if header_space_id:
        _space_id(header_space_id)

    populated, assigned = ensure_ids(serialized)
    target = resolve_target(session.http, header_space_id, title)
    payload = build_payload(header, populated, warehouse_id)
    counts = summarize_definition(populated)
    if not _as_bool(get("confirm_deploy"), False):
        return {
            "action": "deploy",
            "dry_run": True,
            "operation": target.operation,
            "space_id": target.space_id or None,
            "title": title,
            "ids_to_assign": assigned,
            "counts": counts,
            "note": "dry run: nothing changed; set confirm_deploy=true to apply",
        }

    if target.operation == "update":
        update_space(session.http, target.space_id, payload)
        space_id = target.space_id
    else:
        created = create_space(session.http, payload)
        space_id = _text(created.get("space_id") or created.get("id"))
        if not space_id:
            raise GenieApiError(0, "Genie create returned no space_id")
    header["space_id"] = space_id
    result: dict[str, object] = {
        "action": "deploy",
        "dry_run": False,
        "operation": "created" if target.operation == "create" else "updated",
        "space_id": space_id,
        "url": space_url(session.host, space_id),
        "ids_assigned": assigned,
        "counts": counts,
    }
    if target.operation == "create" or assigned:
        result["files"] = serialized_to_files(populated, header)
        result["note"] = "write these files back: they carry the new IDs and space_id"
    return result


def _check(get: Callable[[str], object], session: _Session) -> dict[str, object]:
    raw_files = get("space_files")
    if raw_files:
        header, serialized = files_to_definition(parse_space_files(raw_files))
        source = "files"
        space_id = _text(header.get("space_id"))
        title = definition_title(header)
    elif get("space_id"):
        space_id = _space_id(get("space_id"))
        space = get_space(session.http, space_id)
        if space is None:
            raise ValueError(f"space {space_id} was not found or is not visible")
        serialized = _serialized(space)
        title = _text(space.get("title"))
        source = "live"
    else:
        raise ValueError("check needs space_id (live) or space_files (offline)")
    mappings = parse_mapping_set(get("reconciliation_mappings"))
    report = build_report(
        serialized,
        mappings,
        session.sql,
        _as_bool(get("check_sql"), True),
    )
    return {
        "action": "check",
        "source": source,
        "space_id": space_id,
        "title": title,
        **report,
    }


def _dump(result: dict[str, object]) -> str:
    return json.dumps(result, default=str, ensure_ascii=False)


def _error(message: str) -> str:
    return json.dumps({"error": message})


class GenieSpaceSyncTool(BaseTool):
    """Extract, deploy and check a Genie space as code."""

    name: str = "Genie Space Sync"
    description: str = (
        "Treats a Genie space as code. action=extract reads a space and returns its five "
        "YAML files (the backup artifact). action=deploy creates or updates a space from "
        "those files; it is a dry run unless confirm_deploy=true. action=check verifies a "
        "space (live, or offline from space_files) against the deployed Unity Catalog "
        "metric views: example and benchmark SQL as row counts, snippets, column settings, "
        "double-scaled percentages, prose identifiers that match no deployed name, and "
        "optional reconciliation mappings. Returns a JSON verdict list with pass, fail "
        "and review counts; review entries are hints for a human."
    )
    args_schema: Type[BaseModel] = GenieSpaceSyncSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: object) -> None:
        default_config: dict[str, object] = {}
        for key in _CONFIG_KEYS:
            value = kwargs.pop(key, None)
            if value is not None:
                default_config[key] = value
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: object) -> str:
        def get(key: str) -> object:
            value = kwargs.get(key)
            return value if value not in (None, "") else self._default_config.get(key)

        action = _text(get("action")).lower()
        if action not in _ACTIONS:
            return _error(f"action must be one of: {', '.join(_ACTIONS)}")
        logger.info("[GenieSpaceSync] action=%s", action)
        try:
            session = _open_session(
                _text(get("databricks_host")), _text(get("warehouse_id"))
            )
            if action == "extract":
                return _dump(_extract(get, session))
            if action == "deploy":
                return _dump(_deploy(get, session))
            return _dump(_check(get, session))
        except (GenieApiError, SpaceFilesError, RuntimeError, ValueError) as exc:
            return _error(str(exc))
