"""Backend-specific read/delete helpers for the memory browser.

``records_router`` picks one of three implementations per operation based on the
group's active backend — Databricks Vector Search, Lakebase pgvector, or the
local (LanceDB/SQLite) store. Each pair returns the same shape so the router
stays backend-agnostic.
"""

from typing import Any, Dict, List, Optional, Tuple

from src.utils.memory_paths import local_memory_store_dir

from .dependencies import logger

# The local (LanceDB) storage layer limits the table SCAN before sorting by
# created_at, so a small limit yields an arbitrary slice rather than the newest
# records. When browsing we scan the whole store (up to this cap) and sort +
# paginate ourselves so the returned page is truly the newest. Matches the
# storage layer's own scan cap.
_BROWSE_FULL_SCAN_LIMIT = 50_000


# ---------------------------------------------------------------------------
# Browse
# ---------------------------------------------------------------------------


async def _browse_lakebase_records(
    lakebase_cfg: Any,
    *,
    group_id: str,
    scope: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Read records from the unified Lakebase memory table.

    Returns ``(records, total)`` where ``total`` is the count of records
    matching the group (and optional scope) filter, for client pagination.
    """
    from sqlalchemy import text

    from src.db.lakebase_session import get_lakebase_session

    instance_name = getattr(lakebase_cfg, "instance_name", None)
    table_name = lakebase_cfg.memory_table
    where = ["group_id = :group_id"]
    params: Dict[str, Any] = {
        "group_id": group_id,
        "limit": limit,
        "offset": offset,
    }
    if scope:
        where.append("metadata->>'scope' LIKE :scope_prefix")
        params["scope_prefix"] = f"{scope}%"

    where_clause = " AND ".join(where)
    async with get_lakebase_session(
        instance_name=instance_name, group_id=group_id
    ) as session:
        count_sql = text(f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}")
        total = int((await session.execute(count_sql, params)).scalar() or 0)
        sql = text(
            f"SELECT id, content, metadata, created_at, updated_at, agent, score "
            f"FROM {table_name} "
            f"WHERE {where_clause} "
            f"ORDER BY created_at DESC "
            f"LIMIT :limit OFFSET :offset"
        )
        result = await session.execute(sql, params)
        records: List[Dict[str, Any]] = []
        for row in result.fetchall():
            metadata_val = row[2]
            metadata = (
                metadata_val
                if isinstance(metadata_val, dict)
                else _safe_json(metadata_val)
            )
            records.append(
                {
                    "id": row[0],
                    "content": row[1],
                    "scope": metadata.get("scope", "/"),
                    "categories": metadata.get("categories") or [],
                    "importance": float(metadata.get("importance") or row[6] or 0.5),
                    "source": metadata.get("source") or row[5] or None,
                    "private": bool(metadata.get("private") or False),
                    "metadata": {
                        k: v
                        for k, v in metadata.items()
                        if k
                        not in (
                            "scope",
                            "categories",
                            "importance",
                            "source",
                            "private",
                        )
                    },
                    "created_at": str(row[3]) if row[3] else None,
                    "last_accessed": str(row[4]) if row[4] else None,
                }
            )
        return records, total


def _browse_default_records(
    *,
    group_id: str,
    scope: Optional[str],
    limit: int,
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Read records from the LOCAL SQLite memory store for the group.

    Opens the SAME ``LocalMemoryStorage`` the runtime writes through — the
    ``memory.db`` under the group's store directory.

    It used to set ``CREWAI_STORAGE_DIR`` and construct a bare ``Memory()``,
    trusting crewAI to resolve ``memory/memories.lance`` underneath it. crewAI
    is gone: ``Memory()`` with no storage now falls back to an in-process dict,
    so the browser reported an EMPTY store no matter how much the runtime had
    persisted (13 records on disk, 0 shown). No embedder is needed here —
    browsing lists and counts rows, it never embeds a query.

    Returns ``(records, total)`` where ``total`` is the full record count in
    the store for this scope, so the client can paginate beyond one page.
    """
    from pathlib import Path

    try:
        from src.services.memory.local_storage_backend import LocalMemoryStorage
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Default memory browse failed (local storage missing): %s", exc)
        return [], 0

    # ONE deterministic store per group at the known memory root
    # (KASAL_MEMORY_DIR) — the exact path the runtime writes to, so the browser
    # never reads a different location than the writer used. (Legacy per-crew
    # stores are intentionally NOT read; the unified backend keeps one per group.)
    store_dir = local_memory_store_dir(group_id)
    if not store_dir.is_dir():
        logger.info(
            "[memory/records] No local memory store for group %s at %s",
            group_id,
            store_dir,
        )
        return [], 0
    storage_dirs: List[Path] = [store_dir]

    aggregated: List[Dict[str, Any]] = []
    total = 0
    try:
        for storage_dir in storage_dirs:
            try:
                # The runtime's own store, opened directly: memory.db under the
                # group's store dir (see CrewMemoryService._build_local_storage).
                db_path = storage_dir / "memory.db"
                if not db_path.is_file():
                    logger.info("[memory/records] No memory.db in %s", storage_dir)
                    continue
                storage = LocalMemoryStorage(db_path)
                # True store count for this scope, so the client knows whether
                # more pages exist beyond the one being returned.
                if hasattr(storage, "count"):
                    try:
                        total += int(storage.count(scope_prefix=scope))
                    except Exception:  # pragma: no cover - best-effort
                        logger.debug("Local memory count failed", exc_info=True)
                # IMPORTANT: the storage layer's list_records limits the SCAN
                # before sorting by created_at, so a small limit returns an
                # arbitrary slice (storage order), NOT the newest records. To
                # return the true newest page we scan the whole store here and
                # sort + paginate after merging below. (Capped to avoid
                # unbounded reads; matches the storage scan cap.)
                fetched = storage.list_records(
                    scope_prefix=scope,
                    limit=_BROWSE_FULL_SCAN_LIMIT,
                    offset=0,
                )
                crew_id = storage_dir.name.removeprefix("kasal_default_")
                for r in fetched:
                    record = _memory_record_to_dict(r)
                    md = record.setdefault("metadata", {})
                    md.setdefault("_crew_id", crew_id)
                    md.setdefault("_storage_path", str(storage_dir))
                    aggregated.append(record)
            except Exception as exc:
                logger.warning(
                    "Failed to read local memory store %s: %s", storage_dir, exc
                )
    finally:
        pass

    aggregated.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    return aggregated[offset : offset + limit], total


# ---------------------------------------------------------------------------
# Row mapping
# ---------------------------------------------------------------------------


def _row_to_record_dict(
    row: List[Any],
    columns: List[str],
    positions: Dict[str, int],
) -> Dict[str, Any]:
    """Map a Databricks similarity-search row to a UI-friendly record dict."""

    def at(col: str) -> Any:
        idx = positions.get(col)
        if idx is None or idx >= len(row):
            return None
        return row[idx]

    metadata = _safe_json(at("metadata")) or {}
    categories = _safe_json_list(at("categories"))
    # Promote provenance fields into metadata so the UI can render them.
    for key in ("crew_id", "agent_id", "session_id", "llm_model"):
        metadata.setdefault(key, at(key))

    return {
        "id": at("id"),
        "content": at("content"),
        "scope": at("scope") or "/",
        "categories": categories,
        "importance": float(at("importance") or 0.5),
        "source": at("source") or None,
        "private": bool(at("private") or False),
        "metadata": metadata,
        "created_at": str(at("created_at")) if at("created_at") else None,
        "last_accessed": str(at("last_accessed")) if at("last_accessed") else None,
    }


def _memory_record_to_dict(record: Any) -> Dict[str, Any]:
    """Map a ``crewai.memory.types.MemoryRecord`` into the UI payload."""
    if hasattr(record, "model_dump"):
        data = record.model_dump()
    else:
        data = dict(record.__dict__)
    created_at = data.get("created_at")
    last_accessed = data.get("last_accessed")
    return {
        "id": data.get("id"),
        "content": data.get("content") or "",
        "scope": data.get("scope") or "/",
        "categories": data.get("categories") or [],
        "importance": float(data.get("importance") or 0.5),
        "source": data.get("source"),
        "private": bool(data.get("private") or False),
        "metadata": data.get("metadata") or {},
        "created_at": str(created_at) if created_at else None,
        "last_accessed": str(last_accessed) if last_accessed else None,
    }


def _safe_json(value: Any) -> Dict[str, Any]:
    import json as _json

    if not value:
        return {}
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = _json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _safe_json_list(value: Any) -> List[Any]:
    import json as _json

    if not value:
        return []
    if isinstance(value, list):
        return list(value)
    try:
        parsed = _json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


async def _delete_lakebase_records(
    lakebase_cfg: Any,
    *,
    group_id: str,
    scope: Optional[str],
) -> int:
    """Delete Lakebase unified-memory rows scoped to this group."""
    from sqlalchemy import text

    from src.db.lakebase_session import get_lakebase_session

    instance_name = getattr(lakebase_cfg, "instance_name", None)
    table_name = lakebase_cfg.memory_table
    where = ["group_id = :group_id"]
    params: Dict[str, Any] = {"group_id": group_id}
    if scope:
        where.append("metadata->>'scope' LIKE :scope_prefix")
        params["scope_prefix"] = f"{scope}%"

    async with get_lakebase_session(
        instance_name=instance_name, group_id=group_id
    ) as session:
        sql = text(f"DELETE FROM {table_name} WHERE {' AND '.join(where)}")
        result = await session.execute(sql, params)
        return int(getattr(result, "rowcount", 0) or 0)


def _delete_default_records(
    *,
    group_id: str,
    scope: Optional[str],
) -> int:
    """Wipe the local SQLite store for the group on the backend host.

    With ``scope``, deletes only records matching that prefix through the same
    ``LocalMemoryStorage`` the runtime writes — previously it built a bare
    ``Memory()`` under ``CREWAI_STORAGE_DIR``, which since crewAI's removal is an
    in-process dict, so a scoped delete silently removed nothing and reported 0.
    Without ``scope`` the whole group store directory is removed.
    """
    import shutil
    from pathlib import Path

    # ONE deterministic store per group at the known memory root — the same path
    # the runtime writes to and the browser reads (legacy per-crew stores untouched).
    store_dir = local_memory_store_dir(group_id)
    if not store_dir.is_dir():
        return 0
    storage_dirs: List[Path] = [store_dir]

    deleted = 0
    # Scope-filtered delete → use the Memory API so we leave other records
    # intact in each store.
    if scope:
        try:
            from src.services.memory.local_storage_backend import LocalMemoryStorage
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Default memory delete failed (local storage missing): %s", exc
            )
            return 0
        for storage_dir in storage_dirs:
            try:
                db_path = storage_dir / "memory.db"
                if not db_path.is_file():
                    continue
                storage = LocalMemoryStorage(db_path)
                if not hasattr(storage, "delete"):
                    continue
                result = storage.delete(scope_prefix=scope)
                if isinstance(result, int):
                    deleted += result
                else:
                    deleted += 1
            except Exception as exc:
                logger.warning(
                    "Failed to delete from local memory store %s: %s",
                    storage_dir,
                    exc,
                )
        return deleted

    # Wholesale wipe → remove each per-crew directory.
    for storage_dir in storage_dirs:
        try:
            shutil.rmtree(storage_dir)
            deleted += 1
            logger.info("Removed local memory store %s", storage_dir)
        except Exception as exc:
            logger.warning(
                "Failed to remove local memory store %s: %s", storage_dir, exc
            )
    return deleted
