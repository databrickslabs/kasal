"""Session routing for knowledge / document embeddings.

When the active memory backend is **Lakebase**, document embeddings are stored
in and read from the Lakebase memory instance (the ``kasal.documentation_embeddings``
table) — the same pgvector instance that holds CrewAI crew memory — rather than
the application's main database. Otherwise the app session is used (SQLite
locally / Postgres).

The memory-backend *config rows* always live in the app DB, so we read the
active config from the app session, then open a Lakebase session for the actual
embedding storage/search when Lakebase is the active backend.
"""
import os
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


def emit_knowledge_span(event_type: str, attrs: dict) -> None:
    """Emit a one-shot OTel span recording a knowledge store/search decision.

    The crew SUBPROCESS's ``logger.info`` lines do NOT reach the OTel logs
    table, but tool spans DO reach otel_spans — so the only way to observe how
    the subprocess search resolves its store (Lakebase vs app DB) is a span.
    Best-effort: never let diagnostics break the search/ingest path.
    """
    try:
        from opentelemetry import trace as _otel_trace

        tracer = _otel_trace.get_tracer("kasal.knowledge")
        with tracer.start_as_current_span("kasal.knowledge.store") as span:
            if not span.is_recording():
                return
            span.set_attribute("kasal.event_type", event_type)
            for key, value in attrs.items():
                if value is None:
                    continue
                span.set_attribute(f"kasal.knowledge.{key}", value)
    except Exception as e:  # pragma: no cover - diagnostics must not raise
        logger.debug(f"[KNOWLEDGE] Could not emit knowledge span: {e}")


# The dedicated knowledge_embeddings table for uploaded knowledge, created
# complete (incl. the pgvector embedding column) so the creating connection owns
# it. pgvector ('vector' type + vector_cosine_ops) lives in public, which is on
# the Lakebase search_path. This is a NEW table — the legacy
# documentation_embeddings is owned by another role and cannot be altered.
_KNOWLEDGE_TABLE = "knowledge_embeddings"
_KNOWLEDGE_CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {_KNOWLEDGE_TABLE} (
        id SERIAL PRIMARY KEY,
        source VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        content TEXT NOT NULL,
        doc_metadata JSON,
        group_id VARCHAR(100),
        file_path VARCHAR,
        created_by VARCHAR(255),
        embedding vector(1024),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
    )
"""
_KNOWLEDGE_INDEX_SQL = (
    f"CREATE INDEX IF NOT EXISTS idx_knowledge_emb_group_id ON {_KNOWLEDGE_TABLE} (group_id)",
    f"CREATE INDEX IF NOT EXISTS idx_knowledge_emb_file_path ON {_KNOWLEDGE_TABLE} (file_path)",
    f"CREATE INDEX IF NOT EXISTS idx_knowledge_emb_created_by ON {_KNOWLEDGE_TABLE} (created_by)",
    f"CREATE INDEX IF NOT EXISTS idx_knowledge_emb_embedding "
    f"ON {_KNOWLEDGE_TABLE} USING hnsw (embedding vector_cosine_ops)",
)
_KNOWLEDGE_ALTER_SQL = {
    "group_id": f"ALTER TABLE {_KNOWLEDGE_TABLE} ADD COLUMN IF NOT EXISTS group_id VARCHAR(100)",
    "file_path": f"ALTER TABLE {_KNOWLEDGE_TABLE} ADD COLUMN IF NOT EXISTS file_path VARCHAR",
    "created_by": f"ALTER TABLE {_KNOWLEDGE_TABLE} ADD COLUMN IF NOT EXISTS created_by VARCHAR(255)",
    "embedding": f"ALTER TABLE {_KNOWLEDGE_TABLE} ADD COLUMN IF NOT EXISTS embedding vector(1024)",
}
_KNOWLEDGE_REQUIRED_COLS = ("embedding", "group_id", "file_path", "created_by")

_KNOWLEDGE_OWNERSHIP_HINT = (
    "The Lakebase table kasal.knowledge_embeddings is missing pgvector columns "
    "{missing} and this connection is not its owner, so it cannot be altered. "
    "Have a role that owns it run:\n"
    "  DROP TABLE kasal.knowledge_embeddings;  -- the app will recreate it\n"
    "or add the columns explicitly."
)


async def ensure_lakebase_doc_table(session: AsyncSession) -> None:
    """Ensure kasal.knowledge_embeddings exists with the pgvector schema.

    - Table missing  -> CREATE it complete (this connection owns it).
    - Table present, columns present -> no-op (no DDL, no ownership needed).
    - Table present, columns missing -> ALTER to add them; if the connection
      doesn't own the table, raise a clear, actionable error. Runs in the
      caller's transaction.
    """
    from sqlalchemy import text

    result = await session.execute(text(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{_KNOWLEDGE_TABLE}'"
    ))
    existing = {row[0] for row in result.fetchall()}

    if not existing:
        # Fresh table: create it complete so this connection is the owner.
        await session.execute(text(_KNOWLEDGE_CREATE_SQL))
        for idx in _KNOWLEDGE_INDEX_SQL:
            await session.execute(text(idx))
        return

    missing = [c for c in _KNOWLEDGE_REQUIRED_COLS if c not in existing]
    if not missing:
        return  # already correct — no DDL needed

    try:
        for col in _KNOWLEDGE_REQUIRED_COLS:
            if col in missing:
                await session.execute(text(_KNOWLEDGE_ALTER_SQL[col]))
        for idx in _KNOWLEDGE_INDEX_SQL:
            await session.execute(text(idx))
    except Exception as e:
        raise RuntimeError(_KNOWLEDGE_OWNERSHIP_HINT.format(missing=missing)) from e


async def resolve_lakebase_instance(
    app_session: AsyncSession,
    group_id: Optional[str],
) -> Optional[str]:
    """Return the Lakebase instance name if the active memory backend is Lakebase.

    Returns None (use the app DB) when the active backend is not Lakebase or the
    config cannot be read.
    """
    try:
        from src.services.memory_config_service import MemoryConfigService
        from src.schemas.memory_backend import MemoryBackendType

        config = await MemoryConfigService(app_session).get_active_config(group_id)
        backend_type = getattr(config, "backend_type", None) if config else None
        if config and config.backend_type == MemoryBackendType.LAKEBASE:
            instance = None
            lakebase_config = getattr(config, "lakebase_config", None)
            if lakebase_config is not None:
                instance = getattr(lakebase_config, "instance_name", None)
            resolved = instance or os.getenv("LAKEBASE_INSTANCE_NAME", "kasal-lakebase")
            logger.info(
                f"[KNOWLEDGE] resolve_lakebase_instance: group={group_id} "
                f"backend=LAKEBASE -> instance={resolved}"
            )
            return resolved
        # Not Lakebase — record WHY so a deployed run shows whether the
        # subprocess saw 'no active config' vs a non-Lakebase backend.
        logger.info(
            f"[KNOWLEDGE] resolve_lakebase_instance: group={group_id} "
            f"backend={backend_type if config else 'NO_ACTIVE_CONFIG'} -> using app DB"
        )
        emit_knowledge_span(
            "knowledge_store_resolve",
            {
                "group_id": group_id,
                "lakebase": False,
                "backend": str(backend_type) if config else "no_active_config",
            },
        )
        return None
    except Exception as e:
        logger.warning(
            f"[KNOWLEDGE] resolve_lakebase_instance FAILED for group={group_id} "
            f"({type(e).__name__}: {e}); using app DB for embeddings"
        )
        emit_knowledge_span(
            "knowledge_store_resolve",
            {"group_id": group_id, "lakebase": False, "error": f"{type(e).__name__}: {e}"[:300]},
        )
        return None


import re

# Lakebase objects (tables, schema) created by Databricks are owned by the
# databricks_superuser role; individual principals are NOINHERIT members of it,
# so DDL/DML on those objects only works after explicitly assuming the role.
# Configurable / disable with empty string.
_KNOWLEDGE_DB_ROLE = os.getenv("LAKEBASE_KNOWLEDGE_ROLE", "databricks_superuser")
_SAFE_ROLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


async def _assume_knowledge_role(session: AsyncSession) -> None:
    """Best-effort ``SET ROLE`` so the Lakebase session operates as the role that
    owns the kasal objects (databricks_superuser by default). Savepoint-guarded so
    a non-member / missing role leaves the session usable."""
    role = _KNOWLEDGE_DB_ROLE
    if not role or not _SAFE_ROLE.match(role):
        return
    from sqlalchemy import text
    try:
        await session.execute(text("SAVEPOINT knowledge_set_role"))
        await session.execute(text(f'SET ROLE "{role}"'))
        await session.execute(text("RELEASE SAVEPOINT knowledge_set_role"))
        logger.info(f"[KNOWLEDGE] Operating as role '{role}' on Lakebase session")
    except Exception as e:
        logger.warning(f"[KNOWLEDGE] Could not SET ROLE '{role}' ({e}); continuing as connection role")
        try:
            await session.execute(text("ROLLBACK TO SAVEPOINT knowledge_set_role"))
        except Exception:
            pass


@asynccontextmanager
async def knowledge_embedding_session(
    app_session: AsyncSession,
    group_id: Optional[str],
    user_token: Optional[str] = None,
) -> AsyncGenerator[Tuple[AsyncSession, bool], None]:
    """Yield ``(session, is_lakebase)`` for document-embedding storage/search.

    - Lakebase active  -> a session on the Lakebase instance, with the owning
      role assumed (SET ROLE). The underlying ``get_lakebase_session`` context
      commits/rolls back on exit, so the caller should NOT commit
      (``is_lakebase`` is True).
    - Otherwise        -> the app session (``is_lakebase`` False); the caller is
      responsible for ``commit()``/``rollback()`` on writes.
    """
    instance = await resolve_lakebase_instance(app_session, group_id)
    if instance:
        from src.db.lakebase_session import get_lakebase_session

        logger.info(f"[KNOWLEDGE] Using Lakebase instance '{instance}' for document embeddings")
        emit_knowledge_span(
            "knowledge_store_session",
            {"group_id": group_id, "lakebase": True, "instance": instance},
        )
        async with get_lakebase_session(
            instance_name=instance, group_id=group_id, user_token=user_token
        ) as lb_session:
            await _assume_knowledge_role(lb_session)
            yield lb_session, True
    else:
        # App-DB fallback. Previously SILENT — the single biggest blind spot:
        # on deployed Databricks Apps the crew SUBPROCESS landing here (while
        # ingest used Lakebase in the API process) is exactly how a search
        # reads the wrong/empty store and returns "No relevant information".
        logger.info(
            f"[KNOWLEDGE] Using APP DB (not Lakebase) for document embeddings "
            f"(group={group_id}) — ingest and search MUST agree on this store"
        )
        emit_knowledge_span(
            "knowledge_store_session",
            {"group_id": group_id, "lakebase": False, "instance": "app_db"},
        )
        yield app_session, False
