"""Enforcing the knowledge TTL on disk, not just in search results.

Expiry was already applied in two places: search excludes rows past the TTL, and
an upload purges before embedding. Between them they make expired knowledge
*unreachable* — but not *gone*. A workspace where someone attaches a file and
never returns keeps those rows until somebody else uploads, which in a quiet
workspace can be indefinitely.

That is the wrong half of a retention promise to get right. "We keep uploads for
seven days" has to be true of the database, not only of what the search will
show you. This is the sweep that makes it true, run daily from ``main.py``.
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


async def sweep_expired_knowledge() -> int:
    """Delete knowledge chunks past the TTL, across every workspace.

    Returns the number of rows removed. Never raises: this runs on a background
    loop, and a failed sweep must not take the loop down with it — the next one
    is an hour or a day away and the data is already excluded from search.
    """
    from src.services.knowledge.embedding_service import KNOWLEDGE_TTL_DAYS

    if KNOWLEDGE_TTL_DAYS <= 0:
        return 0

    cutoff = datetime.utcnow() - timedelta(days=KNOWLEDGE_TTL_DAYS)

    try:
        from src.db.session import get_isolated_db_session
        from src.models.documentation_embedding import KnowledgeEmbedding
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        async with get_isolated_db_session() as session:
            repository = DocumentationEmbeddingRepository(
                session, model=KnowledgeEmbedding
            )
            removed = await repository.delete_expired_all(cutoff)
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[knowledge] TTL sweep failed: %s", exc)
        return 0

    if removed:
        logger.info(
            "[knowledge] TTL sweep removed %d chunk(s) older than %d day(s)",
            removed,
            KNOWLEDGE_TTL_DAYS,
        )
    return removed
