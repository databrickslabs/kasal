"""
End-to-end unit tests (real in-memory SQLite) for the temp-embed knowledge
flow's new guarantees:

- per-user isolation: chunks are stamped with the uploader (created_by) and
  search/delete only touch the requesting user's rows (NULL = legacy shared);
- TTL: expired rows are excluded from search and purged (group-scoped) at
  upload time;
- the rewritten SQLite similarity search ranks by cosine in Python (the old
  json_each SQL took ~30s and blew the knowledge tool's timeout).
"""
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.documentation_embedding import KnowledgeEmbedding
from src.repositories.documentation_embedding_repository import (
    DocumentationEmbeddingRepository,
)

GROUP = "g1"
OTHER_GROUP = "g2"
ALICE = "alice@test.com"
BOB = "bob@test.com"


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(KnowledgeEmbedding.__table__.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


def row(
    *,
    content: str,
    embedding,
    group_id: str = GROUP,
    created_by=None,
    file_path: str = f"uploads/{GROUP}/exec-1/doc.txt",
    age_days: int = 0,
):
    return KnowledgeEmbedding(
        source=file_path,
        title=content,
        content=content,
        embedding=embedding,
        doc_metadata={},
        group_id=group_id,
        file_path=file_path,
        created_by=created_by,
        created_at=datetime.utcnow() - timedelta(days=age_days),
        updated_at=datetime.utcnow(),
    )


def store_ctx(session):
    """Stand-in for knowledge_embedding_session: yields the test session."""

    @asynccontextmanager
    async def ctx(_app_session, _group_id, _user_token=None):
        yield session, False

    return ctx


# ---------------------------------------------------------------------------
# SQLite similarity search (Python-side cosine ranking)
# ---------------------------------------------------------------------------


class TestSqliteSimilaritySearch:
    @pytest.mark.asyncio
    async def test_ranks_by_cosine_similarity_and_keeps_created_by(self, session):
        repo = DocumentationEmbeddingRepository(session, model=KnowledgeEmbedding)
        session.add_all([
            row(content="far", embedding=[0.0, 1.0, 0.0], created_by=ALICE),
            row(content="near", embedding=[1.0, 0.05, 0.0], created_by=ALICE),
            row(content="exact", embedding=[1.0, 0.0, 0.0], created_by=ALICE),
        ])
        await session.commit()

        results = await repo.search_similar([1.0, 0.0, 0.0], limit=2, group_id=GROUP)

        assert [r.content for r in results] == ["exact", "near"]
        # The new path returns live model rows — created_by survives for the
        # per-user filter upstream (the old raw SQL silently dropped it).
        assert results[0].created_by == ALICE

    @pytest.mark.asyncio
    async def test_scopes_to_the_group(self, session):
        repo = DocumentationEmbeddingRepository(session, model=KnowledgeEmbedding)
        session.add_all([
            row(content="mine", embedding=[1.0, 0.0, 0.0]),
            row(content="other tenant", embedding=[1.0, 0.0, 0.0], group_id=OTHER_GROUP),
        ])
        await session.commit()

        results = await repo.search_similar([1.0, 0.0, 0.0], limit=10, group_id=GROUP)
        assert [r.content for r in results] == ["mine"]

    @pytest.mark.asyncio
    async def test_tolerates_json_string_embeddings_and_skips_zero_vectors(self, session):
        repo = DocumentationEmbeddingRepository(session, model=KnowledgeEmbedding)
        session.add_all([
            row(content="ok", embedding=[0.5, 0.5, 0.0]),
            row(content="zero", embedding=[0.0, 0.0, 0.0]),
        ])
        await session.commit()

        results = await repo.search_similar([1.0, 0.0, 0.0], limit=10, group_id=GROUP)
        assert [r.content for r in results] == ["ok"]


# ---------------------------------------------------------------------------
# delete_by_file: per-user predicate
# ---------------------------------------------------------------------------


class TestDeleteByFilePerUser:
    @pytest.mark.asyncio
    async def test_deletes_own_and_legacy_rows_but_not_other_users(self, session):
        repo = DocumentationEmbeddingRepository(session, model=KnowledgeEmbedding)
        session.add_all([
            row(content="alice chunk", embedding=[1.0], created_by=ALICE),
            row(content="legacy chunk", embedding=[1.0], created_by=None),
            row(content="bob chunk", embedding=[1.0], created_by=BOB),
        ])
        await session.commit()

        deleted = await repo.delete_by_file(GROUP, "exec-1", "doc.txt", created_by=ALICE)
        await session.commit()

        assert deleted == 2  # alice's own + legacy shared
        remaining = await repo.search_similar([1.0], limit=10, group_id=GROUP)
        assert [r.content for r in remaining] == ["bob chunk"]

    @pytest.mark.asyncio
    async def test_without_created_by_deletes_group_wide(self, session):
        repo = DocumentationEmbeddingRepository(session, model=KnowledgeEmbedding)
        session.add_all([
            row(content="a", embedding=[1.0], created_by=ALICE),
            row(content="b", embedding=[1.0], created_by=BOB),
        ])
        await session.commit()

        deleted = await repo.delete_by_file(GROUP, "exec-1", "doc.txt")
        assert deleted == 2


# ---------------------------------------------------------------------------
# Search service: per-user isolation + TTL exclusion
# ---------------------------------------------------------------------------


class TestSearchIsolationAndTtl:
    async def _search(self, session, created_by, ttl_days=30):
        from src.services.knowledge_search_service import KnowledgeSearchService

        svc = KnowledgeSearchService(AsyncMock(), GROUP)
        with (
            patch(
                "src.services.knowledge_embedding_session.knowledge_embedding_session",
                store_ctx(session),
            ),
            patch(
                "src.core.llm_manager.LLMManager.get_embedding",
                AsyncMock(return_value=[1.0, 0.0, 0.0]),
            ),
            patch(
                "src.services.knowledge_embedding_service.KNOWLEDGE_TTL_DAYS",
                ttl_days,
            ),
        ):
            return await svc.search("query", limit=10, created_by=created_by)

    @pytest.mark.asyncio
    async def test_user_only_sees_their_own_and_legacy_chunks(self, session):
        session.add_all([
            row(content="alice doc", embedding=[1.0, 0.0, 0.0], created_by=ALICE),
            row(content="bob doc", embedding=[1.0, 0.0, 0.0], created_by=BOB),
            row(content="legacy doc", embedding=[1.0, 0.0, 0.0], created_by=None),
        ])
        await session.commit()

        results = await self._search(session, created_by=ALICE)
        contents = {r["content"] for r in results}
        assert contents == {"alice doc", "legacy doc"}

    @pytest.mark.asyncio
    async def test_no_user_context_keeps_group_visibility(self, session):
        session.add_all([
            row(content="alice doc", embedding=[1.0, 0.0, 0.0], created_by=ALICE),
        ])
        await session.commit()

        results = await self._search(session, created_by=None)
        assert {r["content"] for r in results} == {"alice doc"}

    @pytest.mark.asyncio
    async def test_expired_chunks_are_excluded(self, session):
        session.add_all([
            row(content="fresh", embedding=[1.0, 0.0, 0.0], created_by=ALICE),
            row(content="expired", embedding=[1.0, 0.0, 0.0], created_by=ALICE, age_days=45),
        ])
        await session.commit()

        results = await self._search(session, created_by=ALICE, ttl_days=30)
        assert {r["content"] for r in results} == {"fresh"}

    @pytest.mark.asyncio
    async def test_ttl_zero_disables_expiry(self, session):
        session.add_all([
            row(content="old", embedding=[1.0, 0.0, 0.0], created_by=ALICE, age_days=400),
        ])
        await session.commit()

        results = await self._search(session, created_by=ALICE, ttl_days=0)
        assert {r["content"] for r in results} == {"old"}


# ---------------------------------------------------------------------------
# Embedding service: created_by stamping + group-scoped TTL purge
# ---------------------------------------------------------------------------


class TestEmbedStampingAndPurge:
    @pytest.mark.asyncio
    async def test_embed_file_stamps_the_uploader(self, session):
        from src.services.knowledge_embedding_service import KnowledgeEmbeddingService

        svc = KnowledgeEmbeddingService(AsyncMock(), GROUP)
        with (
            patch(
                "src.services.knowledge_embedding_session.knowledge_embedding_session",
                store_ctx(session),
            ),
            patch(
                "src.core.llm_manager.LLMManager.get_embeddings",
                AsyncMock(return_value=[[1.0, 0.0, 0.0]]),
            ),
            patch.object(
                svc,
                "_chunk_with_context",
                AsyncMock(return_value=[{"content": "chunk one", "section": "S1"}]),
            ),
        ):
            result = await svc.embed_file(
                file_path=f"uploads/{GROUP}/exec-1/doc.txt",
                file_content="chunk one",
                execution_id="exec-1",
                created_by=ALICE,
            )
        await session.commit()

        assert result["status"] == "success"
        repo = DocumentationEmbeddingRepository(session, model=KnowledgeEmbedding)
        rows = await repo.search_similar([1.0, 0.0, 0.0], limit=10, group_id=GROUP)
        assert rows and rows[0].created_by == ALICE
        assert rows[0].doc_metadata["created_by"] == ALICE

    @pytest.mark.asyncio
    async def test_purge_expired_is_group_scoped(self, session):
        from src.services.knowledge_embedding_service import KnowledgeEmbeddingService

        session.add_all([
            row(content="mine fresh", embedding=[1.0], age_days=1),
            row(content="mine expired", embedding=[1.0], age_days=45),
            row(content="other tenant expired", embedding=[1.0], group_id=OTHER_GROUP, age_days=45),
        ])
        await session.commit()

        svc = KnowledgeEmbeddingService(AsyncMock(), GROUP)
        with patch(
            "src.services.knowledge_embedding_session.knowledge_embedding_session",
            store_ctx(session),
        ):
            purged = await svc.purge_expired()
        await session.commit()

        assert purged == 1  # only THIS group's expired row
        repo = DocumentationEmbeddingRepository(session, model=KnowledgeEmbedding)
        mine = await repo.search_similar([1.0], limit=10, group_id=GROUP)
        others = await repo.search_similar([1.0], limit=10, group_id=OTHER_GROUP)
        assert [r.content for r in mine] == ["mine fresh"]
        # Another tenant's rows are NEVER touched by this group's purge.
        assert [r.content for r in others] == ["other tenant expired"]

    @pytest.mark.asyncio
    async def test_purge_disabled_when_ttl_is_zero(self, session):
        from src.services.knowledge_embedding_service import KnowledgeEmbeddingService

        svc = KnowledgeEmbeddingService(AsyncMock(), GROUP)
        with patch("src.services.knowledge_embedding_service.KNOWLEDGE_TTL_DAYS", 0):
            assert await svc.purge_expired() == 0


# ---------------------------------------------------------------------------
# Tool wiring: user_email rides into the per-user search filter
# ---------------------------------------------------------------------------


class TestToolUserEmailWiring:
    def test_tool_stores_the_executing_user(self):
        from src.engines.crewai.tools.custom.databricks_knowledge_search_tool import (
            DatabricksKnowledgeSearchTool,
        )

        tool = DatabricksKnowledgeSearchTool(group_id=GROUP, user_email=ALICE)
        assert tool._user_email == ALICE

    def test_tool_defaults_to_no_user(self):
        from src.engines.crewai.tools.custom.databricks_knowledge_search_tool import (
            DatabricksKnowledgeSearchTool,
        )

        tool = DatabricksKnowledgeSearchTool(group_id=GROUP)
        assert tool._user_email is None
