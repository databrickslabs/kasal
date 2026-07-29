"""Knowledge retention: the TTL, and making it true of the database.

Expiry was already applied at search time and before each upload, which makes
expired uploads unreachable but not gone — a workspace where nobody uploads
again keeps them indefinitely. That is the wrong half of a retention promise to
get right.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestTtlDefault:
    def test_the_window_is_seven_days(self):
        """Attachments, not a curated corpus: someone drops a PDF to ask about
        it. Thirty days was longer than the intent, and the shorter the window
        the smaller the answer to "what user data do you still hold"."""
        from src.services.knowledge.embedding_service import KNOWLEDGE_TTL_DAYS

        assert KNOWLEDGE_TTL_DAYS == 7

    def test_it_is_configurable(self, monkeypatch):
        import importlib

        monkeypatch.setenv("KNOWLEDGE_TTL_DAYS", "1")
        module = importlib.reload(
            importlib.import_module("src.services.knowledge.embedding_service")
        )
        assert module.KNOWLEDGE_TTL_DAYS == 1

        monkeypatch.delenv("KNOWLEDGE_TTL_DAYS")
        importlib.reload(module)


class TestSweep:
    @pytest.mark.asyncio
    async def test_it_deletes_past_the_cutoff(self):
        from src.services.knowledge.retention import sweep_expired_knowledge

        repository = MagicMock()
        repository.delete_expired_all = AsyncMock(return_value=42)
        session = MagicMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.commit = AsyncMock()

        with (
            patch("src.db.session.get_isolated_db_session", return_value=session),
            patch(
                "src.repositories.documentation_embedding_repository."
                "DocumentationEmbeddingRepository",
                return_value=repository,
            ),
        ):
            assert await sweep_expired_knowledge() == 42

        cutoff = repository.delete_expired_all.await_args.args[0]
        assert cutoff < datetime.utcnow() - timedelta(days=6)
        session.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_disabled_ttl_deletes_nothing(self):
        """0 must mean "keep everything", not "everything is expired"."""
        from src.services.knowledge import retention

        with patch("src.services.knowledge.embedding_service.KNOWLEDGE_TTL_DAYS", 0):
            assert await retention.sweep_expired_knowledge() == 0

    @pytest.mark.asyncio
    async def test_a_failure_never_escapes(self):
        """It runs on a background loop; a raised error would take the loop down
        and silently end retention for the process's lifetime."""
        from src.services.knowledge.retention import sweep_expired_knowledge

        with patch(
            "src.db.session.get_isolated_db_session",
            side_effect=RuntimeError("database gone"),
        ):
            assert await sweep_expired_knowledge() == 0


class TestReuploadIdentity:
    """Three uploads of one PDF stored three copies of all 110 chunks, and a
    15-result search came back with 4 distinct passages — the rest was the same
    text again. Nothing malfunctioned: the embed appended and nothing
    deduplicated afterwards.
    """

    @pytest.mark.asyncio
    async def test_identical_content_is_recognised(self, tmp_path):
        import hashlib

        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
        from sqlalchemy.orm import sessionmaker

        from src.models.documentation_embedding import KnowledgeEmbedding
        from src.repositories.documentation_embedding_repository import (
            DocumentationEmbeddingRepository,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(KnowledgeEmbedding.__table__.create)
        maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        path = "uploads/g1/s1/paper.pdf"
        digest = hashlib.sha256(b"the extracted text").hexdigest()
        async with maker() as session:
            repository = DocumentationEmbeddingRepository(
                session, model=KnowledgeEmbedding
            )
            for i in range(3):
                session.add(
                    KnowledgeEmbedding(
                        source=path,
                        title=f"chunk {i}",
                        content=f"chunk {i}",
                        embedding=[0.1],
                        doc_metadata={"content_hash": digest},
                        group_id="g1",
                        file_path=path,
                        created_by="user@example.com",
                    )
                )
            await session.flush()

            stored = await repository.find_content_hash("g1", path, "user@example.com")
            assert stored == digest, "the same file re-uploaded must be recognised"

            changed = hashlib.sha256(b"a revised document").hexdigest()
            assert stored != changed, "changed content must NOT be skipped"

            # Another user's hash is not readable — it would reveal that they
            # uploaded a particular document.
            assert (
                await repository.find_content_hash("g1", path, "other@example.com")
            ) is None

            removed = await repository.delete_by_path("g1", path, "user@example.com")
            assert removed == 3, "replacing a changed file removes the old chunks"
            assert (
                await repository.find_content_hash("g1", path, "user@example.com")
            ) is None

        await engine.dispose()
