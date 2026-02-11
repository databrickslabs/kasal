"""Unit tests for documentation_embeddings_router endpoints.

Tests all endpoints using direct async function calls with mocked service
dependencies and TestClient for seed-all and list-view integration tests.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
from datetime import datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.documentation_embeddings_router import (
    create_documentation_embedding,
    search_documentation_embeddings,
    get_documentation_embeddings,
    get_recent_documentation_embeddings,
    get_documentation_embedding,
    delete_documentation_embedding,
    seed_all_documentation_embeddings,
    router,
    get_documentation_embedding_service,
)
from src.core.exceptions import NotFoundError
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.utils.user_context import GroupContext

from tests.unit.router.conftest import register_exception_handlers


def gc():
    """Create a valid GroupContext for testing."""
    return GroupContext(
        group_ids=["g1"],
        group_email="u@x.com",
        email_domain="x.com",
        user_role="admin",
    )


def make_embedding(eid=1, source="test_src", title="Test Doc"):
    """Create a mock documentation embedding object."""
    now = datetime.utcnow()
    return SimpleNamespace(
        id=eid,
        source=source,
        title=title,
        content="Test content",
        embedding=[0.1, 0.2, 0.3],
        doc_metadata={"category": "test"},
        created_at=now,
        updated_at=now,
    )


# ---------------------------------------------------------------------------
# POST /documentation-embeddings/
# ---------------------------------------------------------------------------

class TestCreateDocumentationEmbedding:
    """Tests for create_documentation_embedding endpoint."""

    @pytest.mark.asyncio
    async def test_create_success(self):
        svc = AsyncMock()
        emb = make_embedding()
        svc.create_documentation_embedding = AsyncMock(return_value=emb)

        create_data = DocumentationEmbeddingCreate(
            source="src",
            title="T",
            content="C",
            embedding=[0.1],
        )

        result = await create_documentation_embedding(
            embedding=create_data,
            service=svc,
            group_context=gc(),
            x_forwarded_access_token=None,
            x_auth_request_access_token=None,
        )

        assert result.id == 1
        svc.create_documentation_embedding.assert_called_once_with(
            create_data, user_token=None
        )

    @pytest.mark.asyncio
    async def test_create_with_oauth_token_priority(self):
        svc = AsyncMock()
        svc.create_documentation_embedding = AsyncMock(return_value=make_embedding())

        create_data = DocumentationEmbeddingCreate(
            source="src",
            title="T",
            content="C",
            embedding=[0.1],
        )

        await create_documentation_embedding(
            embedding=create_data,
            service=svc,
            group_context=gc(),
            x_forwarded_access_token="forwarded-token",
            x_auth_request_access_token="oauth-token",
        )

        # OAuth2-Proxy token takes priority
        svc.create_documentation_embedding.assert_called_once_with(
            create_data, user_token="oauth-token"
        )

    @pytest.mark.asyncio
    async def test_create_with_forwarded_token_only(self):
        svc = AsyncMock()
        svc.create_documentation_embedding = AsyncMock(return_value=make_embedding())

        create_data = DocumentationEmbeddingCreate(
            source="src",
            title="T",
            content="C",
            embedding=[0.1],
        )

        await create_documentation_embedding(
            embedding=create_data,
            service=svc,
            group_context=gc(),
            x_forwarded_access_token="db-token",
            x_auth_request_access_token=None,
        )

        svc.create_documentation_embedding.assert_called_once_with(
            create_data, user_token="db-token"
        )


# ---------------------------------------------------------------------------
# GET /documentation-embeddings/search
# ---------------------------------------------------------------------------

class TestSearchDocumentationEmbeddings:
    """Tests for search_documentation_embeddings endpoint."""

    @pytest.mark.asyncio
    async def test_search_returns_results(self):
        svc = AsyncMock()
        embs = [make_embedding(1), make_embedding(2)]
        svc.search_similar_embeddings = AsyncMock(return_value=embs)

        result = await search_documentation_embeddings(
            service=svc,
            query_embedding=[0.1, 0.2],
            limit=5,
            group_context=gc(),
        )

        assert len(result) == 2
        svc.search_similar_embeddings.assert_called_once_with(
            query_embedding=[0.1, 0.2], limit=5
        )

    @pytest.mark.asyncio
    async def test_search_empty_results(self):
        svc = AsyncMock()
        svc.search_similar_embeddings = AsyncMock(return_value=[])

        result = await search_documentation_embeddings(
            service=svc,
            query_embedding=[0.5],
            limit=10,
            group_context=gc(),
        )

        assert result == []


# ---------------------------------------------------------------------------
# GET /documentation-embeddings/
# ---------------------------------------------------------------------------

class TestGetDocumentationEmbeddings:
    """Tests for get_documentation_embeddings endpoint (list view)."""

    @pytest.mark.asyncio
    async def test_list_no_filter(self):
        svc = AsyncMock()
        svc.get_documentation_embeddings = AsyncMock(
            return_value=[make_embedding()]
        )

        result = await get_documentation_embeddings(
            service=svc, skip=0, limit=100, source=None, title=None,
            group_context=gc(),
        )

        assert len(result) == 1
        assert result[0]["embedding"] == []  # cleared for list view
        assert result[0]["id"] == 1
        svc.get_documentation_embeddings.assert_called_once_with(0, 100)

    @pytest.mark.asyncio
    async def test_list_by_source(self):
        svc = AsyncMock()
        svc.search_by_source = AsyncMock(return_value=[make_embedding()])

        result = await get_documentation_embeddings(
            service=svc, skip=0, limit=100, source="my_source", title=None,
            group_context=gc(),
        )

        assert len(result) == 1
        svc.search_by_source.assert_called_once_with("my_source", 0, 100)

    @pytest.mark.asyncio
    async def test_list_by_title(self):
        svc = AsyncMock()
        svc.search_by_title = AsyncMock(return_value=[make_embedding()])

        result = await get_documentation_embeddings(
            service=svc, skip=0, limit=100, source=None, title="Doc",
            group_context=gc(),
        )

        assert len(result) == 1
        svc.search_by_title.assert_called_once_with("Doc", 0, 100)

    @pytest.mark.asyncio
    async def test_list_source_takes_priority_over_title(self):
        """When both source and title are provided, source filter wins."""
        svc = AsyncMock()
        svc.search_by_source = AsyncMock(return_value=[])

        result = await get_documentation_embeddings(
            service=svc, skip=0, limit=100, source="src", title="title",
            group_context=gc(),
        )

        assert result == []
        svc.search_by_source.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_with_pagination(self):
        svc = AsyncMock()
        svc.get_documentation_embeddings = AsyncMock(return_value=[])

        await get_documentation_embeddings(
            service=svc, skip=10, limit=20, source=None, title=None,
            group_context=gc(),
        )

        svc.get_documentation_embeddings.assert_called_once_with(10, 20)


# ---------------------------------------------------------------------------
# GET /documentation-embeddings/recent
# ---------------------------------------------------------------------------

class TestGetRecentDocumentationEmbeddings:
    """Tests for get_recent_documentation_embeddings endpoint."""

    @pytest.mark.asyncio
    async def test_recent_returns_results(self):
        svc = AsyncMock()
        embs = [make_embedding(i) for i in range(5)]
        svc.get_recent_embeddings = AsyncMock(return_value=embs)

        result = await get_recent_documentation_embeddings(
            service=svc, limit=5, group_context=gc(),
        )

        assert len(result) == 5
        svc.get_recent_embeddings.assert_called_once_with(5)


# ---------------------------------------------------------------------------
# GET /documentation-embeddings/{embedding_id}
# ---------------------------------------------------------------------------

class TestGetDocumentationEmbeddingById:
    """Tests for get_documentation_embedding endpoint."""

    @pytest.mark.asyncio
    async def test_get_by_id_success(self):
        svc = AsyncMock()
        emb = make_embedding(42)
        svc.get_documentation_embedding = AsyncMock(return_value=emb)

        result = await get_documentation_embedding(
            embedding_id=42, service=svc, group_context=gc(),
        )

        assert result.id == 42

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self):
        svc = AsyncMock()
        svc.get_documentation_embedding = AsyncMock(return_value=None)

        with pytest.raises(NotFoundError):
            await get_documentation_embedding(
                embedding_id=999, service=svc, group_context=gc(),
            )


# ---------------------------------------------------------------------------
# DELETE /documentation-embeddings/{embedding_id}
# ---------------------------------------------------------------------------

class TestDeleteDocumentationEmbedding:
    """Tests for delete_documentation_embedding endpoint."""

    @pytest.mark.asyncio
    async def test_delete_success(self):
        svc = AsyncMock()
        svc.delete_documentation_embedding = AsyncMock(return_value=True)

        result = await delete_documentation_embedding(
            embedding_id=1, service=svc, group_context=gc(),
        )

        assert result["message"] == "Documentation embedding deleted successfully"

    @pytest.mark.asyncio
    async def test_delete_not_found(self):
        svc = AsyncMock()
        svc.delete_documentation_embedding = AsyncMock(return_value=False)

        with pytest.raises(NotFoundError):
            await delete_documentation_embedding(
                embedding_id=999, service=svc, group_context=gc(),
            )


# ---------------------------------------------------------------------------
# POST /documentation-embeddings/seed-all
# ---------------------------------------------------------------------------

class TestSeedAllDocumentationEmbeddings:
    """Tests for seed_all_documentation_embeddings endpoint."""

    @pytest.mark.asyncio
    async def test_seed_all_success(self):
        with patch(
            "src.api.documentation_embeddings_router.seed_documentation_embeddings",
            new_callable=AsyncMock,
            create=True,
        ) as mock_seed:
            # Patch the import inside the function
            with patch(
                "src.seeds.documentation.seed_documentation_embeddings",
                new_callable=AsyncMock,
            ) as mock_inner_seed:
                result = await seed_all_documentation_embeddings(
                    group_context=gc(),
                    x_forwarded_access_token=None,
                    x_auth_request_access_token=None,
                )

        assert result["success"] is True
        assert "successfully" in result["message"]

    @pytest.mark.asyncio
    async def test_seed_all_with_oauth_token(self):
        with patch(
            "src.seeds.documentation.seed_documentation_embeddings",
            new_callable=AsyncMock,
        ) as mock_seed:
            result = await seed_all_documentation_embeddings(
                group_context=gc(),
                x_forwarded_access_token="db-token",
                x_auth_request_access_token="oauth-token",
            )

        assert result["success"] is True
        mock_seed.assert_called_once_with(user_token="oauth-token")

    @pytest.mark.asyncio
    async def test_seed_all_error_returns_failure(self):
        with patch(
            "src.seeds.documentation.seed_documentation_embeddings",
            new_callable=AsyncMock,
            side_effect=Exception("Seeding error"),
        ):
            result = await seed_all_documentation_embeddings(
                group_context=gc(),
                x_forwarded_access_token=None,
                x_auth_request_access_token=None,
            )

        assert result["success"] is False
        assert "Failed" in result["message"]
        assert "Seeding error" in result["message"]


# ---------------------------------------------------------------------------
# Router configuration
# ---------------------------------------------------------------------------

class TestRouterConfiguration:
    """Tests for router prefix and tags."""

    def test_router_config(self):
        assert router.prefix == "/documentation-embeddings"
        assert "documentation-embeddings" in router.tags

    def test_router_has_expected_endpoints(self):
        route_paths = [route.path for route in router.routes]
        expected = [
            "/documentation-embeddings/",
            "/documentation-embeddings/search",
            "/documentation-embeddings/recent",
            "/documentation-embeddings/{embedding_id}",
            "/documentation-embeddings/seed-all",
        ]
        for path in expected:
            assert path in route_paths, f"Missing route: {path}"
