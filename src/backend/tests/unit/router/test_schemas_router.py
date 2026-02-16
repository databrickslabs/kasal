"""Unit tests for schemas_router endpoints.

Tests all CRUD endpoints using direct async function calls with mocked
SchemaService dependencies.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace
from datetime import datetime

from fastapi import HTTPException

from src.api.schemas_router import (
    get_all_schemas,
    get_schemas_by_type,
    get_schema_by_name,
    create_schema,
    update_schema,
    delete_schema,
    router,
)
from src.schemas.schema import SchemaCreate, SchemaUpdate, SchemaListResponse
from src.utils.user_context import GroupContext


def gc():
    """Create a valid GroupContext for testing."""
    return GroupContext(
        group_ids=["g1"],
        group_email="u@x.com",
        email_domain="x.com",
        user_role="admin",
    )


def make_schema(sid=1, name="test_schema"):
    """Create a mock schema response object."""
    now = datetime.utcnow()
    return SimpleNamespace(
        id=sid,
        name=name,
        description="A test schema",
        schema_type="data_model",
        schema_definition={"type": "object"},
        field_descriptions={},
        keywords=["test"],
        tools=[],
        example_data=None,
        created_at=now,
        updated_at=now,
    )


def make_list_response(schemas=None, count=None):
    """Create a mock SchemaListResponse."""
    schemas = schemas or []
    return SimpleNamespace(
        schemas=schemas,
        count=count if count is not None else len(schemas),
    )


# ---------------------------------------------------------------------------
# GET /schemas
# ---------------------------------------------------------------------------

class TestGetAllSchemas:
    """Tests for get_all_schemas endpoint."""

    @pytest.mark.asyncio
    async def test_returns_all_schemas(self):
        svc = AsyncMock()
        resp = make_list_response([make_schema(1), make_schema(2)], 2)
        svc.get_all_schemas = AsyncMock(return_value=resp)

        result = await get_all_schemas(service=svc, group_context=gc())

        assert result.count == 2
        assert len(result.schemas) == 2
        svc.get_all_schemas.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_empty_list(self):
        svc = AsyncMock()
        svc.get_all_schemas = AsyncMock(return_value=make_list_response())

        result = await get_all_schemas(service=svc, group_context=gc())

        assert result.count == 0
        assert result.schemas == []

    @pytest.mark.asyncio
    async def test_propagates_exception(self):
        svc = AsyncMock()
        svc.get_all_schemas = AsyncMock(side_effect=Exception("db error"))

        with pytest.raises(Exception, match="db error"):
            await get_all_schemas(service=svc, group_context=gc())


# ---------------------------------------------------------------------------
# GET /schemas/by-type/{schema_type}
# ---------------------------------------------------------------------------

class TestGetSchemasByType:
    """Tests for get_schemas_by_type endpoint."""

    @pytest.mark.asyncio
    async def test_returns_matching_schemas(self):
        svc = AsyncMock()
        resp = make_list_response([make_schema()], 1)
        svc.get_schemas_by_type = AsyncMock(return_value=resp)

        result = await get_schemas_by_type(
            schema_type="data_model", service=svc, group_context=gc()
        )

        assert result.count == 1
        svc.get_schemas_by_type.assert_called_once_with("data_model")

    @pytest.mark.asyncio
    async def test_returns_empty_for_unknown_type(self):
        svc = AsyncMock()
        svc.get_schemas_by_type = AsyncMock(return_value=make_list_response())

        result = await get_schemas_by_type(
            schema_type="nonexistent", service=svc, group_context=gc()
        )

        assert result.count == 0


# ---------------------------------------------------------------------------
# GET /schemas/{schema_name}
# ---------------------------------------------------------------------------

class TestGetSchemaByName:
    """Tests for get_schema_by_name endpoint."""

    @pytest.mark.asyncio
    async def test_returns_schema(self):
        svc = AsyncMock()
        schema_obj = make_schema(1, "user_schema")
        svc.get_schema_by_name = AsyncMock(return_value=schema_obj)

        result = await get_schema_by_name(
            schema_name="user_schema", service=svc, group_context=gc()
        )

        assert result.name == "user_schema"
        svc.get_schema_by_name.assert_called_once_with("user_schema")

    @pytest.mark.asyncio
    async def test_not_found_raises(self):
        svc = AsyncMock()
        svc.get_schema_by_name = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Schema not found")
        )

        with pytest.raises(HTTPException) as exc_info:
            await get_schema_by_name(
                schema_name="missing", service=svc, group_context=gc()
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# POST /schemas
# ---------------------------------------------------------------------------

class TestCreateSchema:
    """Tests for create_schema endpoint."""

    @pytest.mark.asyncio
    async def test_create_success(self):
        svc = AsyncMock()
        new_schema = make_schema(3, "new_schema")
        svc.create_schema = AsyncMock(return_value=new_schema)

        schema_data = SchemaCreate(
            name="new_schema",
            description="New",
            schema_type="data_model",
            schema_definition={"type": "object"},
        )

        result = await create_schema(
            schema_data=schema_data, service=svc, group_context=gc()
        )

        assert result.name == "new_schema"
        assert result.id == 3
        svc.create_schema.assert_called_once_with(schema_data)

    @pytest.mark.asyncio
    async def test_create_duplicate_raises(self):
        svc = AsyncMock()
        svc.create_schema = AsyncMock(
            side_effect=HTTPException(status_code=400, detail="Schema already exists")
        )

        schema_data = SchemaCreate(
            name="existing",
            description="Dup",
            schema_type="data_model",
            schema_definition={"type": "object"},
        )

        with pytest.raises(HTTPException) as exc_info:
            await create_schema(
                schema_data=schema_data, service=svc, group_context=gc()
            )

        assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# PUT /schemas/{schema_name}
# ---------------------------------------------------------------------------

class TestUpdateSchema:
    """Tests for update_schema endpoint."""

    @pytest.mark.asyncio
    async def test_update_success(self):
        svc = AsyncMock()
        updated = make_schema(1, "updated_schema")
        updated.description = "Updated desc"
        svc.update_schema = AsyncMock(return_value=updated)

        update_data = SchemaUpdate(description="Updated desc")

        result = await update_schema(
            schema_name="updated_schema",
            schema_data=update_data,
            service=svc,
            group_context=gc(),
        )

        assert result.description == "Updated desc"
        svc.update_schema.assert_called_once_with("updated_schema", update_data)

    @pytest.mark.asyncio
    async def test_update_not_found(self):
        svc = AsyncMock()
        svc.update_schema = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Schema not found")
        )

        update_data = SchemaUpdate(description="X")

        with pytest.raises(HTTPException) as exc_info:
            await update_schema(
                schema_name="missing",
                schema_data=update_data,
                service=svc,
                group_context=gc(),
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /schemas/{schema_name}
# ---------------------------------------------------------------------------

class TestDeleteSchema:
    """Tests for delete_schema endpoint."""

    @pytest.mark.asyncio
    async def test_delete_success(self):
        svc = AsyncMock()
        svc.delete_schema = AsyncMock(return_value=None)

        result = await delete_schema(
            schema_name="to_delete", service=svc, group_context=gc()
        )

        assert result is None
        svc.delete_schema.assert_called_once_with("to_delete")

    @pytest.mark.asyncio
    async def test_delete_not_found(self):
        svc = AsyncMock()
        svc.delete_schema = AsyncMock(
            side_effect=HTTPException(status_code=404, detail="Schema not found")
        )

        with pytest.raises(HTTPException) as exc_info:
            await delete_schema(
                schema_name="missing", service=svc, group_context=gc()
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_propagates_error(self):
        svc = AsyncMock()
        svc.delete_schema = AsyncMock(side_effect=RuntimeError("db error"))

        with pytest.raises(RuntimeError, match="db error"):
            await delete_schema(
                schema_name="schema1", service=svc, group_context=gc()
            )


# ---------------------------------------------------------------------------
# Router configuration
# ---------------------------------------------------------------------------

class TestRouterConfiguration:
    """Tests for router prefix and tags."""

    def test_router_config(self):
        assert router.prefix == "/schemas"
        assert "schemas" in router.tags

    def test_router_has_expected_endpoints(self):
        route_paths = [route.path for route in router.routes]
        expected = [
            "/schemas",
            "/schemas/by-type/{schema_type}",
            "/schemas/{schema_name}",
        ]
        for path in expected:
            assert path in route_paths, f"Missing route: {path}"

    def test_expected_methods(self):
        methods_by_path = {}
        for route in router.routes:
            for method in route.methods:
                methods_by_path.setdefault(route.path, set()).add(method)

        assert "GET" in methods_by_path["/schemas"]
        assert "POST" in methods_by_path["/schemas"]
        assert "GET" in methods_by_path["/schemas/{schema_name}"]
        assert "PUT" in methods_by_path["/schemas/{schema_name}"]
        assert "DELETE" in methods_by_path["/schemas/{schema_name}"]
