"""
Unit tests for PowerBIContextConfigService.

Tests business logic for creating, updating, deleting, and retrieving
business mappings and field synonyms with proper error handling.
"""
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from src.services.powerbi_context_config_service import PowerBIContextConfigService
from src.schemas.powerbi_context_config import (
    PowerBIBusinessMappingCreate,
    PowerBIBusinessMappingUpdate,
    PowerBIFieldSynonymCreate,
    PowerBIFieldSynonymUpdate,
    PowerBIBusinessMappingResponse,
    PowerBIFieldSynonymResponse,
)


# ---------------------------------------------------------------------------
# Stub DB objects
# ---------------------------------------------------------------------------

def _now():
    return datetime.now(timezone.utc)


class StubBusinessMapping:
    def __init__(
        self,
        id=1,
        group_id="grp",
        semantic_model_id="m1",
        natural_term="total sales",
        dax_expression="[Total Sales]",
        description=None,
    ):
        self.id = id
        self.group_id = group_id
        self.semantic_model_id = semantic_model_id
        self.natural_term = natural_term
        self.dax_expression = dax_expression
        self.description = description
        self.created_at = _now()
        self.updated_at = _now()


class StubFieldSynonym:
    def __init__(
        self,
        id=1,
        group_id="grp",
        semantic_model_id="m1",
        field_name="num_customers",
        synonyms=None,
    ):
        self.id = id
        self.group_id = group_id
        self.semantic_model_id = semantic_model_id
        self.field_name = field_name
        self.synonyms = synonyms or ["customer count"]
        self.created_at = _now()
        self.updated_at = _now()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    return AsyncMock()


@pytest.fixture
def mock_business_repo():
    return AsyncMock()


@pytest.fixture
def mock_synonym_repo():
    return AsyncMock()


@pytest.fixture
def service(mock_session, mock_business_repo, mock_synonym_repo):
    with (
        patch(
            "src.services.powerbi_context_config_service.PowerBIBusinessMappingRepository",
            return_value=mock_business_repo,
        ),
        patch(
            "src.services.powerbi_context_config_service.PowerBIFieldSynonymRepository",
            return_value=mock_synonym_repo,
        ),
    ):
        svc = PowerBIContextConfigService(session=mock_session, group_id="grp")
        svc.business_mapping_repo = mock_business_repo
        svc.field_synonym_repo = mock_synonym_repo
        return svc


# ---------------------------------------------------------------------------
# Business Mappings
# ---------------------------------------------------------------------------

class TestCreateBusinessMapping:
    @pytest.mark.asyncio
    async def test_creates_successfully(self, service, mock_business_repo):
        mock_business_repo.get_by_term.return_value = None
        stub = StubBusinessMapping()
        mock_business_repo.create.return_value = stub

        data = PowerBIBusinessMappingCreate(
            natural_term="total sales",
            dax_expression="[Total Sales]",
            semantic_model_id="m1",
        )
        result = await service.create_business_mapping("m1", data)

        assert isinstance(result, PowerBIBusinessMappingResponse)
        assert result.natural_term == "total sales"
        mock_business_repo.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_409_when_duplicate(self, service, mock_business_repo):
        mock_business_repo.get_by_term.return_value = StubBusinessMapping()

        data = PowerBIBusinessMappingCreate(
            natural_term="total sales",
            dax_expression="[Total Sales]",
            semantic_model_id="m1",
        )

        with pytest.raises(HTTPException) as exc_info:
            await service.create_business_mapping("m1", data)

        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_raises_500_on_unexpected_error(self, service, mock_business_repo):
        mock_business_repo.get_by_term.side_effect = RuntimeError("db boom")

        data = PowerBIBusinessMappingCreate(
            natural_term="total sales",
            dax_expression="[Total Sales]",
            semantic_model_id="m1",
        )

        with pytest.raises(HTTPException) as exc_info:
            await service.create_business_mapping("m1", data)

        assert exc_info.value.status_code == 500


class TestUpdateBusinessMapping:
    @pytest.mark.asyncio
    async def test_updates_successfully(self, service, mock_business_repo):
        existing = StubBusinessMapping(id=1, group_id="grp")
        updated = StubBusinessMapping(id=1, dax_expression="[New DAX]")
        mock_business_repo.get.return_value = existing
        mock_business_repo.update.return_value = updated

        data = PowerBIBusinessMappingUpdate(dax_expression="[New DAX]")
        result = await service.update_business_mapping(1, data)

        assert result.dax_expression == "[New DAX]"

    @pytest.mark.asyncio
    async def test_raises_404_when_not_found(self, service, mock_business_repo):
        mock_business_repo.get.return_value = None

        data = PowerBIBusinessMappingUpdate(dax_expression="[X]")

        with pytest.raises(HTTPException) as exc_info:
            await service.update_business_mapping(99, data)

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_raises_404_when_wrong_group(self, service, mock_business_repo):
        wrong_group = StubBusinessMapping(id=1, group_id="other-group")
        mock_business_repo.get.return_value = wrong_group

        data = PowerBIBusinessMappingUpdate(dax_expression="[X]")

        with pytest.raises(HTTPException) as exc_info:
            await service.update_business_mapping(1, data)

        assert exc_info.value.status_code == 404


class TestDeleteBusinessMapping:
    @pytest.mark.asyncio
    async def test_deletes_successfully(self, service, mock_business_repo):
        existing = StubBusinessMapping(id=1, group_id="grp")
        mock_business_repo.get.return_value = existing
        mock_business_repo.delete.return_value = True

        result = await service.delete_business_mapping(1)

        assert result is True

    @pytest.mark.asyncio
    async def test_raises_404_when_not_found(self, service, mock_business_repo):
        mock_business_repo.get.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await service.delete_business_mapping(99)

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_raises_404_when_delete_returns_false(self, service, mock_business_repo):
        existing = StubBusinessMapping(id=1, group_id="grp")
        mock_business_repo.get.return_value = existing
        mock_business_repo.delete.return_value = False

        with pytest.raises(HTTPException) as exc_info:
            await service.delete_business_mapping(1)

        assert exc_info.value.status_code == 404


class TestGetBusinessMappings:
    @pytest.mark.asyncio
    async def test_returns_list(self, service, mock_business_repo):
        mappings = [StubBusinessMapping(id=i) for i in range(3)]
        mock_business_repo.get_by_model.return_value = mappings

        result = await service.get_business_mappings("m1")

        assert len(result) == 3
        assert all(isinstance(r, PowerBIBusinessMappingResponse) for r in result)

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_none(self, service, mock_business_repo):
        mock_business_repo.get_by_model.return_value = []

        result = await service.get_business_mappings("m1")

        assert result == []


# ---------------------------------------------------------------------------
# Field Synonyms
# ---------------------------------------------------------------------------

class TestCreateFieldSynonym:
    @pytest.mark.asyncio
    async def test_creates_successfully(self, service, mock_synonym_repo):
        mock_synonym_repo.get_by_field.return_value = None
        stub = StubFieldSynonym()
        mock_synonym_repo.create.return_value = stub

        data = PowerBIFieldSynonymCreate(
            field_name="num_customers",
            synonyms=["customer count"],
            semantic_model_id="m1",
        )
        result = await service.create_field_synonym("m1", data)

        assert isinstance(result, PowerBIFieldSynonymResponse)
        assert result.field_name == "num_customers"

    @pytest.mark.asyncio
    async def test_raises_409_when_duplicate(self, service, mock_synonym_repo):
        mock_synonym_repo.get_by_field.return_value = StubFieldSynonym()

        data = PowerBIFieldSynonymCreate(
            field_name="num_customers",
            synonyms=["customer count"],
            semantic_model_id="m1",
        )

        with pytest.raises(HTTPException) as exc_info:
            await service.create_field_synonym("m1", data)

        assert exc_info.value.status_code == 409

    @pytest.mark.asyncio
    async def test_raises_500_on_db_error(self, service, mock_synonym_repo):
        mock_synonym_repo.get_by_field.side_effect = RuntimeError("crash")

        data = PowerBIFieldSynonymCreate(
            field_name="num_customers",
            synonyms=["c"],
            semantic_model_id="m1",
        )

        with pytest.raises(HTTPException) as exc_info:
            await service.create_field_synonym("m1", data)

        assert exc_info.value.status_code == 500


class TestUpdateFieldSynonym:
    @pytest.mark.asyncio
    async def test_updates_successfully(self, service, mock_synonym_repo):
        existing = StubFieldSynonym(id=1, group_id="grp")
        updated = StubFieldSynonym(id=1, synonyms=["new alias"])
        mock_synonym_repo.get.return_value = existing
        mock_synonym_repo.update.return_value = updated

        data = PowerBIFieldSynonymUpdate(synonyms=["new alias"])
        result = await service.update_field_synonym(1, data)

        assert result.synonyms == ["new alias"]

    @pytest.mark.asyncio
    async def test_raises_404_when_not_found(self, service, mock_synonym_repo):
        mock_synonym_repo.get.return_value = None

        data = PowerBIFieldSynonymUpdate(synonyms=["x"])

        with pytest.raises(HTTPException) as exc_info:
            await service.update_field_synonym(99, data)

        assert exc_info.value.status_code == 404


class TestDeleteFieldSynonym:
    @pytest.mark.asyncio
    async def test_deletes_successfully(self, service, mock_synonym_repo):
        existing = StubFieldSynonym(id=1, group_id="grp")
        mock_synonym_repo.get.return_value = existing
        mock_synonym_repo.delete.return_value = True

        result = await service.delete_field_synonym(1)

        assert result is True

    @pytest.mark.asyncio
    async def test_raises_404_when_not_found(self, service, mock_synonym_repo):
        mock_synonym_repo.get.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await service.delete_field_synonym(99)

        assert exc_info.value.status_code == 404


class TestGetFieldSynonyms:
    @pytest.mark.asyncio
    async def test_returns_list(self, service, mock_synonym_repo):
        synonyms = [StubFieldSynonym(id=i) for i in range(2)]
        mock_synonym_repo.get_by_model.return_value = synonyms

        result = await service.get_field_synonyms("m1")

        assert len(result) == 2
        assert all(isinstance(r, PowerBIFieldSynonymResponse) for r in result)


# ---------------------------------------------------------------------------
# Bulk / Dict operations
# ---------------------------------------------------------------------------

class TestGetAllContextConfig:
    @pytest.mark.asyncio
    async def test_combines_mappings_and_synonyms(self, service, mock_business_repo, mock_synonym_repo):
        mock_business_repo.get_by_model.return_value = [StubBusinessMapping()]
        mock_synonym_repo.get_by_model.return_value = [StubFieldSynonym()]

        result = await service.get_all_context_config("m1")

        assert len(result.business_mappings) == 1
        assert len(result.field_synonyms) == 1

    @pytest.mark.asyncio
    async def test_empty_when_no_data(self, service, mock_business_repo, mock_synonym_repo):
        mock_business_repo.get_by_model.return_value = []
        mock_synonym_repo.get_by_model.return_value = []

        result = await service.get_all_context_config("m1")

        assert result.business_mappings == []
        assert result.field_synonyms == []


class TestGetContextConfigDict:
    @pytest.mark.asyncio
    async def test_returns_dict_format(self, service, mock_business_repo, mock_synonym_repo):
        mock_business_repo.get_as_dict.return_value = {"total sales": "[Total Sales]"}
        mock_synonym_repo.get_as_dict.return_value = {"num_customers": ["customer count"]}

        result = await service.get_context_config_dict("m1")

        assert result.business_mappings == {"total sales": "[Total Sales]"}
        assert result.field_synonyms == {"num_customers": ["customer count"]}

    @pytest.mark.asyncio
    async def test_returns_none_when_dicts_are_empty(self, service, mock_business_repo, mock_synonym_repo):
        mock_business_repo.get_as_dict.return_value = {}
        mock_synonym_repo.get_as_dict.return_value = {}

        result = await service.get_context_config_dict("m1")

        # Empty dicts are falsy → should be None per service logic
        assert result.business_mappings is None
        assert result.field_synonyms is None

    @pytest.mark.asyncio
    async def test_raises_500_on_error(self, service, mock_business_repo, mock_synonym_repo):
        mock_business_repo.get_as_dict.side_effect = RuntimeError("boom")

        with pytest.raises(HTTPException) as exc_info:
            await service.get_context_config_dict("m1")

        assert exc_info.value.status_code == 500
