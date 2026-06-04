"""
Unit tests for PowerBIBusinessMappingRepository and PowerBIFieldSynonymRepository.

Tests the functionality of Power BI context configuration repositories including
business term mappings and field synonym CRUD operations.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.powerbi_context_config_repository import (
    PowerBIBusinessMappingRepository,
    PowerBIFieldSynonymRepository,
)
from src.models.powerbi_context_config import PowerBIBusinessMapping, PowerBIFieldSynonym


# ---------------------------------------------------------------------------
# Helpers / stub models
# ---------------------------------------------------------------------------

class MockBusinessMapping:
    def __init__(
        self,
        id=1,
        group_id="group1",
        semantic_model_id="model-1",
        natural_term="total sales",
        dax_expression="[Total Sales]",
        description=None,
        created_at=None,
        updated_at=None,
    ):
        self.id = id
        self.group_id = group_id
        self.semantic_model_id = semantic_model_id
        self.natural_term = natural_term
        self.dax_expression = dax_expression
        self.description = description
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)


class MockFieldSynonym:
    def __init__(
        self,
        id=1,
        group_id="group1",
        semantic_model_id="model-1",
        field_name="num_customers",
        synonyms=None,
        created_at=None,
        updated_at=None,
    ):
        self.id = id
        self.group_id = group_id
        self.semantic_model_id = semantic_model_id
        self.field_name = field_name
        self.synonyms = synonyms if synonyms is not None else ["customer count", "customers"]
        self.created_at = created_at or datetime.now(timezone.utc)
        self.updated_at = updated_at or datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    session = AsyncMock(spec=AsyncSession)
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()
    return session


@pytest.fixture
def mapping_repo(mock_session):
    return PowerBIBusinessMappingRepository(session=mock_session)


@pytest.fixture
def synonym_repo(mock_session):
    return PowerBIFieldSynonymRepository(session=mock_session)


def _make_scalar_result(items):
    """Build a mock result that mimics .scalars().all() or .scalars().first()."""
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = items
    mock_scalars.first.return_value = items[0] if items else None
    mock_result = MagicMock()
    mock_result.scalars.return_value = mock_scalars
    return mock_result


# ---------------------------------------------------------------------------
# PowerBIBusinessMappingRepository tests
# ---------------------------------------------------------------------------

class TestBusinessMappingRepositoryInit:
    def test_init_sets_model_and_session(self, mock_session):
        repo = PowerBIBusinessMappingRepository(session=mock_session)
        assert repo.model is PowerBIBusinessMapping
        assert repo.session is mock_session


class TestBusinessMappingGetByModel:
    @pytest.mark.asyncio
    async def test_get_by_model_returns_list(self, mapping_repo, mock_session):
        mappings = [
            MockBusinessMapping(id=1, natural_term="sales"),
            MockBusinessMapping(id=2, natural_term="revenue"),
        ]
        mock_session.execute.return_value = _make_scalar_result(mappings)

        result = await mapping_repo.get_by_model(
            group_id="group1", semantic_model_id="model-1"
        )

        assert len(result) == 2
        assert result[0].natural_term == "sales"
        mock_session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_by_model_empty_returns_empty_list(self, mapping_repo, mock_session):
        mock_session.execute.return_value = _make_scalar_result([])

        result = await mapping_repo.get_by_model(
            group_id="group1", semantic_model_id="no-such-model"
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_get_by_model_different_groups_isolated(self, mapping_repo, mock_session):
        mapping_g1 = MockBusinessMapping(group_id="group1")
        mock_session.execute.return_value = _make_scalar_result([mapping_g1])

        result = await mapping_repo.get_by_model(
            group_id="group1", semantic_model_id="model-1"
        )

        assert len(result) == 1
        assert result[0].group_id == "group1"


class TestBusinessMappingGetByTerm:
    @pytest.mark.asyncio
    async def test_get_by_term_found(self, mapping_repo, mock_session):
        mapping = MockBusinessMapping(natural_term="total sales")
        mock_session.execute.return_value = _make_scalar_result([mapping])

        result = await mapping_repo.get_by_term(
            group_id="group1",
            semantic_model_id="model-1",
            natural_term="total sales",
        )

        assert result is not None
        assert result.natural_term == "total sales"

    @pytest.mark.asyncio
    async def test_get_by_term_not_found_returns_none(self, mapping_repo, mock_session):
        mock_session.execute.return_value = _make_scalar_result([])

        result = await mapping_repo.get_by_term(
            group_id="group1",
            semantic_model_id="model-1",
            natural_term="unknown term",
        )

        assert result is None


class TestBusinessMappingDeleteByModel:
    @pytest.mark.asyncio
    async def test_delete_by_model_returns_rowcount(self, mapping_repo, mock_session):
        mock_result = MagicMock()
        mock_result.rowcount = 3
        mock_session.execute.return_value = mock_result

        count = await mapping_repo.delete_by_model(
            group_id="group1", semantic_model_id="model-1"
        )

        assert count == 3
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_by_model_zero_when_none_exist(self, mapping_repo, mock_session):
        mock_result = MagicMock()
        mock_result.rowcount = 0
        mock_session.execute.return_value = mock_result

        count = await mapping_repo.delete_by_model(
            group_id="group1", semantic_model_id="nonexistent"
        )

        assert count == 0

    @pytest.mark.asyncio
    async def test_delete_by_model_db_error_propagates(self, mapping_repo, mock_session):
        mock_session.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            await mapping_repo.delete_by_model(
                group_id="group1", semantic_model_id="model-1"
            )


class TestBusinessMappingGetAsDict:
    @pytest.mark.asyncio
    async def test_get_as_dict_returns_mapping(self, mapping_repo, mock_session):
        mappings = [
            MockBusinessMapping(natural_term="total sales", dax_expression="[Total Sales]"),
            MockBusinessMapping(
                id=2,
                natural_term="ytd revenue",
                dax_expression="CALCULATE(SUM([Revenue]), DATESYTD([Date]))",
            ),
        ]
        mock_session.execute.return_value = _make_scalar_result(mappings)

        result = await mapping_repo.get_as_dict(
            group_id="group1", semantic_model_id="model-1"
        )

        assert result == {
            "total sales": "[Total Sales]",
            "ytd revenue": "CALCULATE(SUM([Revenue]), DATESYTD([Date]))",
        }

    @pytest.mark.asyncio
    async def test_get_as_dict_empty_returns_empty_dict(self, mapping_repo, mock_session):
        mock_session.execute.return_value = _make_scalar_result([])

        result = await mapping_repo.get_as_dict(
            group_id="group1", semantic_model_id="model-1"
        )

        assert result == {}


# ---------------------------------------------------------------------------
# PowerBIFieldSynonymRepository tests
# ---------------------------------------------------------------------------

class TestFieldSynonymRepositoryInit:
    def test_init_sets_model_and_session(self, mock_session):
        repo = PowerBIFieldSynonymRepository(session=mock_session)
        assert repo.model is PowerBIFieldSynonym
        assert repo.session is mock_session


class TestFieldSynonymGetByModel:
    @pytest.mark.asyncio
    async def test_get_by_model_returns_list(self, synonym_repo, mock_session):
        synonyms = [
            MockFieldSynonym(id=1, field_name="customers"),
            MockFieldSynonym(id=2, field_name="revenue"),
        ]
        mock_session.execute.return_value = _make_scalar_result(synonyms)

        result = await synonym_repo.get_by_model(
            group_id="group1", semantic_model_id="model-1"
        )

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_get_by_model_empty_returns_empty_list(self, synonym_repo, mock_session):
        mock_session.execute.return_value = _make_scalar_result([])

        result = await synonym_repo.get_by_model(
            group_id="group1", semantic_model_id="no-such-model"
        )

        assert result == []


class TestFieldSynonymGetByField:
    @pytest.mark.asyncio
    async def test_get_by_field_found(self, synonym_repo, mock_session):
        synonym = MockFieldSynonym(field_name="num_customers")
        mock_session.execute.return_value = _make_scalar_result([synonym])

        result = await synonym_repo.get_by_field(
            group_id="group1",
            semantic_model_id="model-1",
            field_name="num_customers",
        )

        assert result is not None
        assert result.field_name == "num_customers"

    @pytest.mark.asyncio
    async def test_get_by_field_not_found_returns_none(self, synonym_repo, mock_session):
        mock_session.execute.return_value = _make_scalar_result([])

        result = await synonym_repo.get_by_field(
            group_id="group1",
            semantic_model_id="model-1",
            field_name="unknown_field",
        )

        assert result is None


class TestFieldSynonymDeleteByModel:
    @pytest.mark.asyncio
    async def test_delete_by_model_returns_rowcount(self, synonym_repo, mock_session):
        mock_result = MagicMock()
        mock_result.rowcount = 2
        mock_session.execute.return_value = mock_result

        count = await synonym_repo.delete_by_model(
            group_id="group1", semantic_model_id="model-1"
        )

        assert count == 2
        mock_session.flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_by_model_zero_rows(self, synonym_repo, mock_session):
        mock_result = MagicMock()
        mock_result.rowcount = 0
        mock_session.execute.return_value = mock_result

        count = await synonym_repo.delete_by_model(
            group_id="group1", semantic_model_id="nonexistent"
        )

        assert count == 0


class TestFieldSynonymGetAsDict:
    @pytest.mark.asyncio
    async def test_get_as_dict_returns_mapping(self, synonym_repo, mock_session):
        synonyms = [
            MockFieldSynonym(
                id=1,
                field_name="num_customers",
                synonyms=["customer count", "customers"],
            ),
            MockFieldSynonym(
                id=2,
                field_name="total_revenue",
                synonyms=["revenue", "income"],
            ),
        ]
        mock_session.execute.return_value = _make_scalar_result(synonyms)

        result = await synonym_repo.get_as_dict(
            group_id="group1", semantic_model_id="model-1"
        )

        assert result == {
            "num_customers": ["customer count", "customers"],
            "total_revenue": ["revenue", "income"],
        }

    @pytest.mark.asyncio
    async def test_get_as_dict_empty(self, synonym_repo, mock_session):
        mock_session.execute.return_value = _make_scalar_result([])

        result = await synonym_repo.get_as_dict(
            group_id="group1", semantic_model_id="model-1"
        )

        assert result == {}

    @pytest.mark.asyncio
    async def test_get_as_dict_db_error_propagates(self, synonym_repo, mock_session):
        mock_session.execute.side_effect = RuntimeError("connection lost")

        with pytest.raises(RuntimeError, match="connection lost"):
            await synonym_repo.get_as_dict(
                group_id="group1", semantic_model_id="model-1"
            )
