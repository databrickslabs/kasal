"""
Unit tests for the PowerBI context configuration schemas.

Covers:
- PowerBIBusinessMappingBase: required fields, optional fields, validation
- PowerBIBusinessMappingCreate: inherits base + semantic_model_id
- PowerBIBusinessMappingUpdate: all-optional fields
- PowerBIBusinessMappingInDB / PowerBIBusinessMappingResponse: ORM-mode
- PowerBIFieldSynonymBase: required fields, list type
- PowerBIFieldSynonymCreate / PowerBIFieldSynonymUpdate / PowerBIFieldSynonymInDB
- PowerBIContextConfigBulkResponse: list composition
- PowerBIContextConfigDict: optional dict fields, defaults to None
"""
import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

from src.schemas.powerbi_context_config import (
    PowerBIBusinessMappingBase,
    PowerBIBusinessMappingCreate,
    PowerBIBusinessMappingUpdate,
    PowerBIBusinessMappingInDB,
    PowerBIBusinessMappingResponse,
    PowerBIFieldSynonymBase,
    PowerBIFieldSynonymCreate,
    PowerBIFieldSynonymUpdate,
    PowerBIFieldSynonymInDB,
    PowerBIFieldSynonymResponse,
    PowerBIContextConfigBulkResponse,
    PowerBIContextConfigDict,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)


def _mapping_in_db(**overrides) -> dict:
    defaults = dict(
        id=1,
        group_id="grp-001",
        semantic_model_id="sm-001",
        natural_term="revenue",
        dax_expression="SUM(Sales[Revenue])",
        description=None,
        created_at=_NOW,
        updated_at=_NOW,
    )
    defaults.update(overrides)
    return defaults


def _synonym_in_db(**overrides) -> dict:
    defaults = dict(
        id=1,
        group_id="grp-001",
        semantic_model_id="sm-001",
        field_name="OrderDate",
        synonyms=["order date", "purchase date"],
        created_at=_NOW,
        updated_at=_NOW,
    )
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# BusinessMappingBase
# ---------------------------------------------------------------------------

class TestPowerBIBusinessMappingBase:

    def test_valid_minimal(self):
        m = PowerBIBusinessMappingBase(
            natural_term="revenue",
            dax_expression="SUM(Sales[Revenue])",
        )
        assert m.natural_term == "revenue"
        assert m.dax_expression == "SUM(Sales[Revenue])"
        assert m.description is None

    def test_valid_with_description(self):
        m = PowerBIBusinessMappingBase(
            natural_term="gross profit",
            dax_expression="[Revenue] - [COGS]",
            description="Total revenue minus cost of goods sold",
        )
        assert m.description == "Total revenue minus cost of goods sold"

    def test_description_defaults_to_none(self):
        m = PowerBIBusinessMappingBase(
            natural_term="x", dax_expression="y"
        )
        assert m.description is None

    def test_missing_natural_term_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIBusinessMappingBase(dax_expression="SUM(x)")
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "natural_term" in fields

    def test_missing_dax_expression_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIBusinessMappingBase(natural_term="revenue")
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "dax_expression" in fields

    def test_missing_both_required_fields_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIBusinessMappingBase()
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "natural_term" in fields
        assert "dax_expression" in fields

    def test_explicit_none_description(self):
        m = PowerBIBusinessMappingBase(
            natural_term="x", dax_expression="y", description=None
        )
        assert m.description is None

    def test_empty_string_description(self):
        m = PowerBIBusinessMappingBase(
            natural_term="x", dax_expression="y", description=""
        )
        assert m.description == ""


# ---------------------------------------------------------------------------
# BusinessMappingCreate
# ---------------------------------------------------------------------------

class TestPowerBIBusinessMappingCreate:

    def test_valid_create(self):
        c = PowerBIBusinessMappingCreate(
            natural_term="sales",
            dax_expression="SUM(Sales[Amount])",
            semantic_model_id="sm-abc",
        )
        assert c.natural_term == "sales"
        assert c.semantic_model_id == "sm-abc"

    def test_inherits_base_optional_description(self):
        c = PowerBIBusinessMappingCreate(
            natural_term="x", dax_expression="y", semantic_model_id="sm-1"
        )
        assert c.description is None

    def test_missing_semantic_model_id_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIBusinessMappingCreate(natural_term="x", dax_expression="y")
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "semantic_model_id" in fields

    def test_missing_base_fields_raises(self):
        with pytest.raises(ValidationError):
            PowerBIBusinessMappingCreate(semantic_model_id="sm-1")

    def test_all_fields_present(self):
        c = PowerBIBusinessMappingCreate(
            natural_term="n",
            dax_expression="d",
            description="desc",
            semantic_model_id="sm-99",
        )
        assert c.natural_term == "n"
        assert c.dax_expression == "d"
        assert c.description == "desc"
        assert c.semantic_model_id == "sm-99"


# ---------------------------------------------------------------------------
# BusinessMappingUpdate
# ---------------------------------------------------------------------------

class TestPowerBIBusinessMappingUpdate:

    def test_empty_update_is_valid(self):
        """All fields are optional, so an empty update is valid."""
        u = PowerBIBusinessMappingUpdate()
        assert u.natural_term is None
        assert u.dax_expression is None
        assert u.description is None

    def test_partial_update_natural_term_only(self):
        u = PowerBIBusinessMappingUpdate(natural_term="new term")
        assert u.natural_term == "new term"
        assert u.dax_expression is None

    def test_partial_update_dax_expression_only(self):
        u = PowerBIBusinessMappingUpdate(dax_expression="CALCULATE(SUM(x))")
        assert u.dax_expression == "CALCULATE(SUM(x))"

    def test_full_update(self):
        u = PowerBIBusinessMappingUpdate(
            natural_term="updated",
            dax_expression="NEW_EXPR()",
            description="updated desc",
        )
        assert u.natural_term == "updated"
        assert u.dax_expression == "NEW_EXPR()"
        assert u.description == "updated desc"


# ---------------------------------------------------------------------------
# BusinessMappingInDB / Response
# ---------------------------------------------------------------------------

class TestPowerBIBusinessMappingInDB:

    def test_valid_instantiation(self):
        m = PowerBIBusinessMappingInDB(**_mapping_in_db())
        assert m.id == 1
        assert m.group_id == "grp-001"
        assert m.semantic_model_id == "sm-001"
        assert m.natural_term == "revenue"

    def test_missing_id_raises(self):
        data = _mapping_in_db()
        del data["id"]
        with pytest.raises(ValidationError):
            PowerBIBusinessMappingInDB(**data)

    def test_from_attributes_config(self):
        """Schema has from_attributes=True for ORM compatibility."""
        assert PowerBIBusinessMappingInDB.model_config.get("from_attributes") is True

    def test_with_description(self):
        m = PowerBIBusinessMappingInDB(
            **_mapping_in_db(description="Total revenue KPI")
        )
        assert m.description == "Total revenue KPI"

    def test_response_is_same_as_in_db(self):
        """Response schema inherits InDB without adding fields."""
        m = PowerBIBusinessMappingResponse(**_mapping_in_db())
        assert m.id == 1
        assert m.natural_term == "revenue"


# ---------------------------------------------------------------------------
# FieldSynonymBase
# ---------------------------------------------------------------------------

class TestPowerBIFieldSynonymBase:

    def test_valid_minimal(self):
        s = PowerBIFieldSynonymBase(
            field_name="OrderDate",
            synonyms=["order date", "purchase date"],
        )
        assert s.field_name == "OrderDate"
        assert s.synonyms == ["order date", "purchase date"]

    def test_synonyms_is_list(self):
        s = PowerBIFieldSynonymBase(field_name="x", synonyms=["a"])
        assert isinstance(s.synonyms, list)

    def test_empty_synonyms_list(self):
        s = PowerBIFieldSynonymBase(field_name="x", synonyms=[])
        assert s.synonyms == []

    def test_missing_field_name_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIFieldSynonymBase(synonyms=["a"])
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "field_name" in fields

    def test_missing_synonyms_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIFieldSynonymBase(field_name="x")
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "synonyms" in fields

    def test_synonyms_with_many_entries(self):
        synonyms = [f"alias_{i}" for i in range(20)]
        s = PowerBIFieldSynonymBase(field_name="Metric", synonyms=synonyms)
        assert len(s.synonyms) == 20


# ---------------------------------------------------------------------------
# FieldSynonymCreate / Update / InDB / Response
# ---------------------------------------------------------------------------

class TestPowerBIFieldSynonymCreate:

    def test_valid_create(self):
        c = PowerBIFieldSynonymCreate(
            field_name="Revenue",
            synonyms=["income", "earnings"],
            semantic_model_id="sm-XYZ",
        )
        assert c.semantic_model_id == "sm-XYZ"
        assert c.synonyms == ["income", "earnings"]

    def test_missing_semantic_model_id_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIFieldSynonymCreate(field_name="f", synonyms=["a"])
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "semantic_model_id" in fields


class TestPowerBIFieldSynonymUpdate:

    def test_empty_update_is_valid(self):
        u = PowerBIFieldSynonymUpdate()
        assert u.field_name is None
        assert u.synonyms is None

    def test_partial_field_name_only(self):
        u = PowerBIFieldSynonymUpdate(field_name="NewName")
        assert u.field_name == "NewName"
        assert u.synonyms is None

    def test_partial_synonyms_only(self):
        u = PowerBIFieldSynonymUpdate(synonyms=["s1", "s2"])
        assert u.synonyms == ["s1", "s2"]

    def test_full_update(self):
        u = PowerBIFieldSynonymUpdate(field_name="f", synonyms=["a", "b"])
        assert u.field_name == "f"
        assert u.synonyms == ["a", "b"]


class TestPowerBIFieldSynonymInDB:

    def test_valid_instantiation(self):
        s = PowerBIFieldSynonymInDB(**_synonym_in_db())
        assert s.id == 1
        assert s.field_name == "OrderDate"
        assert s.synonyms == ["order date", "purchase date"]

    def test_from_attributes_config(self):
        assert PowerBIFieldSynonymInDB.model_config.get("from_attributes") is True

    def test_missing_id_raises(self):
        data = _synonym_in_db()
        del data["id"]
        with pytest.raises(ValidationError):
            PowerBIFieldSynonymInDB(**data)

    def test_response_inherits_in_db(self):
        s = PowerBIFieldSynonymResponse(**_synonym_in_db())
        assert s.id == 1
        assert s.field_name == "OrderDate"


# ---------------------------------------------------------------------------
# PowerBIContextConfigBulkResponse
# ---------------------------------------------------------------------------

class TestPowerBIContextConfigBulkResponse:

    def _make_bulk(self, num_mappings=2, num_synonyms=1):
        mappings = [
            PowerBIBusinessMappingResponse(**_mapping_in_db(id=i, natural_term=f"term_{i}"))
            for i in range(num_mappings)
        ]
        synonyms = [
            PowerBIFieldSynonymResponse(**_synonym_in_db(id=i, field_name=f"field_{i}"))
            for i in range(num_synonyms)
        ]
        return PowerBIContextConfigBulkResponse(
            business_mappings=mappings,
            field_synonyms=synonyms,
        )

    def test_valid_bulk_response(self):
        bulk = self._make_bulk()
        assert len(bulk.business_mappings) == 2
        assert len(bulk.field_synonyms) == 1

    def test_empty_lists_are_valid(self):
        bulk = PowerBIContextConfigBulkResponse(
            business_mappings=[], field_synonyms=[]
        )
        assert bulk.business_mappings == []
        assert bulk.field_synonyms == []

    def test_missing_business_mappings_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIContextConfigBulkResponse(
                field_synonyms=[]
            )
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "business_mappings" in fields

    def test_missing_field_synonyms_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            PowerBIContextConfigBulkResponse(
                business_mappings=[]
            )
        fields = {e["loc"][0] for e in exc_info.value.errors()}
        assert "field_synonyms" in fields

    def test_types_in_lists(self):
        bulk = self._make_bulk(num_mappings=3, num_synonyms=2)
        assert all(isinstance(m, PowerBIBusinessMappingResponse) for m in bulk.business_mappings)
        assert all(isinstance(s, PowerBIFieldSynonymResponse) for s in bulk.field_synonyms)


# ---------------------------------------------------------------------------
# PowerBIContextConfigDict
# ---------------------------------------------------------------------------

class TestPowerBIContextConfigDict:

    def test_empty_is_valid(self):
        """All fields are optional; empty instantiation is valid."""
        cfg = PowerBIContextConfigDict()
        assert cfg.business_mappings is None
        assert cfg.field_synonyms is None

    def test_both_fields_none_by_default(self):
        cfg = PowerBIContextConfigDict()
        assert cfg.business_mappings is None
        assert cfg.field_synonyms is None

    def test_with_business_mappings_only(self):
        cfg = PowerBIContextConfigDict(
            business_mappings={"revenue": "SUM(Sales[Revenue])"}
        )
        assert cfg.business_mappings == {"revenue": "SUM(Sales[Revenue])"}
        assert cfg.field_synonyms is None

    def test_with_field_synonyms_only(self):
        cfg = PowerBIContextConfigDict(
            field_synonyms={"OrderDate": ["order date", "purchase date"]}
        )
        assert cfg.field_synonyms == {"OrderDate": ["order date", "purchase date"]}
        assert cfg.business_mappings is None

    def test_with_both_fields(self):
        cfg = PowerBIContextConfigDict(
            business_mappings={"gross profit": "[Rev] - [COGS]"},
            field_synonyms={"Region": ["territory", "area"]},
        )
        assert cfg.business_mappings["gross profit"] == "[Rev] - [COGS]"
        assert cfg.field_synonyms["Region"] == ["territory", "area"]

    def test_explicit_none_values(self):
        cfg = PowerBIContextConfigDict(business_mappings=None, field_synonyms=None)
        assert cfg.business_mappings is None
        assert cfg.field_synonyms is None

    def test_empty_dicts_are_valid(self):
        cfg = PowerBIContextConfigDict(business_mappings={}, field_synonyms={})
        assert cfg.business_mappings == {}
        assert cfg.field_synonyms == {}

    def test_nested_dict_values_preserved(self):
        """Values inside the dicts can be complex structures."""
        cfg = PowerBIContextConfigDict(
            business_mappings={"kpi": {"expr": "SUM(x)", "format": "currency"}},
        )
        assert isinstance(cfg.business_mappings["kpi"], dict)
        assert cfg.business_mappings["kpi"]["format"] == "currency"
