"""JSON Schema → Pydantic conversion.

The old converter flattened everything one level deep, which made a schema look
enforced while validating nothing. These tests pin the properties a gate built
on the model actually depends on.
"""

import pytest
from pydantic import BaseModel, ValidationError

from src.services.agent_builder.schema_converter import build_model_from_schema

ENVELOPE = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "findings": {"type": "array", "items": {"$ref": "#/$defs/Finding"}},
        "open_questions": {"type": "array", "items": {"type": "string"}},
        "limitations": {"type": "string"},
    },
    "required": ["summary", "findings"],
    "$defs": {
        "Finding": {
            "type": "object",
            "properties": {
                "claim": {"type": "string"},
                "source": {"type": "string"},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "kind": {"enum": ["fact", "estimate"]},
            },
            "required": ["claim", "source", "confidence"],
        }
    },
}


def _valid_payload():
    return {
        "summary": "A sufficiently long summary of the findings.",
        "findings": [
            {"claim": "c", "source": "https://example.com", "confidence": 0.9}
        ],
    }


class TestNesting:
    def test_array_of_refs_becomes_a_real_submodel(self):
        model = build_model_from_schema("Envelope", ENVELOPE)
        instance = model.model_validate(_valid_payload())
        finding = instance.findings[0]
        # The whole point: NOT a bare dict. A Dict[str, Any] would accept
        # anything and make every downstream gate vacuous.
        assert isinstance(finding, BaseModel)
        assert finding.claim == "c"

    def test_junk_list_items_are_rejected(self):
        model = build_model_from_schema("Envelope", ENVELOPE)
        payload = _valid_payload()
        payload["findings"] = [1, 2, 3]
        with pytest.raises(ValidationError):
            model.model_validate(payload)

    def test_unresolvable_ref_degrades_instead_of_raising(self):
        schema = {
            "type": "object",
            "properties": {"thing": {"$ref": "#/$defs/Nope"}},
        }
        model = build_model_from_schema("Loose", schema)
        assert model is not None
        model.model_validate({"thing": {"anything": True}})

    def test_recursive_ref_does_not_hang(self):
        schema = {
            "type": "object",
            "properties": {"child": {"$ref": "#/$defs/Node"}},
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {"child": {"$ref": "#/$defs/Node"}},
                }
            },
        }
        model = build_model_from_schema("Recursive", schema)
        assert model is not None


class TestRequiredness:
    def test_required_fields_are_required(self):
        """The old converter had this exactly inverted: it gave required fields
        a default of None and optional fields no default at all, so a model
        could omit the one field the schema insisted on."""
        model = build_model_from_schema("Envelope", ENVELOPE)
        with pytest.raises(ValidationError) as exc:
            model.model_validate({"findings": []})
        assert "summary" in str(exc.value)

    def test_optional_fields_default_to_none(self):
        model = build_model_from_schema("Envelope", ENVELOPE)
        instance = model.model_validate(_valid_payload())
        assert instance.limitations is None

    def test_nested_required_is_enforced(self):
        model = build_model_from_schema("Envelope", ENVELOPE)
        payload = _valid_payload()
        payload["findings"] = [{"claim": "c"}]  # no source, no confidence
        with pytest.raises(ValidationError):
            model.model_validate(payload)


class TestConstraints:
    def test_numeric_bounds_apply(self):
        model = build_model_from_schema("Envelope", ENVELOPE)
        payload = _valid_payload()
        payload["findings"][0]["confidence"] = 5
        with pytest.raises(ValidationError):
            model.model_validate(payload)

    def test_enum_becomes_a_closed_set(self):
        model = build_model_from_schema("Envelope", ENVELOPE)
        payload = _valid_payload()
        payload["findings"][0]["kind"] = "nonsense"
        with pytest.raises(ValidationError):
            model.model_validate(payload)

    def test_string_length_applies(self):
        model = build_model_from_schema(
            "S",
            {"type": "object", "properties": {"a": {"type": "string", "minLength": 5}}},
        )
        with pytest.raises(ValidationError):
            model.model_validate({"a": "hi"})

    def test_array_min_items_applies(self):
        model = build_model_from_schema(
            "A",
            {
                "type": "object",
                "properties": {
                    "xs": {"type": "array", "minItems": 2, "items": {"type": "string"}}
                },
            },
        )
        with pytest.raises(ValidationError):
            model.model_validate({"xs": ["one"]})

    def test_length_constraint_on_a_number_does_not_crash_the_build(self):
        """A slightly wrong schema must not take the whole crew build down —
        Pydantic raises at class creation when given min_length for an int."""
        model = build_model_from_schema(
            "N",
            {
                "type": "object",
                "properties": {"n": {"type": "integer", "minLength": 3}},
            },
        )
        assert model is not None
        model.model_validate({"n": 1})


class TestDegenerateSchemas:
    def test_property_less_root_still_yields_a_class(self):
        model = build_model_from_schema(
            "EmptySchema", {"properties": {}, "description": "Empty model"}
        )
        assert model is not None and issubclass(model, BaseModel)
        assert model.__doc__ == "Empty model"

    def test_non_dict_schema_returns_none(self):
        assert build_model_from_schema("Bad", ["not", "a", "schema"]) is None

    def test_nullable_union_is_accepted(self):
        model = build_model_from_schema(
            "U", {"type": "object", "properties": {"a": {"type": ["string", "null"]}}}
        )
        assert model.model_validate({"a": None}).a is None
        assert model.model_validate({"a": "x"}).a == "x"
