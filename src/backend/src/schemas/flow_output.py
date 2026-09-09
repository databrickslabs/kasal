"""Bounded JSON output contracts carried by a flow, never written to catalog tasks."""

import json
import re
from typing import Any

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator


class FlowOutputContract(BaseModel):
    model_config = ConfigDict(extra="forbid")
    crew_id: str
    task_id: str
    name: str = Field(min_length=1, max_length=120)
    schema_definition: dict[str, Any]

    @field_validator("schema_definition")
    @classmethod
    def validate_schema(cls, schema: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(schema, allow_nan=False)) > 24000:
            raise ValueError("Output schema is too large")
        if schema.get("type") != "object":
            raise ValueError("Output schema must describe an object")
        allowed = {
            "type",
            "properties",
            "items",
            "required",
            "description",
            "title",
            "enum",
            "minimum",
            "maximum",
            "minLength",
            "maxLength",
            "minItems",
            "maxItems",
            "additionalProperties",
        }

        def visit(node: Any, depth: int = 0) -> None:
            if depth > 6 or not isinstance(node, dict):
                raise ValueError("Use an inline schema with at most six nesting levels")
            if set(node) - allowed:
                raise ValueError(
                    "Use plain inline fields, not schema references or executable expressions"
                )
            if not isinstance(node.get("type"), str) or node.get("type") not in {
                "object",
                "array",
                "string",
                "integer",
                "number",
                "boolean",
            }:
                raise ValueError("Every output field needs a concrete JSON type")
            if "additionalProperties" in node and not isinstance(
                node["additionalProperties"], bool
            ):
                raise ValueError("additionalProperties must be a boolean")
            if node.get("type") == "object":
                properties = node.get("properties", {})
                if (
                    not isinstance(properties, dict)
                    or not properties
                    or len(properties) > 40
                ):
                    raise ValueError("Objects need between one and forty fields")
                for name, child in properties.items():
                    if not isinstance(name, str) or not re.fullmatch(
                        r"[A-Za-z][A-Za-z0-9_]{0,79}", name
                    ):
                        raise ValueError("Use simple field names")
                    visit(child, depth + 1)
            if node.get("type") == "array":
                visit(node.get("items"), depth + 1)

        visit(schema)
        try:
            Draft202012Validator.check_schema(schema)
        except Exception as exc:
            raise ValueError("Invalid JSON output schema") from exc
        return schema
