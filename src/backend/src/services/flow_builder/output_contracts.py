"""Validate flow-local output contracts and compile editable routing conditions."""

import json
import re
from typing import Any

from src.schemas.flow_generation import (
    CrewFlowPlan,
    RouteCondition,
    RouteConditionGroup,
)
from src.schemas.flow_output import FlowOutputContract


def plan_contracts(
    plan: CrewFlowPlan, catalog: dict[str, Any]
) -> dict[str, FlowOutputContract]:
    contracts = {}
    for contract in plan.output_contracts:
        crew = catalog.get(contract.crew_id)
        if not crew or contract.crew_id not in plan.crew_ids:
            raise ValueError("An output contract must belong to a selected crew")
        if contract.task_id != crew["tasks"][-1]["id"]:
            raise ValueError(
                "Apply the output contract to the source crew's final task"
            )
        if contract.crew_id in contracts:
            raise ValueError("Only one output contract is allowed per crew")
        contracts[contract.crew_id] = contract
    return contracts


def schema_field(schema: dict[str, Any], path: str) -> dict[str, Any]:
    """Resolve only declared fields; no substring guessing or external references."""
    current = schema
    for segment in path.split("."):
        name = segment.removesuffix("[]")
        if name not in current.get("required", []):
            raise ValueError(
                f"Routing field {path!r} must be required in the output contract"
            )
        current = current.get("properties", {}).get(name, {})
        if segment.endswith("[]"):
            current = current.get("items", {}) if current.get("type") == "array" else {}
    return current


def validate_term(
    term: RouteCondition, schema: dict[str, Any], subject: str = ""
) -> None:
    path = f"{subject}[].{term.field}" if subject else term.field
    field = schema_field(schema, path)
    kind = field.get("type")
    if kind not in {"string", "integer", "number", "boolean"}:
        raise ValueError(f"Routing field {path!r} is not a declared scalar output")
    value = term.value
    if kind == "boolean" and (
        not isinstance(value, bool) or term.operator not in {"==", "!="}
    ):
        raise ValueError("Boolean routing fields require a boolean equality comparison")
    if kind in {"integer", "number"} and (
        isinstance(value, bool)
        or not isinstance(value, (float, int))
        or term.operator == "contains"
    ):
        raise ValueError("Numeric routing fields require a numeric comparison")
    if kind == "string" and (
        not isinstance(value, str) or term.operator not in {"==", "!=", "contains"}
    ):
        raise ValueError(
            "Text routing fields require a text equality or contains comparison"
        )
    if "enum" in field and term.operator in {"==", "!="} and value not in field["enum"]:
        raise ValueError("Routing value is outside the field's declared choices")
    if subject and not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", term.field):
        raise ValueError("Same-item conditions must use fields directly on the item")


def compile_groups(groups: list[RouteConditionGroup], schema: dict[str, Any]) -> str:
    parts: list[str] = []
    suffixes = {
        "==": "",
        "!=": "__ne",
        ">": "__gt",
        ">=": "__gte",
        "<": "__lt",
        "<=": "__lte",
        "contains": "__contains",
    }
    for group in groups:
        for term in group.terms:
            validate_term(term, schema, group.subject)
        if group.subject:
            names = [term.field + suffixes[term.operator] for term in group.terms]
            if len(names) != len(set(names)):
                raise ValueError("Duplicate comparisons on the same item field")
            arguments = ", ".join(
                f"{name}={term.value!r}" for name, term in zip(names, group.terms)
            )
            expression = f"where({group.subject!r}, {arguments})"
        else:
            terms = []
            for term in group.terms:
                lookup = f"state.get({term.field!r}, '')"
                terms.append(
                    f"{term.value!r} in {lookup}"
                    if term.operator == "contains"
                    else f"{lookup} {term.operator} {term.value!r}"
                )
            expression = " and ".join(terms)
            if len(terms) > 1:
                expression = f"({expression})"
        parts.append((f"{group.connector.lower()} " if parts else "") + expression)
    return " ".join(parts)


def apply_flow_output_contract(
    spec: dict[str, Any], task_id: Any, flow_data: dict[str, Any] | None
) -> dict[str, Any]:
    """Overlay a serialized contract at task assembly, leaving catalog data intact."""
    matches = []
    for node in (flow_data or {}).get("nodes", []):
        raw = node.get("data", {}).get("outputContract")
        if not raw or str(raw.get("task_id")) != str(task_id):
            continue
        contract = FlowOutputContract.model_validate(raw)
        if str(node.get("data", {}).get("crewId")) != contract.crew_id:
            raise ValueError("Output contract does not belong to its flow crew")
        matches.append(contract)
    if not matches:
        return spec
    contract = matches[0]
    if any(item != contract for item in matches[1:]):
        raise ValueError("Conflicting flow output contracts for the same task")
    return {
        **spec,
        "output_pydantic": None,
        "output_schema": contract.schema_definition,
        "output_schema_name": contract.name,
        "expected_output": spec.get("expected_output", "")
        + "\nReturn the complete deliverable as JSON following this output contract. "
        "Derive routing fields from the evidence using their descriptions; do not invent evidence.\n"
        + json.dumps(contract.schema_definition, separators=(",", ":")),
    }
