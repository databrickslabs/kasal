"""Small catalog previews with on-demand detail for ambiguous or routed plans."""

from typing import Any

from pydantic import Field, ValidationInfo, model_validator

from src.schemas.flow_generation import CrewFlowPlan


class FlowPlanningStep(CrewFlowPlan):
    # Internal planning protocol; this field is never part of the canvas response.
    detail_crew_ids: list[str] = Field(default_factory=list, max_length=24)
    stage_assignments: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def bind_final_output_tasks(cls, value: Any, info: ValidationInfo) -> Any:
        """Task identity is deterministic application data, not a model decision.

        Only the generation service supplies this group-scoped catalog. Persisted
        flow contracts still validate their explicit task IDs normally.
        """
        catalog = (info.context or {}).get("catalog")
        if not isinstance(value, dict) or not isinstance(catalog, dict):
            return value
        contracts = value.get("output_contracts")
        selected = value.get("crew_ids")
        if not isinstance(contracts, list) or not isinstance(selected, list):
            return value
        resolved = []
        for contract in contracts:
            if not isinstance(contract, dict):
                resolved.append(contract)
                continue
            crew_id = contract.get("crew_id")
            crew = catalog.get(crew_id) if isinstance(crew_id, str) else None
            if crew and crew_id in selected and crew.get("tasks"):
                contract = {**contract, "task_id": crew["tasks"][-1]["id"]}
            resolved.append(contract)
        return {**value, "output_contracts": resolved}


def compact_catalog(catalog: dict) -> dict:
    """Keep every candidate visible without sending task bodies/output specs."""
    previews = {}
    for cid, crew in catalog.items():
        tasks = []
        for task in crew["tasks"]:
            description = " ".join(task["description"].split())
            tasks.append(
                {
                    "name": task["name"],
                    "summary": description[:240],
                    "has_more_detail": len(description) > 240,
                }
            )
        previews[cid] = {"name": crew["name"], "tasks": tasks}
    return previews
