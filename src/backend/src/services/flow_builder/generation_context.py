"""Small catalog previews with on-demand detail for ambiguous or routed plans."""

from pydantic import Field

from src.schemas.flow_generation import CrewFlowPlan


class FlowPlanningStep(CrewFlowPlan):
    # Internal planning protocol; this field is never part of the canvas response.
    detail_crew_ids: list[str] = Field(default_factory=list, max_length=24)
    stage_assignments: dict[str, str] = Field(default_factory=dict)


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
