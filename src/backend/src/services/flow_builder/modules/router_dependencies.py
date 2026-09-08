"""Resolve router dependencies before constructing methods, independent of order."""

from typing import Any


def route_method_name(router_name: str, route_name: str, index: int) -> str:
    return f"route_{router_name}_{route_name}_{index}"


def build_crew_to_method(
    listener_crews: list,
    starting_points: list,
    frontend_starting_points: list,
    routers: list | None = None,
    all_tasks: dict | None = None,
) -> dict[str, str]:
    """Include routed crews even when their generating router appears later.

    Only index route methods that will actually be created (have runnable tasks).
    The shared naming function keeps this pre-registration aligned with creation.
    """
    result: dict[str, str] = {}
    for info in listener_crews:
        if info[1]:
            result.setdefault(str(info[1]), info[0])
    for method_name, task_ids, _objs, _name, _data in starting_points:
        ids = {str(task_id) for task_id in task_ids}
        for config in frontend_starting_points:
            if str(config.get("taskId")) in ids:
                if config.get("crewId"):
                    result.setdefault(str(config["crewId"]), method_name)
                break
    for index, config in enumerate(routers or []):
        name = config.get("name", f"router_{index}")
        for route, tasks in config.get("routes", {}).items():
            runnable = [task for task in tasks if task.get("id") in (all_tasks or {})]
            for task in runnable:
                if task.get("crewId"):
                    result.setdefault(
                        str(task["crewId"]), route_method_name(name, route, index)
                    )
    return result


def resolve_router_upstream(
    config: dict[str, Any],
    crew_to_method: dict[str, str],
    default_method: str,
) -> str:
    """Legacy routers may omit a crew; an explicit missing crew is an error."""
    crew_id = config.get("listenToCrewId")
    if not crew_id:
        return default_method
    method = crew_to_method.get(str(crew_id))
    if method is None:
        raise ValueError(
            f"Router {config.get('name', '(unnamed)')!r} waits on crew {crew_id!r}, "
            "but this flow has no executable method for that crew. "
            "Check its tasks and incoming connections."
        )
    return method


def validate_router_upstreams(dependencies: dict[str, str], methods: dict) -> None:
    """Validate once all route methods exist, including forward references."""
    for router, upstream in dependencies.items():
        if upstream not in methods:
            raise ValueError(
                f"Router {router!r} depends on missing flow method {upstream!r}"
            )
