"""A router names the crew it waits for; the backend resolves the method.

It used to name the METHOD — `listener_2` — which meant the frontend predicted
a name only this side generates. It predicted by indexing its own arrays, which
hold one entry per edge and per task while methods are named per CREW, so any
crew appearing twice shifted every later index and the router named a method
that was never created. It never fired, and the run reported COMPLETED having
done half its work.

These pin the resolution that replaced the prediction: the map is built from the
tuples the naming actually came from, so it cannot drift from it.
"""

import pytest

from src.services.flow_builder.modules.router_dependencies import (
    build_crew_to_method,
    resolve_router_upstream,
    validate_router_upstreams,
)


class TestCrewToMethod:
    def test_a_listener_crew_resolves_to_its_method(self):
        listeners = [("listener_0", "crew-email", [], [], "Email", [], "OR")]

        assert build_crew_to_method(listeners, [], {}) == {"crew-email": "listener_0"}

    def test_a_starting_point_crew_resolves_through_its_task(self):
        starting = [("starting_point_0", ["t1"], [], "News", None)]
        frontend = [{"taskId": "t1", "crewId": "crew-news"}]

        assert build_crew_to_method([], starting, frontend) == {
            "crew-news": "starting_point_0"
        }

    def test_a_crew_with_several_incoming_edges_maps_once(self):
        """The shape that broke the old prediction: the email crew occupies two
        slots, so index-based naming pushed the next crew to listener_2."""
        listeners = [
            ("listener_0", "crew-email", [], [], "Email", [], "AND"),
            ("listener_1", "crew-classify", [], [], "Classify", [], "NONE"),
        ]

        resolved = build_crew_to_method(listeners, [], {})

        assert resolved["crew-classify"] == "listener_1"
        assert "listener_2" not in resolved.values()

    def test_a_start_crew_with_two_tasks_maps_once(self):
        """The other half: startingPoints holds one entry per task."""
        starting = [
            ("starting_point_0", ["t-a1", "t-a2"], [], "A", None),
            ("starting_point_1", ["t-b1"], [], "B", None),
        ]
        frontend = [
            {"taskId": "t-a1", "crewId": "crew-a"},
            {"taskId": "t-a2", "crewId": "crew-a"},
            {"taskId": "t-b1", "crewId": "crew-b"},
        ]

        resolved = build_crew_to_method([], starting, frontend)

        assert resolved == {"crew-a": "starting_point_0", "crew-b": "starting_point_1"}

    def test_ids_are_compared_as_strings(self):
        """A crew id arrives as a UUID on one side and a string on the other."""
        import uuid

        crew_uuid = uuid.uuid4()
        listeners = [("listener_0", crew_uuid, [], [], "C", [], "OR")]

        assert build_crew_to_method(listeners, [], {}) == {str(crew_uuid): "listener_0"}

    @pytest.mark.parametrize("missing", [None, ""])
    def test_a_crew_without_an_id_is_skipped_rather_than_keyed_on_nothing(
        self, missing
    ):
        listeners = [("listener_0", missing, [], [], "C", [], "OR")]

        assert build_crew_to_method(listeners, [], {}) == {}

    def test_the_first_method_for_a_crew_wins(self):
        """setdefault, so a crew appearing twice keeps its first method."""
        listeners = [
            ("listener_0", "crew-a", [], [], "A", [], "OR"),
            ("listener_1", "crew-a", [], [], "A", [], "OR"),
        ]

        assert build_crew_to_method(listeners, [], {}) == {"crew-a": "listener_0"}


class TestConditionalCrewDependencies:
    def test_unknown_explicit_crew_does_not_fall_back_to_start(self):
        with pytest.raises(ValueError, match="no executable method"):
            resolve_router_upstream(
                {"listenToCrewId": "missing"}, {}, "starting_point_0"
            )

    def test_a_route_without_runnable_tasks_is_not_a_dependency(self):
        routers = [{"routes": {"number": [{"id": "deleted-task", "crewId": "number"}]}}]
        assert build_crew_to_method([], [], [], routers, {}) == {}

    def test_legacy_router_without_crew_uses_start(self):
        assert resolve_router_upstream({}, {}, "starting_point_0") == "starting_point_0"

    def test_missing_generated_method_fails_before_execution(self):
        with pytest.raises(ValueError, match="missing flow method"):
            validate_router_upstreams({"router_number": "route_black_number_1"}, {})


@pytest.mark.asyncio
async def test_router_without_any_starting_point_is_rejected():
    from src.services.flow_builder.modules.flow_builder import FlowBuilder

    with pytest.raises(ValueError, match="missing flow method 'starting_point_0'"):
        await FlowBuilder._create_dynamic_flow(
            [], [], [{"name": "no_start", "routes": {"a": []}}], {}, {}, flow_config={}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_routers", [False, True])
@pytest.mark.parametrize(
    "number,expected",
    [(100, ["black", "number", "green"]), (101, ["black", "number", "white"])],
)
async def test_real_flow_runs_the_branch_after_number_finishes(
    reverse_routers, number, expected
):
    """Exercise the real scheduler for run 415, with deterministic crew outputs."""
    import json
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, patch

    from src.services.flow_builder.modules.flow_builder import FlowBuilder
    from src.services.flow_builder.runtime import start

    calls = []
    agent = SimpleNamespace(role="Worker", llm=SimpleNamespace(model="example-model"))
    tasks = {
        name: SimpleNamespace(description=name, expected_output="JSON", agent=agent)
        for name in ("black", "number", "green", "white")
    }

    def starting_method(**kwargs):
        @start()
        async def starting_point_0(self):
            calls.append("black")
            output = '{"word":"black"}'
            self.state["starting_point_0"] = output
            return output

        return starting_point_0

    def build_crew(**kwargs):
        async def kickoff_async(inputs):
            name = kwargs["name"]
            calls.append(name)
            if name in ("green", "white"):
                assert json.loads(inputs["previous_output"]) == {"number": number}
            return SimpleNamespace(
                raw=json.dumps(
                    {"number": number} if name == "number" else {"word": name}
                )
            )

        return SimpleNamespace(kickoff_async=kickoff_async)

    harness = SimpleNamespace(
        build_task=lambda **kw: SimpleNamespace(**kw),
        build_crew=build_crew,
        process=lambda value: value,
    )
    routers = [
        {
            "name": "black",
            "listenToCrewId": "black",
            "routes": {
                "to_number": [
                    {"id": "number", "crewId": "number", "crewName": "number"}
                ]
            },
            "routeConditions": {"to_number": "state.get('word') == 'black'"},
        },
        {
            "name": "number",
            "listenToCrewId": "number",
            "routes": {
                "to_green": [{"id": "green", "crewId": "green", "crewName": "green"}],
                "to_white": [{"id": "white", "crewId": "white", "crewName": "white"}],
            },
            "routeConditions": {
                "to_green": "state.get('number', '') == 100",
                "to_white": "state.get('number', '') > 100",
            },
        },
    ]
    if reverse_routers:
        routers.reverse()
    module = "src.services.flow_builder.modules.flow_builder"
    with (
        patch(
            f"{module}.FlowMethodFactory.create_starting_point_crew_method",
            side_effect=starting_method,
        ),
        patch(f"{module}.active_harness", return_value=harness),
        patch(
            f"{module}.get_model_context_limits",
            new=AsyncMock(return_value=(10000, 1000)),
        ),
        patch(
            "src.services.security.tool_capability_manifest.run_crew_security_checks"
        ),
    ):
        flow = await FlowBuilder._create_dynamic_flow(
            [("starting_point_0", ["black"], [tasks["black"]], "black", {})],
            [],
            routers,
            {},
            tasks,
            flow_config={
                "startingPoints": [
                    {"taskId": "black", "crewId": "black", "crewName": "black"}
                ]
            },
        )
        await flow.kickoff_async()
    assert calls == expected
