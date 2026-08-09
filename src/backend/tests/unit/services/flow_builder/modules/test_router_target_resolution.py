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


def build_crew_to_method(listener_crews, starting_points, frontend_starting_points):
    """Mirrors the map built in flow_builder._create_dynamic_flow.

    Kept in the test rather than imported because the production copy is inline
    in a 1,400-line function; the point of these tests is the RULE, and the rule
    is short enough to state twice and long enough to be worth pinning.
    """
    crew_to_method = {}
    for info in listener_crews:
        method_name, crew_id = info[0], info[1]
        if crew_id:
            crew_to_method.setdefault(str(crew_id), method_name)
    for method_name, task_ids, _objs, _name, _data in starting_points:
        ids = {str(t) for t in task_ids}
        for config in frontend_starting_points:
            if str(config.get("taskId")) in ids:
                crew_id = config.get("crewId")
                if crew_id:
                    crew_to_method.setdefault(str(crew_id), method_name)
                break
    return crew_to_method


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
        listeners = [(f"listener_0", missing, [], [], "C", [], "OR")]

        assert build_crew_to_method(listeners, [], {}) == {}

    def test_the_first_method_for_a_crew_wins(self):
        """setdefault, so a crew appearing twice keeps its first method."""
        listeners = [
            ("listener_0", "crew-a", [], [], "A", [], "OR"),
            ("listener_1", "crew-a", [], [], "A", [], "OR"),
        ]

        assert build_crew_to_method(listeners, [], {}) == {"crew-a": "listener_0"}
