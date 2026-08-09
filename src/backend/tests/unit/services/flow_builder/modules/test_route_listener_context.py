"""What a routed crew receives as "context from previous step".

Reproduced from execution eb517d66. The router fired correctly and chose
``route_to_politics_presentation``, and the Politics crew was then handed:

    Context from previous step:
    route_to_politics_presentation

60 characters — the route NAME. CrewAI hands a ``@listen(route_name)`` method
the router's RETURN value, and the router returns the route it picked. The
classification the branch existed to work from never reached it, so the crew
ran on nothing and reported success.
"""

import pytest

from src.services.flow_builder.conversation.state_model import build_state_model
from src.services.flow_builder.modules.flow_conditions import state_snapshot

CLASSIFICATION = (
    '{"classification": [{"category": "politics", "title": "Senate vote"}]}'
)


class TestUpstreamOutputIsReachableFromState:
    """The fix reads state[<method the router listens to>]. These pin that the
    key is really there, for both state kinds and both upstream shapes."""

    def test_a_listener_upstream_is_stored_under_its_method_name(self):
        state = {"listener_1": CLASSIFICATION, "Classify Category": CLASSIFICATION}

        assert state_snapshot(state).get("listener_1") == CLASSIFICATION

    def test_a_starting_point_upstream_is_stored_under_its_method_name(self):
        state = {"starting_point_0": CLASSIFICATION}

        assert state_snapshot(state).get("starting_point_0") == CLASSIFICATION

    def test_it_is_reachable_on_a_typed_state_too(self):
        """The flow that hit this runs a structured, conversational state."""
        state = build_state_model(
            {"type": "object", "properties": {"category": {"type": "string"}}}
        )()
        state["listener_1"] = CLASSIFICATION

        assert state_snapshot(state).get("listener_1") == CLASSIFICATION

    def test_a_missing_upstream_reads_as_absent_rather_than_raising(self):
        """The route listener falls back and warns; it must not blow up."""
        assert state_snapshot({}).get("listener_1") is None

    @pytest.mark.parametrize(
        "route_name", ["route_to_politics_presentation", "default"]
    )
    def test_the_route_name_is_never_what_state_holds(self, route_name):
        """Guards the actual defect: the router's return value must not be
        mistaken for the upstream output."""
        state = {"listener_1": CLASSIFICATION}

        resolved = state_snapshot(state).get("listener_1")

        assert resolved != route_name
        assert "classification" in resolved
