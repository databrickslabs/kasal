"""How a write merges with what a channel already holds.

Without reducers a flow state can only overwrite, which is correct for `topic`
and useless for anything that accumulates. `replace` stays the default, so a
flow that declares no reducer behaves exactly as it did.
"""

import pytest

from src.services.flow_builder.conversation.channels import (
    REPLACE,
    apply_reducer,
    normalize_reducer,
)
from src.services.flow_builder.conversation.state_model import build_state_model
from src.services.flow_builder.conversation.turn import ConversationState


class TestReducers:
    @pytest.mark.parametrize(
        "reducer,current,incoming,expected",
        [
            ("replace", "old", "new", "new"),
            ("append", ["a"], ["b"], ["a", "b"]),
            ("append", None, ["a"], ["a"]),
            # A node writing one item should not have to remember the brackets.
            ("append", ["a"], "b", ["a", "b"]),
            ("merge", {"a": 1}, {"b": 2}, {"a": 1, "b": 2}),
            ("merge", {"a": 1}, {"a": 2}, {"a": 2}),
            ("merge", None, {"a": 1}, {"a": 1}),
            ("add", 2, 3, 5),
            ("add", None, 1, 1),
        ],
    )
    def test_merge_rules(self, reducer, current, incoming, expected):
        assert apply_reducer(reducer, current, incoming) == expected

    def test_add_on_a_non_number_replaces_rather_than_raising(self):
        # A reducer runs on state the user cannot see; a crash there is far
        # worse than a wrong-looking value they can inspect.
        assert apply_reducer("add", 1, "seven") == "seven"

    def test_an_unknown_reducer_falls_back_to_replace(self):
        # A schema is authored data. A typo in it must not make the flow
        # unrunnable — it is logged instead.
        assert normalize_reducer("appendd") == REPLACE
        assert normalize_reducer(None) == REPLACE

    def test_reducer_names_are_case_insensitive(self):
        assert normalize_reducer("APPEND") == "append"


class TestChannelsOnTheModel:
    def test_declared_reducers_reach_the_class(self):
        model = build_state_model(
            {"properties": {"notes": {"reducer": "append"}, "topic": {}}}
        )

        assert model.__reducers__ == {"notes": "append"}

    def test_merge_applies_them_and_update_does_not(self):
        # The distinction is the point: `update` seeds a value, `merge`
        # combines one with what is there. Seeding a conversation with `merge`
        # would double it; merging a turn with `update` would erase it.
        model = build_state_model({"properties": {"notes": {"reducer": "append"}}})

        state = model()
        state.update({"notes": ["seeded"]})
        state.merge({"notes": ["added"]})

        assert state.notes == ["seeded", "added"]

    def test_a_reducer_implies_its_shape(self):
        # An `add` channel left untyped would default to None, and the first
        # `state.count + 1` in a node would die on NoneType.
        model = build_state_model(
            {
                "properties": {
                    "count": {"reducer": "add"},
                    "items": {"reducer": "append"},
                    "ctx": {"reducer": "merge"},
                }
            }
        )

        state = model()
        assert (state.count, state.items, state.ctx) == (0, [], {})

    def test_merge_rejects_a_channel_the_state_does_not_have(self):
        model = build_state_model({"properties": {"topic": {}}})

        with pytest.raises(ValueError, match="topci"):
            model().merge({"topci": "typo"})

    def test_a_flow_channel_overrides_an_inherited_one(self):
        # ConversationState declares `messages` as append; a flow that wants
        # it replaced should win on its own state.
        model = build_state_model(
            {"properties": {"messages": {"reducer": "replace"}}},
            base=ConversationState,
        )

        state = model()
        state.merge({"messages": [{"role": "user", "content": "a"}]})
        state.merge({"messages": [{"role": "user", "content": "b"}]})

        assert [m["content"] for m in state.messages] == ["b"]

    def test_inherited_channels_keep_their_reducers_by_default(self):
        model = build_state_model({"properties": {"topic": {}}}, base=ConversationState)

        assert model.__reducers__["messages"] == "append"

    def test_a_conversational_base_builds_even_with_no_declared_channels(self):
        # The base's own channels are worth having; a flow need not declare
        # anything to hold a conversation.
        model = build_state_model({}, base=ConversationState)

        assert model is not None
        assert "messages" in model.model_fields

    def test_no_schema_and_no_base_still_yields_nothing(self):
        assert build_state_model({}) is None
