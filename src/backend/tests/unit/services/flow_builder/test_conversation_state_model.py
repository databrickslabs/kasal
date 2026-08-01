"""A declared state schema, compiled into the class a flow actually runs on.

``Flow`` has supported a typed state from the start; the builder read
``state_config["model"]`` and passed it nowhere, so every flow ran on a bare
dict. A dict accepts any key — which is how a misspelled input reached state,
the condition reading the correct name saw nothing, and the flow branched as
though the value had never been supplied.

The compatibility constraint is what shapes this: every condition ever written
for a flow uses dict access (`state.get("has_results", "")` is what the UI
generates). On a plain pydantic model those RAISE, so turning typed state on
would have broken every existing flow — and, since conditions now fail loudly
instead of reading as false, broken them noisily.
"""

import asyncio

import pytest

from src.services.flow_builder.conversation.state_model import build_state_model
from src.services.flow_builder.modules.flow_methods import crew_inputs_from_state
from src.services.flow_builder.modules.flow_state import FlowStateManager
from src.services.flow_builder.runtime import Flow, start

SCHEMA = {
    "type": "object",
    "properties": {
        "topic": {"type": "string"},
        "has_results": {"type": "boolean"},
        "count": {"type": "integer"},
        "tags": {"type": "array"},
        "meta": {"type": "object"},
    },
}


def flow_on(schema):
    """A one-method flow whose state is the compiled schema."""

    model = build_state_model(schema)

    class _Flow(Flow):
        initial_state = model

        @start()
        def go(self):
            return "ok"

    return _Flow


class TestBuildStateModel:
    def test_declared_fields_become_fields(self):
        model = build_state_model(SCHEMA)
        assert set(model.model_fields) == {
            "topic",
            "has_results",
            "count",
            "tags",
            "meta",
            "id",
        }

    def test_every_field_has_a_default(self):
        # A flow's state is constructed with NO arguments at kickoff. A field
        # without a default would make the state impossible to instantiate,
        # i.e. the flow could never start.
        state = build_state_model(SCHEMA)()
        assert state.topic == ""
        assert state.has_results is False
        assert state.count == 0
        assert state.tags == []
        assert state.meta == {}

    def test_id_is_always_present(self):
        # `id` is the checkpoint handle — `_restore_state` and the `{"id": ...}`
        # resume kickoff both need it. A schema that forgets it would produce an
        # unresumable flow.
        assert "id" in build_state_model({"properties": {"x": {"type": "string"}}})()

    def test_an_undeclared_type_is_left_untyped_rather_than_rejected(self):
        model = build_state_model({"properties": {"whatever": {}}})
        assert model is not None
        assert model().get("whatever") is None

    @pytest.mark.parametrize(
        "schema", [None, "not a schema", {}, {"type": "object"}, {"properties": {}}]
    )
    def test_nothing_usable_yields_no_model(self, schema):
        # Falling back to the dict is what keeps every already-authored flow
        # running. A malformed schema must not fail a kickoff.
        assert build_state_model(schema) is None

    def test_a_field_that_is_not_an_identifier_is_skipped(self):
        model = build_state_model(
            {"properties": {"ok": {"type": "string"}, "not a name": {"type": "string"}}}
        )
        assert set(model.model_fields) == {"ok", "id"}


class TestConditionsKeepWorking:
    """The reason the generated state answers to all three access forms."""

    @pytest.mark.parametrize(
        "condition",
        [
            'state.get("has_results", "") == True',  # what the UI generates
            'state["has_results"] == True',
            "state.has_results == True",
            '"has_results" in state',
            "len(state.keys()) > 0",
        ],
    )
    def test_every_access_form_evaluates(self, condition):
        state = build_state_model(SCHEMA)()
        state["has_results"] = True

        assert FlowStateManager.evaluate_condition(state, condition) is True

    def test_a_false_condition_is_false_not_an_error(self):
        state = build_state_model(SCHEMA)()

        assert (
            FlowStateManager.evaluate_condition(
                state, 'state.get("has_results", "") == True'
            )
            is False
        )


class TestKickoffInputs:
    def test_an_input_reaches_typed_state(self):
        flow = flow_on(SCHEMA)()

        asyncio.run(flow.kickoff_async({"topic": "lebanese news"}))

        assert flow.state.topic == "lebanese news"
        assert flow.state["topic"] == "lebanese news"

    def test_a_misspelled_input_raises_instead_of_vanishing(self):
        # The whole point. On a dict state this input lands under the wrong key,
        # the condition reading `topic` sees "", and the run looks like a
        # success that answered the wrong question.
        with pytest.raises(ValueError) as excinfo:
            asyncio.run(flow_on(SCHEMA)().kickoff_async({"topci": "typo"}))

        assert "topci" in str(excinfo.value)
        assert "topic" in str(excinfo.value)  # names what the state does accept

    def test_the_resume_id_is_never_treated_as_an_input(self):
        flow = flow_on(SCHEMA)()

        asyncio.run(flow.kickoff_async({"id": "run-123", "topic": "x"}))

        assert flow.state.topic == "x"


class TestRuntimeWrites:
    """The flow writes to its own state; only INPUTS are checked."""

    def test_the_builder_can_store_previous_output(self):
        # `self.state["previous_output"] = ...` between methods — nobody would
        # ever declare that in a schema, and rejecting it would make typed state
        # unusable.
        state = build_state_model(SCHEMA)()
        state["previous_output"] = "crew output"

        assert state["previous_output"] == "crew output"

    def test_a_state_operation_can_write_an_undeclared_variable(self):
        state = build_state_model(SCHEMA)()
        state["computed_by_a_node"] = 42

        assert state.get("computed_by_a_node") == 42

    def test_initial_values_apply_through_update(self):
        # `create_init_method` calls `self.state.update(initial_values)`, a dict
        # method — providing it is what lets that code stay as it is.
        state = build_state_model(SCHEMA)()
        state.update({"topic": "seeded", "count": 3})

        assert (state.topic, state.count) == ("seeded", 3)

    def test_an_initial_value_the_state_cannot_hold_raises(self):
        with pytest.raises(ValueError, match="nonesuch"):
            build_state_model(SCHEMA)().update({"nonesuch": 1})


class TestCrewInputs:
    def test_runtime_writes_reach_the_crew_too(self):
        # pydantic keeps extras off `__dict__`, so the old `vars()` read would
        # drop exactly the values a downstream task interpolates.
        flow = flow_on(SCHEMA)()
        asyncio.run(flow.kickoff_async({"topic": "x"}))
        flow.state["op_var"] = 7

        inputs = crew_inputs_from_state(flow)

        assert inputs["topic"] == "x"
        assert inputs["op_var"] == 7

    def test_the_checkpoint_handle_is_not_passed_to_a_crew(self):
        flow = flow_on(SCHEMA)()
        asyncio.run(flow.kickoff_async({"topic": "x"}))

        assert "id" not in crew_inputs_from_state(flow)

    def test_a_dict_state_is_unchanged(self):
        class _Dict(Flow):
            @start()
            def go(self):
                return "ok"

        flow = _Dict()
        asyncio.run(flow.kickoff_async({"topic": "x"}))

        assert crew_inputs_from_state(flow) == {"topic": "x"}
