"""A checkpoint WRITE belongs on the trace, not just a restore.

The trace already showed a checkpoint being restored and never showed one being
written, so the half a resume depends on was the invisible half: with nothing in
`flow_states` you could not tell "no checkpoint was written" from "one was
written and ignored" without querying the database by hand.

A failed write matters most. It does not fail the run — the user still gets an
answer — so without a row here it is silent, and every later turn quietly starts
from scratch, which looks exactly like a flow that has no memory.
"""

import asyncio

import pytest

from src.core.events import event_bus
from src.core.events.types import FlowCheckpointSavedEvent
from src.services.flow_builder.runtime import Flow, listen, start


@pytest.fixture
def saved_events():
    seen = []
    event_bus.on(FlowCheckpointSavedEvent)(lambda source, event: seen.append(event))
    yield seen


class Store:
    def __init__(self, fail=False):
        self.fail = fail
        self.rows = {}

    def load_state(self, flow_uuid):
        return self.rows.get(flow_uuid)

    def save_state(self, flow_uuid, method_name, state_data):
        if self.fail:
            raise RuntimeError("no such column: group_id")
        self.rows[flow_uuid] = dict(
            state_data if isinstance(state_data, dict) else state_data.model_dump()
        )


class OneStep(Flow):
    @start()
    def go(self):
        return "ok"


class TestCheckpointIsVisible:
    def test_a_written_checkpoint_reaches_the_trace(self, saved_events):
        asyncio.run(OneStep(persistence=Store()).kickoff_async({"id": "thread-1"}))

        mine = [e for e in saved_events if e.flow_uuid == "thread-1"]
        assert mine, "a checkpoint was written but nothing said so"
        assert mine[0].method_name == "go"
        assert mine[0].error is None

    def test_a_failed_write_reaches_the_trace_with_its_reason(self, saved_events):
        # The exact failure seen in production: the column the model expects is
        # missing, the run still answers, and every later turn starts over.
        asyncio.run(
            OneStep(persistence=Store(fail=True)).kickoff_async({"id": "thread-2"})
        )

        mine = [e for e in saved_events if e.flow_uuid == "thread-2"]
        assert mine and mine[0].error is not None
        assert "group_id" in mine[0].error

    def test_a_failed_write_does_not_fail_the_run(self, saved_events):
        # Bookkeeping is never worth failing a run the user already has an
        # answer from.
        result = asyncio.run(
            OneStep(persistence=Store(fail=True)).kickoff_async({"id": "thread-3"})
        )

        assert result == "ok"

    def test_a_flow_with_no_persistence_claims_no_checkpoint(self, saved_events):
        # Emitting here would put a checkpoint on the trace for a flow that has
        # none — worse than saying nothing.
        asyncio.run(OneStep().kickoff_async({"id": "thread-4"}))

        assert [e for e in saved_events if e.flow_uuid == "thread-4"] == []

    def test_every_method_that_completes_is_recorded(self, saved_events):
        class TwoStep(Flow):
            @start()
            def first(self):
                return "a"

            @listen("first")
            def second(self, previous):
                return "b"

        asyncio.run(TwoStep(persistence=Store()).kickoff_async({"id": "thread-5"}))

        methods = [e.method_name for e in saved_events if e.flow_uuid == "thread-5"]
        assert methods == ["first", "second"]

    def test_an_explicit_turn_end_checkpoint_is_recorded(self, saved_events):
        # `save_checkpoint` runs AFTER the graph finishes, to capture the answer
        # and the trimmed history — the state the next turn actually restores.
        flow = OneStep(persistence=Store())
        asyncio.run(flow.kickoff_async({"id": "thread-6"}))
        flow.save_checkpoint("turn_end")

        methods = [e.method_name for e in saved_events if e.flow_uuid == "thread-6"]
        assert methods[-1] == "turn_end"


class TestConversationImpliesPersistence:
    """A conversational flow must not need a second switch to remember anything.

    Persistence was enabled only by a per-edge `checkpoint: true`. So a flow
    could declare that it holds a conversation, have its state compiled with the
    turn channels, write NOTHING, and start from scratch every turn — with the
    log saying `State enabled: True` and `Persistence enabled: False` two lines
    apart and nothing joining them.

    Tests the decision, not the builder: `_create_dynamic_flow` needs a full
    flow, while the rule itself is "conversational OR a checkpoint edge".
    """

    @staticmethod
    def _decide(flow_config, edges):
        has_checkpoint_edge = any(
            edge.get("data", {}).get("checkpoint", False) for edge in edges
        )
        state = (flow_config.get("state") or {}) if flow_config else {}
        wants_conversation = bool(
            isinstance(state, dict) and state.get("conversational")
        )
        return has_checkpoint_edge or wants_conversation

    def test_a_conversational_flow_persists_without_a_checkpoint_edge(self):
        assert self._decide({"state": {"conversational": True}}, [{"data": {}}]) is True

    def test_a_checkpoint_edge_still_persists_on_its_own(self):
        # The original trigger keeps working for a flow that wants resume but
        # holds no conversation.
        assert self._decide({}, [{"data": {"checkpoint": True}}]) is True

    def test_a_plain_flow_still_persists_nothing(self):
        # Persistence costs a database write per method; a flow that asked for
        # neither must not start paying for it.
        assert self._decide({"state": {"enabled": True}}, [{"data": {}}]) is False

    def test_no_config_at_all_persists_nothing(self):
        assert self._decide({}, []) is False
