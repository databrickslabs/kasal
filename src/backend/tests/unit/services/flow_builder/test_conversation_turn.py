"""A flow that holds a conversation across turns.

The mechanism under test is the one the design rests on: Kasal runs every flow
in a fresh subprocess, so turn 2 is a NEW ``Flow`` object with no memory of turn
1. Continuity comes entirely from restoring the thread's checkpoint and merging
the new turn into it through each channel's reducer.

So the tests construct a new instance per turn on purpose — anything that only
works while one object stays alive would pass a same-instance test and fail in
production.
"""

import asyncio

import pytest

from src.services.flow_builder.conversation.state_model import build_state_model
from src.services.flow_builder.conversation.thread import thread_state_uuid
from src.services.flow_builder.conversation.turn import (
    ConversationState,
    append_assistant_message,
    close_turn,
    trim_messages,
    turn_inputs,
)
from src.services.flow_builder.runtime import Flow, listen, start


class Store:
    """The checkpoint table, reduced to what a thread needs from it."""

    def __init__(self):
        self.rows = {}

    def load_state(self, flow_uuid):
        return self.rows.get(flow_uuid)

    def save_state(self, flow_uuid, method_name, state_data):
        data = state_data if isinstance(state_data, dict) else state_data.model_dump()
        self.rows[flow_uuid] = dict(data)


def conversational_flow(schema=None):
    model = build_state_model(schema or {}, base=ConversationState)

    class _Chat(Flow):
        initial_state = model

        @start()
        def answer(self):
            return f"echo: {self.state.last_user_message}"

        @listen("answer")
        def finish(self, previous):
            return previous

    return _Chat


def run_turn(flow_cls, store, thread, line):
    """One turn: a fresh instance, exactly as a subprocess would give it."""
    flow = flow_cls(persistence=store)
    result = asyncio.run(flow.kickoff_async({"id": thread, **turn_inputs(line)}))
    close_turn(flow.state, result)
    flow.save_checkpoint("turn_end")
    return flow


class TestThreadKey:
    def test_the_same_conversation_and_flow_derive_the_same_lineage(self):
        first = thread_state_uuid("session-1", "flow-1", "group-1")
        again = thread_state_uuid("session-1", "flow-1", "group-1")

        assert first == again

    def test_two_flows_in_one_conversation_do_not_share_a_lineage(self):
        # The reason the session id cannot be the key directly: "Use existing"
        # routes each message to whichever capability matches, and a shared key
        # would have turn 2 of one flow restore the state of the other.
        a = thread_state_uuid("session-1", "flow-1", "group-1")
        b = thread_state_uuid("session-1", "flow-2", "group-1")

        assert a != b

    def test_two_groups_do_not_share_a_lineage(self):
        a = thread_state_uuid("session-1", "flow-1", "group-1")
        b = thread_state_uuid("session-1", "flow-1", "group-2")

        assert a != b

    @pytest.mark.parametrize(
        "session,flow", [(None, "flow-1"), ("session-1", None), ("", "flow-1")]
    )
    def test_no_thread_without_both_halves(self, session, flow):
        # No thread means a one-shot run, which is what every flow does today.
        assert thread_state_uuid(session, flow) is None


class TestTurnOneAdoptsTheThread:
    def test_the_first_turn_saves_under_the_thread_key(self):
        # The bug this exists to stop: `_restore_state` returned early when the
        # lineage was empty — always true on turn 1 — so the id was never
        # adopted, the turn saved under its random uuid4, and every later turn
        # started over.
        store = Store()
        thread = thread_state_uuid("session-1", "flow-1")

        flow = run_turn(conversational_flow(), store, thread, "hello")

        assert flow.state.id == thread
        assert list(store.rows) == [thread]

    def test_a_thread_without_persistence_still_carries_its_id(self):
        # Nothing to restore from, but the id identifies the run for tracing.
        flow = conversational_flow()()
        asyncio.run(flow.kickoff_async({"id": "thread-x"}))

        assert flow.state.id == "thread-x"


class TestConversationAcrossTurns:
    def test_history_accumulates_across_separate_instances(self):
        store = Store()
        thread = thread_state_uuid("session-1", "flow-1")
        flow_cls = conversational_flow()

        for line in ["lebanese news", "and swiss news?", "summarize both"]:
            flow = run_turn(flow_cls, store, thread, line)

        said = [m["content"] for m in flow.state.messages if m["role"] == "user"]
        assert said == ["lebanese news", "and swiss news?", "summarize both"]
        assert len(flow.state.messages) == 6  # a reply recorded for each

    def test_the_newest_turn_is_readable_on_its_own(self):
        store = Store()
        thread = thread_state_uuid("session-1", "flow-1")
        flow_cls = conversational_flow()

        run_turn(flow_cls, store, thread, "first")
        flow = run_turn(flow_cls, store, thread, "second")

        # A condition should not have to index into a list to route on what was
        # just said.
        assert flow.state.last_user_message == "second"

    def test_every_node_runs_on_every_turn(self):
        # `_completed` used to persist across kickoffs, so a second turn fired
        # no listeners at all and still reported success.
        store = Store()
        thread = thread_state_uuid("session-1", "flow-1")
        ran = []

        model = build_state_model({}, base=ConversationState)

        class _Chat(Flow):
            initial_state = model

            @start()
            def a(self):
                ran.append("a")
                return "a"

            @listen("a")
            def b(self, previous):
                ran.append("b")
                return "b"

        for line in ["one", "two"]:
            flow = _Chat(persistence=store)
            asyncio.run(flow.kickoff_async({"id": thread, **turn_inputs(line)}))

        assert ran == ["a", "b", "a", "b"]

    def test_a_declared_channel_accumulates_beside_the_conversation(self):
        store = Store()
        thread = thread_state_uuid("session-1", "flow-1")
        model = build_state_model(
            {
                "properties": {
                    "findings": {"reducer": "append"},
                    "turns": {"reducer": "add"},
                }
            },
            base=ConversationState,
        )

        class _Chat(Flow):
            initial_state = model

            @start()
            def work(self):
                self.state.merge({"turns": 1, "findings": ["f"]})
                return "done"

        for line in ["one", "two", "three"]:
            flow = _Chat(persistence=store)
            asyncio.run(flow.kickoff_async({"id": thread, **turn_inputs(line)}))

        assert flow.state.turns == 3
        assert flow.state.findings == ["f", "f", "f"]

    def test_two_conversations_with_one_flow_stay_separate(self):
        store = Store()
        flow_cls = conversational_flow()
        alice = thread_state_uuid("session-alice", "flow-1")
        bob = thread_state_uuid("session-bob", "flow-1")

        run_turn(flow_cls, store, alice, "alice one")
        run_turn(flow_cls, store, bob, "bob one")
        flow = run_turn(flow_cls, store, alice, "alice two")

        said = [m["content"] for m in flow.state.messages if m["role"] == "user"]
        assert said == ["alice one", "alice two"]


class TestTurnBookkeeping:
    def test_a_turn_records_the_answer_it_produced(self):
        state = build_state_model({}, base=ConversationState)()
        state.merge(turn_inputs("what is the weather?"))

        close_turn(state, "sunny")

        assert state.messages[-1] == {"role": "assistant", "content": "sunny"}

    def test_an_answer_the_flow_recorded_itself_is_not_duplicated(self):
        # `append_assistant_message` stays explicit; `close_turn` only fills a
        # gap. A flow whose last method is a router would otherwise contribute
        # the ROUTE NAME as its reply.
        state = build_state_model({}, base=ConversationState)()
        state.merge(turn_inputs("hello"))
        append_assistant_message(state, "the real answer")

        close_turn(state, "has_results")

        assert [m["content"] for m in state.messages if m["role"] == "assistant"] == [
            "the real answer"
        ]

    def test_a_structured_result_is_recorded_as_json_not_a_repr(self):
        from pydantic import BaseModel

        class Answer(BaseModel):
            query: str
            found: int

        state = build_state_model({}, base=ConversationState)()
        state.merge(turn_inputs("search"))

        close_turn(state, Answer(query="news", found=6))

        recorded = state.messages[-1]["content"]
        assert '"query":"news"' in recorded.replace(" ", "")
        assert "query='news'" not in recorded

    def test_history_is_bounded(self):
        state = build_state_model({}, base=ConversationState)()
        state.merge(
            {"messages": [{"role": "user", "content": str(i)} for i in range(10)]}
        )

        trim_messages(state, limit=4)

        assert [m["content"] for m in state.messages] == ["6", "7", "8", "9"]

    def test_a_blank_turn_appends_nothing(self):
        assert turn_inputs("") == {}
        assert turn_inputs(None) == {}
