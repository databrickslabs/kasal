"""Streamed deltas say whose turn they belong to.

From a live chat: the memory recall planner's JSON (``{"query": …,
"alternatives": […]}``) was typed into the chat as the assistant's answer. Chat
opts the agent's LLM object into streaming and forwards every chunk whose
``source`` is that object — and memory's recall planner, memory labelling and
LLM guardrails all call the SAME object. Identity cannot tell those apart;
attribution can: the agent's own turn is called with ``from_agent``, the
internal calls name nobody.
"""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.core.events.types import LLMStreamChunkEvent
from src.core.llm.transport.completion import OpenAICompletion

MODEL = "some-unregistered-selfhosted-model-v9"


def _stream(*texts: str):
    return iter(
        [
            SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        finish_reason="stop" if i == len(texts) - 1 else None,
                        delta=SimpleNamespace(
                            content=t,
                            tool_calls=None,
                            reasoning_content=None,
                            reasoning=None,
                        ),
                    )
                ],
                usage=None,
            )
            for i, t in enumerate(texts)
        ]
    )


def _llm() -> OpenAICompletion:
    llm = OpenAICompletion(model=MODEL, api_key="x", max_tokens=8192, stream=True)
    object.__setattr__(llm, "_client", MagicMock())
    return llm


def _chunks(emitted):
    return [e for e in emitted if isinstance(e, LLMStreamChunkEvent)]


class TestChunksCarryTheCallsAttribution:
    def test_the_agents_turn_stamps_its_agent_and_task_on_every_delta(self):
        llm = _llm()
        agent, task = object(), object()
        llm.client.chat.completions.create.return_value = _stream("Hel", "lo")
        emitted: list = []
        with patch(
            "src.core.llm.transport.completion.event_bus.emit",
            side_effect=lambda source, event: emitted.append(event),
        ):
            llm.call(
                [{"role": "user", "content": "hi"}], from_agent=agent, from_task=task
            )
        chunks = _chunks(emitted)
        assert [c.chunk for c in chunks] == ["Hel", "lo"]
        assert all(c.from_agent is agent and c.from_task is task for c in chunks)

    def test_an_internal_call_on_the_same_object_stamps_nobody(self):
        # The memory planner's call: same LLM object, still streaming, but it
        # names no agent — so a forwarder can drop it.
        llm = _llm()
        llm.client.chat.completions.create.side_effect = [
            _stream("answer"),
            _stream("{}"),
        ]
        emitted: list = []
        agent = object()
        with patch(
            "src.core.llm.transport.completion.event_bus.emit",
            side_effect=lambda source, event: emitted.append(event),
        ):
            llm.call([{"role": "user", "content": "hi"}], from_agent=agent)
            llm.call([{"role": "system", "content": "rewrite as a query"}])
        by_text = {c.chunk: c for c in _chunks(emitted)}
        assert by_text["answer"].from_agent is agent
        assert by_text["{}"].from_agent is None and by_text["{}"].from_task is None


class TestTheScopeItself:
    def test_a_nested_call_that_names_nobody_inherits_the_outer_turn(self):
        # The budget wrap-up is a nested self.call() with no attribution; its
        # deltas are still the agent's answer.
        llm = _llm()
        agent = object()
        with llm._attributed(None, agent):
            with llm._attributed(None, None):
                assert llm._call_attribution() == (None, agent)
            assert llm._call_attribution() == (None, agent)
        assert llm._call_attribution() == (None, None)

    def test_threads_do_not_see_each_others_turn(self):
        # The agent answers on a worker thread while memory labelling runs on
        # the save thread, on the same object.
        llm = _llm()
        agent = object()
        seen: list = []
        with llm._attributed(None, agent):
            t = threading.Thread(target=lambda: seen.append(llm._call_attribution()))
            t.start()
            t.join()
        assert seen == [(None, None)]

    def test_a_deepcopy_gets_its_own_scope(self):
        import copy

        llm = _llm()
        with llm._attributed(None, "agent"):
            clone = copy.deepcopy(llm)
            assert clone._call_attribution() == (None, None)
