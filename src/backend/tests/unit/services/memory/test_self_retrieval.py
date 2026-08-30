"""A task must not be handed its own previous answers.

The incident: a crew task returned a 25-item list repeated five times, and its
prompt carried two near-identical copies of that same list, recalled from
memory. The cause was structural, not a dedup gap —

    WRITE  "[crew task: {name}] {description}\\nResult: {answer}"
    READ   query = task.description

— the record stored the retrieval KEY inside the retrieved document. On a saved
crew the description is byte-identical every run, so the stored vector contained
the query as a literal substring: the match is ~0.98, the 0.35 relevance floor
never fires, and every prior run of a task is retrieved into its own next prompt
BY CONSTRUCTION, once per run. A model shown its own prior answer to the same
question repeats it in over 90% of cases (Xu et al., NeurIPS 2022).

It also disabled maintenance: the invariant `[crew task: X] {description}`
prefix is why the exact-hash consolidation could not see two results differing
by two swapped entries, and why the near-duplicate merge — which truncates at
300 characters — compared two identical prefixes and never reached the answers.

No reference system fuses a request with its answer. Mem0 persists the extracted
fact and keeps raw turns in a separate table; Graphiti embeds the one-sentence
fact on an edge and keeps the episode in its own search scope; LangMem and
LlamaIndex store extracted facts. Provenance is metadata everywhere.
"""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.services.memory.run.pending import clear_pending_memory
from src.services.memory.run.persist import flush_memory_writes, make_memory_output_sink
from src.services.memory.run.recall import _select_records, build_memory_preamble
from src.services.memory.text import says_the_same


@pytest.fixture(autouse=True)
def _clean_overlay():
    clear_pending_memory()
    yield
    clear_pending_memory()


def _record(content, *, kind="episodic", metadata=None, source="crew_task"):
    return SimpleNamespace(
        content=content, kind=kind, metadata=metadata or {}, source=source
    )


class _Mem:
    root_scope = None


LIST_A = (
    "LangGraph CrewAI OpenAI Agents SDK Microsoft Agent Framework Google ADK "
    "LlamaIndex Haystack Semantic Kernel AutoGen/AG2 Mastra Pydantic AI "
    "smolagents LangChain AutoGen AG2"
)
# The same recollection, two entries swapped — what the store actually held.
LIST_B = (
    "LangGraph CrewAI OpenAI Agents SDK Microsoft Agent Framework Google ADK "
    "LlamaIndex Haystack Semantic Kernel AutoGen Mastra Pydantic AI "
    "smolagents LangChain AutoGen/AG2 AG2"
)


class TestTheRecordIsTheAnswerAlone:
    def test_the_task_description_is_not_in_the_stored_text(self):
        """The retrieval key must not live inside the retrieved document."""
        memory = MagicMock()
        memory.root_scope = "/g1"
        memory.recall.return_value = []
        done = threading.Event()
        memory.remember.side_effect = lambda *a, **k: done.set()

        sink = make_memory_output_sink(memory)
        task = SimpleNamespace(
            id="t1",
            name="gather_frameworks",
            description="Research and identify prominent agentic AI frameworks",
            agent=SimpleNamespace(role="Researcher"),
        )
        sink(task=task, output=SimpleNamespace(raw=LIST_A, agent="Researcher"))
        assert done.wait(timeout=5)
        flush_memory_writes(timeout=5)

        stored = memory.remember.call_args.args[0]
        assert stored == LIST_A
        assert "Research and identify" not in stored
        assert "crew task" not in stored

    def test_provenance_survives_as_metadata(self):
        """Nothing is lost — it moves to where it can filter but not score."""
        memory = MagicMock()
        memory.root_scope = "/g1"
        memory.recall.return_value = []
        done = threading.Event()
        memory.remember.side_effect = lambda *a, **k: done.set()

        sink = make_memory_output_sink(memory)
        sink(
            task=SimpleNamespace(
                id="t1", name="gather", description="Find the frameworks"
            ),
            output=SimpleNamespace(raw=LIST_A, agent="Researcher"),
        )
        assert done.wait(timeout=5)
        flush_memory_writes(timeout=5)

        metadata = memory.remember.call_args.kwargs["metadata"]
        assert metadata["task_name"] == "gather"
        assert metadata["task_description"] == "Find the frameworks"

    def test_the_block_still_names_the_task(self):
        """Provenance is printed from metadata, so the prompt reads the same."""
        mem = MagicMock()
        mem.root_scope = None
        mem.recall.return_value = [
            _record("42 frameworks found", metadata={"task_name": "gather"})
        ]

        block = build_memory_preamble(mem, "which frameworks?")

        assert "[crew_task · gather]" in block
        assert "42 frameworks found" in block


class TestOneRecollectionTakesOneSlot:
    def test_two_records_saying_the_same_thing_collapse(self):
        """The two the store actually held: one list, two entries swapped —
        invisible to the exact-hash consolidation, and both recalled."""
        selected = _select_records(_Mem(), [_record(LIST_A), _record(LIST_B)], limit=6)

        assert len(selected) == 1

    def test_the_freed_slot_goes_to_something_new(self):
        """Non-redundancy inside the selection PROMOTES the next distinct
        memory; applied after a trim it could only shrink the block."""
        records = [_record(LIST_A), _record(LIST_B)] + [
            _record(f"a genuinely different finding number {i} about tooling")
            for i in range(5)
        ]

        selected = _select_records(_Mem(), records, limit=6)

        assert len(selected) == 6
        assert sum(1 for r in selected if r.content in (LIST_A, LIST_B)) == 1

    def test_different_findings_are_both_kept(self):
        """The gate must not eat genuinely distinct records."""
        records = [
            _record("The Swiss market grew four percent last quarter overall"),
            _record("The German market shrank two percent over the same period"),
        ]

        assert len(_select_records(_Mem(), records, limit=6)) == 2

    def test_storage_records_are_compared_even_with_nothing_in_flight(self):
        """The old code compared content only inside the pending overlay and
        returned early when nothing was in flight — so on an ordinary turn six
        storage records reached the prompt completely uncompared."""
        selected = _select_records(_Mem(), [_record(LIST_A), _record(LIST_B)], limit=6)

        assert [r.content for r in selected] == [LIST_A]


class TestSimilarity:
    def test_word_order_is_ignored(self):
        assert says_the_same(LIST_A, LIST_B, 0.8) is True

    def test_short_strings_need_exact_equality(self):
        """'deadline is Friday' and 'deadline is Monday' share two tokens of
        three and are opposite facts."""
        assert says_the_same("deadline is Friday", "deadline is Monday", 0.8) is False

    def test_identical_short_strings_still_match(self):
        assert says_the_same("done", "done", 0.8) is True

    def test_unrelated_texts_do_not_match(self):
        assert (
            says_the_same(
                "The Swiss market grew four percent last quarter overall",
                "Kubernetes operators reconcile desired state continuously here",
                0.8,
            )
            is False
        )
