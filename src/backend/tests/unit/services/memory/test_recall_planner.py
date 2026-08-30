"""Deep recall — the Memory Tuning knobs that used to be dropped, now driving
Memory.recall: query distillation (query_analysis_threshold), exploration
rounds (exploration_budget, confidence_threshold_low/high,
complex_query_threshold), and the raw mode the duplicate check relies on.
"""

from types import SimpleNamespace

from src.services.memory.engine import Memory
from src.services.memory.engine.recall_planner import (
    RecallPlan,
    analyze_query,
    deep_recall,
    merge_hits,
    needs_exploration,
    rank,
)
from src.services.memory.engine.types import MemoryRecord

LONG_TASK = (
    "Search for and gather the latest news stories from Switzerland published "
    "today. Identify the top 5-7 most significant stories across categories such "
    "as politics, economy, society, and international relations."
)


class _FakeLLM:
    def __init__(self, *replies: str):
        self.replies = list(replies)
        self.calls: list = []

    def call(self, messages, *args, **kwargs):
        self.calls.append(messages)
        return self.replies.pop(0) if self.replies else "{}"


def _rec(rid: str, sim: float | None, content: str = "note") -> MemoryRecord:
    meta = {} if sim is None else {"similarity": sim}
    return MemoryRecord(id=rid, content=content, scope="/g", metadata=meta)


class _Store:
    """Storage stand-in: hits per query text, remembers what was searched."""

    def __init__(self, hits: dict[str, list[MemoryRecord]], empty: bool = False):
        self.hits = hits
        self.empty = empty
        self.searched: list[str] = []

    def has_records(self, scope):
        return not self.empty

    def search(self, query, limit=10, scope=None, score_threshold=None):
        self.searched.append(query)
        return list(self.hits.get(query, []))


def _memory(store, llm=None, **knobs) -> Memory:
    return Memory(storage=store, llm=llm, root_scope="/g", **knobs)


ANALYSIS = '{"query": "latest Switzerland news today", "alternatives": ["Swiss news report", "Aarau shooting"], "complexity": 0.4}'


class TestAnalyzeQuery:
    def test_distils_and_lists_alternatives(self):
        plan = analyze_query(_FakeLLM(ANALYSIS), LONG_TASK)
        assert plan.analyzed
        assert plan.query == "latest Switzerland news today"
        assert plan.alternatives == ["Swiss news report", "Aarau shooting"]
        assert plan.complexity == 0.4

    def test_bad_reply_falls_back_to_the_raw_query(self):
        plan = analyze_query(_FakeLLM("sorry, no"), LONG_TASK)
        assert plan == RecallPlan(query=LONG_TASK)

    def test_raising_llm_falls_back(self):
        class Boom:
            def call(self, *a, **k):
                raise RuntimeError("down")

        assert analyze_query(Boom(), LONG_TASK) == RecallPlan(query=LONG_TASK)

    def test_alternatives_never_repeat_the_query_and_are_capped(self):
        reply = '{"query": "q1", "alternatives": ["q1", "Q1", "a", "b", "c", "d"], "complexity": 2}'
        plan = analyze_query(_FakeLLM(reply), "x" * 300)
        assert plan.alternatives == ["a", "b", "c"]
        assert plan.complexity == 1.0


class TestDistillation:
    def test_long_query_is_distilled_and_both_searches_run(self):
        store = _Store({"latest Switzerland news today": [_rec("r1", 0.9)]})
        memory = _memory(store, _FakeLLM(ANALYSIS), exploration_budget=0)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert outcome.plan.query == "latest Switzerland news today"
        assert store.searched == ["latest Switzerland news today", LONG_TASK]
        assert [r.id for r in outcome.records] == ["r1"]

    def test_short_query_is_searched_as_is_without_an_llm_call(self):
        llm = _FakeLLM(ANALYSIS)
        store = _Store({"swiss news": [_rec("r1", 0.9)]})
        memory = _memory(store, llm, exploration_budget=0)
        outcome = deep_recall(memory, "swiss news", limit=5, search=store.search)
        assert llm.calls == []
        assert not outcome.plan.analyzed
        assert store.searched == ["swiss news"]

    def test_threshold_zero_always_analyses(self):
        llm = _FakeLLM(ANALYSIS)
        store = _Store({})
        memory = _memory(store, llm, query_analysis_threshold=0, exploration_budget=0)
        deep_recall(memory, "swiss news", limit=5, search=store.search)
        assert len(llm.calls) == 1

    def test_empty_store_never_calls_the_llm(self):
        llm = _FakeLLM(ANALYSIS)
        store = _Store({}, empty=True)
        memory = _memory(store, llm)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert llm.calls == []
        assert outcome.records == [] and outcome.rounds == 0

    def test_no_llm_is_plain_search(self):
        store = _Store({LONG_TASK: [_rec("r1", 0.7)]})
        memory = _memory(store, None)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert [r.id for r in outcome.records] == ["r1"]
        assert store.searched == [LONG_TASK]


class TestExploration:
    def test_low_confidence_spends_the_budget_on_alternatives(self):
        store = _Store(
            {
                "latest Switzerland news today": [_rec("weak", 0.3)],
                "Aarau shooting": [_rec("hit", 0.9)],
            }
        )
        memory = _memory(store, _FakeLLM(ANALYSIS), exploration_budget=1)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert outcome.rounds == 1
        assert (
            "Swiss news report" in store.searched and "Aarau shooting" in store.searched
        )
        assert [r.id for r in outcome.records] == ["hit", "weak"]
        assert outcome.best_score == 0.9

    def test_high_confidence_skips_exploration(self):
        store = _Store({"latest Switzerland news today": [_rec("hit", 0.95)]})
        llm = _FakeLLM(ANALYSIS)
        memory = _memory(store, llm, exploration_budget=3)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert outcome.rounds == 0
        assert len(llm.calls) == 1  # analysis only

    def test_budget_zero_is_shallow_only(self):
        store = _Store({"latest Switzerland news today": []})
        llm = _FakeLLM(ANALYSIS)
        memory = _memory(store, llm, exploration_budget=0)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert outcome.rounds == 0 and len(llm.calls) == 1

    def test_second_round_asks_the_llm_for_new_queries(self):
        store = _Store({"Bern parliament vote": [_rec("late", 0.85)]})
        llm = _FakeLLM(ANALYSIS, '{"alternatives": ["Bern parliament vote"]}')
        memory = _memory(store, llm, exploration_budget=2)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert outcome.rounds == 2
        assert len(llm.calls) == 2
        assert [r.id for r in outcome.records] == ["late"]

    def test_exploration_stops_when_no_new_queries_come_back(self):
        store = _Store({})
        llm = _FakeLLM(ANALYSIS, "{}")
        memory = _memory(store, llm, exploration_budget=5)
        outcome = deep_recall(memory, LONG_TASK, limit=5, search=store.search)
        assert outcome.rounds == 1  # the analysis alternatives, then nothing new

    def test_complex_query_explores_between_the_bounds(self):
        memory = SimpleNamespace(
            confidence_threshold_high=0.8,
            confidence_threshold_low=0.5,
            complex_query_threshold=0.7,
        )
        assert needs_exploration(memory, 0.65, 0.9) is True
        assert needs_exploration(memory, 0.65, 0.2) is False
        assert needs_exploration(memory, 0.4, 0.0) is True
        assert needs_exploration(memory, 0.85, 1.0) is False
        assert needs_exploration(memory, None, 0.0) is True


class TestRanking:
    def test_merge_keeps_the_better_scored_copy_and_ranks_highest_first(self):
        merged = merge_hits(
            [_rec("a", 0.5), _rec("b", None)], [_rec("a", 0.8), _rec("c", 0.6)]
        )
        assert [r.id for r in rank(merged, 10)] == ["a", "c", "b"]
        assert [r.id for r in rank(merged, 2)] == ["a", "c"]


class TestMemoryRecallModes:
    def test_raw_mode_is_one_literal_search_with_no_llm_call(self):
        store = _Store({LONG_TASK: [_rec("r1", 0.9)]})
        llm = _FakeLLM(ANALYSIS)
        memory = _memory(store, llm)
        hits = memory.recall(LONG_TASK, limit=5, mode="raw")
        assert [r.id for r in hits] == ["r1"]
        assert llm.calls == [] and store.searched == [LONG_TASK]

    def test_auto_mode_reports_the_distilled_query_on_the_completed_event(self):
        from src.core.events import MemoryQueryCompletedEvent, event_bus

        seen: list = []

        def _on(source, event):
            seen.append(event)

        event_bus.register_handler(MemoryQueryCompletedEvent, _on)
        try:
            store = _Store({"latest Switzerland news today": [_rec("r1", 0.9)]})
            memory = _memory(store, _FakeLLM(ANALYSIS), exploration_budget=0)
            memory.recall(LONG_TASK, limit=5)
        finally:
            event_bus.off(MemoryQueryCompletedEvent, _on)
        assert seen and seen[-1].distilled_query == "latest Switzerland news today"
        assert seen[-1].exploration_rounds == 0
        assert seen[-1].query == LONG_TASK

    def test_every_knob_is_a_declared_field(self):
        memory = Memory(
            consolidation_threshold=0.9,
            consolidation_limit=7,
            confidence_threshold_high=0.9,
            confidence_threshold_low=0.4,
            complex_query_threshold=0.6,
            exploration_budget=2,
            query_analysis_threshold=50,
            default_importance=0.3,
        )
        assert (memory.consolidation_threshold, memory.consolidation_limit) == (0.9, 7)
        assert (memory.confidence_threshold_high, memory.confidence_threshold_low) == (
            0.9,
            0.4,
        )
        assert (memory.complex_query_threshold, memory.exploration_budget) == (0.6, 2)
        assert (memory.query_analysis_threshold, memory.default_importance) == (50, 0.3)
