"""Tests for save-time memory analysis — the source of concept/graph data.

Records saved without ``categories`` are invisible in the Cognitive Memory
Browser's concept and graph views (both aggregate category co-occurrence), so
these tests pin the labelling pass: it runs, it is skipped when the caller
already knows better, and it never turns a save into a failure.
"""

from kasal_engine.memory import Memory


class _FakeLLM:
    """Configured-LLM stand-in: records calls, replies with a canned string."""

    def __init__(self, reply: str):
        self.reply = reply
        self.calls: list = []

    def call(self, messages, *args, **kwargs):
        self.calls.append(messages)
        return self.reply


_GOOD_REPLY = (
    '{"categories": ["Swiss News", "energy_markets", "swiss news"], '
    '"importance": 0.82, '
    '"extracted_metadata": {"entities": ["SRF"], "dates": ["2026-07-25"], '
    '"topics": ["energy"]}}'
)

_LONG = (
    "User: what is happening in Swiss energy markets? "
    "Assistant: prices fell after the new grid deal was signed."
)


class TestSaveTimeAnalysis:
    def test_labels_record_with_categories_importance_and_entities(self):
        llm = _FakeLLM(_GOOD_REPLY)
        record = Memory(llm=llm, root_scope="/g1").remember(_LONG)

        # Normalised to kebab-case and deduped, so naming variants of the same
        # concept collapse into one graph node.
        assert record.categories == ["swiss-news", "energy-markets"]
        assert record.importance == 0.82
        assert record.metadata["entities"] == ["SRF"]
        assert record.metadata["topics"] == ["energy"]
        assert len(llm.calls) == 1

    def test_parses_json_wrapped_in_a_code_fence(self):
        llm = _FakeLLM(f"```json\n{_GOOD_REPLY}\n```")
        record = Memory(llm=llm, root_scope="/g1").remember(_LONG)
        assert record.categories == ["swiss-news", "energy-markets"]

    def test_parses_json_wrapped_in_prose(self):
        llm = _FakeLLM(f"Sure! Here you go:\n{_GOOD_REPLY}\nHope that helps.")
        record = Memory(llm=llm, root_scope="/g1").remember(_LONG)
        assert record.categories == ["swiss-news", "energy-markets"]

    def test_agent_role_is_persisted_as_provenance_metadata(self):
        # The scope stays the tenant boundary (the Databricks backend filters
        # scope by EXACT match), so the writer rides in metadata instead.
        record = Memory(root_scope="/g1").remember(
            _LONG, agent_role="Direct User Helper"
        )
        assert record.metadata["agent_role"] == "Direct User Helper"
        assert record.scope == "/g1"


class TestAnalysisIsSkipped:
    def test_caller_supplied_categories_and_importance_win(self):
        llm = _FakeLLM(_GOOD_REPLY)
        record = Memory(llm=llm, root_scope="/g1").remember(
            _LONG, categories=["manual"], importance=0.9
        )
        assert record.categories == ["manual"]
        assert record.importance == 0.9
        assert llm.calls == [], "no LLM call when nothing needs analysing"

    def test_short_fragments_are_not_worth_a_call(self):
        llm = _FakeLLM(_GOOD_REPLY)
        record = Memory(llm=llm, root_scope="/g1").remember("too short")
        assert record.categories == []
        assert llm.calls == []

    def test_analyze_on_save_false_disables_it(self):
        llm = _FakeLLM(_GOOD_REPLY)
        record = Memory(llm=llm, root_scope="/g1", analyze_on_save=False).remember(
            _LONG
        )
        assert record.categories == []
        assert llm.calls == []

    def test_no_llm_configured_saves_unlabelled(self):
        record = Memory(root_scope="/g1").remember(_LONG)
        assert record.categories == []
        assert record.importance == 0.5

    def test_default_importance_applies_when_nothing_supplies_one(self):
        record = Memory(root_scope="/g1", default_importance=0.25).remember(_LONG)
        assert record.importance == 0.25


class TestAnalysisFailuresNeverBreakTheSave:
    def test_raising_llm_still_saves(self):
        class _BoomLLM:
            def call(self, *args, **kwargs):
                raise RuntimeError("endpoint down")

        record = Memory(llm=_BoomLLM(), root_scope="/g1").remember(_LONG)
        assert record.categories == []
        assert record.importance == 0.5

    def test_non_json_reply_still_saves(self):
        record = Memory(
            llm=_FakeLLM("I think this is about cats."), root_scope="/g1"
        ).remember(_LONG)
        assert record.categories == []

    def test_malformed_fields_are_coerced_not_fatal(self):
        # MemoryAnalysis is deliberately tolerant: stringified JSON lists and
        # out-of-range importance are coerced rather than raised.
        llm = _FakeLLM(
            '{"categories": "[\\"grid-deal\\"]", "importance": 7, '
            '"extracted_metadata": "not-an-object"}'
        )
        record = Memory(llm=llm, root_scope="/g1").remember(_LONG)
        assert record.categories == ["grid-deal"]
        assert record.importance == 1.0
