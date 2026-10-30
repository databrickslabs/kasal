"""What reaches a fact store from one write. It keeps only what it extracts,
and the extractor can only find what it is shown: with the record store's
1,400-character budget, a campaign report that opened with a paragraph of
method reached memory as a one-line note."""

import json
from types import SimpleNamespace

from src.services.memory.run.persist import (
    _persist_task_output,
    flush_memory_writes,
    format_turn_for_memory,
    remember_async,
)
from src.services.memory.text import readable_output

INTRO = "Genie ran a query over the campaign fact table joined to spend. " * 40
FINDING = "| Premium OLED Spring Showcase | CAD 4,000 | 4.36x | CAD 28.20 |"
ANSWER = f"{INTRO}\n\n{FINDING}"


class FactStore:
    extracts_facts = True
    root_scope = "/grp"
    read_only = False

    def __init__(self):
        self.written = []

    def remember(self, content, **kwargs):
        self.written.append(content)


class TestTurnBudget:
    def test_a_fact_store_sees_figures_after_a_long_opening(self):
        assert len(INTRO) > 1400
        assert "4.36x" in format_turn_for_memory("best campaign?", ANSWER, FactStore())

    def test_a_record_store_keeps_its_short_record(self):
        text = format_turn_for_memory("best campaign?", ANSWER)
        assert "4.36x" not in text and len(text) < 1500

    def test_the_whole_turn_reaches_the_extractor(self):
        store = FactStore()
        remember_async(store, format_turn_for_memory("best campaign?", ANSWER, store))
        assert flush_memory_writes(timeout=10) == 0
        (written,) = store.written
        assert "4.36x" in written

    def test_a_task_result_is_labelled_with_its_task(self):
        store = FactStore()
        task = SimpleNamespace(name="report", description="Find the best campaign.")
        _persist_task_output(store, task, SimpleNamespace(raw=ANSWER, agent="Analyst"))
        assert flush_memory_writes(timeout=10) == 0
        (written,) = store.written
        assert written.startswith("Task: Find the best campaign. Result: ")
        assert "4.36x" in written


class TestReadableOutput:
    """Tool output that arrives as JSON, whoever the tool is."""

    GENIE_LIKE = json.dumps(
        {
            "response_id": "r-1",
            "status": "COMPLETED",
            "final_answer": "Best: Premium OLED — ROAS 4.36x\nWorst: Value Blitz",
            "progress_steps": None,
            "query_item": None,
            "meta": {"rows": 5, "tags": []},
        }
    )

    def test_json_becomes_key_value_lines_without_the_empty_ones(self):
        text = readable_output(self.GENIE_LIKE)
        assert (
            "final_answer: Best: Premium OLED — ROAS 4.36x\nWorst: Value Blitz" in text
        )
        assert "meta.rows: 5" in text
        assert "progress_steps" not in text and "query_item" not in text

    def test_escaped_characters_come_back_as_themselves(self):
        assert readable_output(json.dumps({"name": "Nestlé"})) == "name: Nestlé"

    def test_json_inside_a_string_is_opened(self):
        nested = json.dumps({"result": json.dumps({"roas": "4.36x"})})
        assert readable_output(nested) == "result.roas: 4.36x"

    def test_anything_else_is_returned_as_is(self):
        for text in ("plain answer", "{not json", ""):
            assert readable_output(text) == text
