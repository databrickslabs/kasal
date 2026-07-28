"""Shaping a trace row for a response: masking, and previewing.

Previewing exists because the timeline draws one-line labels from rows that
carry entire transcripts — A2UI compose prompts alone run past 30,000 characters
(the component catalog is in them). Shipping that for every row means the
browser downloads, and then holds, a run's whole transcript to render a list.

The rule these pin down: the LIST is previews, the DETAIL is whole, and a
trimmed row still reports its TRUE size — a label reading "(2,000 chars)" for a
34,000-char prompt is not a smaller truth, it is a wrong one.
"""

from src.schemas.execution_trace import ExecutionTraceItem
from src.services.trace.row_view import preview_trace


def _item(**overrides):
    data = {
        "id": 1,
        "run_id": 1,
        "job_id": "job-1",
        "event_source": "Assistant",
        "event_context": "ctx",
        "event_type": "llm_call",
        "output": {"content": "short"},
        "trace_metadata": {"model": "m"},
    }
    data.update(overrides)
    return ExecutionTraceItem.model_validate(data)


class TestPreview:
    def test_a_long_prompt_is_trimmed_and_its_true_size_recorded(self):
        item = _item(trace_metadata={"model": "m", "prompt": "p" * 34000})

        previewed = preview_trace(item, limit=2000)

        assert len(previewed.trace_metadata["prompt"]) == 2000
        assert previewed.trace_metadata["prompt_chars"] == 34000, "the label reads this"
        assert previewed.trace_metadata["preview"] is True

    def test_output_content_is_trimmed_too(self):
        item = _item(output={"content": "c" * 12000})

        previewed = preview_trace(item, limit=2000)

        assert len(previewed.output["content"]) == 2000
        assert previewed.trace_metadata["content_chars"] == 12000

    def test_nested_extra_data_is_trimmed(self):
        """A2UI rows carry the same text under output.extra_data."""
        item = _item(output={"content": "x", "extra_data": {"prompt": "p" * 9000}})

        previewed = preview_trace(item, limit=2000)

        assert len(previewed.output["extra_data"]["prompt"]) == 2000
        assert previewed.trace_metadata["prompt_chars"] == 9000

    def test_a_short_row_is_returned_untouched_and_unmarked(self):
        """Most rows are small; they must read exactly as they did before."""
        item = _item()

        previewed = preview_trace(item, limit=2000)

        assert previewed is item
        assert "preview" not in (previewed.trace_metadata or {})

    def test_zero_disables_trimming(self):
        """The default for every existing caller: rows come back whole."""
        item = _item(trace_metadata={"prompt": "p" * 34000})

        previewed = preview_trace(item, limit=0)

        assert len(previewed.trace_metadata["prompt"]) == 34000

    def test_non_string_fields_are_left_alone(self):
        item = _item(output={"content": {"nested": "object"}, "duration_ms": 12.5})

        previewed = preview_trace(item, limit=10)

        assert previewed.output["content"] == {"nested": "object"}
        assert previewed.output["duration_ms"] == 12.5
