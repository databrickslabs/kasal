"""What the bridge puts in a trace when an event carries a structured output.

A TaskOutput/CrewOutput is a pydantic model, so ``str()`` on it yields its repr —
``description='...' name='...' raw='...' agent='...'``. That repr was what
reached the trace table, the chat's task summary, and anything else that reads a
trace as text. Seen on a real flow run: every ``task_completed`` row was an
object repr with the answer quoted and escaped one field along.
"""

from types import SimpleNamespace

from src.services.otel_tracing.event_bridge import _get_output


class _TaskOutput:
    """Quacks like the pydantic output: a repr, with ``raw`` inside it."""

    raw = "# Swiss Food Security and Agricultural Innovation"
    description = "Research and collect current news…"

    def __str__(self) -> str:
        return f"description={self.description!r} raw={self.raw!r}"


class TestStructuredOutputIsUnwrapped:
    def test_a_structured_output_yields_its_raw_text(self):
        text = _get_output(SimpleNamespace(output=_TaskOutput()))

        assert text == "# Swiss Food Security and Agricultural Innovation"
        assert "description=" not in text

    def test_a_plain_string_output_is_untouched(self):
        assert _get_output(SimpleNamespace(output="already text")) == "already text"

    def test_something_with_no_text_field_still_stringifies(self):
        # Never lose the value: an int, an enum, anything without a text field
        # still reaches the trace as its string form.
        assert _get_output(SimpleNamespace(output=123)) == "123"

    def test_the_first_populated_attribute_wins(self):
        # The attribute order is the contract — `output` before `result` before
        # the rest — and unwrapping must not change which one is chosen.
        event = SimpleNamespace(output=_TaskOutput(), result="ignored")
        assert _get_output(event) == _TaskOutput.raw

    def test_no_output_attribute_is_empty(self):
        assert _get_output(SimpleNamespace()) == ""
