"""Reasoning and empty answers on the RESPONSES API path.

The chat path collected reasoning through ``split_message_content``; the
Responses path had no equivalent, so ``_reasoning_text`` stayed empty there —
the reasoning never reached the trace, the UI panel, or the empty-answer
recovery, and copy-pasting the chat fix would have been a no-op.

Two independent defects were verified against openai 2.32.0 and are pinned here:

1. ``Response.output_text`` is a PROPERTY that joins ``output_text`` blocks and
   returns ``""`` when there are none — never ``None``. Guarding the manual
   fallback with ``text is None`` made it dead code for every real SDK object.
2. Nothing populated ``_reasoning_text`` on this path at all.
"""

import pytest

from src.core.llm.transport.response_parsing import (
    REDACTED_REASONING,
    responses_reasoning_text,
)


class _Block:
    def __init__(self, text):
        self.text = text
        self.type = "summary_text"


class _Reasoning:
    type = "reasoning"

    def __init__(self, summary=None, content=None, encrypted_content=None):
        self.summary = summary or []
        self.content = content or []
        self.encrypted_content = encrypted_content


class _Message:
    type = "message"

    def __init__(self, content=None):
        self.content = content or []


class _Response:
    def __init__(self, output):
        self.output = output


class TestResponsesReasoningText:
    def test_reads_summary_blocks(self):
        response = _Response([_Reasoning(summary=[_Block("first"), _Block(" second")])])

        assert responses_reasoning_text(response) == "first second"

    def test_reads_content_blocks(self):
        response = _Response([_Reasoning(content=[_Block("thinking")])])

        assert responses_reasoning_text(response) == "thinking"

    def test_reads_both_shapes_together(self):
        response = _Response([_Reasoning(summary=[_Block("a")], content=[_Block("b")])])

        assert responses_reasoning_text(response) == "ab"

    def test_reports_redaction_when_the_text_is_encrypted(self):
        """The model reasoned and the provider withheld it — not the same as a
        model that never reasons."""
        response = _Response([_Reasoning(encrypted_content="opaque-blob")])

        assert responses_reasoning_text(response) == REDACTED_REASONING

    def test_real_text_beats_the_redaction_flag(self):
        response = _Response(
            [_Reasoning(summary=[_Block("visible")], encrypted_content="blob")]
        )

        assert responses_reasoning_text(response) == "visible"

    def test_ignores_non_reasoning_items(self):
        response = _Response([_Message(content=[_Block("the answer")])])

        assert responses_reasoning_text(response) == ""

    @pytest.mark.parametrize("output", [None, [], "not a list"])
    def test_survives_an_unusable_output(self, output):
        assert responses_reasoning_text(_Response(output)) == ""

    def test_accepts_dict_shaped_items(self):
        """_block_field reads dicts as well as SDK objects."""
        response = _Response(
            [{"type": "reasoning", "summary": [{"text": "as a dict"}]}]
        )

        assert responses_reasoning_text(response) == "as a dict"


class TestAgainstTheRealSDKObject:
    def test_output_text_is_empty_string_not_none(self):
        """The fact that made the manual fallback dead code."""
        from openai.types.responses import Response

        assert isinstance(Response.__dict__["output_text"], property)

        reasoning_only = Response.construct(output=[])

        assert reasoning_only.output_text == ""
        assert reasoning_only.output_text is not None

    def test_reasoning_is_extracted_from_a_real_sdk_item(self):
        from openai.types.responses import ResponseReasoningItem
        from openai.types.responses.response_reasoning_item import Summary

        item = ResponseReasoningItem(
            id="r1",
            type="reasoning",
            summary=[Summary(text="the model thought this", type="summary_text")],
        )

        assert responses_reasoning_text(_Response([item])) == "the model thought this"
