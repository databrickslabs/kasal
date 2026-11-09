"""LLM judge: prompt, reply parsing, failure handling, and the Kasal completion wrapper."""

from src.services.tools.genie_space_sync_utils.eval.judge import (
    JudgeReply,
    judge,
    parse_reply,
)


def test_a_json_verdict_is_read_even_with_surrounding_text():
    assert parse_reply('Sure: {"correct": true, "reason": "matches"}') == (
        True,
        "matches",
    )
    assert parse_reply('{"correct": "yes"}') == (None, "")
    assert parse_reply("no json here") == (None, "")


def test_the_judge_passes_question_reference_and_answer_and_reads_the_verdict():
    seen = []

    def complete(system, user):
        seen.append((system, user))
        return JudgeReply(
            '{"correct": false, "reason": "wrong total"}', total_tokens=120
        )

    result = judge(complete, "How many?", "Expected 42", "It is 41")
    assert result.correct is False
    assert result.reason == "wrong total"
    assert result.total_tokens == 120
    user = seen[0][1]
    assert "QUESTION:" in user and "Expected 42" in user and "It is 41" in user


def test_a_failed_call_or_unreadable_reply_is_an_error_not_a_verdict():
    def broken(system, user):
        raise RuntimeError("endpoint down")

    result = judge(broken, "q", "r", "a")
    assert result.correct is None
    assert "endpoint down" in result.error
    assert judge(lambda s, u: JudgeReply("garbage"), "q", "r", "a").error
