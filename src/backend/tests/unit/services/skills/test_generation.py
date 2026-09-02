"""Tests for SkillGenerationService: one focused LLM call, JSON out, validated
before it returns, one retry with the validator's own errors."""

import asyncio
import json

from src.services.skills import generation


class _Group:
    primary_group_id = "g1"
    group_ids = ["g1"]


def _install(monkeypatch, replies, template_text="SYSTEM TEMPLATE"):
    """Stub the LLM (sequential replies) and the template lookup; record calls."""
    calls = []

    async def completion(**kwargs):
        calls.append(kwargs)
        reply = replies.pop(0)
        # The service asks for the served model (a tuple return), like the
        # real LLMManager.completion does under ``with_served_model``.
        return (reply, "served-model") if kwargs.get("with_served_model") else reply

    async def template(name, gc):
        return template_text

    monkeypatch.setattr(generation.LLMManager, "completion", completion)
    monkeypatch.setattr(
        generation.TemplateService, "get_effective_template_content", template
    )
    return calls


GOOD = json.dumps(
    {
        "name": "writing-release-notes",
        "description": "Use when drafting release notes. Trigger when the user mentions releases.",
        "body": "# Writing release notes\n\n## When to use this skill\nAny release.\n\n## 1. Lead with what changed\nBecause readers skim.\n",
    }
)


def test_blank_page_draft_is_validated_and_uses_the_template(monkeypatch):
    calls = _install(monkeypatch, [GOOD])
    out = asyncio.run(
        generation.SkillGenerationService.draft("a skill for release notes", _Group())
    )
    assert out["valid"] is True and out["errors"] == []
    assert out["name"] == "writing-release-notes"
    assert calls[0]["messages"][0] == {"role": "system", "content": "SYSTEM TEMPLATE"}
    assert calls[0]["messages"][1]["content"].startswith("MODE: blank page")
    assert calls[0]["extra_headers"]  # Databricks telemetry header present


def test_capture_mode_feeds_the_transcript(monkeypatch):
    calls = _install(monkeypatch, [GOOD])
    transcript = [
        {"role": "user", "content": "write release notes"},
        {"role": "assistant", "content": "here"},
        {"role": "user", "content": "no — lead with what changed, not who did it"},
        {"role": "system", "content": "ignored"},
    ]
    asyncio.run(
        generation.SkillGenerationService.draft(
            "save this as a skill", _Group(), transcript=transcript, model="m-1"
        )
    )
    user = calls[0]["messages"][1]["content"]
    assert user.startswith("MODE: capture")
    assert "lead with what changed" in user
    assert "ignored" not in user
    assert calls[0]["model"] == "m-1"


def test_invalid_draft_is_retried_once_with_the_validator_errors(monkeypatch):
    bad = json.dumps({"name": "Not Kebab", "description": "d", "body": "b"})
    calls = _install(monkeypatch, [bad, GOOD])
    out = asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert out["valid"] is True
    assert len(calls) == 2
    retry = calls[1]["messages"][-1]["content"]
    assert retry.startswith("That draft failed validation")


def test_second_failure_is_returned_with_errors_not_raised(monkeypatch):
    bad = json.dumps({"name": "Not Kebab", "description": "d", "body": "b"})
    _install(monkeypatch, [bad, bad])
    out = asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert out["valid"] is False
    assert out["errors"]
    assert out["name"] == "Not Kebab"  # the card shows the draft + the errors


def test_unparseable_reply_becomes_an_invalid_empty_draft(monkeypatch):
    _install(monkeypatch, ["not json at all", "still not json"])
    out = asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert out["valid"] is False
    assert out["name"] == ""


def test_template_failure_falls_back_to_the_seed(monkeypatch):
    calls = _install(monkeypatch, [GOOD])

    async def boom(name, gc):
        raise RuntimeError("db down")

    monkeypatch.setattr(
        generation.TemplateService, "get_effective_template_content", boom
    )
    asyncio.run(generation.SkillGenerationService.draft("x", _Group()))
    assert "Return ONLY a JSON object" in calls[0]["messages"][0]["content"]


def test_reports_the_served_model_and_the_attempt_count(monkeypatch):
    _install(monkeypatch, [GOOD])
    out = asyncio.run(
        generation.SkillGenerationService.draft("a skill", _Group(), model="picker-key")
    )
    assert out["model"] == "served-model" and out["attempts"] == 1

    _install(monkeypatch, [json.dumps({"name": "Bad Name!"}), GOOD])
    out = asyncio.run(generation.SkillGenerationService.draft("a skill", _Group()))
    assert out["valid"] is True and out["attempts"] == 2


def test_served_name_drops_the_none_substitution_suffix():
    assert generation._served_name("gpt-x (for 'None')", None) == "gpt-x"
    assert generation._served_name("gpt-x (for 'k')", "k") == "gpt-x (for 'k')"
    assert generation._served_name(None, "k") == "k"
    assert generation._served_name("", None) is None


def test_with_a_session_the_draft_is_a_run_with_one_call_recorded_per_attempt(
    monkeypatch,
):
    _install(monkeypatch, [json.dumps({"name": "Bad Name!"}), GOOD])
    events = []

    async def open_run(session, **kwargs):
        events.append(("open", session, kwargs["transcript_turns"]))
        return "job-9"

    async def record_call(job_id, **kwargs):
        events.append(("call", job_id, kwargs["attempt"], kwargs["model"]))
        assert kwargs["prompt"].startswith("[system]\nSYSTEM TEMPLATE")
        assert kwargs["duration_ms"] >= 0

    async def close_run(job_id, **kwargs):
        events.append(("close", job_id, kwargs.get("result", {}).get("valid")))

    monkeypatch.setattr(generation.draft_run, "open_run", open_run)
    monkeypatch.setattr(generation.draft_run, "record_call", record_call)
    monkeypatch.setattr(generation.draft_run, "close_run", close_run)
    out = asyncio.run(
        generation.SkillGenerationService.draft("a skill", _Group(), session="S")
    )
    assert out["job_id"] == "job-9" and out["attempts"] == 2
    assert events == [
        ("open", "S", 0),
        ("call", "job-9", 1, "served-model"),
        ("call", "job-9", 2, "served-model"),
        ("close", "job-9", True),
    ]


def test_an_llm_failure_fails_the_run_and_still_raises(monkeypatch):
    async def completion(**kwargs):
        raise RuntimeError("endpoint down")

    async def template(name, gc):
        return "T"

    monkeypatch.setattr(generation.LLMManager, "completion", completion)
    monkeypatch.setattr(
        generation.TemplateService, "get_effective_template_content", template
    )
    closed = []

    async def open_run(session, **kwargs):
        return "job-x"

    async def close_run(job_id, **kwargs):
        closed.append((job_id, kwargs.get("error")))

    monkeypatch.setattr(generation.draft_run, "open_run", open_run)
    monkeypatch.setattr(generation.draft_run, "close_run", close_run)
    try:
        asyncio.run(
            generation.SkillGenerationService.draft("a skill", _Group(), session="S")
        )
    except RuntimeError as exc:
        assert "endpoint down" in str(exc)
    else:
        raise AssertionError("expected the LLM failure to propagate")
    assert closed == [("job-x", "endpoint down")]
