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
        return replies.pop(0)

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
