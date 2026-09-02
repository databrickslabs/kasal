"""Tests for SlideRefineService: one focused LLM call returning ONE slide,
one retry when the reply holds none, recorded as a run."""

import asyncio

from src.services.decks import slide_refine
from src.services.decks.slide_refine import (
    SlideRefineService,
    first_slide_section,
    run_name,
)


class _Group:
    primary_group_id = "g1"
    group_ids = ["g1"]
    group_email = "dev@example.com"


SLIDE = '<section class="slide"><h1>Two</h1><section><p>nested</p></section></section>'


def _install(monkeypatch, replies, template_text="SYSTEM TEMPLATE"):
    calls = []

    async def completion(**kwargs):
        calls.append(kwargs)
        reply = replies.pop(0)
        return (reply, "served-model") if kwargs.get("with_served_model") else reply

    async def template(name, gc):
        return template_text

    monkeypatch.setattr(slide_refine.LLMManager, "completion", completion)
    monkeypatch.setattr(
        slide_refine.TemplateService, "get_effective_template_content", template
    )
    return calls


def test_first_slide_section_takes_one_finished_slide_fenced_or_bare():
    assert first_slide_section("Here:\n```html\n" + SLIDE + "\n```\nDone.") == SLIDE
    assert first_slide_section(SLIDE) == SLIDE
    assert first_slide_section('```html\n<section class="slide"><h1>cut') is None
    assert first_slide_section("no slide here") is None
    two = SLIDE + '\n<section class="slide"><h1>Three</h1></section>'
    assert first_slide_section(two) == SLIDE  # only the first


def test_run_name_names_the_edit():
    assert (
        run_name("refine", "bigger title", "3 of 8")
        == "Slide refine (slide 3 of 8): bigger title"
    )
    assert run_name("add", "", "4 of 9") == "New slide (slide 4 of 9)"
    assert len(run_name("refine", "x" * 200, "")) <= len("Slide refine: ") + 80


def test_refine_sends_the_slide_and_reference_and_returns_the_section(monkeypatch):
    calls = _install(monkeypatch, ["```html\n" + SLIDE + "\n```"])
    out = asyncio.run(
        SlideRefineService.refine(
            mode="refine",
            instruction="bigger title",
            group_context=_Group(),
            slide='<section class="slide"><h1>old</h1></section>',
            reference='<section class="slide"><h1>Cover</h1></section>',
            position="2 of 8",
            model="picker-key",
        )
    )
    assert out["section"] == SLIDE and out["error"] is None
    assert (
        out["model"] == "served-model"
        and out["attempts"] == 1
        and out["job_id"] is None
    )
    system, user = calls[0]["messages"]
    assert system == {"role": "system", "content": "SYSTEM TEMPLATE"}
    assert user["content"].startswith("MODE: revise one slide (slide 2 of 8)")
    assert 'SLIDE TO REVISE:\n<section class="slide"><h1>old</h1>' in user["content"]
    assert "REFERENCE SLIDE" in user["content"] and "Cover" in user["content"]
    assert calls[0]["extra_headers"]


def test_add_mode_shows_both_neighbours(monkeypatch):
    calls = _install(monkeypatch, [SLIDE])
    out = asyncio.run(
        SlideRefineService.refine(
            mode="add",
            instruction="pricing",
            group_context=_Group(),
            before='<section class="slide">B</section>',
            after=None,
            position="3 of 9",
        )
    )
    assert out["section"] == SLIDE
    user = calls[0]["messages"][1]["content"]
    assert user.startswith("MODE: add a new slide (slide 3 of 9)")
    assert (
        "THE SLIDE BEFORE IT:\n<section" in user
        and "(none — this will be the last slide)" in user
    )


def test_a_reply_without_a_slide_is_retried_once_then_reported(monkeypatch):
    calls = _install(
        monkeypatch, ["Sure, here is prose.", "```html\n" + SLIDE + "\n```"]
    )
    out = asyncio.run(
        SlideRefineService.refine(
            mode="refine",
            instruction="x",
            group_context=_Group(),
            slide='<section class="slide">s</section>',
        )
    )
    assert out["section"] == SLIDE and out["attempts"] == 2
    assert calls[1]["messages"][-1]["content"].startswith(
        "That reply did not contain a slide"
    )

    _install(monkeypatch, ["nope", "still nope"])
    out = asyncio.run(
        SlideRefineService.refine(
            mode="refine",
            instruction="x",
            group_context=_Group(),
            slide='<section class="slide">s</section>',
        )
    )
    assert out["section"] is None and out["attempts"] == 2
    assert out["error"] == "The model did not return a slide."


def test_refine_without_a_slide_and_an_unknown_mode_are_rejected(monkeypatch):
    _install(monkeypatch, [SLIDE])
    for kwargs in ({"mode": "refine", "slide": ""}, {"mode": "erase", "slide": "x"}):
        try:
            asyncio.run(
                SlideRefineService.refine(
                    instruction="x", group_context=_Group(), **kwargs
                )
            )
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for {kwargs}")


def test_with_a_session_the_edit_is_a_run_with_one_call_recorded_per_attempt(
    monkeypatch,
):
    _install(monkeypatch, ["prose", SLIDE])
    events = []

    async def open_run(session, **kwargs):
        events.append(("open", session, kwargs["trigger_type"], kwargs["run_name"]))
        return "job-7"

    async def record_call(job_id, **kwargs):
        events.append(("call", job_id, kwargs["attempt"], kwargs["source"]))
        assert kwargs["prompt"].startswith("[system]\nSYSTEM TEMPLATE")

    async def close_run(job_id, **kwargs):
        events.append(("close", job_id, kwargs.get("message"), kwargs.get("error")))

    monkeypatch.setattr(slide_refine.generation_run, "open_run", open_run)
    monkeypatch.setattr(slide_refine.generation_run, "record_call", record_call)
    monkeypatch.setattr(slide_refine.generation_run, "close_run", close_run)
    out = asyncio.run(
        SlideRefineService.refine(
            mode="refine",
            instruction="tidy",
            group_context=_Group(),
            slide='<section class="slide">s</section>',
            position="1 of 2",
            session="S",
        )
    )
    assert out["job_id"] == "job-7"
    assert events == [
        ("open", "S", "slide_refine", "Slide refine (slide 1 of 2): tidy"),
        ("call", "job-7", 1, "Decks"),
        ("call", "job-7", 2, "Decks"),
        ("close", "job-7", "Slide refined", None),
    ]


def test_an_llm_failure_fails_the_run_and_still_raises(monkeypatch):
    async def completion(**kwargs):
        raise RuntimeError("endpoint down")

    async def template(name, gc):
        return "T"

    monkeypatch.setattr(slide_refine.LLMManager, "completion", completion)
    monkeypatch.setattr(
        slide_refine.TemplateService, "get_effective_template_content", template
    )
    closed = []

    async def open_run(session, **kwargs):
        return "job-x"

    async def close_run(job_id, **kwargs):
        closed.append((job_id, kwargs.get("error")))

    monkeypatch.setattr(slide_refine.generation_run, "open_run", open_run)
    monkeypatch.setattr(slide_refine.generation_run, "close_run", close_run)
    try:
        asyncio.run(
            SlideRefineService.refine(
                mode="refine",
                instruction="x",
                group_context=_Group(),
                slide='<section class="slide">s</section>',
                session="S",
            )
        )
    except RuntimeError as exc:
        assert "endpoint down" in str(exc)
    else:
        raise AssertionError("expected the LLM failure to propagate")
    assert closed == [("job-x", "endpoint down")]
