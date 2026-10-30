"""A skill draft started in the background: the run is open (and owned by the
caller's workspace) before any LLM call, and the draft runs under that job id."""

import asyncio
from types import SimpleNamespace

import pytest

from src.services.skills import draft_job


def _group(token=None):
    return SimpleNamespace(
        primary_group_id="g1",
        group_ids=["g1"],
        group_email="dev@example.com",
        access_token=token,
    )


def _stub(monkeypatch, *, job_id="job-1", draft=None):
    seen = {"owners": [], "drafts": [], "closed": [], "contexts": []}

    async def open_run(session, **kwargs):
        seen["open"] = (session, kwargs)
        return job_id

    async def close_run(jid, **kwargs):
        seen["closed"].append((jid, kwargs.get("error")))

    async def default_draft(request, group_context, **kwargs):
        seen["drafts"].append((request, group_context, kwargs))
        return {"job_id": kwargs["job_id"]}

    monkeypatch.setattr(draft_job.draft_run, "open_run", open_run)
    monkeypatch.setattr(draft_job.draft_run, "close_run", close_run)
    monkeypatch.setattr(
        draft_job.sse_manager,
        "register_job_owner",
        lambda jid, gid: seen["owners"].append((jid, gid)),
    )
    monkeypatch.setattr(
        draft_job.UserContext,
        "set_group_context",
        lambda gc: seen["contexts"].append(gc),
    )
    monkeypatch.setattr(
        draft_job.UserContext,
        "set_user_token",
        lambda token: seen["contexts"].append(token),
    )
    monkeypatch.setattr(
        draft_job.SkillGenerationService, "draft", staticmethod(draft or default_draft)
    )
    return seen


def test_start_answers_with_the_open_run_and_drafts_under_it(monkeypatch):
    seen = _stub(monkeypatch)
    group = _group(token="tok")

    async def run():
        job_id = await draft_job.start(
            "a skill",
            group,
            "SESSION",
            transcript=[{"role": "user", "content": "hi"}],
            model="m",
        )
        # The job id exists before the draft has run a single step.
        assert seen["drafts"] == []
        await asyncio.gather(*draft_job._tasks)
        return job_id

    assert asyncio.run(run()) == "job-1"
    session, kwargs = seen["open"]
    assert session == "SESSION" and kwargs["transcript_turns"] == 1
    assert kwargs["group_context"] is group and kwargs["model"] == "m"
    assert seen["owners"] == [("job-1", "g1")]
    request, group_context, kw = seen["drafts"][0]
    assert request == "a skill" and group_context is group
    assert kw["job_id"] == "job-1" and kw["model"] == "m"
    # The background task carries the caller's identity for the LLM credentials.
    assert seen["contexts"] == [group, "tok"]
    assert not draft_job._tasks


def test_a_run_that_could_not_be_opened_is_an_error_not_a_lost_answer(monkeypatch):
    seen = _stub(monkeypatch, job_id=None)
    with pytest.raises(RuntimeError, match="skill draft run"):
        asyncio.run(draft_job.start("a skill", _group(), "S"))
    assert seen["drafts"] == [] and seen["owners"] == []


def test_a_failed_background_draft_is_logged_not_raised(monkeypatch):
    async def boom(request, group_context, **kwargs):
        raise RuntimeError("endpoint down")

    seen = _stub(monkeypatch, draft=boom)

    async def run():
        await draft_job.start("a skill", _group(), "S")
        results = await asyncio.gather(*draft_job._tasks, return_exceptions=True)
        assert results == [None]

    asyncio.run(run())
    # draft() already closed the run as FAILED; nothing is closed twice.
    assert seen["closed"] == []


def test_a_cancelled_draft_closes_its_run(monkeypatch):
    started = None  # an Event must be created inside the running loop

    async def slow(request, group_context, **kwargs):
        started.set()
        await asyncio.sleep(10)

    seen = _stub(monkeypatch, draft=slow)

    async def run():
        nonlocal started
        started = asyncio.Event()
        await draft_job.start("a skill", _group(), "S")
        await started.wait()
        (task,) = draft_job._tasks
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    assert seen["closed"] == [("job-1", "Draft cancelled")]
