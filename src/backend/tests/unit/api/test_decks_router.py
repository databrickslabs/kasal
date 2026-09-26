"""The decks router: one endpoint, revising one slide."""

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from src.api.decks_router import router
from src.core.exceptions import KasalError
from src.dependencies.providers import get_group_context, get_smart_db_session
from src.services.decks.slide_refine import SlideRefineService
from src.utils.user_context import GroupContext


def _client(monkeypatch, refine):
    app = FastAPI()
    app.include_router(router)
    # The real app maps KasalError to its status code (main.py); this bare app
    # needs the same so a 400 reads as a 400 rather than an unhandled error.
    app.add_exception_handler(
        KasalError,
        lambda request, exc: JSONResponse(
            status_code=getattr(exc, "status_code", 500), content={"detail": str(exc)}
        ),
    )
    app.dependency_overrides[get_group_context] = lambda: GroupContext(
        group_ids=["g1"], group_email="dev@example.com", email_domain="example.com"
    )

    async def session():
        yield "SESSION"

    app.dependency_overrides[get_smart_db_session] = session
    monkeypatch.setattr(SlideRefineService, "refine", staticmethod(refine))
    return TestClient(app)


def test_refine_slide_passes_the_edit_through_and_returns_the_section(monkeypatch):
    seen = {}

    async def refine(**kwargs):
        seen.update(kwargs)
        return {
            "section": '<section class="slide">new</section>',
            "error": None,
            "model": "m",
            "attempts": 1,
            "job_id": "j",
            "duration_ms": 12.5,
        }

    client = _client(monkeypatch, refine)
    res = client.post(
        "/decks/slides/refine",
        json={
            "mode": "refine",
            "instruction": "bigger",
            "slide": '<section class="slide">old</section>',
            "reference": '<section class="slide">c</section>',
            "position": "2 of 8",
            "model": "k",
        },
    )
    assert res.status_code == 200, res.text
    assert res.json()["section"].startswith("<section") and res.json()["job_id"] == "j"
    assert (
        seen["mode"] == "refine"
        and seen["instruction"] == "bigger"
        and seen["session"] == "SESSION"
    )
    assert seen["position"] == "2 of 8" and seen["model"] == "k"


def test_a_bad_edit_is_a_400(monkeypatch):
    async def refine(**kwargs):
        raise ValueError("a refine needs the slide to revise")

    res = _client(monkeypatch, refine).post(
        "/decks/slides/refine", json={"mode": "refine", "instruction": "x"}
    )
    assert res.status_code == 400 and "needs the slide" in res.text


def test_an_oversized_slide_is_rejected_by_the_schema(monkeypatch):
    async def refine(**kwargs):
        raise AssertionError("must not be called")

    res = _client(monkeypatch, refine).post(
        "/decks/slides/refine",
        json={"mode": "refine", "instruction": "x", "slide": "x" * 60_001},
    )
    assert res.status_code == 422


def test_start_slide_edit_returns_run_id_and_forwards_capabilities(monkeypatch):
    from unittest.mock import AsyncMock

    from src.services.decks import agent_refine

    start = AsyncMock(return_value={"job_id": "agent-run"})
    monkeypatch.setattr(agent_refine, "start_slide_refinement", start)
    client = _client(monkeypatch, AsyncMock())
    response = client.post(
        "/decks/slides/refine/start",
        json={
            "mode": "refine",
            "instruction": "Search online",
            "slide": "current",
            "mcp_servers": ["browser"],
            "agentbricks_endpoints": ["researcher"],
        },
    )
    assert response.status_code == 200
    assert response.json() == {"job_id": "agent-run"}
    body, session, context = start.call_args.args
    assert body.mcp_servers == ["browser"]
    assert body.agentbricks_endpoints == ["researcher"]
    assert session == "SESSION"
    assert context.group_ids == ["g1"]
