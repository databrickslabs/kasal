"""Tests for Kasal↔UC skill sync (services/skills/uc_sync).

The three UC calls are stubbed; what matters is the ORDER and SHAPE of the
lifecycle — create securable (parent/skill_id as query params) -> PUT SKILL.md +
each bundle file to the Files API -> finalize — and that a SKILL.md carrying the
skill's front-matter + body is what gets uploaded.
"""

import asyncio
from types import SimpleNamespace

import pytest

import src.services.skills.uc_sync as uc_sync
from src.core.exceptions import KasalError, NotFoundError
from src.services.skills.uc_sync import SkillUcSyncService


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}
        self.text = text

    def json(self):
        return self._json


class FakeClient:
    """Records every call; routes by method + URL suffix to canned responses."""

    def __init__(
        self, *, create=None, finalize=None, put=None, get=None, get_routes=None
    ):
        self.calls = []
        self._create = create or FakeResponse(200, {})
        self._finalize = finalize or FakeResponse(200, {"name": "skills/c.s.x"})
        self._put = put or FakeResponse(204)
        self._get = get or FakeResponse(200, {"skills": []})
        #: url-suffix -> FakeResponse (first match wins; declare specific
        #: suffixes first). A LIST value serves its items in order (paging).
        self._get_routes = get_routes or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, params=None, json=None):
        self.calls.append(("POST", url, {"params": params, "json": json}))
        return self._finalize if url.endswith("/finalize") else self._create

    async def put(self, url, headers=None, content=None):
        self.calls.append(("PUT", url, {"content": content}))
        return self._put

    async def get(self, url, headers=None, params=None):
        self.calls.append(("GET", url, {"params": params}))
        for suffix, resp in self._get_routes.items():
            if url.endswith(suffix):
                if isinstance(resp, list):
                    return resp.pop(0) if len(resp) > 1 else resp[0]
                return resp
        return self._get


def _skill(**over):
    base = dict(
        name="basic-math",
        description="Arithmetic helper",
        body="## Add\n1+1=2\n",
        license=None,
        compatibility=None,
        skill_metadata={},
        files=[SimpleNamespace(path="references/notes.md", content="hi")],
    )
    base.update(over)
    return SimpleNamespace(**base)


def _service(skill, client, monkeypatch):
    svc = SkillUcSyncService(
        session=None,
        group_context=SimpleNamespace(primary_group_id="g1", access_token=None),
    )

    async def _auth():
        return {"Authorization": "Bearer t"}, "https://ws"

    svc._auth = _auth  # type: ignore[assignment]

    async def _get_skill(sid, gc):
        return skill

    async def _list_skills(gc):
        return [skill] if skill else []

    svc._skills = SimpleNamespace(get_skill=_get_skill, list_skills=_list_skills)
    # setattr via monkeypatch so the real httpx.AsyncClient is restored after the
    # test — a bare assignment would swap the global class for the whole session.
    monkeypatch.setattr(uc_sync.httpx, "AsyncClient", lambda *a, **k: client)
    return svc


def test_push_runs_create_then_uploads_then_finalize(monkeypatch):
    client = FakeClient(
        finalize=FakeResponse(200, {"name": "skills/kasal.default.basic-math"})
    )
    svc = _service(_skill(), client, monkeypatch)

    result = asyncio.run(svc.push_skill(1, "kasal", "default"))

    kinds = [(m, u) for (m, u, _extra) in client.calls]
    # order: create -> PUT SKILL.md -> PUT bundle file -> finalize
    assert kinds[0] == ("POST", "https://ws/api/2.1/unity-catalog/skills")
    assert kinds[1] == (
        "PUT",
        "https://ws/api/2.0/fs/files/Skills/kasal/default/basic-math/SKILL.md",
    )
    assert kinds[2] == (
        "PUT",
        "https://ws/api/2.0/fs/files/Skills/kasal/default/basic-math/references/notes.md",
    )
    assert kinds[3] == (
        "POST",
        "https://ws/api/2.1/unity-catalog/skills/kasal.default.basic-math/finalize",
    )
    assert result == {"name": "skills/kasal.default.basic-math"}


def test_create_passes_parent_and_skill_id_as_query_params(monkeypatch):
    client = FakeClient()
    svc = _service(_skill(), client, monkeypatch)
    asyncio.run(svc.push_skill(1, "kasal", "default"))
    create = next(
        c for c in client.calls if c[0] == "POST" and c[1].endswith("/skills")
    )
    assert create[2]["params"] == {
        "parent": "schemas/kasal.default",
        "skill_id": "basic-math",
    }


def test_uploaded_skill_md_carries_frontmatter_and_body(monkeypatch):
    client = FakeClient()
    svc = _service(_skill(), client, monkeypatch)
    asyncio.run(svc.push_skill(1, "kasal", "default"))
    put_md = next(
        c for c in client.calls if c[0] == "PUT" and c[1].endswith("/SKILL.md")
    )
    content = put_md[2]["content"].decode("utf-8")
    assert "name: basic-math" in content
    assert "description: Arithmetic helper" in content
    assert "## Add" in content


def test_push_is_idempotent_when_skill_already_exists(monkeypatch):
    """A re-push (securable exists) must continue to re-upload + finalize, not error."""
    client = FakeClient(
        create=FakeResponse(409, {"error_code": "ALREADY_EXISTS"}, "already exists")
    )
    svc = _service(_skill(), client, monkeypatch)
    result = asyncio.run(svc.push_skill(1, "kasal", "default"))
    assert any(u.endswith("/finalize") for (_m, u, _e) in client.calls)
    assert result  # finalized despite the 409 on create


def test_push_raises_when_skill_missing(monkeypatch):
    client = FakeClient()
    svc = _service(None, client, monkeypatch)
    with pytest.raises(NotFoundError):
        asyncio.run(svc.push_skill(999, "kasal", "default"))


def test_push_raises_on_finalize_failure(monkeypatch):
    client = FakeClient(finalize=FakeResponse(500, {}, "boom"))
    svc = _service(_skill(), client, monkeypatch)
    with pytest.raises(KasalError):
        asyncio.run(svc.push_skill(1, "kasal", "default"))


def test_invalid_skill_name_rejected_before_any_call(monkeypatch):
    client = FakeClient()
    svc = _service(_skill(name="bad name/with spaces"), client, monkeypatch)
    from src.core.exceptions import BadRequestError

    with pytest.raises(BadRequestError):
        asyncio.run(svc.push_skill(1, "kasal", "default"))
    assert client.calls == []  # rejected before touching UC


def test_list_uc_skills_returns_the_skills_array(monkeypatch):
    client = FakeClient(
        get=FakeResponse(200, {"skills": [{"name": "skills/kasal.default.basic-math"}]})
    )
    svc = _service(_skill(), client, monkeypatch)
    out = asyncio.run(svc.list_uc_skills("kasal", "default"))
    assert out == [{"name": "skills/kasal.default.basic-math"}]
    listed = next(c for c in client.calls if c[0] == "GET")
    assert listed[2]["params"] == {"parent": "schemas/kasal.default"}


def test_push_all_publishes_every_visible_skill(monkeypatch):
    client = FakeClient()
    svc = _service(_skill(), client, monkeypatch)
    results = asyncio.run(svc.push_all_skills("kasal", "default"))
    assert results == [{"name": "basic-math", "status": "ok"}]
    # The one skill was taken through the full lifecycle (finalize hit once).
    assert sum(1 for (_m, u, _e) in client.calls if u.endswith("/finalize")) == 1


def test_push_all_records_per_skill_errors_without_aborting(monkeypatch):
    """A failing skill is recorded, not raised — the batch continues."""
    client = FakeClient(finalize=FakeResponse(500, {}, "boom"))
    svc = _service(_skill(), client, monkeypatch)
    results = asyncio.run(svc.push_all_skills("kasal", "default"))
    assert results[0]["status"] == "error"
    assert "boom" in results[0]["error"]


def test_import_all_summarizes_ok_and_error(monkeypatch):
    """import_all lists UC skills then imports each; one failing import is
    recorded rather than aborting the pull."""
    client = FakeClient(
        get=FakeResponse(
            200, {"skills": [{"bundle_name": "good"}, {"bundle_name": "bad"}]}
        )
    )
    svc = _service(_skill(), client, monkeypatch)

    async def _import_one(_client, _headers, _host, _cat, _sch, sid):
        if sid == "bad":
            raise KasalError(detail="download failed")
        return SimpleNamespace(name=sid)

    svc._import_one = _import_one  # type: ignore[assignment]

    results = asyncio.run(svc.import_all_skills("kasal", "default"))
    assert results == [
        {"name": "good", "status": "ok"},
        {"name": "bad", "status": "error", "error": "download failed"},
    ]


# --- pull recursion, pagination, single-auth batches ------------------------

NESTED_SKILL_MD = (
    "---\nname: basic-math\ndescription: Arithmetic helper\n---\n\n"
    "# Basic math\n\nAdd numbers.\n"
)


def test_pull_recurses_into_bundle_subdirectories(monkeypatch):
    """Push writes nested paths (references/...); pull must walk them back out.
    The flat listing skipped is_directory entries, so a push->pull round-trip
    silently dropped every nested file — including the references/ files
    Kasal's own builtin skills carry."""
    client = FakeClient(
        get_routes={
            "references/notes.md": FakeResponse(200, text="nested!"),
            "/SKILL.md": FakeResponse(200, text=NESTED_SKILL_MD),
            "/basic-math/references": FakeResponse(
                200, {"contents": [{"name": "notes.md", "is_directory": False}]}
            ),
            "/basic-math": FakeResponse(
                200,
                {
                    "contents": [
                        {"name": "SKILL.md", "is_directory": False},
                        {"name": "references", "is_directory": True},
                    ]
                },
            ),
        }
    )
    svc = _service(None, client, monkeypatch)
    captured = {}

    async def _create(payload, gc, source=None, files=None):
        captured["files"] = files
        return SimpleNamespace(id=1, name=payload.name)

    svc._skills.create_skill = _create  # type: ignore[attr-defined]

    asyncio.run(svc.import_skill("kasal", "default", "basic-math"))

    assert captured["files"] == [{"path": "references/notes.md", "content": "nested!"}]


def test_list_follows_pagination(monkeypatch):
    """A schema larger than one page must not silently truncate the listing."""
    pages = [
        FakeResponse(
            200, {"skills": [{"name": "skills/c.s.a"}], "next_page_token": "t2"}
        ),
        FakeResponse(200, {"skills": [{"name": "skills/c.s.b"}]}),
    ]
    client = FakeClient(get_routes={"unity-catalog/skills": pages})
    svc = _service(None, client, monkeypatch)

    out = asyncio.run(svc.list_uc_skills("c", "s"))

    assert [x["name"] for x in out] == ["skills/c.s.a", "skills/c.s.b"]
    second = client.calls[1]
    assert second[2]["params"]["page_token"] == "t2"


def test_import_all_resolves_auth_once(monkeypatch):
    """The batch pull shares one auth resolution and one client, as documented."""
    client = FakeClient()  # default GET: {"skills": []}
    svc = _service(None, client, monkeypatch)
    count = {"n": 0}
    orig = svc._auth

    async def counting():
        count["n"] += 1
        return await orig()

    svc._auth = counting  # type: ignore[assignment]

    assert asyncio.run(svc.import_all_skills("kasal", "default")) == []
    assert count["n"] == 1
