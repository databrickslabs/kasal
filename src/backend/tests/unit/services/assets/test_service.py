"""ChatAssetService: accepts images within limits, scopes by group, writes on
a private connection, and describes an asset without its bytes."""

import asyncio
from types import SimpleNamespace

import pytest

from src.services.assets import service as service_module
from src.services.assets.service import (
    MAX_BYTES,
    AssetValidationError,
    ChatAssetService,
    asset_ref,
)


class _Group:
    primary_group_id = "g1"
    group_ids = ["g1", "g2"]
    group_email = "dev@example.com"


class _Repo:
    def __init__(self, session):
        self.bound_to = session
        self.created = []
        self.deleted = []

    async def create(self, data):
        self.created.append(data)
        return SimpleNamespace(
            id="a1", **{k: v for k, v in data.items() if k != "data"}
        )

    async def get(self, asset_id, group_ids):
        self.got = (asset_id, group_ids)
        return (
            SimpleNamespace(id=asset_id, data=b"png", mime="image/png")
            if asset_id == "a1"
            else None
        )

    async def list_for_session(self, session_id, group_ids):
        return [
            SimpleNamespace(
                id="a1",
                name="n",
                mime="image/png",
                size=3,
                width=1,
                height=1,
                session_id=session_id,
            )
        ]

    async def delete(self, asset_id, group_ids):
        self.deleted.append((asset_id, group_ids))
        return asset_id == "a1"


class _Session:
    def __init__(self):
        self.commits = 0

    async def commit(self):
        self.commits += 1


def _service(monkeypatch):
    """The service on a request session, with the PRIVATE write connection
    stubbed (see the service: writes never touch the shared SQLite connection)."""
    request_session, iso = _Session(), _Session()
    repos = []

    class Repo(_Repo):
        def __init__(self, sess):
            super().__init__(sess)
            repos.append(self)

    class _IsoCtx:
        async def __aenter__(self):
            return iso

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(service_module, "get_isolated_db_session", lambda: _IsoCtx())
    return (
        ChatAssetService(request_session, repository_class=Repo),
        request_session,
        iso,
        repos,
    )


def test_upload_stores_the_image_on_a_private_connection_with_tenant_stamps(
    monkeypatch,
):
    service, request_session, iso, repos = _service(monkeypatch)
    out = asyncio.run(
        service.upload(
            data=b"\x89PNG...",
            name="shot.png",
            mime="image/PNG",
            width=1280,
            height=720,
            session_id="s1",
            group_context=_Group(),
        )
    )
    writer = repos[-1]
    assert writer.bound_to is iso  # the private connection, not the request session
    row = writer.created[0]
    assert row["group_id"] == "g1" and row["created_by_email"] == "dev@example.com"
    assert row["mime"] == "image/png" and row["size"] == 7 and row["session_id"] == "s1"
    assert out == {
        "id": "a1",
        "name": "shot.png",
        "mime": "image/png",
        "size": 7,
        "width": 1280,
        "height": 720,
        "session_id": "s1",
        "ref": "asset:a1",
    }
    assert iso.commits == 1 and request_session.commits == 0
    assert asset_ref("x") == "asset:x"


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"mime": "image/svg+xml", "data": b"<svg/>"}, "not an accepted image type"),
        ({"mime": "image/png", "data": b""}, "empty"),
        ({"mime": "image/png", "data": b"x" * (MAX_BYTES + 1)}, "limit is 10 MB"),
    ],
)
def test_upload_rejects_wrong_types_empty_and_oversized(kwargs, message, monkeypatch):
    service, request_session, iso, _ = _service(monkeypatch)
    with pytest.raises(AssetValidationError) as exc:
        asyncio.run(
            service.upload(
                name="f",
                width=None,
                height=None,
                session_id=None,
                group_context=_Group(),
                **kwargs,
            )
        )
    assert message in str(exc.value)
    assert iso.commits == 0 and request_session.commits == 0


def test_get_and_list_use_the_request_session_and_are_group_scoped(monkeypatch):
    service, request_session, _iso, _ = _service(monkeypatch)
    reader = service.repository
    assert reader.bound_to is request_session
    assert asyncio.run(service.get("a1", _Group())).mime == "image/png"
    assert reader.got == ("a1", ["g1", "g2"])
    assert asyncio.run(service.get("nope", _Group())) is None
    listed = asyncio.run(service.list_for_session("s1", _Group()))
    assert listed[0]["ref"] == "asset:a1" and "data" not in listed[0]


def test_delete_writes_on_the_private_connection(monkeypatch):
    service, request_session, iso, repos = _service(monkeypatch)
    assert asyncio.run(service.delete("a1", _Group())) is True
    assert repos[-1].bound_to is iso and repos[-1].deleted == [("a1", ["g1", "g2"])]
    assert iso.commits == 1
    assert asyncio.run(service.delete("nope", _Group())) is False
    assert iso.commits == 1 and request_session.commits == 0


def test_a_context_with_no_groups_sees_nothing(monkeypatch):
    """No groups must mean no access — never "no filter"."""
    service, _s, _iso, _ = _service(monkeypatch)

    class _NoGroups:
        group_ids = []

    reader = service.repository
    asyncio.run(service.get("a1", _NoGroups()))
    assert reader.got == ("a1", [])  # the repository receives an EMPTY filter, not None


def test_repository_treats_an_empty_group_list_as_no_match():
    """The repository side of the same rule, without a database: the SQL is
    never built for an empty filter."""
    from src.repositories.chat_asset_repository import ChatAssetRepository

    class _NeverExecutes:
        async def execute(self, stmt):
            raise AssertionError("must not query with an empty group filter")

    repo = ChatAssetRepository(_NeverExecutes())
    assert asyncio.run(repo.get("a1", [])) is None
    assert asyncio.run(repo.list_for_session("s1", [])) == []
    assert asyncio.run(repo.delete("a1", [])) is False
