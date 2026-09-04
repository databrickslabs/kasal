"""The chat assets router: upload, serve, delete."""

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from src.api.chat_assets_router import router
from src.core.dependencies import get_group_context, get_smart_db_session
from src.core.exceptions import KasalError
from src.services.assets.service import AssetValidationError, ChatAssetService
from src.utils.user_context import GroupContext


def _client(monkeypatch, **overrides):
    app = FastAPI()
    app.include_router(router)
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
    for name, fn in overrides.items():
        monkeypatch.setattr(ChatAssetService, name, fn)
    return TestClient(app)


def test_upload_passes_the_image_and_its_reported_size_through(monkeypatch):
    seen = {}

    async def upload(self, **kwargs):
        seen.update(kwargs)
        return {
            "id": "a1",
            "name": kwargs["name"],
            "mime": kwargs["mime"],
            "size": len(kwargs["data"]),
            "width": kwargs["width"],
            "height": kwargs["height"],
            "session_id": kwargs["session_id"],
            "ref": "asset:a1",
        }

    client = _client(monkeypatch, upload=upload)
    res = client.post(
        "/chat/assets",
        files={"file": ("shot.png", b"\x89PNG", "image/png")},
        data={"session_id": "s1", "width": "1280", "height": "720"},
    )
    assert res.status_code == 200, res.text
    assert res.json()["ref"] == "asset:a1" and res.json()["width"] == 1280
    assert (
        seen["data"] == b"\x89PNG"
        and seen["mime"] == "image/png"
        and seen["session_id"] == "s1"
    )


def test_a_rejected_upload_is_a_400(monkeypatch):
    async def upload(self, **kwargs):
        raise AssetValidationError("'image/svg+xml' is not an accepted image type")

    res = _client(monkeypatch, upload=upload).post(
        "/chat/assets", files={"file": ("x.svg", b"<svg/>", "image/svg+xml")}
    )
    assert res.status_code == 400 and "not an accepted" in res.text


def test_get_serves_the_bytes_with_their_type_and_404s_when_missing(monkeypatch):
    async def get(self, asset_id, group_context):
        return (
            SimpleNamespace(data=b"PNGBYTES", mime="image/png")
            if asset_id == "a1"
            else None
        )

    client = _client(monkeypatch, get=get)
    res = client.get("/chat/assets/a1")
    assert res.status_code == 200 and res.content == b"PNGBYTES"
    assert res.headers["content-type"].startswith("image/png")
    assert "immutable" in res.headers["cache-control"]
    assert client.get("/chat/assets/zz").status_code == 404


def test_delete(monkeypatch):
    async def delete(self, asset_id, group_context):
        return asset_id == "a1"

    client = _client(monkeypatch, delete=delete)
    assert client.delete("/chat/assets/a1").status_code == 204
    assert client.delete("/chat/assets/zz").status_code == 404
