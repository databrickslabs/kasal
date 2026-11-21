"""Genie REST helpers, the requests-backed transport, and deploy target resolution."""

import pytest

from src.services.tools.genie_space_sync_utils import genie_api
from src.services.tools.genie_space_sync_utils.deploy_plan import (
    build_payload,
    resolve_target,
)
from src.services.tools.genie_space_sync_utils.genie_api import (
    GenieApiError,
    create_space,
    get_space,
    list_spaces,
    requests_http,
    space_url,
    update_space,
)
from src.services.tools.genie_space_sync_utils.space_files import SpaceFilesError

from ._fakes import SPACE_ID, FakeGenie

ROOT = "/api/2.0/genie/spaces"


def test_list_spaces_follows_page_tokens():
    pages = {
        None: {"spaces": [{"space_id": "1", "title": "A"}], "next_page_token": "p2"},
        "p2": {"spaces": [{"space_id": "2", "title": "B"}]},
    }
    http = FakeGenie(
        {("GET", ""): lambda params, body: pages[(params or {}).get("page_token")]}
    )
    spaces = list_spaces(http)

    assert [space["space_id"] for space in spaces] == ["1", "2"]
    assert http.calls[1][2] is None


def test_get_space_returns_none_only_for_404():
    missing = FakeGenie({("GET", f"/{SPACE_ID}"): GenieApiError(404, "no")})
    assert get_space(missing, SPACE_ID) is None

    forbidden = FakeGenie({("GET", f"/{SPACE_ID}"): GenieApiError(403, "no")})
    with pytest.raises(GenieApiError) as raised:
        get_space(forbidden, SPACE_ID)
    assert raised.value.status == 403


def test_get_space_asks_for_the_serialized_definition_by_default():
    seen: list[dict[str, str] | None] = []

    def route(params, body):
        seen.append(params)
        return {"space_id": SPACE_ID}

    get_space(FakeGenie({("GET", f"/{SPACE_ID}"): route}), SPACE_ID)
    get_space(FakeGenie({("GET", f"/{SPACE_ID}"): route}), SPACE_ID, serialized=False)
    assert seen == [{"include_serialized_space": "true"}, None]


def test_create_and_update_use_post_and_patch():
    payload = {"title": "T"}
    http = FakeGenie(
        {
            ("POST", ""): {"space_id": "new"},
            ("PATCH", f"/{SPACE_ID}"): {"space_id": SPACE_ID},
        }
    )
    assert create_space(http, payload) == {"space_id": "new"}
    assert update_space(http, SPACE_ID, payload) == {"space_id": SPACE_ID}
    assert [call[0] for call in http.calls] == ["POST", "PATCH"]


def test_space_url_is_built_from_the_host_without_a_trailing_slash():
    url = space_url("https://example.azuredatabricks.net/", SPACE_ID)
    assert url == f"https://example.azuredatabricks.net/genie/rooms/{SPACE_ID}"


class _Response:
    def __init__(self, status: int, body: object) -> None:
        self.status_code = status
        self._body = body
        self.text = str(body)
        self.content = b"x" if body is not None else b""

    def json(self) -> object:
        return self._body


def test_requests_http_maps_http_errors_to_genie_api_error(monkeypatch):
    responses = [_Response(500, {"error": "boom"})]
    monkeypatch.setattr(genie_api.requests, "request", lambda *a, **k: responses.pop(0))
    http = requests_http("https://example.azuredatabricks.net", {"Authorization": "x"})

    with pytest.raises(GenieApiError) as raised:
        http("GET", "", None, None)
    assert raised.value.status == 500


def test_requests_http_returns_the_json_object_and_sends_telemetry(monkeypatch):
    sent: dict[str, object] = {}

    def fake_request(method, url, **kwargs):
        sent.update(method=method, url=url, **kwargs)
        return _Response(200, {"spaces": []})

    monkeypatch.setattr(genie_api.requests, "request", fake_request)
    http = requests_http("https://example.azuredatabricks.net/", {"Authorization": "x"})

    assert http("GET", "", None, {"page_token": "t"}) == {"spaces": []}
    assert sent["url"] == "https://example.azuredatabricks.net" + ROOT
    headers = sent["headers"]
    assert isinstance(headers, dict)
    assert headers["Authorization"] == "x"
    assert "User-Agent" in headers


def test_requests_http_turns_network_failures_into_genie_api_error(monkeypatch):
    import requests

    def down(*args, **kwargs):
        raise requests.ConnectionError("unreachable")

    monkeypatch.setattr(genie_api.requests, "request", down)
    http = requests_http("https://example.azuredatabricks.net", {})

    with pytest.raises(GenieApiError) as raised:
        http("GET", "", None, None)
    assert raised.value.status == 0


def test_resolve_target_prefers_a_header_id_that_still_exists():
    http = FakeGenie({("GET", f"/{SPACE_ID}"): {"space_id": SPACE_ID}})
    target = resolve_target(http, SPACE_ID, "Sales")
    assert (target.operation, target.space_id) == ("update", SPACE_ID)
    assert http.methods() == ["GET"]


def test_resolve_target_falls_back_to_a_unique_title_match():
    http = FakeGenie(
        {
            ("GET", f"/{SPACE_ID}"): GenieApiError(404, "gone"),
            ("GET", ""): {
                "spaces": [
                    {"space_id": "other", "title": "Other"},
                    {"space_id": "found", "title": "Sales"},
                ]
            },
        }
    )
    target = resolve_target(http, SPACE_ID, "Sales")
    assert (target.operation, target.space_id) == ("update", "found")


def test_resolve_target_creates_when_nothing_matches():
    http = FakeGenie({("GET", ""): {"spaces": []}})
    target = resolve_target(http, "", "Sales")
    assert (target.operation, target.space_id) == ("create", "")


def test_resolve_target_refuses_an_ambiguous_title():
    http = FakeGenie(
        {
            ("GET", ""): {
                "spaces": [
                    {"space_id": "one", "title": "Sales"},
                    {"space_id": "two", "title": "Sales"},
                ]
            }
        }
    )
    with pytest.raises(SpaceFilesError):
        resolve_target(http, "", "Sales")


def test_build_payload_carries_the_warehouse_and_the_serialized_space():
    payload = build_payload(
        {"title": " Sales ", "description": "d"}, {"version": 2}, "wh-1"
    )
    assert payload == {
        "warehouse_id": "wh-1",
        "title": "Sales",
        "description": "d",
        "serialized_space": '{"version": 2}',
    }
