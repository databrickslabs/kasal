"""Genie Spaces REST calls, behind an injected HTTP callable.

``requests_http`` is the only piece that touches the network. Everything else
takes a ``GenieHttp`` so the lookup, pagination and error rules are unit-tested
without a workspace. Paths are relative to ``/api/2.0/genie/spaces``.
"""

from __future__ import annotations

from collections.abc import Callable

import requests

from src.utils.telemetry import KasalProduct, get_user_agent_header

GENIE_SPACES_PATH = "/api/2.0/genie/spaces"
_TIMEOUT_SECONDS = 60
_MAX_PAGES = 100

#: (method, path relative to the spaces root, JSON body, query params) -> JSON object.
GenieHttp = Callable[
    [str, str, dict[str, object] | None, dict[str, str] | None], dict[str, object]
]


class GenieApiError(RuntimeError):
    """A Genie call failed; ``status`` is the HTTP status (0 when no response)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"[{status}] {message}")
        self.status = status


def _as_dict(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise GenieApiError(0, "Genie returned a non-object JSON body")
    return {str(key): item for key, item in value.items()}


def requests_http(host: str, headers: dict[str, str]) -> GenieHttp:
    """A ``GenieHttp`` bound to one workspace host (already trust-checked by the caller)."""
    base = host.rstrip("/")
    request_headers = {
        **headers,
        "Content-Type": "application/json",
        **get_user_agent_header(KasalProduct.GENIE),
    }

    def _call(
        method: str,
        path: str,
        body: dict[str, object] | None,
        params: dict[str, str] | None,
    ) -> dict[str, object]:
        url = f"{base}{GENIE_SPACES_PATH}{path}"
        try:
            response = requests.request(  # host trust-checked by the tool
                method,
                url,
                headers=request_headers,
                json=body,
                params=params,
                timeout=_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            raise GenieApiError(
                0, f"{method} {path or '/'} failed: {type(exc).__name__}"
            ) from exc
        if response.status_code >= 400:
            raise GenieApiError(
                response.status_code,
                f"{method} {path or '/'}: {response.text[:300]}",
            )
        return _as_dict(response.json() if response.content else {})

    return _call


def list_spaces(http: GenieHttp) -> list[dict[str, object]]:
    """Every space the caller can see, following ``next_page_token``."""
    spaces: list[dict[str, object]] = []
    token = ""
    for _ in range(_MAX_PAGES):
        page = http("GET", "", None, {"page_token": token} if token else None)
        items = page.get("spaces")
        if isinstance(items, list):
            spaces.extend(item for item in items if isinstance(item, dict))
        next_token = page.get("next_page_token")
        if not isinstance(next_token, str) or not next_token:
            return spaces
        token = next_token
    raise GenieApiError(0, f"more than {_MAX_PAGES} pages of Genie spaces")


def get_space(
    http: GenieHttp, space_id: str, *, serialized: bool = True
) -> dict[str, object] | None:
    """A space, or None when it does not exist. Other errors (403 included) raise.

    A 403 is raised rather than read as "missing": treating it as missing would
    let a deploy create a duplicate space the caller is not allowed to see.
    """
    params = {"include_serialized_space": "true"} if serialized else None
    try:
        return http("GET", f"/{space_id}", None, params)
    except GenieApiError as exc:
        if exc.status == 404:
            return None
        raise


def create_space(http: GenieHttp, payload: dict[str, object]) -> dict[str, object]:
    return http("POST", "", payload, None)


def update_space(
    http: GenieHttp, space_id: str, payload: dict[str, object]
) -> dict[str, object]:
    return http("PATCH", f"/{space_id}", payload, None)


def space_url(host: str, space_id: str) -> str:
    return f"{host.rstrip('/')}/genie/rooms/{space_id}"
