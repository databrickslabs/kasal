"""Fabric ``getDefinition`` calls: the POST, its long-running operation, and the PBIR-Legacy fallback.

Synchronous (``requests``), like ``pipeline_config``; the async callers run it in a worker thread. Every request
goes through the shared throttling retry (``http_retry``). A report that the service cannot convert to PBIR
(``FailedToExportReport``) is read in its legacy shape and converted locally (``legacy_report``).

Loaded by ``pipeline_config`` both as a package module and, for the standalone CLI, by file path: the sibling
imports below mirror ``pipeline_config``'s two branches.
"""

from __future__ import annotations

import logging
import time
from types import ModuleType
from urllib.parse import urlparse

import requests

if __package__:
    from . import legacy_report as _legacy_module
    from .http_retry import send_with_throttle_retry as _send_with_retry

    _legacy: ModuleType = _legacy_module
else:
    import importlib.util as _ilu
    from pathlib import Path as _Path

    def _load_sibling(mod_name: str, filename: str) -> ModuleType:
        spec = _ilu.spec_from_file_location(
            mod_name, _Path(__file__).resolve().parent / filename
        )
        if spec is None or spec.loader is None:
            raise ImportError(filename)
        module = _ilu.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    _legacy = _load_sibling("kasal_legacy_report_cli", "legacy_report.py")
    _send_with_retry = _load_sibling(
        "kasal_http_retry_cli", "http_retry.py"
    ).send_with_throttle_retry

logger = logging.getLogger(__name__)

_FABRIC_TRUSTED_SUFFIX = ".fabric.microsoft.com"
_OPERATION_POLLS = 60
_POLL_INTERVAL_S = 2
_ERROR_TEXT_CHARS = 400


class FabricDefinitionError(RuntimeError):
    """A getDefinition call, or its long-running operation, did not succeed. The message carries the
    service's error code when it gave one (``Report_Report_FailedToExportReport``)."""


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _error_text(response: requests.Response) -> str:
    try:
        body: object = response.json()
    except ValueError:
        body = response.text
    return str(body)[:_ERROR_TEXT_CHARS]


def _definition_parts(response: requests.Response) -> list[dict[str, object]]:
    data: object = response.json()
    definition = data.get("definition") if isinstance(data, dict) else None
    parts = definition.get("parts") if isinstance(definition, dict) else None
    return (
        [part for part in parts if isinstance(part, dict)]
        if isinstance(parts, list)
        else []
    )


def trusted_location(location: str) -> bool:
    """The operation poll carries the Fabric token, so its Location must be https on a Fabric host with no
    userinfo or port."""
    parsed = urlparse(location)
    host = parsed.hostname or ""
    return (
        parsed.scheme == "https"
        and host.endswith(_FABRIC_TRUSTED_SUFFIX)
        and parsed.netloc.lower() == host
    )


def follow_operation(
    accepted: requests.Response, headers: dict[str, str], poll_timeout: int, label: str
) -> requests.Response:
    """Poll a Fabric 202 operation to its ``/result`` (a 200 response).

    Raises :class:`FabricDefinitionError` when the Location is not trusted, the operation fails (with the
    service's error code) or it does not finish in time.
    """
    location = accepted.headers.get("Location") or ""
    if not trusted_location(location):
        raise FabricDefinitionError(f"{label}: 202 without a trusted Location")
    for attempt in range(_OPERATION_POLLS):
        time.sleep(_POLL_INTERVAL_S)
        poll = _send_with_retry(
            lambda: requests.get(location, headers=headers, timeout=poll_timeout),  # Location pinned to https Fabric above — nosemgrep: tmp.databricks.ssrf.requests-user-url  # fmt: skip
            f"{label} poll",
        ).json()
        status = poll.get("status")
        if status == "Succeeded":
            logger.info("%s: succeeded after %d poll(s)", label, attempt + 1)
            result = _send_with_retry(
                lambda: requests.get(location + "/result", headers=headers, timeout=60),  # Location pinned to https Fabric above — nosemgrep: tmp.databricks.ssrf.requests-user-url  # fmt: skip
                f"{label} result",
            )
            if result.status_code != 200:
                raise FabricDefinitionError(
                    f"{label} result HTTP {result.status_code}: {_error_text(result)}"
                )
            return result
        if status == "Failed":
            error = poll.get("error") or {}
            code = error.get("errorCode", "") if isinstance(error, dict) else ""
            message = error.get("message", "") if isinstance(error, dict) else ""
            raise FabricDefinitionError(
                f"{label} operation failed: {code}: {message}".rstrip(": ")
            )
    raise FabricDefinitionError(
        f"{label}: did not finish after {_OPERATION_POLLS * _POLL_INTERVAL_S}s"
    )


def post_definition(
    url: str, token: str, label: str, poll_timeout: int = 30
) -> requests.Response:
    """POST a getDefinition call and return its 200 response, following the 202 operation when there is one."""
    headers = _headers(token)
    response = _send_with_retry(lambda: requests.post(url, headers=headers, timeout=120), label)  # fixed Fabric host, ids validated by the caller — nosemgrep: tmp.databricks.ssrf.requests-user-url  # fmt: skip
    if response.status_code == 202:
        return follow_operation(response, headers, poll_timeout, label)
    if response.status_code != 200:
        raise FabricDefinitionError(
            f"{label} HTTP {response.status_code}: {_error_text(response)}"
        )
    return response


def fetch_report_parts(
    definition_url: str, token: str
) -> tuple[list[dict[str, object]], bool]:
    """Raw PBIR parts (base64 payloads) of a report, and whether the PBIR-Legacy fallback was used.

    ``definition_url`` is the ``.../reports/{id}/getDefinition`` URL without a query. Tries ``format=PBIR``
    first; only a failed conversion (``FailedToExportReport``) falls back to the legacy definition, which is
    converted locally. Any other failure propagates.
    """
    try:
        parts = _definition_parts(
            post_definition(definition_url + "?format=PBIR", token, "Report definition")
        )
        return parts, False
    except FabricDefinitionError as error:
        if not _legacy.is_conversion_failure(str(error)):
            raise
    logger.warning(
        "Report definition: PBIR conversion failed; using the converted legacy definition"
    )
    legacy_parts = _definition_parts(
        post_definition(definition_url, token, "Report definition (legacy)")
    )
    converted: list[dict[str, object]] = _legacy.to_pbir_parts(legacy_parts)
    return converted, True
