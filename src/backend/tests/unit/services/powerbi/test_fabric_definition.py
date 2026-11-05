"""Fabric getDefinition: throttling on every call, the 202 operation, the PBIR-Legacy fallback, and the
definition returned by extract_report_definition. Every HTTP call is faked; no time is spent sleeping.
"""

import base64
import json
from unittest.mock import patch

import pytest

from src.services.powerbi import fabric_definition, pipeline_config
from src.services.powerbi import legacy_report

WS = "11111111-2222-3333-4444-555555555555"
RPT = "66666666-7777-8888-9999-000000000000"
DEF_URL = (
    f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/reports/{RPT}/getDefinition"
)
LOCATION = "https://api.fabric.microsoft.com/v1/operations/op1"


class _Resp:
    def __init__(self, status_code=200, body=None, headers=None, text=""):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self.text = text or (json.dumps(body) if body is not None else "")

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


def _b64(content):
    return base64.b64encode(json.dumps(content).encode()).decode()


def _pbir_parts():
    return {
        "definition": {
            "parts": [
                {
                    "path": "definition/report.json",
                    "payload": _b64({"x": 1}),
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }


def _legacy_parts():
    report = {
        "config": "{}",
        "filters": "[]",
        "sections": [
            {
                "name": "p1",
                "displayName": "P",
                "ordinal": 0,
                "config": "{}",
                "filters": "[]",
                "visualContainers": [],
            }
        ],
    }
    return {
        "definition": {
            "parts": [
                {
                    "path": "report.json",
                    "payload": _b64(report),
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }


@pytest.fixture(autouse=True)
def no_sleep():
    with patch("time.sleep"):
        yield


def _queue(monkeypatch, name, responses):
    calls = []

    def fake(*args, **kwargs):
        calls.append((args, kwargs))
        return responses.pop(0)

    monkeypatch.setattr(fabric_definition.requests, name, fake)
    return calls


def test_pbir_definition_is_returned_as_is(monkeypatch):
    _queue(monkeypatch, "post", [_Resp(200, _pbir_parts())])
    parts, legacy = fabric_definition.fetch_report_parts(DEF_URL, "tok")
    assert legacy is False
    assert parts == [
        {
            "path": "definition/report.json",
            "payload": _b64({"x": 1}),
            "payloadType": "InlineBase64",
        }
    ]


def test_failed_pbir_conversion_falls_back_to_the_legacy_definition(monkeypatch):
    failure = _Resp(
        400,
        {"errorCode": "Report_Report_FailedToExportReport", "message": "cannot export"},
    )
    posts = _queue(monkeypatch, "post", [failure, _Resp(200, _legacy_parts())])
    parts, legacy = fabric_definition.fetch_report_parts(DEF_URL, "tok")
    assert legacy is True
    assert posts[0][0][0] == DEF_URL + "?format=PBIR"
    assert posts[1][0][0] == DEF_URL
    assert {p["path"] for p in parts} >= {
        "definition/pages/pages.json",
        "definition/pages/p1/page.json",
    }


def test_a_conversion_failure_reported_by_the_operation_also_falls_back(monkeypatch):
    accepted = _Resp(202, headers={"Location": LOCATION})
    failed = _Resp(
        200,
        {
            "status": "Failed",
            "error": {
                "errorCode": "Report_Report_FailedToExportReport",
                "message": "x",
            },
        },
    )
    _queue(monkeypatch, "post", [accepted, _Resp(200, _legacy_parts())])
    _queue(monkeypatch, "get", [failed])
    _, legacy = fabric_definition.fetch_report_parts(DEF_URL, "tok")
    assert legacy is True


def test_other_failures_propagate(monkeypatch):
    _queue(monkeypatch, "post", [_Resp(403, {"errorCode": "Unauthorized"})])
    with pytest.raises(fabric_definition.FabricDefinitionError, match="HTTP 403"):
        fabric_definition.fetch_report_parts(DEF_URL, "tok")


def test_operation_is_polled_and_its_result_read(monkeypatch):
    _queue(monkeypatch, "post", [_Resp(202, headers={"Location": LOCATION})])
    gets = _queue(
        monkeypatch,
        "get",
        [
            _Resp(200, {"status": "Running"}),
            _Resp(200, {"status": "Succeeded"}),
            _Resp(200, _pbir_parts()),
        ],
    )
    parts, legacy = fabric_definition.fetch_report_parts(DEF_URL, "tok")
    assert legacy is False and len(parts) == 1
    assert [c[0][0] for c in gets] == [LOCATION, LOCATION, LOCATION + "/result"]


def test_untrusted_location_is_never_polled(monkeypatch):
    _queue(
        monkeypatch,
        "post",
        [_Resp(202, headers={"Location": "https://evil.example.com/op"})],
    )
    gets = _queue(monkeypatch, "get", [])
    with pytest.raises(
        fabric_definition.FabricDefinitionError, match="trusted Location"
    ):
        fabric_definition.post_definition(DEF_URL, "tok", "Report definition")
    assert gets == []


def test_throttled_definition_call_is_retried_after_retry_after(monkeypatch):
    posts = _queue(
        monkeypatch,
        "post",
        [_Resp(429, headers={"Retry-After": "3"}), _Resp(200, _pbir_parts())],
    )
    with patch("time.sleep") as sleep:
        fabric_definition.post_definition(DEF_URL, "tok", "Report definition")
    assert len(posts) == 2
    assert sleep.call_args.args[0] == 4.0


def test_extract_report_definition_decodes_parts_and_warns_on_legacy_fallback(
    monkeypatch,
):
    failure = _Resp(400, {"errorCode": "Report_Report_FailedToExportReport"})
    _queue(monkeypatch, "post", [failure, _Resp(200, _legacy_parts())])
    result = pipeline_config.extract_report_definition("tok", WS, RPT)
    paths = {p["path"]: p["payload"] for p in result["definition"]["parts"]}
    assert paths["definition/pages/p1/page.json"]["name"] == "p1"
    assert paths["definition/pages/pages.json"]["pageOrder"] == ["p1"]
    assert result["warnings"] and "legacy" in result["warnings"][0]


def test_extract_report_definition_has_no_warning_for_pbir(monkeypatch):
    _queue(monkeypatch, "post", [_Resp(200, _pbir_parts())])
    result = pipeline_config.extract_report_definition("tok", WS, RPT)
    assert result["warnings"] == []
    assert result["definition"]["parts"][0]["payload"] == {"x": 1}


def test_execute_queries_is_retried_on_throttling(monkeypatch):
    posts = _queue(
        monkeypatch,
        "post",
        [_Resp(429, headers={"Retry-After": "1"}), _Resp(200, {"results": []})],
    )
    resp = pipeline_config._execute_queries("tok", WS, RPT, "EVALUATE {1}")
    assert resp.status_code == 200
    assert len(posts) == 2


def test_legacy_converter_is_loaded_for_the_standalone_cli():
    # The standalone CLI loads pipeline_config (and so this module) by file path; it must still import.
    assert legacy_report.CONVERSION_FAILURE == "FailedToExportReport"
    assert fabric_definition.FabricDefinitionError.__mro__[1] is RuntimeError
