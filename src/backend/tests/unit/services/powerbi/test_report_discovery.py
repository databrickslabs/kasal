"""Report discovery: the classic listing is throttled-retried, and the standalone CLI can load the module."""

import importlib.util
import pathlib
from unittest.mock import patch

from src.services.powerbi import report_discovery

WS = "11111111-2222-3333-4444-555555555555"
DS = "aaaaaaaa-2222-3333-4444-555555555555"


class _Resp:
    def __init__(self, status_code, body=None, headers=None):
        self.status_code, self._body, self.headers = status_code, body, headers or {}
        self.text = ""

    def json(self):
        return self._body


def test_listing_is_retried_on_throttling_and_the_match_is_returned():
    responses = [
        _Resp(429, headers={"Retry-After": "1"}),
        _Resp(200, {"value": [{"id": "r1", "name": "Sales", "datasetId": DS}]}),
    ]
    with (
        patch("requests.get", side_effect=lambda *a, **k: responses.pop(0)) as get,
        patch("src.services.powerbi.http_retry.time.sleep") as sleep,
    ):
        assert report_discovery.discover_report_id("tok", WS, DS) == "r1"
    assert get.call_count == 2
    sleep.assert_called_once_with(2.0)


def test_scan_result_match_needs_no_http_call():
    scan = {"workspaces": [{"reports": [{"id": "r9", "name": "X", "datasetId": DS}]}]}
    with patch("requests.get") as get:
        assert report_discovery.discover_report_id("tok", WS, DS, scan) == "r9"
    get.assert_not_called()


def test_the_standalone_loader_can_load_report_discovery_by_path():
    path = pathlib.Path(report_discovery.__file__)
    spec = importlib.util.spec_from_file_location("kasal_report_discovery_check", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module.send_with_throttle_retry)
