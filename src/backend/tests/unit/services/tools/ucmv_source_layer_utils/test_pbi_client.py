"""Power BI client helpers: key cleaning, DAX quoting, GUID checks, refresh choice."""

import pytest

from src.services.tools.ucmv_source_layer_utils import pbi_client
from src.services.tools.ucmv_source_layer_utils.pbi_client import (
    PbiClient,
    bare_key,
    dax_column,
    dax_table,
    dax_value,
)

GUID = "11111111-2222-3333-4444-555555555555"


def test_bare_key_strips_table_qualifier():
    assert bare_key("Sales[Amount]") == "Amount"
    assert bare_key("[n]") == "n"
    assert bare_key("plain") == "plain"


def test_dax_quoting_escapes_quotes_and_brackets():
    assert dax_table("Excel's Table") == "'Excel''s Table'"
    assert dax_column("T", "a]b") == "'T'[a]]b]"
    assert dax_value(None) == "BLANK()"
    assert dax_value(True) == "TRUE()"
    assert dax_value('say "hi"') == '"say ""hi"""'


def test_client_rejects_non_guid_ids():
    with pytest.raises(ValueError, match="workspace_id must be a GUID"):
        PbiClient("token", "not-a-guid", GUID)
    with pytest.raises(ValueError, match="dataset_id must be a GUID"):
        PbiClient("token", GUID, "x")


class _FakeClient(PbiClient):
    def __init__(self, responses):
        super().__init__("token", GUID, GUID)
        self._responses = responses
        self.calls = []

    def _refreshes(self, top):
        self.calls.append(top)
        return self._responses[top]


def test_last_completed_refresh_uses_newest_when_completed():
    client = _FakeClient(
        {1: [{"status": "Completed", "endTime": "2026-11-04T09:00:00Z"}], 20: []}
    )
    assert client.last_completed_refresh() == "2026-11-04T09:00:00Z"
    assert client.calls == [1]


def test_last_completed_refresh_looks_back_when_newest_failed():
    client = _FakeClient(
        {
            1: [{"status": "Failed", "endTime": "2026-11-05T09:00:00Z"}],
            20: [
                {"status": "Failed", "endTime": "2026-11-05T09:00:00Z"},
                {"status": "Completed", "endTime": "2026-11-03T08:00:00Z"},
                {"status": "Completed", "endTime": "2026-11-02T08:00:00Z"},
            ],
        }
    )
    assert client.last_completed_refresh() == "2026-11-03T08:00:00Z"


def test_last_completed_refresh_none_when_never_completed():
    client = _FakeClient({1: [], 20: [{"status": "Failed"}]})
    assert client.last_completed_refresh() is None


def test_clean_rows_renames_keys():
    rows = pbi_client.clean_rows([{"Sales[Month]": "2024-01-01", "[n]": 3}])
    assert rows == [{"Month": "2024-01-01", "n": 3}]


def test_executequeries_waits_out_throttling_then_returns_rows(monkeypatch):
    class _Resp:
        def __init__(self, status_code, body=None, headers=None):
            self.status_code, self._body, self.headers = status_code, body or {}, headers or {}
            self.text = ""

        def json(self):
            return self._body

    responses = [
        _Resp(429, headers={"Retry-After": "2"}),
        _Resp(200, {"results": [{"tables": [{"rows": [{"[Amount]": 3}]}]}]}),
    ]
    sleeps = []

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def post(self, *args, **kwargs):
            return responses.pop(0)

    monkeypatch.setattr(pbi_client.httpx, "Client", _Client)
    monkeypatch.setattr("src.services.powerbi.http_retry.time.sleep", sleeps.append)
    client = PbiClient("token", GUID, GUID)
    assert client.dax("EVALUATE {1}") == [{"Amount": 3}]
    assert sleeps == [3.0]
