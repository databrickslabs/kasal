"""Power BI / Fabric requests carrying a bearer token go only where intended.

Apps security scan (2026-10-28): the Fabric 202 ``Location`` was polled with the
Fabric token and no host check, and the Power BI connector put agent-supplied
workspace / dataset ids into its REST path unchecked, so a ``/``, ``?`` or
``..`` re-targeted the request that carries the token.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.services.converters.formats.powerbi.connector import PowerBIConnector
from src.services.powerbi import pipeline_config

GUID = "8f1c3a2e-5b6d-4e7f-9a0b-1c2d3e4f5a6b"
FORGED = "https://attacker.example.com/v1/operations/1"
TRUSTED = "https://api.fabric.microsoft.com/v1/operations/123"


def _accepted(location):
    resp = MagicMock(status_code=202)
    resp.headers = {"Location": location}
    return resp


def _succeeded():
    poll = MagicMock(status_code=200)
    poll.json.return_value = {"status": "Succeeded", "definition": {"parts": []}}
    return poll


# Both Fabric polling sites, each as (call, value it returns when refused).
SITES = {
    "tmdl": lambda: pipeline_config.fetch_tmdl_parts("tok", GUID, GUID),
    "report": lambda: pipeline_config.extract_report_definition("tok", GUID, GUID),
}


class TestFabricPollingFollowsOnlyFabric:
    @pytest.mark.parametrize("site", SITES)
    @pytest.mark.parametrize(
        "location",
        [
            None,
            FORGED,
            "http://api.fabric.microsoft.com/v1/operations/1",
            "https://api.fabric.microsoft.com.attacker.example.com/x",
            "https://user:pw@api.fabric.microsoft.com/x",
            "https://api.fabric.microsoft.com:8443/x",
            "https://169.254.169.254/latest/meta-data",
        ],
    )
    def test_an_untrusted_location_is_never_polled(self, site, location):
        with (
            patch("requests.post", return_value=_accepted(location)),
            patch("requests.get") as get,
            patch("time.sleep"),
        ):
            assert SITES[site]() is None
        get.assert_not_called()

    @pytest.mark.parametrize("site", SITES)
    def test_a_fabric_location_is_polled_to_its_result(self, site):
        with (
            patch("requests.post", return_value=_accepted(TRUSTED)),
            patch("requests.get", return_value=_succeeded()) as get,
            patch("time.sleep"),
        ):
            SITES[site]()
        assert [c.args[0] for c in get.call_args_list] == [TRUSTED, TRUSTED + "/result"]


class TestConnectorPathIds:
    def _connector(self, group_id, dataset_id):
        connector = PowerBIConnector(
            semantic_model_id=dataset_id, group_id=group_id, access_token="tok"
        )
        connector._connected = True
        connector._access_token = "tok"
        return connector

    @pytest.mark.parametrize(
        "group_id", ["../admin", "ws/../../x", "ws?x=1", "ws#frag", "ws%2Fx", "a.b"]
    )
    def test_a_crafted_id_is_refused_before_the_request(self, group_id):
        connector = self._connector(group_id, GUID)
        with patch("requests.post") as post, pytest.raises(ValueError):
            connector._execute_dax_query('EVALUATE ROW("x", 1)')
        post.assert_not_called()

    def test_a_guid_reaches_the_executequeries_endpoint(self):
        connector = self._connector(GUID, GUID)
        ok = MagicMock(status_code=200)
        ok.json.return_value = {"results": []}
        with patch("requests.post", return_value=ok) as post:
            connector._execute_dax_query('EVALUATE ROW("x", 1)')
        url = post.call_args.args[0]
        assert url == (
            f"https://api.powerbi.com/v1.0/myorg/groups/{GUID}/datasets/{GUID}"
            "/executeQueries"
        )
