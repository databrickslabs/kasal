"""pipeline_config builds Power BI / Fabric / Entra URLs only from valid ids, and
turns DAX filter strings into escaped SQL literals.

Apps security scan follow-up (2026-10-28):

* The Pipeline Config Generator tool passes the agent's workspace / dataset /
  report ids straight here, and they went into REST paths unchecked — a "/",
  "?", "#" or ".." re-targeted the request that carries the bearer token.
* ``_resolve_referenced_measure_dax`` wrapped a DAX string in single quotes raw,
  so ``x' OR '1'='1`` became a forged predicate in deployed metric-view SQL.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.services.powerbi import pipeline_config as gc

GUID = "8f1c3a2e-5b6d-4e7f-9a0b-1c2d3e4f5a6b"


class TestPathIds:
    @pytest.mark.parametrize("value", [GUID, "contoso.onmicrosoft.com", "ab-12"])
    def test_guids_and_tenant_domains_pass(self, value):
        assert gc._pbi_id(value) == value

    @pytest.mark.parametrize(
        "value", ["../admin", "ws/x", "ws?x=1", "ws#f", "a..b", ".", "", "ws%2F"]
    )
    def test_anything_that_could_retarget_the_request_is_refused(self, value):
        with pytest.raises(ValueError):
            gc._pbi_id(value)

    def test_a_crafted_dataset_id_never_reaches_the_request(self):
        with patch("requests.post") as post, pytest.raises(ValueError):
            gc.extract_relationships("tok", GUID, "../../admin/groups")
        post.assert_not_called()

    def test_a_crafted_tenant_never_reaches_the_token_endpoint(self):
        with patch("requests.post") as post, pytest.raises(ValueError):
            gc.get_token("evil.example.com/x?", "client", "secret")
        post.assert_not_called()

    def test_execute_queries_posts_to_the_dataset_endpoint(self):
        ok = MagicMock(status_code=200)
        ok.json.return_value = {"results": [{"tables": [{"rows": []}]}]}
        with patch("requests.post", return_value=ok) as post:
            gc.extract_relationships("tok", GUID, GUID)
        assert post.call_args.args[0] == (
            f"https://api.powerbi.com/v1.0/myorg/groups/{GUID}/datasets/{GUID}"
            "/executeQueries"
        )
        body = post.call_args.kwargs["json"]
        assert body["queries"] == [{"query": "EVALUATE INFO.VIEW.RELATIONSHIPS()"}]


class TestDaxFilterLiterals:
    def test_a_quote_in_a_dax_string_stays_in_the_literal(self):
        res = gc._resolve_referenced_measure_dax(
            "CALCULATE(SUM(Sales[amount]), Sales[region] = \"x' OR '1'='1\")"
        )
        assert res["base_filters"] == ["region = 'x'' OR ''1''=''1'"]

    def test_a_trailing_backslash_cannot_escape_the_closing_quote(self):
        res = gc._resolve_referenced_measure_dax(
            'CALCULATE(SUM(Sales[amount]), Sales[path] = "C:\\\\")'
        )
        assert res["base_filters"] == ["path = 'C:\\\\\\\\'"]
