"""Workspace, dataset, report and tenant ids are one URL path segment each: every request
built from them carries a bearer token or a credential."""

import pytest

from src.services.powerbi import fabric_definition
from src.services.powerbi.ids import (
    execute_queries_url,
    path_id,
    report_definition_url,
    semantic_model_definition_url,
    token_url,
)

GUID = "11111111-2222-3333-4444-555555555555"
ESCAPES = [
    "../../../../v1.0/myorg/groups/other/datasets/other",
    f"{GUID}/../other",
    f"{GUID}?x=1",
    f"{GUID}#frag",
    f"{GUID}%2f..",
    "..",
    "",
    None,
]


@pytest.mark.parametrize("bad", ESCAPES)
def test_an_id_that_would_re_target_the_request_is_refused(bad):
    for build in (
        lambda: path_id(bad),
        lambda: execute_queries_url(bad, GUID),
        lambda: execute_queries_url(GUID, bad or "x/y"),
        lambda: semantic_model_definition_url(GUID, bad),
        lambda: report_definition_url(bad, GUID),
        lambda: token_url(bad),
    ):
        with pytest.raises(ValueError):
            build()


def test_guids_and_tenant_domains_build_the_expected_urls():
    assert execute_queries_url(GUID) == (
        f"https://api.powerbi.com/v1.0/myorg/datasets/{GUID}/executeQueries"
    )
    assert execute_queries_url(GUID, GUID).endswith(
        f"/groups/{GUID}/datasets/{GUID}/executeQueries"
    )
    assert token_url("contoso.onmicrosoft.com").startswith(
        "https://login.microsoftonline.com/contoso.onmicrosoft.com/"
    )


def test_the_definition_call_refuses_a_url_that_is_not_one_workspace_item(monkeypatch):
    """An agent-supplied report id of ../../<other item>/refresh? sent the token's POST there."""
    sent = []
    monkeypatch.setattr(
        fabric_definition.requests, "post", lambda *a, **k: sent.append(a) or None
    )
    evil = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{GUID}/reports/"
        f"../../../workspaces/other/semanticModels/x/refresh?/getDefinition"
    )
    with pytest.raises(ValueError):
        fabric_definition.post_definition(evil, "token", "Report definition")
    assert sent == []


def test_config_generation_rejects_an_id_with_a_path():
    from src.services.tools.pipeline_config_generator_utils.inputs import (
        input_error,
        resolve_inputs,
    )

    inputs = resolve_inputs(
        {"workspace_id": f"{GUID}/../x", "dataset_id": GUID, "access_token": "t"}, {}
    )
    assert "workspace_id" in (input_error(inputs) or "")


def test_reconciliation_dax_escapes_the_names_of_a_mapping():
    """A mapping name with a quote or bracket ended its reference and ran as DAX."""
    from src.services.tools.metric_view_utils.reconciliation import pbi_query

    assert pbi_query._column_ref("It's", "a]b") == "'It''s'[a]]b]"
    assert pbi_query._raw_column_expr("Fact[qty]") == "SUM('Fact'[qty])"
    assert pbi_query._dax_string('Total", SUM(x) //') == '"Total"", SUM(x) //"'
    filt = pbi_query.filter_expression(
        {"table": "Dim'x", "column": "c]d", "values": ["a"]}
    )
    assert "'Dim''x'[c]]d]" in filt and "''''" not in filt
