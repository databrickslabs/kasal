"""Credentials only go where they belong (security review of PR #76).

* The source-layer PBI validation sends the run's Power BI token, so its target
  passes the same allowlist as extraction (https + Power BI host + UUID ids).
* The reconciliation tool's ``databricks_host`` may only name the credential's
  own workspace: its SQL request carries that credential's bearer token.
* Cloning a crew copies only agents and tasks of the caller's workspace.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.exceptions import ForbiddenError
from src.services.tools.uc_metric_view_generator_utils.pbi_api_extraction import (
    validate_pbi_inputs,
)
from src.utils.databricks_auth import AuthContext

WS = "00000000-0000-0000-0000-000000000001"
DS = "00000000-0000-0000-0000-000000000002"


@pytest.mark.parametrize(
    "base_url, ok",
    [
        ("https://api.powerbi.com/v1.0/myorg", True),
        ("", True),  # the default host
        ("http://api.powerbi.com/v1.0/myorg", False),
        ("https://attacker.example/v1.0/myorg", False),
        ("https://api.powerbi.com.attacker.example", False),
    ],
)
def test_the_pbi_target_must_be_an_https_power_bi_host(base_url, ok):
    assert validate_pbi_inputs(WS, DS, base_url)[0] is ok


def test_pbi_validation_never_calls_an_untrusted_host():
    from src.services.tools.uc_metric_view_generator_utils import source_layer_steps

    with (
        patch(
            "src.services.tools.metric_view_utils.source_artifacts.build_pbi_evaluate_fn"
        ) as build,
        patch(
            "src.services.tools.metric_view_utils.source_artifacts.run_pbi_validation",
            return_value={"status": "skipped"},
        ),
    ):
        source_layer_steps.build_source_layer_outputs(
            pipeline=MagicMock(all_specs=[]),
            mquery_tables={},
            config={},
            catalog="main",
            schema="s",
            yaml_output={},
            validate_against_pbi=True,
            access_token="pbi-token",
            workspace_id=WS,
            dataset_id=DS,
            pbi_api_base_url="https://attacker.example/v1.0/myorg",
        )
    build.assert_not_called()


def _auth():
    return AuthContext(
        token="tok",
        workspace_url="https://victim.cloud.databricks.com",
        auth_method="pat",
    )


@pytest.mark.parametrize(
    "override, expected",
    [
        (None, "https://victim.cloud.databricks.com"),
        ("victim.cloud.databricks.com", "https://victim.cloud.databricks.com"),
    ],
)
def test_reconciliation_keeps_the_credentials_own_workspace(override, expected):
    from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

    with patch(
        "src.utils.databricks_auth.get_auth_context",
        new=AsyncMock(return_value=_auth()),
    ):
        auth = UCMVReconciliationTool._authenticate_databricks(override)
    assert auth.workspace_url == expected


def test_reconciliation_refuses_another_workspace_host():
    from src.services.tools.ucmv_reconciliation_tool import UCMVReconciliationTool

    with (
        patch(
            "src.utils.databricks_auth.get_auth_context",
            new=AsyncMock(return_value=_auth()),
        ),
        pytest.raises(ForbiddenError),
    ):
        UCMVReconciliationTool._authenticate_databricks(
            "https://attacker.cloud.databricks.com"
        )


@pytest.mark.asyncio
async def test_clone_copies_no_agent_or_task_of_another_workspace():
    from src.services.catalog.crews import CrewService

    svc = CrewService(session=MagicMock())
    source = SimpleNamespace(
        name="Imported",
        group_id="ws-a",
        agent_ids=["agent-of-ws-b"],
        task_ids=["task-of-ws-b"],
        nodes=[],
        edges=[],
        process="sequential",
        reasoning=False,
        reasoning_llm=None,
        reasoning_config=None,
        manager_llm=None,
        tool_configs={},
        memory=False,
        verbose=False,
        max_rpm=None,
    )
    svc.repository = MagicMock(
        get_by_group=AsyncMock(return_value=source),
        find_by_name_and_group=AsyncMock(return_value=None),
    )
    svc.create_with_group = AsyncMock(return_value=SimpleNamespace(id="new"))
    context = SimpleNamespace(primary_group_id="ws-a", group_ids=["ws-a"])

    with (
        patch("src.services.catalog.agents.AgentService") as agents,
        patch("src.services.catalog.tasks.TaskService") as tasks,
    ):
        # The workspace-checked reads refuse the other workspace's rows.
        agents.return_value.get_with_group_check = AsyncMock(return_value=None)
        tasks.return_value.get_with_group_check = AsyncMock(return_value=None)
        agents.return_value.create_with_group = AsyncMock()
        tasks.return_value.create_with_group = AsyncMock()
        await svc.clone_with_group("crew-id", "Copy", context)

    agents.return_value.get_with_group_check.assert_awaited_once_with(
        "agent-of-ws-b", context
    )
    agents.return_value.create_with_group.assert_not_awaited()
    tasks.return_value.create_with_group.assert_not_awaited()
