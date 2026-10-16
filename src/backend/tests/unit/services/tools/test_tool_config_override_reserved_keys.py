"""A per-agent, per-task or run ``tool_configs`` entry is merged over the
teamspace tool config, but it may not choose who the tool runs as, where a
teamspace key is sent, or whether the teamspace's approval gate applies.

Everything else in an override still applies: the task forms write Power BI
ids, secrets and hosts there, and saved workflows depend on them.
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.tools.tool_factory import ToolFactory

RUN_CONFIG = {"group_id": "team-a", "user_email": "runner@example.com"}


def _factory(title, base_config=None, cls=None, user_token="run-token"):
    factory = ToolFactory(config=dict(RUN_CONFIG), user_token=user_token)
    info = MagicMock()
    info.title = title
    info.id = 99
    info.config = base_config if base_config is not None else {}
    factory._available_tools[title] = info
    ctor = cls or MagicMock(return_value=MagicMock())
    factory._tool_implementations[title] = ctor
    return factory, ctor


class _RealTool:
    """A plain class: a MagicMock invents every attribute, so it cannot tell a
    stamped policy from an absent one."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


class TestKnowledgeToolIdentity:
    def _build(self, override, base_config=None):
        factory, ctor = _factory("DatabricksKnowledgeSearchTool", base_config)
        with (
            patch(
                "src.services.tools.tool_factory.DatabricksKnowledgeSearchTool", ctor
            ),
            patch.object(factory, "_run_in_new_loop", return_value=(True, "ok")),
            patch.object(
                factory, "_validate_databricks_auth", AsyncMock(return_value=(True, ""))
            ),
        ):
            factory.create_tool(
                "DatabricksKnowledgeSearchTool", tool_config_override=override
            )
        return ctor.call_args.kwargs

    def test_an_override_cannot_change_the_group_or_the_user(self):
        kwargs = self._build(
            {
                "group_id": "team-b",
                "user_email": None,
                "user_token": "someone-else",
                "file_paths": ["uploads/team-a/doc.pdf"],
            }
        )

        assert kwargs["group_id"] == "team-a"
        assert kwargs["user_email"] == "runner@example.com"
        assert kwargs["user_token"] == "run-token"
        # The documented keys still apply.
        assert kwargs["file_paths"] == ["uploads/team-a/doc.pdf"]

    def test_a_colleagues_email_is_not_accepted_either(self):
        kwargs = self._build({"user_email": "colleague@example.com"})

        assert kwargs["user_email"] == "runner@example.com"

    def test_identity_wins_even_over_the_teamspace_config(self):
        """The run's identity is set last, after any config."""
        kwargs = self._build({}, base_config={"group_id": "team-b"})

        assert kwargs["group_id"] == "team-a"


class TestTokenIdentity:
    def test_a_jobs_override_cannot_supply_another_token(self):
        factory, ctor = _factory("DatabricksJobsTool")
        with (
            patch("src.utils.user_context.UserContext") as user_context,
            patch(
                "src.utils.databricks_auth.get_auth_context",
                side_effect=Exception("no auth in unit tests"),
            ),
        ):
            user_context.get_user_token.return_value = None
            factory.create_tool(
                "DatabricksJobsTool",
                tool_config_override={"user_token": "someone-else"},
            )

        assert ctor.call_args.kwargs["user_token"] == "run-token"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


class TestEndpointsStayWhereTheTeamspacePutThem:
    def test_serper_base_url_cannot_be_overridden(self):
        factory, ctor = _factory(
            "SerperDevTool", base_config={"base_url": "https://serper.example.com"}
        )
        with patch.object(factory, "_get_api_key", return_value="teamspace-key"):
            factory.create_tool(
                "SerperDevTool",
                tool_config_override={
                    "base_url": "https://elsewhere.example.net",
                    "n_results": 3,
                },
            )

        kwargs = ctor.call_args.kwargs
        assert kwargs["base_url"] == "https://serper.example.com"
        assert kwargs["api_key"] == "teamspace-key"
        assert kwargs["n_results"] == 3

    def test_serper_without_a_teamspace_base_url_keeps_the_default(self):
        factory, ctor = _factory("SerperDevTool")
        with patch.object(factory, "_get_api_key", return_value="teamspace-key"):
            factory.create_tool(
                "SerperDevTool",
                tool_config_override={"base_url": "https://elsewhere.example.net"},
            )

        assert "base_url" not in ctor.call_args.kwargs

    def test_image_generation_base_url_cannot_be_overridden(self):
        factory, ctor = _factory(
            "Image Generation Tool",
            base_config={"base_url": "https://images.example.com/v1"},
        )
        with patch.object(factory, "_get_api_key", return_value="teamspace-key"):
            factory.create_tool(
                "Image Generation Tool",
                tool_config_override={
                    "base_url": "https://elsewhere.example.net/v1",
                    "model": "some-model",
                },
            )

        kwargs = ctor.call_args.kwargs
        assert kwargs["base_url"] == "https://images.example.com/v1"
        assert kwargs["api_key"] == "teamspace-key"
        assert kwargs["model"] == "some-model"


TEAMSPACE_LLM_AND_SQL = {
    "llm_workspace_url": "https://ws.example.com",
    "llm_token": "teamspace-llm-token",
    "databricks_sql_endpoint": "https://ws.example.com/api/2.0/mcp/sql",
    "databricks_pat": "teamspace-pat",
}


class TestATeamspaceCredentialIsNotSentToAnOverrideHost:
    """The teamspace holds the credential, the override names the host: the
    credential stays behind, and the tool falls back to the run's own
    credential (same workspace only) or does without."""

    def test_power_bi_analysis_llm_token(self):
        factory, ctor = _factory(
            "Power BI Comprehensive Analysis Tool", dict(TEAMSPACE_LLM_AND_SQL)
        )

        factory.create_tool(
            "Power BI Comprehensive Analysis Tool",
            tool_config_override={"llm_workspace_url": "https://elsewhere.example.net"},
        )

        kwargs = ctor.call_args.kwargs
        assert kwargs["llm_workspace_url"] == "https://elsewhere.example.net"
        assert not kwargs.get("llm_token")

    def test_mquery_sql_endpoint_pat(self):
        factory, ctor = _factory(
            "M-Query Conversion Pipeline", dict(TEAMSPACE_LLM_AND_SQL)
        )

        factory.create_tool(
            "M-Query Conversion Pipeline",
            tool_config_override={
                "databricks_sql_endpoint": "https://elsewhere.example.net/api/2.0/mcp/sql"
            },
        )

        kwargs = ctor.call_args.kwargs
        assert kwargs["databricks_sql_endpoint"].startswith(
            "https://elsewhere.example.net"
        )
        assert not kwargs.get("databricks_pat")
        assert kwargs["llm_token"] == "teamspace-llm-token"  # its host did not move

    def test_mquery_llm_token(self):
        factory, ctor = _factory(
            "M-Query Conversion Pipeline", dict(TEAMSPACE_LLM_AND_SQL)
        )

        factory.create_tool(
            "M-Query Conversion Pipeline",
            tool_config_override={"llm_workspace_url": "https://elsewhere.example.net"},
        )

        assert not ctor.call_args.kwargs.get("llm_token")

    def test_a_generic_tool_llm_token(self):
        factory, ctor = _factory(
            "UC Metric View Generator", dict(TEAMSPACE_LLM_AND_SQL)
        )

        factory.create_tool(
            "UC Metric View Generator",
            tool_config_override={"llm_workspace_url": "https://elsewhere.example.net"},
        )

        assert not ctor.call_args.kwargs.get("llm_token")

    def test_a_task_that_brings_its_own_token_keeps_it(self):
        factory, ctor = _factory(
            "Power BI Comprehensive Analysis Tool", dict(TEAMSPACE_LLM_AND_SQL)
        )

        factory.create_tool(
            "Power BI Comprehensive Analysis Tool",
            tool_config_override={
                "llm_workspace_url": "https://elsewhere.example.net",
                "llm_token": "task-token",
            },
        )

        assert ctor.call_args.kwargs["llm_token"] == "task-token"


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------


class TestTheTeamspaceApprovalGateHolds:
    def _build(self, base_config, override):
        factory, _ = _factory("ScrapeWebsiteTool", base_config, cls=_RealTool)
        return factory.create_tool("ScrapeWebsiteTool", tool_config_override=override)

    @pytest.mark.parametrize(
        "override",
        [
            {"requires_approval": False},
            {"requires_approval": False, "approval": None},
            {"approval": {"timeout_action": "approve", "timeout_seconds": 1}},
        ],
    )
    def test_an_override_cannot_switch_off_or_weaken_the_gate(self, override):
        tool = self._build({"requires_approval": True}, override)

        assert tool._approval_policy == {}

    def test_the_teamspace_options_are_the_ones_used(self):
        tool = self._build(
            {"approval": {"timeout_seconds": 600, "timeout_action": "reject"}},
            {"approval": {"timeout_seconds": 1, "timeout_action": "approve"}},
        )

        assert tool._approval_policy == {
            "timeout_seconds": 600,
            "timeout_action": "reject",
        }

    def test_an_override_can_still_turn_approval_on(self):
        tool = self._build({}, {"requires_approval": True})

        assert tool._approval_policy == {}

    def test_an_override_cannot_turn_replay_on(self):
        from src.services.tools.tool_policies import replay_policy

        tool = self._build({}, {"replayable": True, "replay": {"ttl_seconds": 60}})

        assert replay_policy(tool) is None

    def test_policy_keys_never_reach_the_constructor(self):
        tool = self._build(
            {"requires_approval": True},
            {"requires_approval": False, "replayable": True},
        )

        for key in ("requires_approval", "approval", "replayable", "replay"):
            assert key not in tool.kwargs


class TestAGatedToolKeepsItsName:
    @pytest.mark.parametrize("tool_name", ["ScrapeWebsiteTool", "SerperDevTool"])
    def test_an_override_cannot_rename_or_redescribe_it(self, tool_name):
        factory, _ = _factory(tool_name, {"requires_approval": True}, cls=_RealTool)
        with patch.object(factory, "_get_api_key", return_value="teamspace-key"):
            tool = factory.create_tool(
                tool_name,
                tool_config_override={
                    "name": "Another approved tool",
                    "description": "something else",
                },
            )

        assert "name" not in tool.kwargs and "description" not in tool.kwargs
        assert tool._approval_policy == {}


class TestGmailReMergeIsClean:
    def test_the_gmail_config_carries_no_reserved_key(self):
        factory, ctor = _factory("Gmail", base_config={"requires_approval": True})
        tool = MagicMock()
        ctor.return_value = tool

        factory.create_tool(
            "Gmail",
            tool_config_override={
                "requires_approval": False,
                "group_id": "team-b",
                "connection_name": "mail",
            },
        )

        kwargs = ctor.call_args.kwargs
        assert kwargs["group_id"] == "team-a"
        assert kwargs["tool_config"] == {"connection_name": "mail"}
        assert tool._approval_policy == {}


# ---------------------------------------------------------------------------
# What the task and agent forms write still applies
# ---------------------------------------------------------------------------


POWER_BI_TASK_OVERRIDE = {
    "workspace_id": "ws-from-task",
    "dataset_id": "ds-from-task",
    "report_id": "rp-from-task",
    "tenant_id": "tenant-from-task",
    "client_id": "client-from-task",
    "client_secret": "secret-from-task",
    "access_token": "pbi-token-from-task",
    "llm_workspace_url": "https://workspace.example.com",
    "llm_token": "llm-token-from-task",
    "user_question": "total sales",
}


class TestPowerBITaskOverridesStillApply:
    def test_the_comprehensive_analysis_tool_gets_every_task_field(self):
        factory, ctor = _factory(
            "Power BI Comprehensive Analysis Tool",
            base_config={"workspace_id": "ws-teamspace", "dataset_id": "ds-teamspace"},
        )

        factory.create_tool(
            "Power BI Comprehensive Analysis Tool",
            tool_config_override=dict(POWER_BI_TASK_OVERRIDE),
        )

        kwargs = ctor.call_args.kwargs
        for key, value in POWER_BI_TASK_OVERRIDE.items():
            assert kwargs[key] == value, key

    def test_a_generic_power_bi_tool_gets_hosts_and_secrets(self):
        override = {
            **POWER_BI_TASK_OVERRIDE,
            "databricks_host": "https://workspace.example.com",
            "pbi_api_base_url": "https://pbi.example.com/v1.0/myorg",
            "catalog": "main",
            "schema_name": "kpis",
        }
        factory, ctor = _factory("UC Metric View Generator")

        factory.create_tool("UC Metric View Generator", tool_config_override=override)

        kwargs = ctor.call_args.kwargs
        for key, value in override.items():
            assert kwargs[key] == value, key


# ---------------------------------------------------------------------------
# Logging: names only
# ---------------------------------------------------------------------------


def test_dropped_keys_are_logged_by_name_never_by_value(caplog):
    factory, _ = _factory("ScrapeWebsiteTool", cls=_RealTool)

    with caplog.at_level(logging.WARNING, logger="src.services.tools.tool_policies"):
        factory.create_tool(
            "ScrapeWebsiteTool",
            tool_config_override={
                "group_id": "team-b-secret-id",
                "base_url": "https://elsewhere.example.net",
            },
        )

    text = caplog.text
    assert "group_id" in text and "base_url" in text
    assert "team-b-secret-id" not in text
    assert "elsewhere.example.net" not in text
