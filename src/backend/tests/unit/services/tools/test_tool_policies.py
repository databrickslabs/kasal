"""Policies ride on the tool config, and must never reach its constructor."""

import logging
from unittest.mock import patch

import pytest

import src.services.tools.scrape_website as scrape_website
from src.services.tools import tool_policies
from src.services.tools.tool_policies import (
    APPROVAL_ATTR,
    DEFAULT_REPLAY_SCOPE,
    DEFAULT_REPLAY_TTL_SECONDS,
    REPLAY_ATTR,
    RESERVED_OVERRIDE_KEYS,
    extract_tool_policies,
    merge_tool_config_override,
    replay_policy,
    stamp_tool_policies,
)


class _Tool:
    """Stands in for a pydantic tool: no field for either policy."""

    def __init__(self, name: str = "PerplexityTool") -> None:
        self.name = name


class TestExtraction:
    def test_a_config_with_no_policy_yields_none_and_is_untouched(self):
        config = {"api_key": "secret", "model": "sonar"}

        assert extract_tool_policies(config) == {}
        assert config == {"api_key": "secret", "model": "sonar"}

    def test_the_keys_are_POPPED_so_they_never_reach_the_constructor(self):
        """The whole reason this runs before `tool_class(**tool_config)`."""
        config = {"api_key": "secret", "requires_approval": True, "replayable": True}

        extract_tool_policies(config)

        assert config == {"api_key": "secret"}

    def test_replayable_true_gets_the_defaults(self):
        policies = extract_tool_policies({"replayable": True})

        assert policies[REPLAY_ATTR] == {
            "ttl_seconds": DEFAULT_REPLAY_TTL_SECONDS,
            "scope": DEFAULT_REPLAY_SCOPE,
        }

    def test_the_options_dict_alone_turns_it_on(self):
        """A config that bothers to set the knobs plainly wants the feature."""
        policies = extract_tool_policies({"replay": {"ttl_seconds": 60}})

        assert policies[REPLAY_ATTR]["ttl_seconds"] == 60

    @pytest.mark.parametrize("bad", [0, -1, "soon", None, 1.5e400])
    def test_a_nonsense_ttl_falls_back_rather_than_disabling_the_policy(self, bad):
        policies = extract_tool_policies({"replay": {"ttl_seconds": bad}})

        assert policies[REPLAY_ATTR]["ttl_seconds"] == DEFAULT_REPLAY_TTL_SECONDS

    def test_an_unknown_scope_falls_back_to_the_default(self):
        policies = extract_tool_policies({"replay": {"scope": "everyone"}})

        assert policies[REPLAY_ATTR]["scope"] == DEFAULT_REPLAY_SCOPE

    def test_approval_keeps_its_shape(self):
        policies = extract_tool_policies(
            {"approval": {"timeout_seconds": 30, "timeout_action": "approve"}}
        )

        assert policies[APPROVAL_ATTR] == {
            "timeout_seconds": 30,
            "timeout_action": "approve",
        }

    def test_both_policies_at_once(self):
        policies = extract_tool_policies(
            {"requires_approval": True, "replayable": True, "api_key": "k"}
        )

        assert set(policies) == {APPROVAL_ATTR, REPLAY_ATTR}


class TestStamping:
    def test_the_instance_carries_the_policy(self):
        tool = _Tool()

        stamp_tool_policies(tool, extract_tool_policies({"replayable": True}))

        assert replay_policy(tool) == {
            "ttl_seconds": DEFAULT_REPLAY_TTL_SECONDS,
            "scope": DEFAULT_REPLAY_SCOPE,
        }

    def test_every_instance_of_a_list_is_stamped(self):
        """One MCP server yields every tool it serves; the policy is the server's."""
        tools = [_Tool("a"), _Tool("b")]

        stamp_tool_policies(tools, extract_tool_policies({"replayable": True}))

        assert all(replay_policy(t) is not None for t in tools)

    def test_a_tool_with_no_policy_reads_as_not_replayable(self):
        assert replay_policy(_Tool()) is None

    def test_stamping_nothing_on_nothing_is_safe(self):
        stamp_tool_policies(None, {})
        stamp_tool_policies(_Tool(), {})

    def test_a_tool_that_refuses_the_attribute_does_not_break_the_run(self):
        class _Frozen(_Tool):
            def __setattr__(self, *_args):  # pragma: no cover - via object.__setattr__
                raise AttributeError("frozen")

        frozen = object.__new__(_Frozen)

        stamp_tool_policies(frozen, extract_tool_policies({"replayable": True}))

        # Losing a policy must not lose the run; absent reads as "call for real".
        assert replay_policy(frozen) is None or isinstance(replay_policy(frozen), dict)


class TestMergingAnOverride:
    """``merge_tool_config_override``: a run, agent or task entry over the
    teamspace config."""

    def test_neither_input_is_mutated(self):
        base = {"requires_approval": True, "model": "a"}
        override = {"model": "b", "group_id": "other", "requires_approval": False}

        merge_tool_config_override("T", base, override)

        assert base == {"requires_approval": True, "model": "a"}
        assert override == {
            "model": "b",
            "group_id": "other",
            "requires_approval": False,
        }

    def test_ordinary_keys_win_and_reserved_ones_are_dropped(self):
        clean, merged, _ = merge_tool_config_override(
            "T",
            {"model": "a", "base_url": "https://teamspace.example.com"},
            {"model": "b", "base_url": "https://other.example.net", "user_token": "t"},
        )

        assert clean == {"model": "b"}
        assert merged == {"model": "b", "base_url": "https://teamspace.example.com"}

    @pytest.mark.parametrize("key", sorted(RESERVED_OVERRIDE_KEYS))
    def test_every_reserved_key_is_dropped(self, key):
        clean, _, _ = merge_tool_config_override("T", {}, {key: "x", "keep": 1})

        assert clean == {"keep": 1}

    def test_the_teamspace_gate_survives(self):
        _, _, policies = merge_tool_config_override(
            "T", {"requires_approval": True}, {"requires_approval": False}
        )

        assert policies[APPROVAL_ATTR] == {}

    def test_an_override_may_add_a_gate(self):
        _, merged, policies = merge_tool_config_override(
            "T", {}, {"approval": {"timeout_seconds": 60}}
        )

        assert policies[APPROVAL_ATTR] == {"timeout_seconds": 60}
        assert "approval" not in merged

    def test_an_override_never_turns_replay_on(self):
        _, _, policies = merge_tool_config_override("T", {}, {"replayable": True})

        assert REPLAY_ATTR not in policies

    def test_no_override_is_the_teamspace_config(self):
        clean, merged, policies = merge_tool_config_override(
            "T", {"replayable": True, "model": "a"}, None
        )

        assert clean == {}
        assert merged == {"model": "a"}
        assert REPLAY_ATTR in policies

    def test_an_added_gate_is_not_reported_as_ignored(self, caplog):
        with caplog.at_level(logging.WARNING, logger=tool_policies.__name__):
            merge_tool_config_override("T", {}, {"requires_approval": True})

        assert caplog.text == ""

    @pytest.mark.parametrize("key", ["name", "description"])
    def test_an_override_cannot_rename_the_tool(self, key):
        clean, merged, _ = merge_tool_config_override(
            "T", {"requires_approval": True}, {key: "Another tool"}
        )

        assert key not in clean and key not in merged


TEAMSPACE_LLM = {
    "llm_workspace_url": "https://ws.example.com",
    "llm_token": "teamspace-llm-token",
    "databricks_sql_endpoint": "https://ws.example.com/api/2.0/mcp/sql",
    "databricks_pat": "teamspace-pat",
}


class TestATeamspaceCredentialStaysWithItsEndpoint:
    """An override that points an endpoint elsewhere does not take the
    teamspace credential for that endpoint with it."""

    @pytest.mark.parametrize(
        "endpoint, credential, moved_to",
        [
            ("llm_workspace_url", "llm_token", "https://elsewhere.example.net"),
            ("llm_workspace_url", "llm_token", "elsewhere.example.net"),
            ("llm_workspace_url", "llm_token", "{llm_workspace_url}"),
            ("llm_workspace_url", "llm_token", "https://[elsewhere.example.net"),
            (
                "databricks_sql_endpoint",
                "databricks_pat",
                "https://elsewhere.example.net/api/2.0/mcp/sql",
            ),
            # Same host, other scheme: the credential would travel in clear.
            (
                "databricks_sql_endpoint",
                "databricks_pat",
                "http://ws.example.com/api/2.0/mcp/sql",
            ),
        ],
    )
    def test_moving_the_endpoint_withholds_the_teamspace_credential(
        self, endpoint, credential, moved_to
    ):
        _, merged, _ = merge_tool_config_override(
            "T", dict(TEAMSPACE_LLM), {endpoint: moved_to}
        )

        assert merged[endpoint] == moved_to
        assert credential not in merged

    def test_an_empty_credential_in_the_override_does_not_bring_it_back(self):
        _, merged, _ = merge_tool_config_override(
            "T",
            dict(TEAMSPACE_LLM),
            {"llm_workspace_url": "https://elsewhere.example.net", "llm_token": ""},
        )

        assert not merged.get("llm_token")

    def test_the_other_pair_is_untouched(self):
        _, merged, _ = merge_tool_config_override(
            "T", dict(TEAMSPACE_LLM), {"llm_workspace_url": "https://other.example.net"}
        )

        assert merged["databricks_pat"] == "teamspace-pat"

    def test_the_override_s_own_credential_goes_with_its_endpoint(self):
        _, merged, _ = merge_tool_config_override(
            "T",
            dict(TEAMSPACE_LLM),
            {
                "llm_workspace_url": "https://elsewhere.example.net",
                "llm_token": "task-token",
            },
        )

        assert merged["llm_token"] == "task-token"

    @pytest.mark.parametrize(
        "same",
        ["https://ws.example.com", "https://WS.example.com/", "ws.example.com", ""],
    )
    def test_the_same_endpoint_keeps_the_teamspace_credential(self, same):
        _, merged, _ = merge_tool_config_override(
            "T", dict(TEAMSPACE_LLM), {"llm_workspace_url": same}
        )

        assert merged["llm_token"] == "teamspace-llm-token"

    def test_a_new_path_on_the_same_host_keeps_the_teamspace_pat(self):
        _, merged, _ = merge_tool_config_override(
            "T",
            dict(TEAMSPACE_LLM),
            {"databricks_sql_endpoint": "https://ws.example.com/sql/1.0/warehouses/a1"},
        )

        assert merged["databricks_pat"] == "teamspace-pat"

    def test_a_teamspace_without_an_endpoint_still_withholds_its_credential(self):
        """The credential was meant for the run's own workspace."""
        _, merged, _ = merge_tool_config_override(
            "T",
            {"llm_token": "teamspace-llm-token"},
            {"llm_workspace_url": "https://elsewhere.example.net"},
        )

        assert "llm_token" not in merged

    def test_the_withheld_credential_is_logged_by_name_only(self, caplog):
        with caplog.at_level(logging.WARNING, logger=tool_policies.__name__):
            merge_tool_config_override(
                "T",
                dict(TEAMSPACE_LLM),
                {"llm_workspace_url": "https://elsewhere.example.net"},
            )

        assert "llm_token" in caplog.text
        assert "teamspace-llm-token" not in caplog.text
        assert "elsewhere.example.net" not in caplog.text


TEAMSPACE_SCRAPE = {
    "website_url": "https://example.com/reports",
    "cookies": {"name": "session", "value": "teamspace-cookie"},
    "headers": {"X-Site-Token": "teamspace-header"},
}


class TestTheScrapeToolsCookiesStayWithItsSite:
    """Configuration → Tools sets ``website_url``, ``cookies`` and ``headers``
    in one JSON block, so the cookies and headers belong to that site."""

    @pytest.mark.parametrize(
        "moved_to",
        [
            "https://example.net/reports",
            "http://example.com/reports",
            "https://example.com:8443/reports",
        ],
    )
    def test_moving_the_site_withholds_cookies_and_headers(self, moved_to):
        _, merged, _ = merge_tool_config_override(
            "ScrapeWebsiteTool", dict(TEAMSPACE_SCRAPE), {"website_url": moved_to}
        )

        assert merged["website_url"] == moved_to
        assert "cookies" not in merged
        assert "headers" not in merged

    def test_another_page_on_the_same_site_keeps_them(self):
        _, merged, _ = merge_tool_config_override(
            "ScrapeWebsiteTool",
            dict(TEAMSPACE_SCRAPE),
            {"website_url": "https://example.com/other"},
        )

        assert merged["cookies"] == TEAMSPACE_SCRAPE["cookies"]
        assert merged["headers"] == TEAMSPACE_SCRAPE["headers"]

    def test_the_override_s_own_cookies_go_with_its_site(self):
        own = {"name": "session", "value": "task-cookie"}
        _, merged, _ = merge_tool_config_override(
            "ScrapeWebsiteTool",
            dict(TEAMSPACE_SCRAPE),
            {"website_url": "https://example.net/", "cookies": own},
        )

        assert merged["cookies"] == own
        assert "headers" not in merged

    def test_the_built_tool_does_not_send_them_to_the_moved_site(self):
        _, merged, _ = merge_tool_config_override(
            "ScrapeWebsiteTool",
            dict(TEAMSPACE_SCRAPE),
            {"website_url": "https://example.net/"},
        )
        seen = {}

        def fake_fetch(url, headers, timeout=15, max_bytes=None):
            seen.update(url=url, headers=dict(headers))
            return "<p>ok</p>"

        with patch.object(scrape_website, "_safe_fetch", fake_fetch):
            scrape_website.ScrapeWebsiteTool(**merged)._run()

        assert seen["url"] == "https://example.net/"
        assert "Cookie" not in seen["headers"]
        assert "X-Site-Token" not in seen["headers"]
