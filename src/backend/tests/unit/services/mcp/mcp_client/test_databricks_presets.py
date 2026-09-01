"""Tests for the Databricks managed-MCP presets and the follow-config heal.

The engine stays server-agnostic (services/tools/mcp_follow follows whatever
the server's config declares); what Kasal knows about the managed Genie
endpoints is preset DATA in databricks_presets, shipped on registration and
healed onto rows that predate it.
"""

from src.services.mcp.mcp_client.databricks_presets import (
    GENIE_ONE_FOLLOW,
    GENIE_SPACE_FOLLOW,
    follow_healed_config,
    follow_preset_for,
)

# --- preset lookup ----------------------------------------------------------


def test_genie_one_url_matches_the_workspace_preset():
    assert (
        follow_preset_for("https://ws.example.com/api/2.0/mcp/genie")
        is GENIE_ONE_FOLLOW
    )
    assert (
        follow_preset_for("https://ws.example.com/api/2.0/mcp/genie/")
        is GENIE_ONE_FOLLOW
    )


def test_genie_space_url_matches_the_per_space_preset():
    assert (
        follow_preset_for("https://ws.example.com/api/2.0/mcp/genie/space-1")
        is GENIE_SPACE_FOLLOW
    )


def test_non_genie_urls_have_no_preset():
    assert follow_preset_for("https://ws.example.com/api/2.0/mcp/sql") is None
    assert follow_preset_for("https://other.example.com/mcp") is None
    assert follow_preset_for(None) is None
    assert follow_preset_for("") is None


def test_presets_include_the_poll_self_follow():
    """Each preset carries ask→poll AND poll→poll, so a poll call the model
    makes directly also blocks until the work is finished."""
    assert [d["start_tool"] for d in GENIE_ONE_FOLLOW] == [
        "genie_ask",
        "genie_poll_response",
    ]
    assert [d["start_tool"] for d in GENIE_SPACE_FOLLOW] == [
        "query_space",
        "poll_response",
    ]


# --- heal computation -------------------------------------------------------


def test_heal_backfills_a_bare_config_and_preserves_other_keys():
    healed = follow_healed_config(
        "https://ws.example.com/api/2.0/mcp/genie", {"note": "keep me"}
    )
    assert healed == {"note": "keep me", "follow": GENIE_ONE_FOLLOW}
    # The backend initializes additional_config to {} — the shape that broke
    # the frontend heal — and None must heal too.
    assert follow_healed_config("https://ws.example.com/api/2.0/mcp/genie", {}) == {
        "follow": GENIE_ONE_FOLLOW
    }
    assert follow_healed_config("https://ws.example.com/api/2.0/mcp/genie", None) == {
        "follow": GENIE_ONE_FOLLOW
    }


def test_heal_never_touches_an_existing_follow_declaration():
    mine = {"follow": [{"start_tool": "x", "poll_tool": "y", "id_params": ["a", "b"]}]}
    assert (
        follow_healed_config("https://ws.example.com/api/2.0/mcp/genie", mine) is None
    )


def test_heal_ignores_unmanaged_urls():
    assert follow_healed_config("https://ws.example.com/api/2.0/mcp/sql", {}) is None


# --- the tool-creation default (no stored data needed) ----------------------


def test_stored_follow_config_wins_over_the_preset():
    from src.services.tools.mcp_integration import _follow_config_for

    mine = [{"start_tool": "x", "poll_tool": "y", "id_params": ["a", "b"]}]
    server = {
        "server_url": "https://ws.example.com/api/2.0/mcp/genie",
        "additional_config": {"follow": mine},
    }
    assert _follow_config_for(server) is mine


def test_managed_genie_rows_get_the_preset_with_no_stored_config():
    """A server registered before the presets existed (additional_config is
    {} or None) still follows — in memory, no migration, no seeder."""
    from src.services.tools.mcp_integration import _follow_config_for

    for config in ({}, None, {"note": "other keys"}):
        server = {
            "server_url": "https://ws.example.com/api/2.0/mcp/genie",
            "additional_config": config,
        }
        assert _follow_config_for(server) is GENIE_ONE_FOLLOW
    space = {
        "server_url": "https://ws.example.com/api/2.0/mcp/genie/space-1",
        "additional_config": {},
    }
    assert _follow_config_for(space) is GENIE_SPACE_FOLLOW


def test_unmanaged_servers_get_no_follow_default():
    from src.services.tools.mcp_integration import _follow_config_for

    assert (
        _follow_config_for(
            {"server_url": "https://other.example.com/mcp", "additional_config": {}}
        )
        is None
    )
    assert _follow_config_for({"server_url": None, "additional_config": None}) is None
