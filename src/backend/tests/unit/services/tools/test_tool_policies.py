"""Policies ride on the tool config, and must never reach its constructor."""

import pytest

from src.services.tools.tool_policies import (
    APPROVAL_ATTR,
    DEFAULT_REPLAY_SCOPE,
    DEFAULT_REPLAY_TTL_SECONDS,
    REPLAY_ATTR,
    extract_tool_policies,
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
