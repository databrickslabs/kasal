"""``confidential_values`` as a tool policy: extracted, merged, stamped, never leaked to the constructor."""

import logging

import pytest
from pydantic import BaseModel, Field

from src.services.tools.base import BaseTool
from src.services.tools.confidential.guard import GUARD_MARK
from src.services.tools.tool_policies import (
    CONFIDENTIAL_ATTR,
    RESERVED_OVERRIDE_KEYS,
    extract_tool_policies,
    merge_tool_config_override,
    stamp_tool_policies,
)


class _Schema(BaseModel):
    question: str = Field(default="")


class _Tool(BaseTool):
    name: str = "policy_tool"
    description: str = "d"
    args_schema: type[BaseModel] = _Schema

    def _run(self, **kwargs: object) -> str:
        return "| A | 1.5 |\n| --- | --- |\n| x | 2.5 |"


def test_flag_is_popped_and_extracted_as_a_policy():
    config = {"confidential_values": True, "base_url": "https://example.com"}
    policies = extract_tool_policies(config)
    assert policies == {CONFIDENTIAL_ATTR: {"enabled": True}}
    assert "confidential_values" not in config


def test_flag_off_or_absent_is_no_policy():
    assert CONFIDENTIAL_ATTR not in extract_tool_policies(
        {"confidential_values": False}
    )
    assert CONFIDENTIAL_ATTR not in extract_tool_policies({})


def test_flag_is_a_reserved_override_key():
    assert "confidential_values" in RESERVED_OVERRIDE_KEYS


def test_teamspace_policy_survives_an_override_that_turns_it_off():
    base = {"confidential_values": True, "spaceId": "s1"}
    override, tool_config, policies = merge_tool_config_override(
        "GenieTool", base, {"confidential_values": False, "max_calls": 2}
    )
    assert CONFIDENTIAL_ATTR in policies
    assert "confidential_values" not in override
    assert "confidential_values" not in tool_config
    assert override == {"max_calls": 2}


def test_override_may_switch_confidentiality_on():
    base = {"spaceId": "s1"}
    _, tool_config, policies = merge_tool_config_override(
        "GenieTool", base, {"confidential_values": True}
    )
    assert policies[CONFIDENTIAL_ATTR] == {"enabled": True}
    assert "confidential_values" not in tool_config


def test_merge_does_not_mutate_its_inputs():
    base = {"confidential_values": True}
    override = {"confidential_values": False}
    merge_tool_config_override("GenieTool", base, override)
    assert base == {"confidential_values": True}
    assert override == {"confidential_values": False}


def test_stamping_installs_the_guard_on_the_instance():
    tool = _Tool()
    stamp_tool_policies(tool, extract_tool_policies({"confidential_values": True}))
    assert getattr(tool, CONFIDENTIAL_ATTR) == {"enabled": True}
    assert getattr(tool, GUARD_MARK) is True
    assert "2.5" not in tool.run(question="q")


def test_stamping_a_list_guards_every_instance():
    first, second = _Tool(), _Tool()
    stamp_tool_policies(
        [first, second], extract_tool_policies({"confidential_values": True})
    )
    assert getattr(first, GUARD_MARK) and getattr(second, GUARD_MARK)


def test_no_confidential_policy_leaves_the_tool_untouched():
    tool = _Tool()
    stamp_tool_policies(tool, extract_tool_policies({}))
    assert not hasattr(tool, GUARD_MARK)
    assert "2.5" in tool.run(question="q")


def test_unguardable_stub_is_logged_not_raised(caplog):
    class Stub:
        pass

    stub = Stub()
    with caplog.at_level(logging.ERROR, logger="src.services.tools.tool_policies"):
        stamp_tool_policies(stub, extract_tool_policies({"confidential_values": True}))
    assert getattr(stub, CONFIDENTIAL_ATTR) == {"enabled": True}
    assert any("NOT redacted" in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("flag", ["confidential_values"])
def test_policy_key_is_not_passed_to_the_constructor(flag):
    config = {flag: True, "api_key": "k"}
    extract_tool_policies(config)
    assert config == {"api_key": "k"}
