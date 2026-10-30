"""mask_run_inputs: the one rule for a run record's inputs, on every path.

ExecutionService (create, resume, GET /executions), ExecutionHistoryService
(the history reads) and the trigger consumer all store or return a run's
inputs through it.
"""

import pytest

from src.services.execution.history import ExecutionHistoryService
from src.services.execution.service import ExecutionService
from src.services.execution.stored_inputs import mask_run_inputs

MASK = "***REDACTED***"


def test_empty_inputs_come_back_as_they_were():
    assert mask_run_inputs(None) is None
    assert mask_run_inputs({}) == {}


def test_agent_tool_configs_are_masked_and_the_input_is_untouched():
    inputs = {
        "agents_yaml": {"a": {"role": "r", "tool_configs": {"T": {"secret": "s"}}}}
    }
    result = mask_run_inputs(inputs)
    assert result["agents_yaml"]["a"] == {
        "role": "r",
        "tool_configs": {"T": {"secret": MASK}},
    }
    assert inputs["agents_yaml"]["a"]["tool_configs"]["T"]["secret"] == "s"


def test_task_tool_callback_and_task_configs_are_masked():
    task = {
        "tool_configs": {"T": {"secret": "s", "max_tokens": 9}},
        "callback_config": {"token": "pat", "volume_path": "/Volumes/a/b/c"},
        # The task form saves the volume callback, PAT included, here too.
        "config": {"callback_config": {"token": "pat"}, "timeout": 30},
    }
    masked = mask_run_inputs({"tasks_yaml": {"t": task}})["tasks_yaml"]["t"]
    assert masked["tool_configs"] == {"T": {"secret": MASK, "max_tokens": 9}}
    assert masked["callback_config"] == {"token": MASK, "volume_path": "/Volumes/a/b/c"}
    assert masked["config"] == {"callback_config": {"token": MASK}, "timeout": 30}


def test_user_inputs_and_run_credentials():
    inputs = {
        "inputs": {"client_secret": "my-secret", "topic": "t", "max_tokens": 5},
        "user_token": "obo-token",
        "trigger_hops": 2,
    }
    result = mask_run_inputs(inputs)
    assert result["inputs"] == {"client_secret": MASK, "topic": "t", "max_tokens": 5}
    assert "user_token" not in result
    assert result["trigger_hops"] == 2


def test_malformed_sections_pass_through():
    inputs = {"agents_yaml": "string", "tasks_yaml": {"t": "not a dict"}}
    assert mask_run_inputs(inputs) == inputs


@pytest.mark.parametrize("service", [ExecutionService, ExecutionHistoryService])
def test_both_services_apply_it(service):
    inputs = {"tasks_yaml": {"t": {"config": {"callback_config": {"token": "pat"}}}}}
    instance = service.__new__(service)  # the method needs no collaborators
    masked = instance._mask_inputs_sensitive_data(inputs)
    assert masked == mask_run_inputs(inputs)
    assert masked["tasks_yaml"]["t"]["config"]["callback_config"]["token"] == MASK
