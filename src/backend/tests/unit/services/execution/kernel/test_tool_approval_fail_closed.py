"""An approval-required tool must not run when no approval row exists.

`_create_approval` returns None when the HITL service creates no row. The hook
then polled a None id until the timeout, and with `timeout_action="approve"` the
tool RAN — although nobody could ever have approved it. It now fails closed, as
it already did when creating the row raised.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.services.execution.kernel import tool_approval
from src.services.execution.runtime import ToolExecutionBlockedError


def test_no_approval_row_blocks_the_tool_even_if_timeout_approves() -> None:
    tool = SimpleNamespace(
        name="dangerous",
        _approval_policy={
            "scope": "call",
            "timeout_action": "approve",
            "timeout_seconds": 0,
        },
    )

    def run(coro, timeout=None):  # stands in for the async bridge
        coro.close()
        return None  # _create_approval found nothing to create

    with (
        patch("src.services.tools.async_bridge.run_async_with_context", run),
        patch("src.services.hitl.notify.notify_input_needed") as notify,
    ):
        hook = tool_approval.make_tool_approval_hook("exec-1", None)
        with pytest.raises(ToolExecutionBlockedError):
            hook(tool, {}, None, None)
    notify.assert_not_called()
