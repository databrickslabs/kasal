"""Credentials in a tool call reach the tool, never the records kept of the call.

Tool arguments are copied into the run log, the stored trace, MLflow spans and the approval
record. A tool that takes ``client_secret`` / ``password`` / ``access_token`` as arguments
had them stored there in plain text."""

import json

from src.services.execution.kernel.tool_approval import _safe_args
from src.services.powerbi.scope.scope_table import scope_csv
from src.services.trace.tool_args import observable_tool_args, tool_args_text

CALL = {
    "workspace_id": "ws",
    "client_secret": "s3cr3t-value",
    "password": "p4ss",
    "access_token": "tok",
    "max_tokens": 100,
    "nested": {"api_key": "k", "name": "n"},
}


def test_observers_get_masked_arguments_as_a_dict_or_as_json_text():
    masked = observable_tool_args(CALL)
    assert isinstance(masked, dict)
    assert masked["workspace_id"] == "ws" and masked["max_tokens"] == 100
    assert masked["nested"]["name"] == "n"
    for text in (str(masked), tool_args_text(CALL), tool_args_text(json.dumps(CALL))):
        assert "s3cr3t-value" not in text and "p4ss" not in text and '"tok"' not in text
        assert "ws" in text
    assert CALL["client_secret"] == "s3cr3t-value"  # the call itself is untouched
    assert tool_args_text("not json") == "not json" and tool_args_text(None) == ""


def test_the_approval_record_masks_credentials():
    safe = _safe_args(CALL)
    assert safe["workspace_id"] == "ws"
    assert "s3cr3t-value" not in str(safe) and "p4ss" not in str(safe)


def test_a_model_name_cannot_run_as_a_spreadsheet_formula():
    from src.services.powerbi.scope.scope_table import SCOPE_FIELDS

    field = SCOPE_FIELDS[0]
    text = scope_csv(
        [{field: '=HYPERLINK("https://example.com","x")'}, {field: "Sales"}]
    )
    lines = text.splitlines()
    assert lines[1].startswith("\"'=HYPERLINK") or lines[1].startswith("'=HYPERLINK")
    assert lines[2].startswith("Sales")


def test_a_credential_in_the_tool_call_is_not_written_to_the_trace_span():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from src.services.otel_tracing.event_bridge import OTelEventBridge

    span = MagicMock()
    bridge = OTelEventBridge(MagicMock(), "job-masked")
    bridge._set_extra_attributes(span, SimpleNamespace(tool_args=CALL))

    written = [
        call[0][1]
        for call in span.set_attribute.call_args_list
        if call[0][0] == "kasal.extra.tool_args"
    ]
    assert written and "s3cr3t-value" not in written[0] and "'ws'" in written[0]
