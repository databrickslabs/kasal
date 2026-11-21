"""Crew 1 takes the model's Power Query parameter values from the Power BI API, so a native query
that splices them in gets the real value instead of an assumed ''."""

from unittest.mock import MagicMock, patch

from src.services.powerbi.dataset_parameters import (
    fetch_dataset_parameters,
    with_live_parameters,
)

WS = "11111111-2222-3333-4444-555555555555"
DS = "66666666-7777-8888-9999-000000000000"


def _response(status, body):
    r = MagicMock(status_code=status)
    r.json.return_value = body
    return r


def test_current_values_replace_the_guessed_todo_entries():
    body = {
        "value": [
            {"name": "RE_Version", "type": "Text", "currentValue": "R100"},
            {"name": "FiscperFilter", "type": "Text", "currentValue": "Full"},
            {"name": "RowLimit", "type": "Number", "currentValue": None},
        ]
    }
    config = {
        "parameter_defaults": {
            "RowLimit": "TODO: set default value",
            "C_data_mesh": "TODO: set default value",
        }
    }
    with patch("requests.get", return_value=_response(200, body)) as get:
        out = with_live_parameters(config, "tok", WS, DS)
    assert (
        get.call_args.args[0]
        == f"https://api.powerbi.com/v1.0/myorg/groups/{WS}/datasets/{DS}/parameters"
    )
    assert out["parameter_defaults"] == {
        "RowLimit": "",
        "C_data_mesh": "TODO: set default value",
        "RE_Version": "R100",
        "FiscperFilter": "Full",
    }


def test_a_failed_call_keeps_the_guesses():
    config = {"parameter_defaults": {"X": "TODO: set default value"}}
    with patch("requests.get", return_value=_response(403, {})):
        assert with_live_parameters(config, "tok", WS, DS)["parameter_defaults"] == {
            "X": "TODO: set default value"
        }


def test_ids_are_validated_before_any_request():
    with patch("requests.get") as get:
        assert fetch_dataset_parameters("tok", "../evil", DS) == {}
    get.assert_not_called()
