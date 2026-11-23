"""The config generator adds the Admin Scanner's calculated columns to its TMDL ones.

``_merge_scanned_calculated_columns`` runs on a ``_ConfigGenRun``; the test builds only the
fields it reads, so no Power BI call is made.
"""

from types import SimpleNamespace

from src.services.tools.pipeline_config_generator_tool import (
    PipelineConfigGeneratorTool,
)

DS_ID = "11111111-2222-3333-4444-555555555555"
DATE_KEY = {
    "name": "date_key",
    "dataType": "Int64",
    "columnType": "Calculated",
    "expression": "YEAR(Fact[posting_date]) * 10000",
}
SCAN = {
    "workspaces": [
        {
            "datasets": [
                {
                    "id": DS_ID,
                    "tables": [
                        {
                            "name": "Fact",
                            "columns": [
                                {
                                    "name": "posting_date",
                                    "dataType": "DateTime",
                                    "columnType": "Data",
                                },
                                {
                                    "name": "amount",
                                    "dataType": "Double",
                                    "columnType": "Data",
                                },
                                DATE_KEY,
                            ],
                        }
                    ],
                }
            ]
        }
    ]
}


def _merge(run_kwargs: dict, config: dict | None = None) -> dict:
    run = SimpleNamespace(
        scan_result=run_kwargs.get("scan_result"),
        dataset_id=DS_ID,
        calculated_columns=run_kwargs.get("calculated_columns", {}),
    )
    cfg = config if config is not None else {}
    PipelineConfigGeneratorTool._merge_scanned_calculated_columns(None, run, cfg)
    return {"run": run, "config": cfg}


def test_scan_fills_the_calculated_columns_and_the_plain_column_kinds():
    out = _merge({"scan_result": SCAN})
    assert out["run"].calculated_columns == {
        "Fact": [
            {
                "name": "date_key",
                "expression": "YEAR(Fact[posting_date]) * 10000",
                "data_type": "Int64",
            }
        ]
    }
    assert out["config"]["calculated_column_kinds"] == {
        "Fact": {"posting_date": "date", "amount": "number"}
    }


def test_tmdl_keeps_precedence_for_a_table_it_describes():
    tmdl = {"Fact": [{"name": "date_key", "expression": "FROM TMDL", "data_type": ""}]}
    out = _merge({"scan_result": SCAN, "calculated_columns": tmdl})
    assert out["run"].calculated_columns == tmdl


def test_no_scan_leaves_the_run_and_config_untouched():
    tmdl = {"Fact": [{"name": "x", "expression": "1", "data_type": ""}]}
    out = _merge({"scan_result": None, "calculated_columns": tmdl})
    assert out["run"].calculated_columns == tmdl
    assert out["config"] == {}


def test_scan_without_calculated_columns_adds_no_kinds():
    plain = {
        "workspaces": [
            {
                "datasets": [
                    {
                        "id": DS_ID,
                        "tables": [
                            {
                                "name": "Fact",
                                "columns": [
                                    {
                                        "name": "amount",
                                        "dataType": "Double",
                                        "columnType": "Data",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        ]
    }
    out = _merge({"scan_result": plain})
    assert out["run"].calculated_columns == {}
    assert "calculated_column_kinds" not in out["config"]


def test_a_malformed_scan_is_fail_open():
    out = _merge(
        {"scan_result": {"workspaces": "not-a-list"}, "calculated_columns": {}}
    )
    assert out["run"].calculated_columns == {}
