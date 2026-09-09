"""CLI compatibility and shared module identity for Power BI configuration."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from src.services.powerbi import pipeline_config
from src.services.tools import generate_config
from src.services.tools.pipeline_config_generator_tool import (
    PipelineConfigGeneratorTool,
)
from src.services.tools.uc_metric_view_generator_tool import UCMetricViewGeneratorTool


def test_tools_use_one_library_without_changing_import_search(monkeypatch):
    monkeypatch.setitem(sys.modules, "generate_config", object())
    before = sys.path.copy()
    assert PipelineConfigGeneratorTool._import_generate_config() is pipeline_config
    assert UCMetricViewGeneratorTool._import_generate_config() is pipeline_config
    assert generate_config.build_config is pipeline_config.build_config
    assert (
        generate_config._detect_cwc_filter_column
        is pipeline_config._detect_cwc_filter_column
    )
    assert sys.path == before


@pytest.mark.parametrize("report_id", [None, "report-id"])
def test_cli_writes_config_from_extracted_data(monkeypatch, tmp_path, report_id):
    output = tmp_path / "pipeline.json"
    args = [
        "generate_config",
        "--workspace-id",
        "workspace",
        "--dataset-id",
        "dataset",
        "--tenant-id",
        "tenant",
        "--client-id",
        "client",
        "--client-secret",
        "secret",
        "--admin-client-id",
        "admin",
        "--admin-client-secret",
        "admin-secret",
        "--catalog",
        "catalog",
        "--schema",
        "schema",
        "--output",
        str(output),
    ]
    if report_id:
        args.extend(["--report-id", report_id])
    monkeypatch.setattr(sys, "argv", args)
    token = Mock(side_effect=["token", "admin-token"])
    relationships = [{"from": "fact", "to": "dimension"}]
    measures = [{"measure_name": "Revenue"}]
    tables = {"fact": {"columns": []}}
    report = {"sections": []}
    config = {"catalog": "catalog", "source": "TODO", "tables": list(tables)}
    mocks = {
        "get_token": token,
        "extract_relationships": Mock(return_value=relationships),
        "extract_measures": Mock(return_value=measures),
        "trigger_admin_scan": Mock(return_value={"workspaces": []}),
        "parse_admin_tables": Mock(return_value=tables),
        "extract_report_definition": Mock(return_value=report),
        "build_config": Mock(return_value=config),
    }
    for name, value in mocks.items():
        monkeypatch.setattr(pipeline_config, name, value)
    generate_config.main()
    assert json.loads(output.read_text()) == config
    assert token.call_args_list[0].args == ("tenant", "client", "secret")
    assert token.call_args_list[1].args == ("tenant", "admin", "admin-secret")
    mocks["extract_relationships"].assert_called_once_with(
        "token", "workspace", "dataset"
    )
    mocks["trigger_admin_scan"].assert_called_once_with("admin-token", "workspace")
    mocks["build_config"].assert_called_once_with(
        relationships,
        measures,
        tables,
        report if report_id else None,
        catalog="catalog",
        schema="schema",
    )
    if report_id:
        mocks["extract_report_definition"].assert_called_once_with(
            "token",
            "workspace",
            report_id,
            tenant_id="tenant",
            client_id="client",
            client_secret="secret",
        )
    else:
        mocks["extract_report_definition"].assert_not_called()


def test_direct_cli_help_works_outside_backend_directory(tmp_path):
    script = Path(generate_config.__file__)
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    assert "--workspace-id" in result.stdout
    assert "--report-id" in result.stdout
