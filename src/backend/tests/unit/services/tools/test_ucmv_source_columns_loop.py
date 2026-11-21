"""The verify-numbers loop reads the source views' real columns once, between a cheap LLM-off
generation (for the source-layer DDL) and the real generation that receives them."""

import json

import pytest

from src.services.tools import ucmv_recon_source_layer
from src.services.tools import ucmv_source_columns
from src.services.tools.ucmv_iterative_recon import run_iterative_ucmv_generation

READ_MODE = json.dumps({"read_from_source_layer": True})
DDL = {"fact_a": {"ddl": "CREATE VIEW c.s.src_fact_a AS SELECT 1", "error": ""}}


class _Generator:
    def __init__(self, source_layer_ddl=DDL):
        self.calls: list[dict] = []
        self.ddl = source_layer_ddl

    def _run(self, **kwargs):
        self.calls.append(dict(kwargs))
        return json.dumps(
            {
                "yaml": {"fact_a": "version: '1.1'\nsource: c.s.src_fact_a\n"},
                "source_layer_ddl": self.ddl,
                "resolved_measures_by_table": {},
                "specs_summary": {},
            }
        )


class _Deployer:
    def __init__(self):
        self.calls: list[dict] = []

    def _run(self, **kwargs):
        self.calls.append(dict(kwargs))
        return json.dumps({"deployment_results": {"fact_a": {"status": "deployed"}}})


class _Reconciler:
    def _run(self, **kwargs):
        return json.dumps({"views": {}, "overall": {"cells_total": 0}})


@pytest.fixture
def fake_layer(monkeypatch):
    """A deployed source layer (no warehouse) and a probe that reads the given columns."""
    state = {"columns": {"src_fact_a": [{"name": "id", "type": "INT"}]}, "probed": []}

    def deploy_layer(deployer, ddl_map, deploy_base):
        return {"deployed": ["fact_a"], "failures": {}}

    def read_columns(deployer, deploy_base, leaves):
        state["probed"].append(sorted(leaves))
        return state["columns"]

    monkeypatch.setattr(ucmv_recon_source_layer, "deploy_source_layer", deploy_layer)
    monkeypatch.setattr(ucmv_source_columns, "read_source_columns", read_columns)
    return state


def _run(generator, config=READ_MODE):
    return run_iterative_ucmv_generation(
        generator=generator,
        deployer=_Deployer(),
        reconciler=_Reconciler(),
        gen_kwargs={"config_json": config, "catalog": "c", "schema_name": "s"},
        deploy_kwargs={"catalog": "c", "schema_name": "s", "warehouse_id": "w1"},
        recon_kwargs={"warehouse_id": "w1"},
        max_cycles=1,
    )


def test_read_mode_runs_a_cheap_pass_then_the_real_pass_with_the_columns(fake_layer):
    generator = _Generator()
    report = _run(generator)

    cheap, real = generator.calls
    assert cheap["use_llm_fallback"] is False
    assert "source_columns_json" not in cheap
    assert "source_columns_json" in real
    assert json.loads(real["source_columns_json"]) == {
        "fact_a": [{"name": "id", "type": "INT"}]
    }
    assert fake_layer["probed"] == [["src_fact_a"]]
    assert report["source_columns_probe"]["views_with_columns"] == 1


def test_later_cycles_reuse_the_columns_without_another_cheap_pass(fake_layer):
    generator = _Generator()
    run_iterative_ucmv_generation(
        generator=generator,
        deployer=_Deployer(),
        reconciler=_Reconciler(),
        gen_kwargs={"config_json": READ_MODE, "catalog": "c", "schema_name": "s"},
        deploy_kwargs={"catalog": "c", "schema_name": "s", "warehouse_id": "w1"},
        recon_kwargs={"warehouse_id": "w1"},
        max_cycles=2,
        target_pct=101.0,
    )
    cheap_passes = [c for c in generator.calls if c.get("use_llm_fallback") is False]
    assert len(cheap_passes) == 1
    assert len(fake_layer["probed"]) == 1


def test_without_read_mode_nothing_changes(fake_layer):
    generator = _Generator()
    _run(generator, config=json.dumps({"read_from_source_layer": False}))
    assert len(generator.calls) == 1
    assert "source_columns_json" not in generator.calls[0]
    assert fake_layer["probed"] == []


def test_when_no_column_can_be_read_the_real_pass_gets_no_columns(fake_layer):
    fake_layer["columns"] = {}
    generator = _Generator()
    report = _run(generator)
    assert len(generator.calls) == 2
    assert "source_columns_json" not in generator.calls[1]
    assert report["source_columns_probe"]["views_with_columns"] == 0
