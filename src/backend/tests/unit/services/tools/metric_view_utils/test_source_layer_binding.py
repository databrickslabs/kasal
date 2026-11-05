"""Unit tests for source_layer_binding: naming, config readers and the pure rebinding
of metric-view specs onto source-layer views (read_from_source_layer)."""

from types import SimpleNamespace

import pytest

from src.services.tools.metric_view_utils.data_classes import MetricViewSpec
from src.services.tools.metric_view_utils.source_layer_binding import (
    SourceLayerBinding,
    bind_metric_views,
    binding_report_lines,
    calc_columns_by_key,
    quote_uc_name,
    raw_source_by_key,
    read_from_source_layer,
    resolve_join_target,
    source_layer_ddl_name,
    source_layer_leaf_name,
    source_layer_naming,
    source_layer_yaml_name,
)

CAT, SCH = "tgt", "ucmv"


def _spec(key, source, joins=(), dims=(), measures=(), filt="", sql=""):
    return MetricViewSpec(
        fact_table_key=key,
        source_table=source,
        view_name=f"ucm_{key.lower()}",
        comment="c",
        joins=list(joins),
        dimensions=list(dims),
        measures=list(measures),
        untranslatable=[],
        source_filter=filt,
        source_sql=sql,
    )


def _ok(key):
    return {"ddl": f"CREATE OR REPLACE VIEW {key}", "todo_steps": [], "error": None}


RAW = {
    "Fact_Sales": "cat.sch.sales",
    "Dim_Plant": "cat.sch.plant",
    "Dim_Material": "cat.sch.material",
}


class TestConfigReaders:
    def test_read_mode_defaults_off(self):
        assert read_from_source_layer({}) is False
        assert read_from_source_layer(None) is False

    @pytest.mark.parametrize(
        "raw,expected",
        [(True, True), ("true", True), ("Yes", True), ("false", False), (False, False)],
    )
    def test_read_mode_accepts_bool_and_string(self, raw, expected):
        assert read_from_source_layer({"read_from_source_layer": raw}) is expected

    def test_naming_defaults_to_src_prefix(self):
        assert source_layer_naming({}) == "src_prefix"

    def test_naming_rejects_unknown_mode(self):
        with pytest.raises(ValueError, match="source_layer_naming"):
            source_layer_naming({"source_layer_naming": "prefix_all"})

    def test_calc_columns_by_key_keeps_names_only(self):
        cfg = {
            "calculated_columns": {
                "Cal": [{"name": "label", "expression": "x"}, {"expression": "no name"}]
            }
        }
        assert calc_columns_by_key(cfg) == {"Cal": frozenset({"label"})}
        assert calc_columns_by_key({"calculated_columns": "bad"}) == {}


class TestNaming:
    @pytest.mark.parametrize(
        "naming,expected",
        [("src_prefix", "src_fact_sales"), ("table_name", "fact_sales")],
    )
    def test_leaf_name_per_mode(self, naming, expected):
        assert source_layer_leaf_name("Fact Sales", naming) == expected

    def test_leaf_name_rejects_unknown_mode(self):
        with pytest.raises(ValueError):
            source_layer_leaf_name("Fact", "bogus")

    def test_ddl_name_back_quotes_every_part(self):
        assert (
            source_layer_ddl_name("acme_catalog", "sales", "src_fact_nps")
            == "`acme_catalog`.`sales`.`src_fact_nps`"
        )

    def test_yaml_name_bare_when_plain_quoted_when_not(self):
        assert source_layer_yaml_name("tgt", "ucmv", "src_fact") == "tgt.ucmv.src_fact"
        assert (
            source_layer_yaml_name("acme-x", "ucmv", "src_fact")
            == "`acme-x`.ucmv.src_fact"
        )

    def test_yaml_and_ddl_agree_on_the_same_object(self):
        leaf = source_layer_leaf_name("Dim_Plant", "table_name")
        ddl = source_layer_ddl_name(CAT, SCH, leaf)
        yml = source_layer_yaml_name(CAT, SCH, leaf)
        assert quote_uc_name(ddl.replace("`", "")) == quote_uc_name(
            yml.replace("`", "")
        )


class TestJoinResolution:
    def test_resolves_full_and_dotted_suffix_names(self):
        raw = {"Dim_Plant": "cat.sch.plant"}
        assert resolve_join_target("cat.sch.plant", raw) == "Dim_Plant"
        assert resolve_join_target("sch.plant", raw) == "Dim_Plant"
        assert resolve_join_target("`cat`.`sch`.`plant`", raw) == "Dim_Plant"

    def test_inline_sql_is_not_a_table_reference(self):
        raw = {"Dim_Plant": "cat.sch.plant"}
        assert (
            resolve_join_target("(SELECT * FROM cat.sch.plant QUALIFY 1)", raw) is None
        )

    def test_ambiguous_names_resolve_to_nothing(self):
        raw = {"A": "x.s.plant", "B": "y.s.plant"}
        assert resolve_join_target("s.plant", raw) is None


class TestBindMetricViews:
    def _bind(self, specs, source_layer=None, naming="src_prefix", calc=None):
        layer = source_layer or {k: _ok(k) for k in RAW}
        return bind_metric_views(
            specs,
            layer,
            catalog=CAT,
            schema=SCH,
            naming=naming,
            raw_by_key=RAW,
            calc_columns=calc or {},
        )

    def _fact_with_two_dims(self):
        joins = [
            {
                "name": "dim_plant",
                "source": "cat.sch.plant",
                "join_on": "source.plant_code = dim_plant.plant_code",
            },
            {
                "name": "dim_material",
                "source": "cat.sch.material",
                "join_on": "source.mat_code = dim_material.mat_code",
            },
        ]
        dims = [{"name": "plant_name", "expr": "dim_plant.plant_name"}]
        return _spec(
            "Fact_Sales",
            "cat.sch.sales",
            joins=joins,
            dims=dims,
            filt="region = 'EU'",
            sql="SELECT 1",
        )

    @pytest.mark.parametrize(
        "naming,fact,plant,material",
        [
            (
                "src_prefix",
                "tgt.ucmv.src_fact_sales",
                "tgt.ucmv.src_dim_plant",
                "tgt.ucmv.src_dim_material",
            ),
            (
                "table_name",
                "tgt.ucmv.fact_sales",
                "tgt.ucmv.dim_plant",
                "tgt.ucmv.dim_material",
            ),
        ],
    )
    def test_fact_and_two_dims_bind_in_both_naming_modes(
        self, naming, fact, plant, material
    ):
        bound, report = self._bind(
            {"Fact_Sales": self._fact_with_two_dims()}, naming=naming
        )
        spec = bound["Fact_Sales"]
        assert spec.source_table == fact
        assert spec.source_filter == "" and spec.source_sql == ""
        sources = {j["name"]: j["source"] for j in spec.joins}
        assert sources == {"dim_plant": plant, "dim_material": material}
        assert report.bound_facts == ["Fact_Sales"]
        assert report.bound_joins == {"Fact_Sales": ["dim_plant", "dim_material"]}
        assert report.folded_facts == ["Fact_Sales"]

    def test_input_specs_are_not_mutated(self):
        original = self._fact_with_two_dims()
        self._bind({"Fact_Sales": original})
        assert original.source_table == "cat.sch.sales"
        assert original.joins[0]["source"] == "cat.sch.plant"
        assert original.source_filter == "region = 'EU'"

    def test_fact_without_usable_view_keeps_raw_binding_with_reason(self):
        layer = {
            "Fact_Sales": {"ddl": "-- ERROR", "todo_steps": [], "error": "boom"},
            "Dim_Plant": _ok("Dim_Plant"),
        }
        bound, report = self._bind(
            {"Fact_Sales": self._fact_with_two_dims()}, source_layer=layer
        )
        spec = bound["Fact_Sales"]
        assert spec.source_table == "cat.sch.sales"
        assert spec.source_filter == "region = 'EU'"
        assert "boom" in report.fallback_facts["Fact_Sales"]
        # Joins still bind where the dimension has a usable view.
        sources = {j["name"]: j["source"] for j in spec.joins}
        assert sources["dim_plant"] == "tgt.ucmv.src_dim_plant"
        assert sources["dim_material"] == "cat.sch.material"
        assert report.fallback_joins["Fact_Sales"]["dim_material"].startswith(
            "`Dim_Material`"
        )

    def test_live_connection_fact_with_no_planned_view_falls_back(self):
        spec = _spec("Live", "")
        bound, report = self._bind({"Live": spec}, source_layer={})
        assert bound["Live"].source_table == ""
        assert "live connection" in report.fallback_facts["Live"]

    def test_inline_join_source_is_left_raw_with_reason(self):
        joins = [
            {
                "name": "dim_plant",
                "source": "(SELECT * FROM cat.sch.plant QUALIFY 1)",
                "join_on": "x",
            }
        ]
        bound, report = self._bind(
            {"Fact_Sales": _spec("Fact_Sales", "cat.sch.sales", joins=joins)}
        )
        assert bound["Fact_Sales"].joins[0]["source"].startswith("(SELECT")
        assert (
            "not a plain PBI table reference"
            in report.fallback_joins["Fact_Sales"]["dim_plant"]
        )

    def test_calculated_column_reference_is_reported_as_untranslated(self):
        measure = SimpleNamespace(sql_expr="SUM(dim_plant.plant_score)")
        spec = _spec(
            "Fact_Sales",
            "cat.sch.sales",
            joins=[{"name": "dim_plant", "source": "cat.sch.plant", "join_on": "x"}],
            measures=[measure],
        )
        _, report = self._bind(
            {"Fact_Sales": spec}, calc={"Dim_Plant": frozenset({"plant_score"})}
        )
        assert report.untranslated_columns == ["tgt.ucmv.src_dim_plant.plant_score"]

    def test_period_labels_come_from_the_calendar_view(self):
        from src.services.tools.metric_view_utils.period_folding import (
            fold_period_dimensions,
        )

        raw = {**RAW, "Dim_Calendar": "cat.sch.calendar"}
        folded = fold_period_dimensions(
            "dim_calendar",
            ["Date", "fiscper", "Past_flag", "Latest_Month_Label", "Latest_Year_Label"],
        )
        assert folded, "period folding should produce canonical period dimensions"
        spec = _spec(
            "Fact_Sales",
            "cat.sch.sales",
            joins=[
                {"name": "dim_calendar", "source": "cat.sch.calendar", "join_on": "x"}
            ],
            dims=folded,
        )
        layer = {k: _ok(k) for k in raw}
        bound, _ = bind_metric_views(
            {"Fact_Sales": spec},
            layer,
            catalog=CAT,
            schema=SCH,
            naming="src_prefix",
            raw_by_key=raw,
            calc_columns={},
        )
        assert bound["Fact_Sales"].joins[0]["source"] == "tgt.ucmv.src_dim_calendar"
        assert all(
            d["expr"].startswith("dim_calendar.")
            for d in bound["Fact_Sales"].dimensions
        )


class TestRawSourceIndex:
    def test_spec_source_wins_and_mquery_only_tables_are_included(self):
        specs = {"Fact_Sales": SimpleNamespace(source_table="cat.sch.sales")}
        mq = {
            "Fact_Sales": SimpleNamespace(source_table="ignored.x.y"),
            "Dim_Plant": SimpleNamespace(source_table="cat.sch.plant"),
            "Empty": SimpleNamespace(source_table=""),
        }
        assert raw_source_by_key(specs, mq) == {
            "Fact_Sales": "cat.sch.sales",
            "Dim_Plant": "cat.sch.plant",
        }


class TestBindingReport:
    def test_report_lines_mention_fallbacks_and_untranslated(self):
        report = SourceLayerBinding(
            naming="src_prefix",
            fallback_facts={"Live": "no planned view"},
            fallback_joins={"Fact_Sales": {"dim_plant": "not rebound"}},
            untranslated_columns=["tgt.ucmv.src_dim_plant.plant_score"],
        )
        text = "\n".join(binding_report_lines(report))
        assert "## Source-Layer Binding" in text
        assert "`Live`: no planned view" in text
        assert "dim_plant" in text
        assert "plant_score" in text

    def test_disabled_report_says_why(self):
        text = "\n".join(
            binding_report_lines(SourceLayerBinding.disabled("bad naming"))
        )
        assert "not applied: bad naming" in text

    def test_to_dict_is_json_ready(self):
        import json

        report = SourceLayerBinding(naming="src_prefix", views={"F": "t.s.src_f"})
        assert json.loads(json.dumps(report.to_dict()))["views"] == {"F": "t.s.src_f"}
