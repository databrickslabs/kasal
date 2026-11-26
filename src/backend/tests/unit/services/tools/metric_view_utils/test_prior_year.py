"""Prior-year measures in the pipeline: translated without the shift, emitted with a window."""

from src.services.tools.metric_view_utils import prior_year
from src.services.tools.metric_view_utils.data_classes import (
    MetricViewSpec,
    TranslationResult,
)
from src.services.tools.metric_view_utils.yaml_measure_render import _window_lines

PY = "CALCULATE([Sales], SAMEPERIODLASTYEAR(Calendar[Date]))"


def _entry(name, dax, home):
    return {
        "measure_name": name,
        "original_name": name,
        "dax_expression": dax,
        "proposed_allocation": home,
        "all_allocations": [{"table": home, "role": "primary"}],
    }


def _measure(name, sql, dax="", original=None):
    return TranslationResult(
        measure_name=name,
        original_name=original or name,
        sql_expr=sql,
        is_translatable=sql is not None,
        skip_reason="",
        dax_expression=dax,
        confidence="high",
        category="single_table",
    )


def _spec(dimensions, measures, joins=()):
    return MetricViewSpec(
        fact_table_key="Fact",
        source_table="c.s.fact",
        view_name="fact",
        comment="",
        joins=list(joins),
        dimensions=dimensions,
        measures=measures,
        untranslatable=[],
    )


def test_the_shift_is_taken_out_and_the_measure_follows_its_base():
    mapping = [
        _entry("Sales", "SUM(Fact[amount])", "Fact"),
        _entry("Sales PY", PY, "Calendar"),
        _entry("Plain", "SUM(Fact[qty])", "Fact"),
    ]
    assert prior_year.take_out_shifts(mapping, {}) == 1
    assert mapping[1]["dax_expression"] == "[Sales]"
    assert prior_year.MARK not in mapping[2]
    prior_year.follow_base_measures(mapping)
    assert mapping[1]["proposed_allocation"] == "Fact"
    assert mapping[1]["all_allocations"] == [{"table": "Fact", "role": "primary"}]


def test_switched_off_nothing_changes():
    mapping = [_entry("Sales PY", PY, "Calendar")]
    assert prior_year.take_out_shifts(mapping, {"prior_year_measures": False}) == 0
    assert mapping[0]["dax_expression"] == PY


def test_a_period_key_view_gets_a_numeric_key_and_a_window():
    mapping = [_entry("Sales PY", PY, "Fact")]
    prior_year.take_out_shifts(mapping, {})
    spec = _spec(
        [
            {"name": "fiscper", "expr": "cal.fiscper"},
            {"name": "country", "expr": "source.country"},
        ],
        [
            _measure("sales", "SUM(source.amount)", original="Sales"),
            _measure("sales_py", "MEASURE(sales)", "[Sales]", "Sales PY"),
            _measure("growth", "MEASURE(sales) - MEASURE(sales_py)"),
        ],
    )
    done = prior_year.add_windows({"Fact": spec}, mapping, {})
    assert done == {"Fact": ["sales_py"]}
    assert [d["name"] for d in spec.dimensions] == ["fiscper", "fiscper_key", "country"]
    assert spec.dimensions[1]["expr"] == "TRY_CAST(cal.fiscper AS INT)"
    windowed = spec.measures[1]
    assert windowed.window_spec == {
        "order": "fiscper_key",
        "range": "current",
        "offset": -1000,
        "semiadditive": "last",
    }
    # the measure and the model's measure list carry the original DAX again
    assert windowed.dax_expression == PY
    assert mapping[0]["dax_expression"] == PY and prior_year.MARK not in mapping[0]
    assert spec.measures[0].window_spec is None and spec.measures[2].window_spec is None


def test_a_month_key_steps_by_a_hundred_and_a_configured_column_by_its_own_step():
    for dimension, config, step in (
        ("calmonth", {}, -100),
        ("fy_period", {"prior_year_period_steps": {"fy_period": 100}}, -100),
    ):
        mapping = [_entry("Sales PY", PY, "Fact")]
        prior_year.take_out_shifts(mapping, config)
        spec = _spec(
            [{"name": dimension, "expr": f"source.{dimension}"}],
            [_measure("sales_py", "SUM(source.amount)", "", "Sales PY")],
        )
        prior_year.add_windows({"Fact": spec}, mapping, config)
        assert spec.measures[0].window_spec["offset"] == step
        assert spec.measures[0].window_spec["order"] == f"{dimension}_key"


def test_without_a_period_key_the_joined_calendar_date_gives_the_month():
    config = {
        "join_key_map": {
            "Calendar": {
                "alias": "dim_calendar",
                "dim_key": "date",
                "dim_columns": ["year", "month_name"],
            }
        }
    }
    mapping = [_entry("Sales PY", PY, "Fact")]
    prior_year.take_out_shifts(mapping, config)
    spec = _spec(
        [{"name": "country", "expr": "source.country"}],
        [_measure("sales_py", "SUM(source.amount)", "", "Sales PY")],
        joins=[{"name": "dim_calendar", "on": "source.order_date = dim_calendar.date"}],
    )
    prior_year.add_windows({"Fact": spec}, mapping, config)
    assert spec.dimensions[-1] == {
        "name": "date_month",
        "expr": "DATE_TRUNC('MONTH', TRY_CAST(dim_calendar.date AS DATE))",
        "comment": spec.dimensions[-1]["comment"],
    }
    assert spec.measures[0].window_spec["order"] == "date_month"
    assert spec.measures[0].window_spec["offset"] == "-1 year"


def test_a_view_without_a_period_does_not_emit_the_measure_or_what_reads_it():
    mapping = [_entry("Sales PY", PY, "Fact")]
    prior_year.take_out_shifts(mapping, {})
    spec = _spec(
        [{"name": "country", "expr": "source.country"}],
        [
            _measure("sales", "SUM(source.amount)", original="Sales"),
            _measure("sales_py", "MEASURE(sales)", "[Sales]", "Sales PY"),
            _measure("growth", "MEASURE(sales) - MEASURE(sales_py)"),
            _measure("growth_pct", "MEASURE(growth) / MEASURE(sales)"),
        ],
    )
    assert prior_year.add_windows({"Fact": spec}, mapping, {}) == {}
    assert [m.measure_name for m in spec.measures] == ["sales"]
    gone = {m.measure_name: m for m in spec.untranslatable}
    assert set(gone) == {"sales_py", "growth", "growth_pct"}
    assert "no period" in gone["sales_py"].skip_reason
    assert gone["sales_py"].sql_expr is None and gone["sales_py"].dax_expression == PY
    assert len(spec.dimensions) == 1


def test_an_untranslated_prior_year_measure_gets_its_dax_back():
    mapping = [_entry("Sales PY", PY, "Fact")]
    prior_year.take_out_shifts(mapping, {})
    spec = _spec([{"name": "fiscper", "expr": "source.fiscper"}], [])
    spec.untranslatable.append(_measure("sales_py", None, "[Sales]", "Sales PY"))
    prior_year.add_windows({"Fact": spec}, mapping, {})
    assert spec.untranslatable[0].dax_expression == PY
    assert len(spec.dimensions) == 1  # no key without a measure that needs it


def test_the_window_is_written_with_its_offset():
    assert _window_lines(
        {
            "order": "fiscper_key",
            "range": "current",
            "offset": -1000,
            "semiadditive": "last",
        }
    ) == [
        "    window:",
        "      - order: fiscper_key",
        "        range: current",
        "        offset: -1000",
        "        semiadditive: last",
    ]
    assert _window_lines({"order": "week", "range": "current"}) == [
        "    window:",
        "      - order: week",
        "        range: current",
        "        semiadditive: last",
    ]
    assert _window_lines(None) == []


def test_a_period_the_fact_does_not_have_is_not_used_for_the_window():
    # The emitter drops a source.<column> dimension the fact is known not to have; a
    # window ordered by its key would stop the whole view from deploying.
    mapping = [_entry("Sales PY", PY, "Fact")]
    prior_year.take_out_shifts(mapping, {})
    spec = _spec(
        [
            {"name": "fiscper", "expr": "source.fiscper"},
            {"name": "order_date", "expr": "source.order_date"},
        ],
        [
            _measure("sales", "SUM(source.amount)", original="Sales"),
            _measure("sales_py", "MEASURE(sales)", "[Sales]", "Sales PY"),
        ],
    )
    spec.fact_columns = ["order_date", "amount"]
    assert prior_year.add_windows({"Fact": spec}, mapping, {}) == {}
    assert [m.measure_name for m in spec.measures] == ["sales"]
    assert [d["name"] for d in spec.dimensions] == ["fiscper", "order_date"]
    assert "no period" in spec.untranslatable[0].skip_reason


def test_the_emitter_does_not_write_a_window_without_its_dimension():
    from src.services.tools.metric_view_utils.yaml_known_columns import (
        drop_phantom_dimensions,
    )

    window = {"order": "fiscper_key", "range": "current", "offset": -1000}
    sales_py = _measure("sales_py", "MEASURE(sales)")
    sales_py.window_spec = dict(window)
    growth = _measure("growth", "MEASURE(sales) - MEASURE(sales_py)")
    latest = _measure("latest_stock", "SUM(source.stock)")
    latest.window_spec = {"order": "week", "range": "current", "semiadditive": "last"}
    base = [_measure("sales", "SUM(source.amount)")]
    dax = [sales_py, growth, latest]
    spec = _spec(
        [
            {"name": "country", "expr": "source.country"},
            {"name": "week", "expr": "source.week"},
        ],
        base + dax,
    )
    spec.fact_columns = ["country", "amount", "stock", "week"]
    drop_phantom_dimensions(spec, base, [], dax, [])
    # the prior-year measure and its reader go; a window whose dimension is there stays
    assert [m.measure_name for m in dax] == ["latest_stock"]
    gone = {m.measure_name: m.skip_reason for m in spec.untranslatable}
    assert "fiscper_key" in gone["sales_py"] and "window measure" in gone["growth"]

    kept = _measure("sales_py", "MEASURE(sales)")
    kept.window_spec = dict(window)
    spec = _spec(
        [
            {"name": "country", "expr": "source.country"},
            {"name": "fiscper_key", "expr": "TRY_CAST(cal.fiscper AS INT)"},
        ],
        base + [kept],
    )
    spec.fact_columns = ["country", "amount"]
    dax = [kept]
    drop_phantom_dimensions(spec, base, [], dax, [])
    assert dax == [kept] and not spec.untranslatable
