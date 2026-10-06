"""Tests for column-based measure allocation (KASAL_FIXES M6 / M7 / M10).

Builds a small two-fact model with known columns and asserts that measures are
homed by the columns their DAX references — an unallocated measure is rescued
(M6), a mis-homed measure is re-homed (M7), a cross-fact ratio is detected, and
a measure referencing a non-existent column is flagged broken (M10).
"""

from src.services.tools.metric_view_utils.data_classes import TableInfo
from src.services.tools.metric_view_utils.measure_allocator import (
    BROKEN_KEY,
    CROSS_FACT_KEY,
    REHOMED_KEY,
    allocate,
    build_fact_column_index,
    build_fact_grain_index,
    build_known_columns,
    build_measure_column_index,
    candidate_fact_keys,
    extract_column_refs,
    extract_measure_refs,
    reallocate_by_columns,
    resolve_effective_columns,
)


# ── Fixtures: a two-fact model ───────────────────────────────────────────────
def _fact_otc() -> TableInfo:
    # OTC fact: KBI value column `fltp` + code column `bic_csubkbi`, grain
    # fiscper + country.
    return TableInfo(
        table_name="Fact_OTC",
        source_table="cat.sch.otc",
        aggregate_columns=[{"name": "fltp", "source_col": "fltp"}],
        group_by_columns=["fiscper", "country", "bic_csubkbi"],
        calculated_columns=[],
        is_fact=True,
        full_sql="",
    )


def _fact_ai() -> TableInfo:
    # AI-invoice fact: invoice counts + zru + a credit-note flag column.
    return TableInfo(
        table_name="AI_Invoice",
        source_table="cat.sch.ai",
        aggregate_columns=[
            {"name": "global_no_of_invoices", "source_col": "global_no_of_invoices"},
            {"name": "zru", "source_col": "zru"},
        ],
        group_by_columns=["fiscper", "country", "bill_type", "bic_ccrednum"],
        calculated_columns=[],
        is_fact=True,
        full_sql="",
    )


def _dim_sku() -> TableInfo:
    # A NON-fact table a measure may "live on" in PBI (M7): it has its own
    # columns but the measure references OTC columns.
    return TableInfo(
        table_name="SKU",
        source_table="cat.sch.sku",
        aggregate_columns=[{"name": "grand_total", "source_col": "grand_total"}],
        group_by_columns=["sku_id"],
        calculated_columns=[],
        is_fact=True,
        full_sql="",
    )


def _tables() -> dict:
    return {"Fact_OTC": _fact_otc(), "AI_Invoice": _fact_ai(), "SKU": _dim_sku()}


# ── Pure extraction ──────────────────────────────────────────────────────────
def test_extract_column_refs_vs_measure_refs():
    dax = "DIVIDE(SUM(Fact_OTC[fltp]), [Some Measure]) + 'AI Invoice'[zru]"
    assert extract_column_refs(dax) == {"fltp", "zru"}
    assert extract_measure_refs(dax) == {"somemeasure"}


def test_norm_is_case_and_space_insensitive():
    assert extract_column_refs("t[NPS Responses]") == {"npsresponses"}
    assert extract_column_refs("t[npsresponses]") == {"npsresponses"}


def test_resolve_effective_columns_follows_measure_refs():
    mapping = [
        {"measure_name": "Responses", "dax_expression": "SUM(CustExp[NPSResponses])"},
        {
            "measure_name": "Pulsed",
            "dax_expression": "SUM(CustExp[CustomerPulsedTotal])",
        },
    ]
    cols_idx, mref_idx = build_measure_column_index(mapping)
    eff = resolve_effective_columns("DIVIDE([Responses], [Pulsed])", cols_idx, mref_idx)
    assert eff == {"npsresponses", "customerpulsedtotal"}


# ── allocate() decisions ─────────────────────────────────────────────────────
def _indexes():
    tables = _tables()
    keys = {"Fact_OTC", "AI_Invoice", "SKU"}
    return (
        build_fact_column_index(tables, keys),
        build_fact_grain_index(tables, keys),
        build_known_columns(tables),
    )


def test_allocate_single_fact():
    fi, gi, known = _indexes()
    d = allocate({"fltp", "bic_csubkbi"}, fi, gi, known)
    assert d.kind == "single"
    assert d.fact == "Fact_OTC"


def test_allocate_broken_column():
    fi, gi, known = _indexes()
    d = allocate({"pct_sku_in_active"}, fi, gi, known)
    assert d.kind == "broken"
    assert "pct_sku_in_active" in d.unknown_columns


def test_allocate_cross_fact_reports_shared_grain():
    fi, gi, known = _indexes()
    # zru is on AI_Invoice, fltp is on Fact_OTC — no single fact owns both.
    d = allocate({"zru", "fltp"}, fi, gi, known)
    assert d.kind == "cross_fact"
    assert set(d.facts) == {"AI_Invoice", "Fact_OTC"}
    # Both facts share fiscper + country grain.
    assert set(d.shared_grain) == {"fiscper", "country"}


def test_allocate_no_columns_for_artifact():
    fi, gi, known = _indexes()
    assert allocate(set(), fi, gi, known).kind == "no_columns"


def test_candidate_fact_keys_excludes_pure_dim_without_alloc():
    tables = _tables()
    tables["Dim_Country"] = TableInfo(
        table_name="Dim_Country",
        source_table="cat.sch.country",
        aggregate_columns=[],
        group_by_columns=["country"],
        calculated_columns=[],
        is_fact=False,
        full_sql="",
    )
    keys = candidate_fact_keys(tables, mapping=[])
    assert "Dim_Country" not in keys  # not a fact, not an allocation target
    assert {"Fact_OTC", "AI_Invoice", "SKU"} <= keys


# ── reallocate_by_columns() orchestration ────────────────────────────────────
def test_m6_rescues_unallocated_measure():
    mapping = [
        {
            "measure_name": "Sum OTC FLTP",
            "original_name": "Sum OTC FLTP",
            "dax_expression": "SUM(Fact_OTC[fltp])",
            # no allocation at all
        }
    ]
    report = reallocate_by_columns(mapping, _tables())
    assert mapping[0]["proposed_allocation"] == "Fact_OTC"
    assert report["rescued"] and report["rescued"][0]["to"] == "Fact_OTC"


def test_m7_rehomes_measure_off_the_table_it_lives_on():
    # Lives on SKU in PBI but references OTC columns → re-home to Fact_OTC.
    mapping = [
        {
            "measure_name": "pct settlement same day",
            "original_name": "% of settlement done same day as delivery",
            "dax_expression": (
                "DIVIDE(CALCULATE(SUM(Fact_OTC[fltp]), Fact_OTC[bic_csubkbi] "
                'IN {"KIOM03312"}), SUM(Fact_OTC[fltp]))'
            ),
            "all_allocations": [{"table": "SKU", "role": "primary"}],
            "proposed_allocation": "SKU",
        }
    ]
    report = reallocate_by_columns(mapping, _tables())
    assert mapping[0]["proposed_allocation"] == "Fact_OTC"
    assert mapping[0][REHOMED_KEY] == {"from": "SKU", "to": "Fact_OTC"}
    assert report["rehomed"][0]["measure"].startswith("% of settlement")


def test_m10_flags_broken_measure_and_unhomes_it():
    mapping = [
        {
            "measure_name": "pct sku inactive v2",
            "original_name": "% of SKU's Inactive v2",
            "dax_expression": "SUM(SKU[% SKU In Active])",
            "all_allocations": [{"table": "SKU", "role": "primary"}],
            "proposed_allocation": "SKU",
        }
    ]
    report = reallocate_by_columns(mapping, _tables())
    assert mapping[0]["proposed_allocation"] == "__unassigned__"
    assert mapping[0]["all_allocations"] == []
    assert BROKEN_KEY in mapping[0]
    assert report["broken"][0]["measure"] == "% of SKU's Inactive v2"


def test_cross_fact_ratio_is_unhomed_and_marked():
    mapping = [
        {
            "measure_name": "Responses",
            "original_name": "Responses",
            "dax_expression": "SUM(AI_Invoice[zru])",
            "proposed_allocation": "AI_Invoice",
        },
        {
            "measure_name": "OTC vol",
            "original_name": "OTC vol",
            "dax_expression": "SUM(Fact_OTC[fltp])",
            "proposed_allocation": "Fact_OTC",
        },
        {
            "measure_name": "Cross Ratio",
            "original_name": "Cross Ratio",
            "dax_expression": "DIVIDE([Responses], [OTC vol])",
            # unallocated
        },
    ]
    report = reallocate_by_columns(mapping, _tables())
    ratio = mapping[2]
    assert ratio["proposed_allocation"] == "__unassigned__"
    assert CROSS_FACT_KEY in ratio
    assert set(ratio[CROSS_FACT_KEY]["facts"]) == {"AI_Invoice", "Fact_OTC"}
    assert report["cross_fact"]


def test_correctly_homed_measure_is_untouched():
    mapping = [
        {
            "measure_name": "Order Accuracy",
            "original_name": "Order Accuracy",
            "dax_expression": "SUM(Fact_OTC[fltp])",
            "all_allocations": [{"table": "Fact_OTC", "role": "primary"}],
            "proposed_allocation": "Fact_OTC",
        }
    ]
    report = reallocate_by_columns(mapping, _tables())
    assert mapping[0]["all_allocations"] == [{"table": "Fact_OTC", "role": "primary"}]
    assert not any(report[k] for k in ("rescued", "rehomed", "broken", "cross_fact"))


def test_disabled_returns_empty_when_no_tables():
    assert reallocate_by_columns([], {}) == {
        "rescued": [],
        "rehomed": [],
        "broken": [],
        "cross_fact": [],
        "ambiguous": [],
    }


def test_plain_source_wildcard_fact_is_not_disturbed():
    # A `SELECT *` source exposes unknown columns — its measure must be left
    # for the measure-driven-fact promotion path, never flagged broken.
    tables = _tables()
    tables["Plain"] = TableInfo(
        table_name="Plain",
        source_table="cat.sch.plain",
        aggregate_columns=[],
        group_by_columns=[],
        calculated_columns=[],
        is_fact=False,
        full_sql="",
    )
    mapping = [
        {
            "measure_name": "T",
            "original_name": "T",
            "dax_expression": "SUM(Plain[Amount])",
            "proposed_allocation": "Plain",
        }
    ]
    report = reallocate_by_columns(mapping, tables)
    assert mapping[0]["proposed_allocation"] == "Plain"
    assert not report["broken"]


# ── End-to-end through the pipeline ──────────────────────────────────────────
class TestAllocatorThroughPipeline:
    """M6/M7/M10 flowing through MetricViewPipeline + the none_allocated
    catch-all (proves the Phase-0 wiring, not just the pure functions)."""

    def _pipeline(self, measures):
        from src.services.tools.metric_view_utils.mquery_parser import MQueryParser
        from src.services.tools.metric_view_utils.pipeline import MetricViewPipeline

        mquery = [
            {
                "table_name": "Fact_OTC",
                "transpiled_sql": "SELECT fiscper, country, bic_csubkbi, SUM(fltp) AS fltp FROM cat.sch.otc GROUP BY fiscper, country, bic_csubkbi",
                "validation_passed": "Yes",
            },
            {
                "table_name": "AI_Invoice",
                "transpiled_sql": "SELECT fiscper, country, SUM(zru) AS zru FROM cat.sch.ai GROUP BY fiscper, country",
                "validation_passed": "Yes",
            },
            {
                "table_name": "SKU",
                "transpiled_sql": "SELECT sku_id, SUM(grand_total) AS grand_total FROM cat.sch.sku GROUP BY sku_id",
                "validation_passed": "Yes",
            },
        ]
        return MetricViewPipeline(
            mapping=measures, mquery_tables=MQueryParser().parse_json(mquery), config={}
        )

    def test_m6_and_m7_land_on_the_right_spec(self):
        measures = [
            {
                "measure_name": "TotalZRU",
                "original_name": "TotalZRU",
                "dax_expression": "SUM(AI_Invoice[zru])",
            },  # M6: unallocated
            {
                "measure_name": "pct settle",
                "original_name": "% settle",
                "dax_expression": "SUM(Fact_OTC[fltp])",  # M7: homed on SKU
                "all_allocations": [{"table": "SKU", "role": "primary"}],
                "proposed_allocation": "SKU",
            },
        ]
        pipe = self._pipeline(measures)
        specs = pipe.run()
        assert "totalzru" in {m.measure_name for m in specs["AI_Invoice"].measures}
        assert "pct_settle" in {m.measure_name for m in specs["Fact_OTC"].measures}
        assert "pct_settle" not in {m.measure_name for m in specs["SKU"].measures}

    def test_m10_broken_measure_documented_in_none_allocated(self):
        from src.services.tools.metric_view_utils.none_allocated_emitter import (
            build_none_allocated_yaml,
        )

        measures = [
            {
                "measure_name": "pct sku inactive v2",
                "original_name": "% of SKU's Inactive v2",
                "dax_expression": "SUM(SKU[% SKU In Active])",
                "all_allocations": [{"table": "SKU", "role": "primary"}],
                "proposed_allocation": "SKU",
            },
        ]
        pipe = self._pipeline(measures)
        pipe.run()
        # Not emitted on the SKU fact.
        assert "pct_sku_inactive_v2" not in {
            m.measure_name for m in pipe.all_specs["SKU"].measures
        }
        yaml_out = pipe.emit_all_yaml(catalog="main", schema="m")
        na = build_none_allocated_yaml(
            measures,
            pipe.all_specs,
            yaml_out,
            pipe.translator,
            pipe._PBI_ARTIFACT_PATTERNS,
        )
        assert na is not None
        assert "Inactive v2" in na and "broken in PBI" in na
