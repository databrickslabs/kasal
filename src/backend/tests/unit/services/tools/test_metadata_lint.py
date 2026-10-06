"""Tests for the UC metric-view metadata lint pass.

One test per rule (a violating case + a clean case), plus:
- the gold-standard fixture (built from tsc_ucm_fact_pe002_new.yml's shape)
  producing zero ERROR-level findings;
- the object-input path (a MetricViewSpec-like object with TranslationResult
  measures) to lock the alternate input contract;
- the summary/format helpers.
"""

from src.services.tools.metric_view_utils.metadata_lint import (
    RULE_ABSOLUTE_UNIT_SUFFIX,
    RULE_DUPLICATE_DIMENSION,
    RULE_KPI_SCENARIO_SUFFIX,
    RULE_MISSING_DISPLAY_NAME,
    RULE_SNAKE_CASE,
    RULE_SYNONYM_CLASH,
    RULE_SYNONYM_LIMIT,
    RULE_WEAK_DESCRIPTION,
    SEVERITY_ERROR,
    LintFinding,
    format_findings,
    lint_metric_view,
    summarize_findings,
)


def _rule_ids(findings):
    return {f.rule_id for f in findings}


def _findings_for(findings, rule_id):
    return [f for f in findings if f.rule_id == rule_id]


def _measure(name, expr="SUM(source.x)", **kw):
    entry = {
        "name": name,
        "expr": expr,
        "display_name": kw.get("display_name", "Display"),
        "synonyms": kw.get("synonyms", []),
        "comment": kw.get("comment", "A sufficiently long description of it."),
    }
    if "format" in kw:
        entry["format"] = kw["format"]
    return entry


def _dimension(name, expr=None, **kw):
    return {
        "name": name,
        "expr": expr or f"source.{name}",
        "display_name": kw.get("display_name", "Display"),
        "synonyms": kw.get("synonyms", []),
        "comment": kw.get("comment", "A sufficiently long description of it."),
    }


# ─── Rule MV001 — snake_case ─────────────────────────────────────────────────


class TestSnakeCase:
    def test_violation(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure("EPL %"),
                _measure("gross+net"),
                _measure("Total Hours"),
            ],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_SNAKE_CASE)
        assert len(findings) == 3
        assert all(f.severity == SEVERITY_ERROR for f in findings)
        # suggestion mirrors the playbook substitutions
        pct = next(f for f in findings if f.field == "EPL %")
        assert "epl_pct" in pct.message

    def test_clean(self):
        spec = {
            "dimensions": [_dimension("plant_code")],
            "measures": [_measure("epl_hours"), _measure("total_sle_actual")],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_SNAKE_CASE)


# ─── Rule MV002 — KPI scenario suffix ────────────────────────────────────────


class TestKpiScenarioSuffix:
    def test_violation_ratio(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure("total_sle", expr="SUM(source.num) / SUM(source.den)")
            ],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_KPI_SCENARIO_SUFFIX)
        assert len(findings) == 1
        assert findings[0].field == "total_sle"

    def test_violation_percentage_format(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure(
                    "asset_utilization",
                    expr="SUM(source.up)",
                    format={"type": "percentage"},
                )
            ],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_KPI_SCENARIO_SUFFIX)
        assert len(findings) == 1

    def test_clean(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure("total_sle_actual", expr="SUM(source.num) / SUM(source.den)"),
                _measure("cost_to_supply_actual_per_uc", expr="SUM(a)/SUM(b)"),
                # a plain summed building block is not a KPI — no suffix needed
                _measure("paid_hours", expr="SUM(source.paid_hours)"),
            ],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_KPI_SCENARIO_SUFFIX)


# ─── Rule MV003 — absolute unit suffix ───────────────────────────────────────


class TestAbsoluteUnitSuffix:
    def test_violation(self):
        # the playbook's concrete failure: bare `epl` (hours) summed raw column
        spec = {
            "dimensions": [],
            "measures": [_measure("epl", expr="SUM(source.epl)")],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_ABSOLUTE_UNIT_SUFFIX)
        assert len(findings) == 1
        assert findings[0].field == "epl"

    def test_clean(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure("epl_hours", expr="SUM(source.epl)"),
                _measure("line_output_uc", expr="SUM(source.production_quantity)"),
                # a ratio is not an absolute building block
                _measure("epl_actual", expr="SUM(source.epl)/SUM(source.paid)"),
            ],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_ABSOLUTE_UNIT_SUFFIX)


# ─── Rule MV004 — duplicate dimensions ───────────────────────────────────────


class TestDuplicateDimensions:
    def test_violation_alias_group(self):
        spec = {
            "dimensions": [
                _dimension("company_code"),
                _dimension("comp_code"),
                _dimension("fiscal_period"),
                _dimension("fiscper"),
            ],
            "measures": [],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_DUPLICATE_DIMENSION)
        assert len(findings) == 2

    def test_violation_name_as_synonym(self):
        spec = {
            "dimensions": [
                _dimension("plant_code", synonyms=["plant_name"]),
                _dimension("plant_name"),
            ],
            "measures": [],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_DUPLICATE_DIMENSION)
        assert len(findings) == 1

    def test_clean(self):
        spec = {
            "dimensions": [
                _dimension("plant_code"),
                _dimension("plant_name"),
                _dimension("fiscal_year"),
                _dimension("region"),
            ],
            "measures": [],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_DUPLICATE_DIMENSION)


# ─── Rule MV005 — synonym clash ──────────────────────────────────────────────


class TestSynonymClash:
    def test_violation(self):
        # the playbook's worst case: "country" on two different fields
        spec = {
            "dimensions": [
                _dimension("geo_country", synonyms=["country", "nation"]),
                _dimension("country_group", synonyms=["Country", "group"]),
            ],
            "measures": [],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_SYNONYM_CLASH)
        assert len(findings) == 1
        assert findings[0].severity == SEVERITY_ERROR
        assert "country" in findings[0].message.lower()

    def test_clean(self):
        spec = {
            "dimensions": [
                _dimension("geo_country", synonyms=["country", "nation"]),
                _dimension("country_group", synonyms=["group", "cluster"]),
            ],
            "measures": [],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_SYNONYM_CLASH)


# ─── Rule MV006 — synonym limit ──────────────────────────────────────────────


class TestSynonymLimit:
    def test_violation(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure("m1", synonyms=[f"s{i}" for i in range(11)]),
            ],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_SYNONYM_LIMIT)
        assert len(findings) == 1
        assert findings[0].severity == SEVERITY_ERROR

    def test_clean(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure("m1", synonyms=[f"s{i}" for i in range(10)]),
            ],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_SYNONYM_LIMIT)


# ─── Rule MV007 — display_name present ───────────────────────────────────────


class TestDisplayName:
    def test_violation(self):
        spec = {
            "dimensions": [_dimension("region", display_name="")],
            "measures": [_measure("epl_hours", display_name="")],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_MISSING_DISPLAY_NAME)
        assert len(findings) == 2

    def test_clean(self):
        spec = {
            "dimensions": [_dimension("region", display_name="Region")],
            "measures": [_measure("epl_hours", display_name="EPL Hours")],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_MISSING_DISPLAY_NAME)


# ─── Rule MV008 — description quality ─────────────────────────────────────────


class TestDescriptionQuality:
    def test_violation_empty_and_short(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure("m_empty", comment=""),
                _measure("m_short", comment="hours"),
            ],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_WEAK_DESCRIPTION)
        assert {f.field for f in findings} == {"m_empty", "m_short"}

    def test_clean(self):
        spec = {
            "dimensions": [],
            "measures": [
                _measure(
                    "m_ok",
                    comment="Total paid hours; unit: hours; default is actual.",
                )
            ],
        }
        assert not _findings_for(lint_metric_view(spec), RULE_WEAK_DESCRIPTION)


# ─── Gold-standard fixture — zero ERROR-level findings ───────────────────────


def _gold_spec():
    """A representative dict fixture mirroring tsc_ucm_fact_pe002_new.yml.

    Shapes match the emitted gold file: snake_case names, unit-suffixed hours
    building blocks, scenario-suffixed KPI ratios, one synonym per field,
    <=10 synonyms, every field with a display_name and a real description.
    """
    return {
        "source": "`cat`.sch.tsc_fact_pe002_new",
        "comment": "PE002 - Line Performance metric view.",
        "dimensions": [
            _dimension(
                "fiscal_year",
                expr="CAST(dim_calendar.Year AS STRING)",
                display_name="Fiscal Year",
                synonyms=["year", "FY", "reporting year"],
                comment="Fiscal year from the calendar dimension (e.g. 2024).",
            ),
            _dimension(
                "fiscal_month",
                expr="LPAD(CAST(dim_calendar.Month AS STRING), 3, '0')",
                display_name="Fiscal Month",
                synonyms=["month", "period"],
                comment="Fiscal month (3-digit period number 001-012).",
            ),
            _dimension(
                "fiscper",
                expr="source.fiscper",
                display_name="Fiscal Year/Period",
                synonyms=["fiscal period", "year period"],
                comment="Fiscal year + period as one STRING 'YYYYPPP'.",
            ),
            _dimension(
                "region",
                expr="dim_region.region",
                display_name="Region",
                synonyms=["geographic region", "area", "territory"],
                comment="Region of the company (aggregate groupings excluded).",
            ),
            _dimension(
                "plant_code",
                expr="source.plant",
                display_name="Plant Code",
                synonyms=["plant id", "werks"],
                comment="SAP plant code identifying the production site.",
            ),
        ],
        "measures": [
            _measure(
                "active_shift_hours",
                expr="SUM(source.active_shift_hours)",
                display_name="Active Shift Hours",
                synonyms=["scheduled hours", "shift hours", "operating hours"],
                comment=(
                    "Building block for asset_utilization_actual. Total active "
                    "shift hours. Unit: hours."
                ),
            ),
            _measure(
                "paid_hours",
                expr="SUM(source.paid_hours)",
                display_name="Paid Hours",
                synonyms=["labour hours", "compensated hours", "staffed hours"],
                comment="Total paid hours; unit: hours; building block.",
            ),
            _measure(
                "epl_hours",
                expr="SUM(source.epl)",
                display_name="External Production Losses (Hours)",
                synonyms=["EPL hours", "external downtime hours"],
                comment="EPL = External Production Losses. Unit: hours.",
            ),
            _measure(
                "line_output_uc",
                expr="SUM(source.production_quantity)",
                display_name="Production Quantity",
                synonyms=["line output quantity", "units produced"],
                comment="Total production quantity in unit cases (UC).",
            ),
            _measure(
                "total_sle_actual",
                expr=(
                    "SUM(source.theoretical_line_hours_rated_speed) "
                    "/ SUM(source.paid_hours)"
                ),
                display_name="Total SLE (Actual)",
                synonyms=["SLE", "system line efficiency", "line efficiency"],
                comment="System Line Efficiency ratio; default KPI; unit: pct.",
                format={"type": "percentage"},
            ),
            _measure(
                "asset_utilization_actual",
                expr="SUM(source.active_shift_hours) / SUM(source.total_hours)",
                display_name="Asset Utilization (Actual)",
                synonyms=["asset utilization", "utilization", "asset use"],
                comment="Asset utilisation ratio; default KPI; unit: pct.",
                format={"type": "percentage"},
            ),
        ],
    }


class TestGoldStandard:
    def test_no_error_level_findings(self):
        findings = lint_metric_view(_gold_spec())
        errors = [f for f in findings if f.severity == SEVERITY_ERROR]
        assert errors == [], f"gold fixture produced ERROR findings: {errors}"

    def test_summary_reports_zero_errors(self):
        summary = summarize_findings(lint_metric_view(_gold_spec()))
        assert summary.has_errors is False
        assert summary.error_count == 0


# ─── Object input path (MetricViewSpec-like) ─────────────────────────────────


class _FakeMeasure:
    """Mimics TranslationResult's measure_name / sql_expr / skip_reason."""

    def __init__(self, measure_name, sql_expr, skip_reason=""):
        self.measure_name = measure_name
        self.sql_expr = sql_expr
        self.skip_reason = skip_reason


class _FakeSpec:
    def __init__(self, dimensions, measures):
        self.dimensions = dimensions
        self.measures = measures


class TestObjectInput:
    def test_translationresult_like_measures(self):
        spec = _FakeSpec(
            dimensions=[{"name": "region", "expr": "dim_region.region"}],
            measures=[
                _FakeMeasure("EPL %", "SUM(source.epl)"),  # snake_case error
                _FakeMeasure("epl", "SUM(source.epl)"),  # unit-suffix warning
            ],
        )
        findings = lint_metric_view(spec)
        assert RULE_SNAKE_CASE in _rule_ids(findings)
        assert RULE_ABSOLUTE_UNIT_SUFFIX in _rule_ids(findings)

    def test_empty_spec_is_clean(self):
        assert lint_metric_view({"dimensions": [], "measures": []}) == []
        assert lint_metric_view({}) == []


# ─── Helpers ─────────────────────────────────────────────────────────────────


class TestSummaryAndFormat:
    def test_summarize_counts(self):
        findings = [
            LintFinding(RULE_SNAKE_CASE, SEVERITY_ERROR, "a", "m"),
            LintFinding(RULE_SNAKE_CASE, SEVERITY_ERROR, "b", "m"),
            LintFinding(RULE_WEAK_DESCRIPTION, "warning", "c", "m"),
        ]
        summary = summarize_findings(findings)
        assert summary.total == 3
        assert summary.error_count == 2
        assert summary.warning_count == 1
        assert summary.by_rule[RULE_SNAKE_CASE] == 2
        assert summary.has_errors is True

    def test_format_clean(self):
        assert format_findings([]) == "metadata-lint: no findings"

    def test_format_orders_errors_first(self):
        findings = [
            LintFinding(RULE_WEAK_DESCRIPTION, "warning", "c", "warn msg"),
            LintFinding(RULE_SNAKE_CASE, SEVERITY_ERROR, "a", "err msg"),
        ]
        out = format_findings(findings)
        assert out.index("[ERROR]") < out.index("[WARNING]")


# ─── LintConfig (crew-supplied, model-specific vocabulary) ───────────────────

from src.services.tools.metric_view_utils.metadata_lint import LintConfig  # noqa: E402


class TestStructuralDuplicateDimension:
    def test_same_source_expr_flagged_without_alias_group(self):
        """Two dims resolving to the same source expr are flagged structurally,
        even for names in no known alias group (the general, vocabulary-free rule)."""
        spec = {
            "dimensions": [
                _dimension("widget_key", expr="dim_w.wkey"),
                _dimension("gadget_ref", expr="dim_w.wkey"),
            ],
            "measures": [],
        }
        findings = _findings_for(lint_metric_view(spec), RULE_DUPLICATE_DIMENSION)
        assert len(findings) == 1
        assert "same source expression" in findings[0].message

    def test_distinct_source_exprs_not_flagged(self):
        spec = {
            "dimensions": [
                _dimension("region", expr="dim_geo.region"),
                _dimension("country", expr="dim_geo.country"),
            ],
            "measures": [],
        }
        assert _findings_for(lint_metric_view(spec), RULE_DUPLICATE_DIMENSION) == []


class TestLintConfigOverride:
    def test_from_dict_custom_scenario_suffixes(self):
        """A model whose scenario vocabulary is budget/forecast: an `_actual`
        ratio is now flagged (not in the list) and `_budget` passes."""
        cfg = LintConfig.from_dict({"scenario_suffixes": ["budget", "forecast", "ly"]})
        spec = {
            "dimensions": [],
            "measures": [
                _measure("margin_actual", expr="SUM(a)/SUM(b)"),
                _measure("margin_budget", expr="SUM(a)/SUM(b)"),
            ],
        }
        flagged = {
            f.field
            for f in _findings_for(
                lint_metric_view(spec, cfg), RULE_KPI_SCENARIO_SUFFIX
            )
        }
        assert "margin_actual" in flagged
        assert "margin_budget" not in flagged

    def test_default_scenario_suffixes_unchanged(self):
        """Without config, the CCH defaults still accept `_actual`."""
        spec = {
            "dimensions": [],
            "measures": [_measure("margin_actual", expr="SUM(a)/SUM(b)")],
        }
        assert _findings_for(lint_metric_view(spec), RULE_KPI_SCENARIO_SUFFIX) == []

    def test_from_dict_custom_unit_suffixes(self):
        cfg = LintConfig.from_dict({"unit_suffixes": ["pallets", "hl"]})
        spec = {
            "dimensions": [],
            "measures": [
                _measure("volume_hl", expr="SUM(source.hl)"),
                _measure("volume_hours", expr="SUM(source.h)"),
            ],
        }
        flagged = {
            f.field
            for f in _findings_for(
                lint_metric_view(spec, cfg), RULE_ABSOLUTE_UNIT_SUFFIX
            )
        }
        # 'hl' is now a known unit → clean; 'hours' no longer known → flagged
        assert "volume_hl" not in flagged
        assert "volume_hours" in flagged

    def test_from_dict_custom_alias_groups(self):
        cfg = LintConfig.from_dict(
            {"dimension_alias_groups": [["sku", "sku_code", "article"]]}
        )
        spec = {
            "dimensions": [
                _dimension("sku", expr="d.a"),
                _dimension("article", expr="d.b"),
            ],
            "measures": [],
        }
        findings = _findings_for(lint_metric_view(spec, cfg), RULE_DUPLICATE_DIMENSION)
        assert any("sku" in f.field and "article" in f.field for f in findings)

    def test_from_dict_ignores_junk_and_falls_back(self):
        cfg = LintConfig.from_dict({"scenario_suffixes": "not-a-list", "bogus": 1})
        # falls back to defaults → `_actual` still recognised
        spec = {
            "dimensions": [],
            "measures": [_measure("m_actual", expr="SUM(a)/SUM(b)")],
        }
        assert (
            _findings_for(lint_metric_view(spec, cfg), RULE_KPI_SCENARIO_SUFFIX) == []
        )

    def test_from_dict_empty_returns_defaults(self):
        assert (
            LintConfig.from_dict({}).scenario_suffixes == LintConfig().scenario_suffixes
        )
        assert LintConfig.from_dict(None).unit_suffixes == LintConfig().unit_suffixes

    def test_custom_min_description_len(self):
        cfg = LintConfig.from_dict({"min_description_len": 100})
        spec = {
            "dimensions": [],
            "measures": [_measure("m", comment="short-ish comment")],
        }
        assert (
            len(_findings_for(lint_metric_view(spec, cfg), RULE_WEAK_DESCRIPTION)) == 1
        )
