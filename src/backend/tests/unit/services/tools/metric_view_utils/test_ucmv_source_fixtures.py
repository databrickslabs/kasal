"""S4/S5/S7 regression tests against the REAL OTC Management M expressions.

Fixtures in ``_otc_mquery_fixtures`` are copied verbatim from the handoff config
(``proposed_config.table_mquery_expressions``); the golden SQL constants here are
the corresponding bodies from ``source_layer/otc_ucm_materialized_views.sql``. Each
test asserts the parser now recovers what the transpiled SQL had discarded:

* S4 — the full ``Value.NativeQuery`` SQL (derived columns, window functions,
  WHERE filters), not just the FROM table.
* S5 — M transform steps carried into SQL (code remaps → nested ``replace()``),
  and untranslatable steps surfaced as ``-- TODO:`` instead of being dropped.
* S7 — a dimension is not also emitted as a fact join, and a join whose key does
  not exist on the fact is dropped.
"""

from src.services.tools.metric_view_utils.data_classes import TableInfo
from src.services.tools.metric_view_utils.join_detector import JoinDetector
from src.services.tools.metric_view_utils.m_transform_folder import (
    MTransformFolder,
)
from src.services.tools.metric_view_utils.mquery_parser import (
    MQueryParser,
    extract_native_query_sql,
    native_query_as_subquery,
)

from ._otc_mquery_fixtures import (
    DIM_COUNTRY_M,
    FACT_CUSTOMEREXP_M,
    FACT_NPS_M,
    FACT_OTC_M,
)


def _norm(sql: str) -> str:
    """Compare SQL ignoring trailing whitespace the M author left on lines."""
    return "\n".join(line.rstrip() for line in sql.strip().split("\n"))


# Body of otc_ucm_fact_nps in source_layer/otc_ucm_materialized_views.sql — the
# native query the transpiled SQL reduced to `SELECT * FROM cust_exp_fact_data_v2`.
FACT_NPS_GOLDEN_SQL = """SELECT *,
    TO_DATE(concat( LEFT(fiscper, 4),'-', RIGHT(fiscper, 2), '-15')) fiscper_date
    FROM (
SELECT
    cg_id,
    country_id,
    fiscper,
    primary_drivers,
    secondary_drivers,
    nps,
    Classification_ID,
    customer_channel,
    customer_rating,
    COUNT(secondary_drivers) over(partition by cg_id, country_id, fiscper) SecondaryDriversCount,
    customer_rating / COUNT(secondary_drivers) over(partition by cg_id, country_id, fiscper) NPS_Contribution_per_Driver
FROM(
  SELECT * EXCEPT (process_run_id, date_email_sent, date_sent),
  CASE WHEN nps >= 9 THEN 1
         WHEN nps >= 7 AND nps < 9 THEN 0
         ELSE -1
    END AS Customer_Rating,
    CASE WHEN nps >= 9 THEN 1
         WHEN nps >= 7 AND nps < 9 THEN 3
         ELSE 2
    END AS Classification_ID
from
  `dc_datalake_prod_001`.`udm_datamart_cust_exp`.`cust_exp_drivers_nps_contribution`
WHERE
  lower(touchpoint) like 'relationship' and
  secondary_drivers in (
    'Accurately invoiced product order delivery',
    'Availability of products',
    'On-time delivery',
    'Reliability of promises',
    'Responsiveness of sales teams'
  )
)

GROUP BY
    cg_id,
    country_id,
    fiscper,
    primary_drivers,
    secondary_drivers,
    nps,
    Classification_ID,
    customer_rating,
    customer_channel
    )"""


class TestS4NativeQueryExtraction:
    def test_fact_nps_native_sql_matches_source_layer(self):
        """The whole native query is recovered verbatim (S4), not just its FROM
        table — matches the deployed otc_ucm_fact_nps body exactly."""
        sql = extract_native_query_sql(FACT_NPS_M)
        assert sql is not None
        assert _norm(sql) == _norm(FACT_NPS_GOLDEN_SQL)

    def test_fact_nps_keeps_derived_columns_and_filters(self):
        """The columns/filters the transpiled SQL dropped are present."""
        sql = extract_native_query_sql(FACT_NPS_M)
        assert "TO_DATE(concat( LEFT(fiscper, 4)" in sql  # derived date
        assert "NPS_Contribution_per_Driver" in sql  # window ratio
        assert "over(partition by cg_id, country_id, fiscper)" in sql
        assert "lower(touchpoint) like 'relationship'" in sql  # WHERE filter
        assert "Responsiveness of sales teams" in sql  # driver filter
        assert "cust_exp_drivers_nps_contribution" in sql  # real base table
        assert "#(lf)" not in sql  # M escapes undone

    def test_fact_otc_native_sql_selects_fiscper_date(self):
        sql = extract_native_query_sql(FACT_OTC_M)
        assert "TO_DATE(concat( LEFT(fiscper, 4)" in sql
        assert "ca_otc010_otc_subkbis" in sql
        assert sql.strip().upper().startswith("SELECT")

    def test_fact_customerexp_native_sql_keeps_hardcoded_filter(self):
        sql = extract_native_query_sql(FACT_CUSTOMEREXP_M)
        assert "MAKE_DATE(YEAR(CURRENT_DATE()) - 2, 1, 1)" in sql
        assert "cust_exp_fact_data" in sql

    def test_dim_country_has_no_native_query(self):
        """Dim_Country uses connector navigation, not a native query → None."""
        assert extract_native_query_sql(DIM_COUNTRY_M) is None

    def test_native_query_as_subquery_wraps(self):
        sub = native_query_as_subquery(FACT_OTC_M, alias="otc_src")
        assert sub.startswith("(\n")
        assert sub.rstrip().endswith(") AS otc_src")

    def test_parse_json_populates_native_query_sql(self):
        """parse_json fills TableInfo.native_query_sql when given the raw M."""
        entries = [
            {
                "table_name": "Fact_NPS",
                "transpiled_sql": (
                    "SELECT country_id, SUM(nps) AS nps "
                    "FROM main.default.cust_exp_fact_data_v2 GROUP BY country_id"
                ),
                "validation_passed": "Yes",
            }
        ]
        tables = MQueryParser().parse_json(entries, {"Fact_NPS": FACT_NPS_M})
        info = tables["Fact_NPS"]
        assert "cust_exp_drivers_nps_contribution" in info.native_query_sql
        assert _norm(info.native_query_sql) == _norm(FACT_NPS_GOLDEN_SQL)

    def test_parse_json_without_expressions_leaves_field_empty(self):
        entries = [
            {
                "table_name": "Fact_NPS",
                "transpiled_sql": "SELECT a, SUM(b) AS b FROM cat.sch.t GROUP BY a",
                "validation_passed": "Yes",
            }
        ]
        tables = MQueryParser().parse_json(entries)
        assert tables["Fact_NPS"].native_query_sql == ""


class TestS5TransformFolding:
    def test_fact_otc_country_remap_and_company_code_todo(self):
        """Fact_OTC: List.Accumulate GB→NIR becomes replace(); the CompanyCodes
        List.Contains filter (cross-table buffer) becomes a TODO, not dropped; the
        commented-out FilterExclude3KBI block never appears."""
        folder = MTransformFolder()
        base = (
            "SELECT fiscper_date, fiscper, comp_code, bic_csubkbi, country, "
            "bic_ccusthie4, fltp FROM cat.sch.ca_otc010_otc_subkbis"
        )
        out = folder.fold_from_mquery(
            base,
            FACT_OTC_M,
            [
                {"name": c}
                for c in [
                    "fiscper_date",
                    "fiscper",
                    "comp_code",
                    "bic_csubkbi",
                    "country",
                    "bic_ccusthie4",
                    "fltp",
                ]
            ],
        )
        assert "replace(country, 'GB', 'NIR')".lower() in out.lower()
        assert any(
            "List.Contains" in t and t.startswith("-- TODO:")
            for t in folder.transform_todos
        )
        # commented-out disabled filter must never be translated
        assert "KIOM03501" not in out
        assert "2026006" not in out

    def test_dim_country_code_remaps_nested(self):
        """Dim_Country: the 5-pair country_id List.Accumulate folds into a nested
        replace() exactly as the deployed dim does; the country text replaces
        (Baltics→Estonia, Republic of Ireland→Ireland) chain rather than clobber."""
        folder = MTransformFolder()
        steps = folder.parse_let_steps(DIM_COUNTRY_M)
        exprs = folder._build_column_transforms(
            [
                s
                for s in steps
                if s.step_type in ("ReplaceValue", "ReplaceValueAccumulate")
            ],
            {},
            [],
        )
        cid = exprs["country_id"].lower()
        assert cid == (
            "replace(replace(replace(replace(replace(country_id, 'roi', 'ie'), "
            "'cr', 'hr'), 'nmd', 'mk'), 'xk', 'kv'), 'fv', 'fi')"
        )
        country = exprs["country"].lower()
        assert "replace(replace(country, 'baltics', 'estonia')" in country
        assert "'republic of ireland', 'ireland')" in country

    def test_dim_country_untranslatable_steps_become_todos(self):
        """Table.Distinct (keyed), Table.Combine (appended rows) are surfaced as
        TODOs — never silently dropped (S5)."""
        folder = MTransformFolder()
        base = "SELECT * FROM cat.sch.cust_exp_dim_company"
        folder.fold_from_mquery(
            base,
            DIM_COUNTRY_M,
            [
                {"name": "country"},
                {"name": "country_id"},
                {"name": "region"},
                {"name": "country_abbreviation"},
            ],
        )
        joined = "\n".join(folder.transform_todos)
        assert "Table.Distinct" in joined
        assert "Table.Combine" in joined

    def test_dim_country_region_filter_translated(self):
        """Table.SelectRows region filter lowers to a WHERE predicate."""
        folder = MTransformFolder()
        steps = folder.parse_let_steps(DIM_COUNTRY_M)
        select_rows = [s for s in steps if s.step_type == "SelectRows"]
        conds = [folder._parse_select_rows(s) for s in select_rows]
        conds = [c for c in conds if c]
        blob = " ".join(conds)
        assert "region = 'Italy'" in blob
        assert "country_abbreviation <> 'RU'" in blob

    def test_fact_customerexp_group_by_todo_and_remap(self):
        """Fact_CustomerExp: GB→IE replace() is carried; Table.Group is a TODO."""
        folder = MTransformFolder()
        base = (
            "SELECT country_id, fiscper_date, distinct_customer_pulsed FROM cat.sch.t"
        )
        out = folder.fold_from_mquery(
            base,
            FACT_CUSTOMEREXP_M,
            [
                {"name": "country_id"},
                {"name": "fiscper_date"},
                {"name": "distinct_customer_pulsed"},
            ],
        )
        assert "replace(country_id, 'GB', 'IE')".lower() in out.lower()
        assert any("Table.Group" in t for t in folder.transform_todos)

    def test_parse_let_steps_skips_source_and_plumbing(self):
        """The source/navigation/buffer/RowLimit assignments are not steps."""
        steps = MTransformFolder.parse_let_steps(FACT_OTC_M)
        types = [s.step_type for s in steps]
        assert "SelectRows" in types  # FilterCompCodes
        assert "ReplaceValueAccumulate" in types  # GB→NIR
        assert "FirstN" not in types  # Kept First Rows plumbing
        # commented-out FilterExclude3KBI must not appear as a live step
        assert all("KIOM03501" not in s.raw_expression for s in steps)


def _t(name, source, gb, agg=None, is_fact=True, full_sql=""):
    return TableInfo(
        table_name=name,
        source_table=source,
        aggregate_columns=(
            agg if agg is not None else [{"name": "v", "source_col": "v"}]
        ),
        group_by_columns=gb,
        calculated_columns=[],
        is_fact=is_fact,
        full_sql=full_sql,
    )


class TestS7JoinDedup:
    def test_dimension_not_emitted_as_fact_join(self):
        """Calendar445 is a dimension (join_key_map) AND appears in fact_join_map;
        it must not also ship as a fact join (S7)."""
        tables = {
            "fact_otc": _t("fact_otc", "cat.sch.fact", ["fiscper_date", "comp_code"]),
            "Calendar445": _t(
                "Calendar445", "cat.sch.dim_calendar", ["date"], agg=[], is_fact=False
            ),
        }
        config = {
            "join_key_map": {
                "Calendar445": {
                    "alias": "dim_calendar445",
                    "join_key": "fiscper_date",
                    "dim_key": "date",
                    "dim_columns": ["year", "month"],
                },
            },
            "fact_join_map": {
                "Calendar445": {
                    "alias": "calendar445",
                    "join_key": "fiscper_date",
                    "target_fact": "fact_otc",
                },
            },
        }
        det = JoinDetector(tables, config)
        fact_joins = det.detect_fact_joins("fact_otc", [], tables["fact_otc"])
        assert all(j["name"] != "calendar445" for j in fact_joins)

    def test_fact_join_dropped_when_key_missing(self):
        """A fact join whose ON references a column the fact lacks (calendar445 on
        source.date when the fact only has date_id) is dropped."""
        tables = {
            "fact_otc": _t("fact_otc", "cat.sch.fact", ["date_id", "comp_code"]),
            "OtherFact": _t("OtherFact", "cat.sch.other", ["date_id"]),
        }
        config = {
            "join_key_map": {},
            "fact_join_map": {
                "OtherFact": {
                    "alias": "other",
                    "target_fact": "fact_otc",
                    "join_on_expr": "source.date = {alias}.date_id",
                },
            },
        }
        det = JoinDetector(tables, config)
        fact_joins = det.detect_fact_joins("fact_otc", [], tables["fact_otc"])
        assert fact_joins == []

    def test_valid_fact_join_kept(self):
        """A fact join whose key DOES exist on the fact is still emitted."""
        tables = {
            "fact_otc": _t("fact_otc", "cat.sch.fact", ["date_id", "comp_code"]),
            "OtherFact": _t("OtherFact", "cat.sch.other", ["date_id"]),
        }
        config = {
            "join_key_map": {},
            "fact_join_map": {
                "OtherFact": {
                    "alias": "other",
                    "target_fact": "fact_otc",
                    "join_on_expr": "source.date_id = {alias}.date_id",
                },
            },
        }
        det = JoinDetector(tables, config)
        fact_joins = det.detect_fact_joins("fact_otc", [], tables["fact_otc"])
        assert len(fact_joins) == 1
        assert fact_joins[0]["name"] == "other"

    def test_duplicate_joins_collapsed(self):
        """Two fact-join configs pointing at the same alias/source/ON collapse to one."""
        tables = {
            "fact_otc": _t("fact_otc", "cat.sch.fact", ["k"]),
            "F1": _t("F1", "cat.sch.f1", ["k"]),
            "F2": _t("F2", "cat.sch.f1", ["k"]),
        }
        config = {
            "join_key_map": {},
            "fact_join_map": {
                "F1": {"alias": "dup", "target_fact": "fact_otc", "join_key": "k"},
                "F2": {"alias": "dup", "target_fact": "fact_otc", "join_key": "k"},
            },
        }
        det = JoinDetector(tables, config)
        fact_joins = det.detect_fact_joins("fact_otc", [], tables["fact_otc"])
        assert len([j for j in fact_joins if j["name"] == "dup"]) == 1
