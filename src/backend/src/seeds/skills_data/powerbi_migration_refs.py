"""Reference files for the ``powerbi-to-metric-views`` skill.

Kept apart from ``powerbi_to_metric_views.py`` so each module stays under the
800-line ceiling. Nothing imports these except that skill module; they reach the
agent as bundled files and are read only when the workflow reaches their phase.
"""

_MATERIALIZED_VIEWS = """# Materialized views: one per Power BI table

Each layer of a Power BI model maps to one Databricks layer:

| Power BI | Databricks |
|---|---|
| Power Query (M) per table: native SQL, applied steps, DAX calculated columns | one materialized view (MV), named after the Power BI table in snake_case |
| Data that exists only inside Power BI (spreadsheets, typed-in tables, helper queries) | a snapshot table `pbi_<table>`, loaded from the published model |
| Relationships | joins in the metric view |
| Measures and visual or page filters | measures in the metric view |

Why materialize: the result is frozen at refresh time, as Power BI's import is, so a
`current_date()` in the SQL behaves like `TODAY()` in the report. Genie does not
recompute joins and window functions for every question, and metric views read only
objects you own. Metric views that read raw source tables have failed in Genie with
permission errors before.

## Building an MV

1. **SQL:** copy the M query's native SQL verbatim, with the model's M parameters
   filled in, and qualify any schema name that relied on the connection's default
   catalog. Cross-check the result against the draft's copy of the M query.
2. **Applied steps:** port every step after the SQL (rename, filter, replace value,
   type change, unpivot, combine, split column, distinct).
3. **Calculated columns:** the table's DAX calculated columns (keys, flags, calendar
   labels).
4. **Rules:**
   - An MV reads base tables and snapshot tables only, never another MV. Repeated
     logic is marked `-- keep in sync with <mv>`, so any refresh order works.
   - Partition only large facts (roughly more than a million rows), for example by
     company and fiscal period.
   - Keep Power BI's column types. Doubles stay `DOUBLE`: a ratio of two `DECIMAL`
     sums is truncated to six decimals.
   - One notebook is the single source of truth for the model's objects. It creates
     the MVs first, then the metric views from their YAML.

### Pitfalls: M and DAX versus SQL

| Power BI does | Mirror it as |
|---|---|
| Text comparison is case-insensitive and ignores trailing spaces; the first casing loaded wins | compare with `lower(rtrim())`; do not "fix" the source casing |
| `STARTOFMONTH` / `ENDOFMONTH` in a calculated column return the row's own date | copy what Power BI returns (check with `EVALUATE`) |
| `DATE(y, 13, 1)` rolls over into the next year | `add_months(make_date(y, 1, 1), m - 1)` |
| DAX `&` with a blank gives `""` | `concat(coalesce(a, ''), ...)` |
| DAX `a + b`: one blank counts as 0, two blanks stay blank; `DIVIDE(x, 0)` is blank | `CASE WHEN COALESCE(a, b) IS NULL ...`, `NULLIF(den, 0)` |
| `DISTINCTCOUNT` counts blank as a value | compare value sets, not counts |
| M `Text.Combine` skips nulls | `concat_ws` |
| M `[col] <> "x"` keeps nulls | `col IS NULL OR col <> 'x'` |
| M `Table.ReplaceValue` with a replace-text replacer replaces substrings | `replace()` |
| M `Table.Distinct(..., {key})` keeps the first row per key | `QUALIFY ROW_NUMBER() ... = 1`, validated |
| M `Table.UnpivotOtherColumns` drops nulls | `stack()` plus `WHERE val IS NOT NULL` |
| M `Table.Combine` unions by column name | project each part onto the model's columns, `NULL` for missing ones |
| M string literals: `""` is a quote; `" & Param & "` splices a parameter | parse the literal, do not regex-replace it |
| Composite model or DirectQuery to another semantic model (`partition ... = entity`) | no M to copy: find the source model in the expressions, dump its definition and mirror that. That model has its own refresh, so its objects get their own job |
| `NORM.S.DIST` / `NORM.S.INV` | Python UDFs (they work inside MVs); first check whether a chain cancels out |
| Calendar "closed period" columns computed from `TODAY()` | the same logic in the calendar MV, evaluated at MV refresh |

## Access control: a row filter on every MV

An MV is refreshed as its **owner** and read with SELECT on the MV only, so the access
rules of its sources never reach the reader. Every MV, and every snapshot holding business
figures, gets one of two shared functions from one access schema. Create them with the
UCMV Source Layer Manager, `action=setup_access` (dry run until `confirm=true`): the
mapping table `row_access_map` (account group -> field -> allowed value, `*` = all) and:

| Power BI | Row filter |
|---|---|
| no RLS | `object_access('<groups>')`: the reader must be in one of the `;`-separated account groups allowed to read the sources (PERMISSION_DENIED otherwise), then sees every row; `''` = everybody with SELECT |
| RLS role on a column (region, company, country, ...) | `value_access('<field>', <column>)`: a reader sees a row if one of their groups has the row's value (or `*`) for that field in `row_access_map` |

Which function is a checkpoint per model (strategy phase): compare with what the Power BI
model restricts (roles in the model definition, `USERPRINCIPALNAME()` / `USERNAME()` /
`CUSTOMDATA()` in DAX). A model without RLS shows every row to everyone who can open the
report. Mirror Power BI. The scope table has one `rls_filter` review row per filtered
table, and the Source Layer Manager turns simple role filters into `value_access`
suggestions. In its `row_filters` config the shorthand is
`{"<table>": {"value_access": {"field": "region", "column": "region"}}}` or
`{"<table>": {"object_access": "grp_a;grp_b"}}`.

```sql
CREATE OR REPLACE MATERIALIZED VIEW <catalog>.<model_schema>.fact_orders
  WITH ROW FILTER <catalog>.<access_schema>.value_access ON ('region', region)
  AS SELECT ...
```

- **The fact carries the RLS column.** Power BI RLS on a dimension filters the fact through
  the relationship; in Unity Catalog a row filter on the dimension MV does NOT filter the
  fact (the metric view's join keeps the fact rows). Put the column on the fact MV too and
  filter both. The Source Layer Manager warns about every fact that a filtered dimension
  does not reach.
- **The customer's own rules.** If access rules already live in a table (a data-mesh rules
  table, an entitlement export), point both functions at it instead of `row_access_map`,
  keeping their names and arguments. Rule changes apply at once, without an MV refresh.
  Where Unity Catalog ABAC policies are available, they can replace the functions.
- **255-character limit:** each constant argument of `ROW FILTER ... ON (...)` holds at most
  255 characters; a longer one fails at CREATE. Keep group lists short (one group per
  audience, not per person); move longer lists into a table.
- **Set only at CREATE:** changing a filter means recreating the MV.
- **Readers need** `EXECUTE` on the functions and `SELECT` on `row_access_map`
  (`setup_access` with `reader_groups` grants both): the filter runs as the reader.
- **Snapshots:** `CREATE OR REPLACE TABLE` drops a row filter, so the snapshot loader
  re-applies it after every load.
- **Dropping an MV leaves its `__materialization_mat_<pipeline>_<name>_1` backing table**
  (unfiltered data) and sometimes `event_log_<pipeline>`: drop them too. `CREATE OR REPLACE`
  reuses the pipeline and leaves nothing behind.
- **Verify after deploy:** `DESCRIBE TABLE EXTENDED` shows a `Row Filter`; as a reader with
  `*` access the MV's count equals its backing table (owner-only, unfiltered).
- **Test a restricted reader without a second account:** run the function body with
  `is_account_group_member(x)` replaced by `x IN (<groups of one reader>)`; controls: a `*`
  group sees everything, a one-value group exactly its rows.

## Snapshots (checkpoint: which tables)

Only for Power BI-only tables that the scope kept, never for visual helpers. Load them
from the published model, which is what the report shows after its last refresh. Write
straight into the table and never print the contents: a typed-in table can hold targets
or budgets, which are KPI values.

- Loadable tables: DAX `EVALUATE` of the table.
- Helper queries that DAX cannot return: decode the query text from the model definition.
- One loader config per model, with a `loaded_at` stamp on every snapshot.
- Re-load snapshots before each MV refresh. Long term, ingest the source into the lake
  and record that as a follow-up.

## Validate every MV against Power BI (counts and verdicts only)

Use a comparison that prints counts and verdicts, never result rows.

- **Dimensions:** row count on both sides. Per key or categorical column, the number of
  distinct values and the members found on one side only (dimension values may be shown).
- **Facts:** row count per period. A per-column comparison of sums may run inside a
  script that prints only `column: matches / differs` per period.
- **A count difference is first a refresh-time question.** The model is frozen at its
  last refresh; the MV reads its source at its own refresh, so it often holds newer rows.
  1. Get the model's last successful refresh time.
  2. Re-count the MV restricted to what was loaded by then: by load stamp, or, without one,
     only periods closed before that time.
  3. Counts equal after the cut: drift, not an error. Note the affected periods; they match
     again after the next model refresh.
  4. Still different: a logic difference (a missed M step, a filter, a join fan-out). Count
     per further dimension (company code, plant, document type) until the differing slice is
     small enough to read off the M.
- Record per MV: row counts on both sides, the verdict and known drift. No sums.
"""

_METRIC_VIEWS = """# Metric views: thin, named, formatted

One YAML per metric view, deployed by the model notebook's last cell
(`CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML`). A metric view is thin: it
exposes MV columns and measures, and holds no logic that belongs in an MV.

## Structure

- **Source:** the fact MV. If Power BI measures add up several facts with the same
  dimensions, `UNION ALL` those fact MVs with a `source_table` column (NULL where a fact
  lacks a column).
- **Joins:** one per Power BI relationship, to the dimension MVs of the same model schema
  only, on the relationship keys. A join must return at most one dimension row per fact
  row. Check that before deploying.
- **No view-level filter:** it applies to every measure. Put filters into the measures.
- **Calendar labels in every view** that has a calendar: year, month, year_month, period
  label and the report's closed-period labels, so period rules work the same everywhere.

## Dimensions: only what a question needs

The MV keeps every column the report uses. The metric view exposes only what Genie
should group or filter by. Leave out of the metric view (not out of the MV):

- sort helpers (Genie orders by the period or the value itself);
- duplicate labels of a period already exposed;
- abbreviation columns next to the full name;
- calendar columns finer than the fact's grain: a monthly fact joined to a week column
  gives wrong weekly answers;
- columns used only by hidden pages.

Keep the geography that a slicer or a row filter needs, even if no visual draws it.
Before removing a column, remove it from the Genie space too.

Descriptions describe the column or measure itself. Cross-cutting query patterns (prior-year
YTD, last N periods) belong in Genie example SQL and filter snippets, not in column comments.

## Measures: the value the report displays

- Use the DAX of the displayed measure, or the single SWITCH branch the visual shows,
  plus the filters of the visual that shows it. Page filters intersect the DAX. A
  slicer's saved selection filters the data; the slicer's value list only limits what
  can be picked.
- A KPI shown by several visuals with different definitions: mirror the table or matrix
  value over the chart value, and say in the notes which one it mirrors.
- Mirror blank semantics (`COALESCE`, `NULLIF`). Leave out dead variables.
- **Chains:** for ratios whose parts have a documented meaning, define the building blocks
  first and build the ratio from them: `MEASURE(numerator) / NULLIF(MEASURE(denominator), 0)`.
  A block must be exactly the ratio's numerator or denominator, with the same filters.
  Blocks go above the ratio in the YAML. Do not invent block names for undocumented codes;
  keep such ratios inline and say why in the notes.
- Drop plain sums with no displayed meaning, and exact duplicates (keep one; the other's
  name becomes a synonym).
- **Page scopes** (code lists in page filters, found in triage): put a readable category
  column on the dimension MV (a `CASE` over the codes), expose it as a dimension, and bake
  each scope into the measures that need it with `FILTER (WHERE <category> IN (...))`. A
  category column answers "which items" questions and covers future scopes, so prefer it
  to one boolean flag per scope. Add a scope only to facts it reaches through the model's
  relationships.

## Naming (Genie follows names more than descriptions)

- **Measures:** snake_case, no special characters (`%` becomes `pct`, `+` becomes `plus`):
  `<kpi>[_<scenario>][_<unit>]`, for example `order_accuracy_pct`, `cycle_time_days`,
  `revenue_actual_amount`. Reports without scenarios drop the scenario part.
- The name without a unit suffix is the value the report shows. Building blocks always
  carry their unit, never a bare KPI name: a bare `fill` once answered a percentage question
  with hours.
- **Dimensions:** one name per concept across views and reports: `company_code`, `country`,
  `business_unit`, `region`, `plant_code` / `plant_name`, `customer_code` / `customer_name`,
  `fiscal_period`, `year`, `month`, `year_month`, `period_label`. Use readable names instead
  of system codes where the meaning is certain.
- The Power BI label becomes `display_name`; business terms and the Power BI measure
  name become `synonyms` (at most 10 per field). A synonym belongs to one field per view.
- Short abbreviations are not resolved through synonyms. Spell them out in names or comments.

## Display format: every measure has one

Genie writes a number the way the measure's `format` says. Without it a ratio comes out as
`0.6362550405764922` instead of `63.63%`, and an amount with thirteen decimals. A comment
such as "Unit: %" does not make Genie format the value.

- Every measure has a `format`, placed after `display_name`. Exceptions: measures whose
  values are so small that fixed decimals would show zero, and unit-less building blocks.
  Say so in the notes.
- Take the format from the model: the measure's `formatString` says whether it is a
  percentage and how many decimals it shows.
- **Percentage standard:** every percentage or ratio measure returns a fraction (0.945
  means 94.5 %) and carries

  ```yaml
  format:
    type: percentage
    decimal_places:
      type: exact
      places: 2
  ```

  Nothing else may multiply such a measure by 100 when it displays it.
- **Check the scale against the data,** never trust the name or the comment. Count the
  groups where the absolute value is above 1.5, by dimension and by month (counts only).
  Nearly all groups above 1.5 means the 0-100 scale. Convert such a measure with `/ 100`
  plus the percentage format, and keep every measure built on it identical in value: compare
  all measures of the view per group before and after the change; only converted measures
  may change, each by exactly times 0.01. In the reconciliation mapping, a converted measure
  whose report value is a plain 0-100 number gets `value_format: raw_number_hundred`.
- **Exception:** a KPI the report itself shows as plain points, without a percent sign,
  stays on its 0-100 scale with `type: number`. Its description says "points 0-100, not a
  fraction, no % format; never multiply by 100".
- **Differences of percentages:** `type: number`, multiplied by 100 in the expression, `_pp`
  in the name, "percentage points" in the description.
- **Amounts, volumes, hours, days, counts:** `type: number` with `decimal_places: {type: max,
  places: 2}`. Per-unit values: `max` with 4 places. `max` drops trailing zeros.
- `type: currency` needs a `currency_code`. Use it only for a measure in one known currency.
- A format changes how a value is shown, not the value. Reconciliation is not affected.

## Descriptions (`comment:`) and notes

- `comment:` is what Genie and Unity Catalog see. Keep it short: what it is, the formula in
  plain words, unit, sign, higher or lower is better, the KPI group, status (for example "on
  hold"), real traps, and `Power BI measure: <name>` as the last line (the mapping tools read it).
- Everything else (technical definitions, code lists, internals, defect notes) goes into
  `# ` note lines directly above `comment:`. Edit such YAML only with tools that keep the notes.
- The view-level `comment:` states the grain, the source, the default scope and the answering
  rules (default period, company total, units).
- **Do not invite invented figures.** A formula in a description lets Genie build a missing
  version from its components. Where the report has no such version (a budget, a prior-year
  variant), the description says so and forbids building one.

## Deploy and verify

1. Scripted YAML edits: parse before and after, and assert that the source, joins and
   measure list changed only as intended. Never run a multi-line regex across a YAML block:
   a truncated YAML deploys as a view without measures.
2. A measure may `MEASURE()` only measures defined above it in the YAML.
3. **Redeploying a metric view drops its direct grants** (inherited grants stay). Count the
   direct grants before the redeploy, re-grant after it, and compare the counts.
4. Deploy, then verify by comparing the deployed definition with the YAML (measures,
   dimensions, source, joins). A failed deploy leaves the old view live, so a column count
   alone proves nothing. After changing `format`, `display_name` or `synonyms`, compare those
   fields too, and count measures without a format.
5. Check that every measure executes, as a row count only. Also check grouped by a dimension:
   window measures often fail only when grouped.
6. **A metric-view change is one change in three places:** the YAML, the Genie space (example
   SQL, benchmarks, snippets, column settings, instruction text) and the reconciliation
   mappings. Renamed or removed measures and dimensions are found and updated in all three.
   Then run the consistency check over the space and export the space.
"""

_GENIE_SPACE = """# Genie space: time intelligence first, everything else minimal

The space is kept as code in the repo: header, configuration, data sources, instructions
and benchmarks. Export the live space after every change and commit it. The repo copy is
the only backup of a space's configuration.

## Start small

1. **The space split** (checkpoint): one space per coherent subject area and audience, not
   per report. Split a large multi-area model into several spaces. A space holds at most 25
   tables and metric views and may read across schemas. A supervisor agent can combine
   spaces but cannot join tables across them, so anything calculated together belongs in one
   space.
2. **Time intelligence, matched to the report's period options.** List them from the triage
   evidence: slicers and their saved selections, period selector tables, time-intelligence
   measures (latest closed month, month to date, year to date, rolling N, prior year). Cover
   exactly those with example SQL and filter snippets. Options the report does not offer are
   not built.
3. **A short instruction text:** default scope, closed-period rule, units. Nothing else.
4. **A handful of benchmarks:** one per period option, plus the headline KPIs.

Everything further down this file is a menu, used only when a user test shows that Genie
gets something wrong.

The session checks only that every example, snippet and benchmark SQL executes, as a row
count. The user runs the benchmark questions, because Genie's answers contain KPI values,
and reports for each one whether it is full, partial or wrong, and what was wrong.

## Tuning order (where knowledge goes)

1. Metric-view metadata: names, comments, display names, synonyms.
2. Space column settings (entity matching, format assistance for text dimensions).
3. SQL examples (trusted assets).
4. Text instructions: the last resort, kept short. Before adding a rule, say why steps 1 to 3
   cannot carry it.

Rules that need reasoning (data lag, year-to-date semantics) need an instruction **and** an
example whose result shows the value the answer must mention. An instruction alone was not
followed.

Genie follows column names more than display names or synonyms, so names carry the meaning.
Time intelligence goes into examples and reusable `sql_snippets.filters`, never into column
descriptions.

## Repo and API hygiene

- Space IDs differ per workspace: a space copied to another workspace gets a new ID. Find the
  live space by its title before patching, and name the repo folder after the live ID.
- Save the live space JSON before each update.
- The API rejects unsorted lists: sort examples, snippets and benchmark questions by `id`,
  and data-source tables by `identifier`.
- Every table an example or benchmark references must be attached as a data source.

## Instruction skeleton (adapt the bracketed parts; keep it under about 4,000 characters)

```
This space covers the <report> report: <subject areas>. It mirrors the Power BI report "<report>".

THE DESCRIPTIONS ARE THE AUTHORITY
- Every view, dimension and measure has a description: definition, formula, unit, higher or
  lower is better, status. Read them and follow them. Never infer a definition from a name.
- Match the user's KPI name to a measure's display name or synonyms.

1. SCOPE
- Default scope = <company total as the report defines it>. Apply it when no company, region
  or country is given. Filter geography with its columns directly.

2. CALENDAR AND CLOSED PERIODS
- Periods are <fiscal calendar>. Filter with the calendar dimensions as in the TEMPLATE example.
- No period given = the latest closed month (label 'Latest Month'), as in the report.
  "Year to date" = months from 1 up to the latest closed month of the current year.
- Data coverage: <per view: first period with data>. Outside it, say there is no data.
- Data lag: <which sources load late>. If a KPI has no value for the latest closed month, answer
  with the most recent month with data, say so explicitly, and never call it "this month".

3. UNITS AND DIRECTION
- Percentages are fractions (0.25 = 25 %): select a percentage measure as it is, never x 100.
  A value calculated from percentages is x 100 and named *_pct; a difference is *_pp.
- A version or KPI without its own measure does not exist in the report: say so; never build it.

4. METRIC VIEW RULES
- Compute each KPI with MEASURE() at the level asked. Never SUM or AVG a ratio across periods
  or members.
- Metric views cannot be joined directly: aggregate each in its own CTE, then join the results.
```

## Example SQL patterns (every space should have these)

Adapt the names and run every example on the warehouse before deploying.

| Pattern | Core |
|---|---|
| Period template | comments listing every period filter, plus a latest-month query |
| Latest month with data lag | CTE `shown_month` = `GROUP BY ALL HAVING MEASURE(kpi) IS NOT NULL ORDER BY year_month DESC LIMIT 1`; a `month_status` column that says whether the latest closed month is loaded |
| Last N months / previous month | anchor on the most recent month with data, then `year_month > add_months(anchor, -N)`; never on `current_date()` |
| Year to date vs same period last year | `month <= latest closed month number` for both years, `MEASURE()` per year |
| Running year to date by month | CTE of months with data; `LATERAL (SELECT MEASURE(kpi) ... WHERE v.year = p.year AND v.month <= p.month)`; check one month against a direct YTD |
| Year over year per month | one CTE grouped by year and month, self-joined on the month |
| Headline overview | one CTE per view, `CROSS JOIN` the single-row results |
| Breakdown or ranking | the KPI by a dimension, ordered, with the scope filter |

**Percentages in example and benchmark SQL:** a percentage measure selected untouched is never
multiplied by 100; its format shows the percent sign. A value calculated from percentages has
no format, so it is multiplied by 100 and named `*_pct`; a difference of percentages is `*_pp`.
Labels such as favourable / unfavourable stay NULL when a value is missing. No `LIMIT` unless
the question asks for a top or bottom N.

**Metric-view quirk:** a dimension-only query returns only the combinations present in the
fact, so a "latest period" lookup can come back empty when the fact has no rows for that period.
Anchor "latest" on `HAVING MEASURE(...) IS NOT NULL ORDER BY period DESC LIMIT 1`.

Add `sql_snippets.filters`, with synonyms, for: latest closed period, current-year year to
date, prior-year year to date (with the month cap from the calendar view), whole-company scope.

## Benchmarks

Start with one question per period option of the report plus the headline KPIs, worded
differently from the examples. The user extends the set while testing. Expected SQL must be
dynamic where the answer moves (the latest period with data), never hard-coded months.

## Evaluation (objective quality score)

Genie Space Sync `action=eval` asks every benchmark question through the Genie conversation
API, compares the answer with the expected result (numeric check with tolerance, optionally an
LLM judge), and reports pass / fail / error per question with latency and cost, plus a score.
Optionally the same questions go to a Fabric data agent, side by side, to compare with what the
business uses today. Run it after every tuning step and keep the score in the status file; with
`statistics_only` the output carries verdicts, never answer values. A benchmark that also sits
in the examples inflates the score: keep them apart.

## Deploy, verify, test

1. Validate the space definition with the repo's validator.
2. Deploy (checkpoint the first time per session). Deploys can add empty lists that the validator
   rejects; remove them.
3. Export the live space into the repo and compare with the committed files. Do this after every
   later change too.
4. Run the consistency check: every example, snippet and benchmark SQL executes (as a count);
   every referenced table is attached; column settings and reconciliation mappings match the
   deployed views.
5. **Checkpoint:** the user runs the benchmarks and the test questions. Fix from the reported
   descriptions, then run the consistency check again.
"""


_MODEL_README = """# <Model name> (`<model_schema>`)

<One or two sentences: which Power BI report(s) this mirrors, for whom, and where users ask questions (Genie
space).> Written for colleagues and business users; the working log of the translation is the status file. No
KPI values in this page.

| | |
|---|---|
| Power BI | report "<name>" (`<report id>`), semantic model `<id>`, workspace `<id>` |
| Databricks | `<catalog>.<model_schema>`: <n> materialized views, <n> metric views, <n> snapshots |
| Genie space | "<title>" (`<space id>`) |
| Refresh | daily after the Power BI refresh at <time> |
| Owner / contact | <team>, report owner <name> |

## What it covers

| Area (report page) | KPIs | Metric view |
|---|---|---|
| | | `ucm_<subject>` |

Not covered: <pages, KPIs or linked reports left out, and why>.

## Sources

<Source tables by area; anything outside Databricks (snapshots of Power BI-only tables) and the follow-up to
ingest it.>

## Access

<Who sees which rows. Default: the Power BI RLS mirrored with `value_access` / `object_access` on every MV.
Describe every deviation and why.>

## Differences from Power BI

<Everything a user comparing numbers should know: documented deviations, renamed columns, logic the views carry
instead of DAX, known model defects and what is returned instead.>

## Using it in Genie

<Time-intelligence rules (latest closed month, YTD, PY), the report's period options, a few example questions.>

## Validation

<Last reconciliation: share of measures aligned, open gap classes; last Genie evaluation score. Dates, no values.>
"""
