"""Migrating a Power BI report to Databricks metric views and a Genie space.

The end-to-end workflow: a draft from the UC Metric View Generator, two-way triage
against the live semantic model and report, one materialized view per kept
table, thin metric views, KPI reconciliation, a Genie space, a refresh job, and a
semantic sync that flags later changes. The body is the procedure; detail sits in
bundled references that load only in the phase that needs them.

Reference contents live in ``powerbi_migration_refs.py`` to keep this module
under the 800-line ceiling.
"""

from src.seeds.skills_data.powerbi_migration_refs import (
    _GENIE_SPACE,
    _MATERIALIZED_VIEWS,
    _METRIC_VIEWS,
    _MODEL_README,
)

_TRIAGE = """# Triage: both directions

The goal is a scope table that says, for every measure, table and column, whether it is
mirrored and why. Triage runs both ways: everything the generator proposes must earn its
place, and everything the report shows must be covered. The user signs the result off
before any object is built.

## Phase 1: read the draft (candidates only)

The draft is a folder or zip of the generator's files. Layouts differ between versions, so
find each part by name and say which parts are missing:

| Part | What it is | Trust |
|---|---|---|
| `*_uc_metric_view.yml` (in `yamls/` or `ucmvs/`) | one metric view per fact table: source tables, inline joins, translated DAX, list of untranslatable measures | candidates only: sources are often placeholders or commented out, and measures are sometimes numerically wrong |
| `*.mapping_candidates.yml` | reconciliation drafts with `binding: TODO` and unresolved measures | a starting point for reconciliation, never final |
| `pipeline_config.json` | table M expressions, measure usage, SWITCH decompositions, filter sets, join key map | the M expressions are copied from the model and are reliable; the rest are the generator's interpretations |
| "PBI-only tables need ingestion" list | tables without a lake source (spreadsheets, typed-in data) with a proposed snapshot name | candidates for snapshots; many are visual helpers that need none |

Write the candidate lists (tables, measures per table, untranslatable measures, snapshot
candidates) into the status file.

## Phase 2: the live evidence

Collect it from the live model and report, not from the draft.

| Evidence | How |
|---|---|
| Model definition: M per table, calculated columns, parameters, relationships, row counts | dump the model's definition (TMDL) and its row counts; then an inventory per table: partition kind, source, rows, how the report uses it |
| Measures and DAX | the model's measure list; metadata DMVs or the INFO functions return measure names (expressions may come back blank over REST) |
| What the report uses | the report definition (PBIR): visual-to-measure and visual-to-table references, with the role of each measure: drawn, filter-only, or indirect (referenced by a drawn measure's DAX) |
| Filters | report-, page- and visual-level filters, and the slicers' saved selections. Page-level fetches alone miss visual filters |
| SWITCH-driven visuals | the selector table that drives a SWITCH, and which branch each visual shows |
| Calculated columns and tables | the definition files; the schema DMVs usually need admin rights |
| Model RLS | the role definitions (table permission filters); membership is set in the service, not in the definition |

Metadata queries are fine in this phase: definitions, row counts and distinct dimension
values. Never evaluate a measure or a numeric fact column into the output.

**Inventory every report on the model, not only the one drafted.** A model often feeds several
reports. List the workspace's reports with the same dataset ID and collect the visual-usage
evidence for each report in scope; the scope decision in phase 0a says which are in.

**Never assume a table belongs to another model.** Check each table's partition source. A table
named after another report can be imported by this model straight from the lake, in which case it
belongs to this model's schema. Only tables that reference another semantic model (entity
partitions, analysis-services connections) live elsewhere.

**Which pages count:** what a business user can reach. In scope: visible pages; hidden pages
reached from a visible page through a navigation button, drillthrough or tooltip; web-URL buttons
to other reports (listed as linked reports and decided in phase 0a). Out of scope: hidden pages
nothing links to (copies, "duplicate of", work pages). Check a button's action type: a navigation
button often carries a stale web URL that is not active. A hidden button counts only if a bookmark
shows it. A button whose target is computed by a measure leads to the pages named in that
measure's DAX; a target that cannot be resolved (for example a typed-in table dumped without its
data) is listed for a manual check.

**Selectors and functions:** a KPI can be visible with no visual referencing its measure, for
example a selector table resolved by a DAX user-defined function. The closure follows the function
body, so every measure the function can return counts as reached. List every selector entry and
resolve it through the function. Entries the function does not resolve have no measure in the
model: log them as gaps, with nothing to translate.

**Take a KPI's definition from the path a user sees.** A variant built from a hidden page (with an
extra filter) was once found to differ from the visible value in most months. The reachable visual
defines the measure and its filters; a hidden page's variant is ignored.

**Reachability of measures:** measures drawn, or used as filters, on reachable pages; measures
named by selector functions and calculated columns and tables; and everything those reference,
transitively. A measure outside this closure is not mirrored, and an existing mapping for it is
removed. A building block reached only through a chain stays.

The reachability closure can be computed from the dumps as a draft scope table, with a decision per
measure, table and column, plus a "why does this table reach the closure" query. The decisions are
heuristics: copy the draft to the scope file, review it, and then the user signs off.

**Page scope filters** (whole-page filters on code lists such as line types, material types or
document types) decide what a KPI counts, so the metric view has to carry them:
1. Compare each page's full value list. Never trust a shortened printout: a cut-off list once made
   two variants look like five. Read the slicers' saved selections and code lists inside measure
   DAX too.
2. Group pages with identical lists into scopes and name each scope by its pages.
3. Translate codes into readable categories from the source's text table.
4. Check which facts each filter reaches through the relationships. A filter on a dimension that
   relates to only some facts does not filter the others.
5. Note stale filters (on a column or value that no longer exists) as model defects and decide the
   intended scope.

**Linked reports** (web-URL buttons to other reports) are listed in the status file but never
translated automatically. The user decides in phase 0a.

## Measures: both directions

**A. Draft to model (is each candidate needed?).** For every drafted measure, find its model
counterpart and keep it if it is drawn on a visible page (values, axis, rows, tooltip), or if it is
load-bearing: in the transitive DAX lineage of a drawn measure (also through SWITCH branches the
visuals select), or used as a visual or page filter that changes displayed values. Otherwise drop
it, with the reason: unused, legacy, display helper, duplicate. Typical drops: format and label
measures, colour and conditional-formatting measures, selector plumbing, measures of hidden pages,
plain sums nobody displays.

**B. Model to draft (is anything missing?).** For every drawn measure and every measure in its
lineage, check that a candidate covers it. Watch for:
- **SWITCH wrappers:** a visual shows one branch; mirror that branch, and list each variant that
  different visuals show.
- **Cut-over measures** (an old source before a period, a new one after): the visual shows the merged
  one. The plain measure of the same name may reconcile perfectly and still be the wrong one.
- **Measures over two fact tables:** one metric view with a `UNION ALL` source.
- **Time intelligence** (prior-year, YTD, MTD): not mirrored as measures; Genie computes them from
  the calendar labels. Record them as "covered by Genie patterns".

## Tables: both directions

Keep a table if at least one of these holds:
- a kept measure reads it, in its DAX or through its lineage;
- a drawn column, or a filter or slicer that changes displayed values, comes from it;
- it is the dimension side of a relationship that a kept fact needs for a drawn grouping or filter.

Drop it (no view, no snapshot) when it is a visual helper: selectors and field parameters, measure
tables, formatting or layout tables, automatic date tables, "enter data" tables that only label
visuals; or when only hidden pages or dropped measures use it.

Careful: a mapping table that a kept measure filters on or looks up (for example a table that drives
a SWITCH, or a company scope table) is data, not decoration. Keep it, as a snapshot if it exists only
in the model.

Check the other direction too: a table the report needs that the draft did not generate, such as a
dimension used only through a relationship, or a table of another semantic model reached through a
composite model.

## Columns: both directions

For each kept table, keep a column if it is a relationship key; read by a kept measure or calculated
column; drawn or used in a filter or slicer; or a descriptive attribute the assistant needs to answer
naturally (names and labels next to a kept key), or a calendar label used for periods. Drop technical
columns (load stamps, package IDs, record numbers, unused source fields), and say so. Check that no
column a kept measure or filter needs was dropped.

## Output: the scope table (checkpoint)

One scope file per model (`scope.csv`), with a summary in the status file. One row per object:

| object_type | pbi_table | name | in_draft | drawn | load_bearing_for | filter_use | decision | reason | mirrored_as |
|---|---|---|---|---|---|---|---|---|---|
| measure | fact_orders | Order Accuracy % | yes | yes (Orders page) | | | keep | drawn | `ucm_fact_orders.order_accuracy_pct` |
| table | selector_time_period | | yes (snapshot) | | | slicer only | drop | visual helper | |
| measure | | Customer Count | no | yes | | | keep | missing in draft | `ucm_fact_orders.customer_count` |

Decisions use a fixed vocabulary: `keep` (own measure, table or column), `snapshot` (copied from the
published model), `fold` (part of another object's SQL or measure), `genie` (answered by a Genie
pattern, no own object), `drop`, and `n/a` (container rows such as measure tables). `review` is
allowed only while drafting. `mirrored_as` names the Databricks object for every `keep` and `snapshot`
row. A measure that is commented out in its YAML still counts.

Show the user the counts per decision, every drop of an item that looks drawn, every table kept although
the draft did not generate it, and the snapshot candidates. Proceed to the build only after sign-off.

## Deciding open rows in buckets

A real scope table has hundreds of open rows. Do not decide them one by one: the scope review groups them by
what the metric views already show, with one proposed decision per bucket, and the user decides buckets
(the UC Metric View Generator returns `report_scope.review`; the UCMV Drift Monitor reviews a signed-off
scope against the deployed views and applies `scope_review_choices` such as `{"L1": "accept", "G2": "drop"}`):

| Bucket | Rows | Proposal |
|---|---|---|
| L1 / L2 | a measure / table whose mirror exists (found by name, display name, synonym or a `PBI measure:` note) | link |
| M1 | a dropped SWITCH wrapper whose branch measures are mirrored | fold (a drop would hide its changes) |
| M2 | dropped (unreached), but a metric view measure was built for it | keep, or take it out of the YAML |
| M3 | dropped, but a step of a mirrored measure | fold (as drop, a change to it would go unnoticed) |
| B1 / B2 / B3 | building blocks used only by conditional formatting / only by dropped measures / only by mirrored ones | drop / drop / fold |
| B4 / B5 | building blocks with open users / branches of a wrapper (the KPI itself) | the user decides |
| C1 / C2 / C3 | unused columns: already a dimension / not a dimension / calculated | keep / drop / drop |
| G* | drawn or filtering measures with nothing built, per table | translate, genie, or drop |

Review again after applying: the remaining rows get new buckets. Chains of steps end at wrappers and Genie
patterns: their branches and date plumbing are decided on their own.
"""

_RECONCILIATION = """# Reconciliation: prove each measure against the report

Compare every mirrored measure with what the report displays, cell by cell: one cell per period
and dimension member. Run it as statistics only (see the ground rules). The comparison is the
only proof that a measure is right. Generated mappings and generated measures are not.

## What a run reports (statistics only)

Per measure, its cells in three groups:
- **comparable** (both sides non-zero): share aligned and largest relative difference. The largest
  difference is taken over comparable cells only;
- **missing in the metric view** (the report has a value, the view is empty or zero);
- **extra in the metric view** (the view has a value, the report is empty or zero).

For each measure below 100 %: the share aligned per period and per member, with its missing and
extra counts. An optional cross-tab of period by member gives one class per cell (aligned, missing,
extra, differ) and the relative difference only. Absolute differences are in the measure's unit, so
absolute and relative differences are never shown together.

A tool that runs reconciliations needs these properties:
- one run at a time against a shared warehouse and service account (a lock file with the PID; a stale
  lock is taken over and says so);
- a timed-out query is retried per measure, then split per period or per member; a measure known to be
  heavy is configured to split from the start;
- periods whose calendar label is NULL in the view (future periods) are not queried, and are counted.

Reading the statistics:
- all periods fail for all members: the definition (filter, formula or the wrong counterpart);
- only the latest periods fail: refresh timing;
- only some members fail: scope or a member-specific filter;
- missing in the view: rows that did not arrive (join, source filter, row filter, drift), or a filter
  that removes too much;
- extra in the view: a filter the report applies and the view does not (page or visual filter,
  closed-period rule, blank handling);
- a relative difference near a round factor (0.99, 0.5, 100): a unit or percent-scale error.

When the statistics do not explain a cause, ask the user what they see in the reconciliation
notebook. Do not open the result tables yourself.

## Mappings

One mapping file per metric view. Its contents:
- **Binding:** the view's full name, the model's workspace and model IDs, the fact table, the time
  dimension, the default dimension, and a dimension map (view column to model table and column).
- **Time:** filter through the calendar dimension, never through a fact column: measures that read
  other facts or calendar columns see nothing otherwise. Model tables without a period column use a
  date-to-period format.
- **Time-intelligence measures** (next month, rolling YTD, MTD) come back empty or reduced when the
  report is grouped by a period column. Group by the calendar's year and month instead.
- **Week grain:** the calendar's week column, with week keys normalised to one comparable form. Only
  weeks before the view's latest open week are compared.
- **Window measures** need their ordering dimension in the grouping. Add it as a grouped (not
  selected) column that is one-to-one with the period.
- **Measures:** one entry per mirrored KPI, with the kind that matches how the report shows it:
  `direct` (a named measure), `raw_column` (the implicit sum of a column), `switch` (one branch of a
  SWITCH, with its selector value), `dimension_conditional`, `composite`. Building blocks are not
  mapped; they are verified through the ratios built on them.
- A visual's page filter goes in as an `extra_filter`, also for SWITCH kinds.
- `value_format` must match what the report returns (raw number, 0-100 number against a fraction view,
  percent string). Tolerance is absolute 0.001 by default. Use relative tolerance for large magnitudes
  and for small ratios, where an absolute tolerance would hide real differences.
- Calendar label columns that a measure reads with a selected-value function: pair the model's label
  column with the view's label column. The label must have one value per period.
- **Drafted mappings are drafts.** In one check the LLM review of candidates was wrong in three of four
  cases. A counterpart is confirmed only by reconciliation. If a candidate type is missing, fix the
  generator deterministically rather than widening its prompt.
- Before calling a mismatch "no counterpart", search the model by the component column (naming drifts:
  spaces against underscores, implicit "Sum of X"), check page filters, and confirm that the view's
  dimension table really relates to the fact in the model. A disconnected table makes filters silent
  no-ops.

## Run and classify

Run all mappings for the last two fiscal years. Each measure below 100 % gets one class, written into
its mapping notes and the status file:

| Class | Evidence | Action |
|---|---|---|
| drift | fails only in periods after the model's last refresh, or the two sides were refreshed at different times | refresh the side that is behind, then re-run |
| model defect | the report contradicts its own definition | put the intended value in the view; log the defect for the report owners |
| structural | no counterpart at that grain (the report repeats a total on every row) | document it; no fix |
| error | everything else | fix the view, the measure or the mapping |

Before classifying a new measure as an error, compare the fact's row counts for the same period on
both sides. Sources with late-arriving rows change past periods too.

Compare dimension tables as well. A dimension that a page filter reads can shrink in the model while
its definition stays the same. When the model has fewer rows than its source, check the source as it
was at the model's last refresh (time travel on the lake table) before suspecting a new filter.

After a fix, re-run only the affected mappings. Done when every mirrored KPI is at 100 % or classified.
Record the run (date, measures, cells, share aligned, no values) in the status file.
"""

_REFRESH_AND_SYNC = """# Refresh job, release and semantic sync

## Refresh job (one per semantic model)

Each model has one refresh job, defined by a small YAML in the model's folder. The YAML holds only what
differs from another model: the job name, the schema, the Power BI workspace and model IDs, the schedule
(a minute that other jobs do not share), the failure notification recipients, the snapshot notebook and
its config if the model has snapshots, and the branch the job runs from while the pull request is open.

Tasks: check the Power BI refresh; decide whether a refresh is needed; load the snapshots (if any); refresh
the views. The check compares the model's last refresh with the views' last refresh. Snapshots reload
before the views, so no view reads a stale snapshot.

Sequence:
1. Write the YAML in the build phase, as soon as the snapshot question is settled.
2. Before the pull request, with the user's OK: point the branch setting at the report branch, commit and
   push (the job reads its notebooks and snapshot config from the remote; the deploy stops when the branch
   is unpushed). Deploy, then trigger one run and check every task's state. A refresh correctly skipped
   because the model has not refreshed counts as a pass. Record the job ID in the status file.
3. Before completing the pull request, remove the branch setting, so the main branch never carries a
   feature branch. Right after the merge, deploy again so the job runs from main, and check that the
   status shows main.

A second report on a model that is already translated needs no new job. A snapshot that holds business
figures gets its row filter set again after every load (see the materialized-view reference).

## Release checklist

1. Status file complete: scope decisions, view validation, reconciliation result, benchmark result, open
   points, the defect list for the report owners.
2. Model README: an overview for colleagues and business users (coverage, sources, access, differences
   from the report, how to use it in Genie, last validation). Short and stable. The working log stays in
   the status file, and the README is updated whenever scope, access or a deviation changes.
3. Tests: the space-as-code tests and the reconciliation tests pass.
4. Pull request into main: what is in it, validation statistics (counts and shares, no values), the steps
   after the merge, and notes for reviewers.
5. Refresh job redeployed from main after the merge.
6. The report joins the semantic sync (below) and its baseline is stored.

## Semantic sync (after release)

A daily job per report compares the model's and the report's definitions with the previously stored
snapshot: tables, columns, measures, calculation items, functions, shared queries, relationships, row
filters, pages, and the fields each reachable page draws or filters on. For every change it looks up the
object's scope decision and says what must happen on the Databricks side.

| Change to | Scope decision | Action |
|---|---|---|
| a mirrored object (`keep`, `snapshot`, `fold`) | with `mirrored_as` | update the mirror, or retire it when the object was removed |
| a `genie` object | | check the Genie pattern or example that stands in for it |
| a `drop` or `n/a` object | | none |
| an object missing from the scope table, or a decision outside the vocabulary | | triage: compute the closure for a draft decision, then fix the scope file |
| an object listed under `ignore` | | none (stored with its reason; no proposal, no ticket) |
| a page, page field, relationship or shared query | (no scope row) | check reachability, joins and the tables it loads |

How it works:
- It downloads the complete definitions on every run, so it sees every change whatever caused it: a
  republished model, a report edited without a refresh, or new rows in a tracked table. It does not
  depend on refresh times or "last modified" dates, which these APIs do not expose without admin rights.
- It reads definitions only, never measure values. Definitions are compared after normalising comments and
  whitespace, so reformatting is not a change.
- **Tracked tables:** small text tables whose rows carry meaning (a status list, for example). Each row
  becomes an object. A status change, such as an item going from on hold to confirmed, becomes a change
  that says to reload the snapshot and enable the measure it gates. Never list fact tables here.
- **LLM proposals:** for each change that needs action, a model-serving endpoint receives the old and new
  definitions and the current Databricks counterpart, and returns JSON: a verdict (update, no change,
  retire, new object, needs a person), a summary, a proposed definition, checks and a confidence. The
  proposal is stored and never applied. A person verifies it against the model, then changes the YAML,
  the mappings and the Genie space together. Only definitions are sent, never data. Calls per run are
  capped, and a failed call becomes a "needs a person" proposal instead of failing the job.
- **Tickets:** for changes that need a person, one work item per report and run, in draft state, with the
  scope decisions, old and new definitions, the proposals, how to handle each change, and the
  reconciliation to re-run. The ticket ID is stored on the change rows so no ticket is created twice. If
  your team has no tracker integration, the job writes the same content to the change table.
- **Ignore list:** objects that change without any change in meaning, such as a load-date parameter that
  every data load sets. Each one needs a reason. Its changes are stored but never proposed.
- Each report has its own job, scheduled a few minutes after the others, because they share one service
  account's request limit.

Setup:
1. Create the two change tables.
2. Credentials for the job live in a secret scope that the deployer can read. Never paste a token into a
   chat, a file or a log; store it with the platform's secret tooling.
3. Per report, once its scope file is signed off: copy an existing sync config, adapt the name, schedule,
   tracker settings, tracked tables and ignore list, and deploy it paused.
4. Run it once. The first run stores the baseline and creates nothing. Check the second run, then unpause.

Daily work: list the open change rows, handle each (YAML, mappings and Genie space in one step), then mark it
done with a note.

## Kasal tools for the same jobs

| Need | Kasal tool |
|---|---|
| Read the report's visuals and their measure references | Power BI Report References Tool |
| Pull model definitions (tables, measures, relationships) | Pipeline Config Generator |
| Draft the scope table from page reachability (phase 2) | Pipeline Config Generator: `report_scope` (rows, CSV, counts); the UC Metric View Generator returns it with `in_kasal` / `mirrored_as` filled and `kasal_gaps` |
| Every filter, slicer selection and page scope; per-measure page filters; links, linked and sibling reports; table inventory (phases 0a, 2) | Pipeline Config Generator `report_scope`: `filters`, `page_scopes`, `measure_page_filters` (also carried into the mapping drafts as `extra_filter`), `links`, `related_reports`, `inventory` |
| Check a signed-off scope file (vocabulary, links that point to nothing, stale rows) | UCMV Drift Monitor: `scope_problems` in the semantic sync output |
| Track data that carries meaning (a KPI status list) | Pipeline Config Generator / UCMV Drift Monitor `tracked_tables` |
| Lint metric view metadata (missing `format`, double-scaled percentages, synonym clashes) | UC Metric View Generator metadata lint (MV001–MV010) |
| Reconcile as statistics only; find 0–100 measures labelled as fractions | UCMV reconciliation tool with `statistics_only` and `scale_check` |
| Create the access functions and their mapping table; row filters; facts a dimension filter does not reach (phase 3) | UCMV Source Layer Manager `action=setup_access`, `row_filters` (`value_access` / `object_access` shorthand), emit warnings |
| Build metric views on the materialized-view layer (phase 4) | UC Metric View Generator with config `read_from_source_layer: true` |
| Decide open scope rows in buckets (phase 2) | `report_scope.review` (UC Metric View Generator), `scope_review_choices` (UCMV Drift Monitor) |
| Benchmark Genie, optionally side by side with a Fabric data agent (phase 6) | Genie Space Sync `action=eval` |
| Semantic sync: model and report changes routed by the scope table (phase 8) | UCMV Drift Monitor with `semantic_snapshot_table`, `report_scope`, `semantic_ignore` |
| Run a DAX query for metadata or a check | Power BI DAX Executor |
| Generate metric view YAML and deploy SQL per fact table | UC Metric View Generator |
| Create the metric views | Metric View Deployer (dry run by default) |
| Check translated measures against their DAX | Metric View Validator |
| Reconcile metric views with the report | UCMV reconciliation tool (`ucmv_reconciliation_tool.py`) |
| Build the Genie space | UCMV Genie Space Config Generator, then Genie Space Generator |
| Map report visuals to metric views | PBI Visual-UCMV Mapper |
| Retry untranslatable measures after a translator upgrade | UCMV Re-evaluation |
| Detect model changes after release (read-only proposal) | UCMV Drift Monitor |

Kasal's generators give you candidates. They do not replace triage, the materialized-view layer, the
snapshots, the reconciliation or the Genie checks. Lessons from the generator's output go into the same
scope file and mappings as hand-written ones.
"""

_STATUS = """# <Report name>: status

Handoff file for the `powerbi-to-metric-views` workflow. Update it at the end of every phase and after
every user decision. Counts, verdicts and dimension values only: no KPI values.

## Identity

| | |
|---|---|
| Branch | `feature/<ticket>_<report>` |
| Power BI | workspace `<id>`, semantic model `<id>`, report `<id>`; last model refresh checked: <date> |
| Draft input | `<folder or zip of the generator output>` |
| Schema | `<catalog>.<model_schema>` (one schema per semantic model) |
| Genie space | `<space_id>` (folder `<space_id>_<slug>/`) |
| Refresh job | `<job_id>` (`refresh_job.yml` in the model folder) |
| Semantic sync | `<job name>`; tracker contact `<e-mail>` |

## Phase

| # | Phase | State | Date | Notes |
|---|---|---|---|---|
| 0 | Set up | | | |
| 0a | Strategy (checkpoint: models, extend or new, sibling and linked reports, row access) | | | |
| 1 | Read the draft | | | |
| 2 | Triage (checkpoint: scope sign-off) | | | |
| 3 | Materialized views and snapshots | | | |
| 4 | Metric views | | | |
| 5 | Reconciliation (statistics only) | | | |
| 6 | Genie space (checkpoint: space split; user runs benchmarks) | | | |
| 7 | Refresh job and pull request | | | |
| 8 | Semantic sync (report in the sync, baseline stored) | | | |

## Decisions (user)

- <date>: <decision> (<why>)

## Scope summary (phase 2, details in the scope file)

| | Draft | kept | dropped | added (missing in draft) |
|---|---|---|---|---|
| tables | | | | |
| measures | | | | |
| snapshots | | | | |

## Objects

| Object | Type | Model source | Rows | Validation |
|---|---|---|---|---|

## Reconciliation

<date>: <n>/<n> measures at 100 %, <cells> cells; open classes: drift … / model defect … / structural …

## Genie

<date>: benchmarks <full> / <partial> / <wrong>; findings from the user's test.

## Model defects (for the report owners)

- <measure / table>: <what the model does> → <what the view returns>

## Open points

-
"""

_BODY = """# Migrating a Power BI report to Databricks metric views and Genie

## When to use this skill

Use it when a Power BI report, or the semantic model behind it, has to be rebuilt on Databricks: its numbers
must equal what the report displays, and business users will then ask questions in a Genie space. Work on one
report at a time. Reports that share a semantic model share one schema, so never build two of them in
parallel.

The state lives in a status file (template: `templates/STATUS.md`) in the model's folder. Read it first when
resuming, and update it at the end of every phase and whenever the user decides something. It is the handoff
to the next session or colleague.

## Ground rules

- **Mirror what the report displays.** Every number must equal the value the visual shows. Where the model
  itself is wrong, return the intended value, note the defect in the measure's notes, and log it in the
  status file's defect list for the report owners.
- **Keep KPI values out of the conversation and out of files** when the data may not leave the process. Allowed:
  definitions (DAX, M, relationships, filters), object and column names and types, row counts, counts of
  matching and differing rows or cells, the share of measures aligned, and categorical values (country
  codes, company codes, KPI codes, period keys). Never: measure results, sums, averages, minimums or maximums of
  fact columns, assistant answers, benchmark results or reconciliation cell values. This holds for every
  channel: query output, error text, files, status files, commit messages and pull request text. Every
  comparison runs in a script that prints only verdicts, and "does this SQL work" is checked as a count
  (`SELECT COUNT(*) FROM (<query>)`). If a value appears anyway, stop, say where it came from, do not repeat it,
  and fix the tool or the query.
- **Discuss the strategy before building** (phase 0a). Agree on the models in scope, what is already
  translated, which sibling reports share the model, and how rows are restricted. Nothing is built before the
  user agrees.
- **One schema per semantic model, not per report.** A second report on a translated model only extends the
  schema (missing tables and measures); it never rebuilds it. Never create, change or drop objects outside the
  schemas of the models in scope and the shared access schema.
- **Object names inside a schema:**

  | Object | Name | Example |
  |---|---|---|
  | materialized view mirroring a model table | the model table's name in snake_case | `fact_orders`, `dim_calendar` |
  | snapshot table (data that exists only in the model) | `pbi_<model table>` | `pbi_company_mapping` |
  | metric view | `ucm_<subject>`, usually the fact it is built on | `ucm_fact_orders` |

- **Every materialized view gets a row filter** from the shared access schema, checked at query time as the
  reader. Without one, every account user reads the view unfiltered. Which filter is a checkpoint per model
  (reference: materialized views, access control).
- **No secrets in files, the conversation or the output.** Authenticate with the platform's CLI sign-in and
  secret scopes. Never ask for a token or password, never create or read a credentials file, and never print a
  secret. If a sign-in has expired, ask the user to renew it.
- **A service principal is enough for Power BI.** It reads definitions (Fabric `getDefinition`) and, where the
  tenant lets service principals use the Power BI APIs and the principal is a workspace member, also runs the
  DAX queries and reads the refresh history. A service account for DAX is the fallback where the tenant only
  allows users (often blocked by MFA / conditional access: check first). Throttling (HTTP 429) is retried.
- **Legacy reports work.** A report the service cannot convert to PBIR (`FailedToExportReport`) is read through
  the legacy fallback and converted locally; the run says so. Saving the report once in PBIR makes it easier to
  review.
- **Sources must be in Unity Catalog.** The materialized views read what the Power BI M queries read. If Power BI
  reads SQL Server, Synapse, Snowflake or files directly, ingest or federate them first (Lakeflow Connect,
  Lakehouse Federation). Plan this in phase 0a: it usually takes longest.
- **Confidential data mode.** Where the AI policy keeps company figures away from assistants, turn on Kasal's
  confidential mode for the workspace or the tools (`confidential_values`): value-returning tools then return
  counts, names and categorical values only, and tools with a value-free mode (reconciliation, Genie eval) run in
  it. Relax it where the policy allows.
- **Checkpoints belong to the user:** measure and table scope, the snapshot choice, anything that deviates from
  the report, and every deploy to shared objects (ask the first time per session).
- **Never trust generated output.** Generated YAML, mapping candidates and LLM-proposed mappings are candidates.
  A measure counts as right only when reconciliation shows it aligned.
- **Tool changes go on a separate branch** from report changes, so parallel report work does not conflict.
- **Shared limits:** one SQL warehouse and one service account serve everybody. Do not start a full
  reconciliation or a Genie benchmark run while another run is active.

## Phases

| # | Phase | Details | Done when |
|---|---|---|---|
| 0 | Set up: branch, status file, report registered | below | status file exists with IDs |
| 0a | Strategy | below | checkpoint: models in scope, new or extend, sibling and linked reports, row access |
| 1 | Read the draft | references/triage.md | candidate lists in the status file |
| 2 | Triage both ways | references/triage.md | checkpoint: scope sign-off |
| 3 | Materialized views and snapshots | references/materialized-views.md | row counts match per period, or the difference is explained by refresh time; checkpoint on snapshots |
| 4 | Metric views | references/metric-views.md | deployed equals the YAML; every measure executes (as a count) and has a format |
| 5 | Reconciliation | references/reconciliation.md | 100 %, or every gap classified |
| 6 | Genie space | references/genie-space.md | checkpoint: space split; every SQL example executes; user runs benchmarks |
| 7 | Refresh job and pull request | references/refresh-and-sync.md | job's first run green; pull request merged; job redeployed from the main branch |
| 8 | Semantic sync | references/refresh-and-sync.md | report in the sync job; baseline stored; status file says so |

Work phase by phase. Finish a phase's "done when" before starting the next. After each phase: update the
status file, commit the report's files on the report branch, and tell the user in a few lines what was decided
and what comes next.

### Phase 0: set up

1. Create the report branch from the main branch, named after the ticket and the report. Ask the user for the
   ticket.
2. Collect the report's identifiers (workspace, semantic model, report) from the user, and register them in the
   project's report registry, so the reconciliation tools know the report.
3. Copy the status template into the model's folder, and fill in the identifiers, the draft's location and the
   ticket. The schema is created only after phase 0a.

### Phase 0a: strategy (checkpoint, before any build)

Work out and present, then agree with the user:
1. **Models the report needs:** the report's own model, plus every other model it reads (composite or
   DirectQuery tables and tables imported from another catalog). List each with its workspace and model ID.
2. **Already translated?** If the model's schema exists, this report only extends it. Diff the report's
   measures, tables and columns (triage) against the existing metric views and mappings, and add what is
   missing. Never rebuild a translated model.
3. **Sibling reports on the same model:** their visual usage belongs in the inventory too, or the schema serves
   one report and breaks the others. The user decides which siblings are in scope now.
4. **Linked reports** (navigation buttons and URLs in the report definition, URL parameter tables): separate
   models, so separate schemas and spaces. List them; the user decides in or out.
5. **Row access:** first the model's own restrictions (role definitions and user-identity functions in DAX).
   Without them every reader sees every row. Then propose a row filter per future view and let the user decide
   (mirror the report, or a row restriction by company or country), with what each option hides from whom.

Record the agreed strategy in the status file under Decisions. Then, with the user's OK, create the schema (its
owner is the creator). Right after creating it, grant the team's colleagues full rights on the schema so every
current and future object in it is covered. Check the grants afterwards.

## Tools

Kasal's tools for each job are listed at the end of `references/refresh-and-sync.md`. The phases above name the
checks; run them as counts and verdicts only. If a tool cannot do what a step needs, extend the tool with a test,
rather than writing a throw-away script that prints query results.

## Lessons that apply everywhere

- **A metric-view change is never only a YAML change:** update the Genie space (examples, benchmarks, snippets,
  column settings, instructions) and the reconciliation mappings in the same step, then run the consistency
  check.
- **Export the Genie space after every change** and commit it. The repo copy is the only backup of a space's
  configuration.
- **Every measure needs a `format`,** or Genie prints the raw value. Percentages are fractions with the
  percentage format. Check the scale against the data, not the name.
- **Redeploying a metric view drops its direct grants.** Count them before, re-grant after, and compare.
- **Only what a user can reach counts.** KPI definitions come from the visible path, not from hidden pages.
  Unreachable measures and their mappings are dropped.
- **A metric view returns only combinations present in the fact.** A "latest period" lookup can be empty while
  the fact has no rows for that period. Anchor "latest" on a measure that is not null.
- **Materialized views freeze `current_date()` at refresh,** like a report's `TODAY()` in calculated columns.
  Compare with the report only after both were refreshed, and check the model's refresh history before calling a
  difference an error.
- **A measure may `MEASURE()` only measures defined above it** in the YAML. A failed deploy leaves the old view
  live, so verify deploys by comparing the deployed definition with the YAML, not by column counts.
- **Scripted YAML edits:** parse before and after, and assert that the source, joins and measure list changed
  only as intended. Never run a multi-line regex across YAML blocks.
- **Genie follows column names more than display names or synonyms.** Names carry the meaning.
- **Genie tuning order:** metric-view metadata, then space column settings, then SQL examples, then instructions
  (the last resort, kept short). Reasoning rules need an instruction and an example that returns the value the
  answer must mention.
- **Row filter constants are limited to 255 characters.** List sources compactly, one entry per source schema.
  A filter can only be set at creation.
- **Dropping a materialized view can leave its backing table behind,** unfiltered. Drop that table too.
  `CREATE OR REPLACE` leaves nothing behind.
"""

_FILES = [
    {"path": "references/materialized-views.md", "content": _MATERIALIZED_VIEWS},
    {"path": "references/metric-views.md", "content": _METRIC_VIEWS},
    {"path": "references/genie-space.md", "content": _GENIE_SPACE},
    {"path": "references/triage.md", "content": _TRIAGE},
    {"path": "references/reconciliation.md", "content": _RECONCILIATION},
    {"path": "references/refresh-and-sync.md", "content": _REFRESH_AND_SYNC},
    {"path": "templates/STATUS.md", "content": _STATUS},
    {"path": "templates/MODEL_README.md", "content": _MODEL_README},
]

SKILL = {
    "name": "powerbi-to-metric-views",
    "description": (
        "Migrate a Power BI report to Databricks: build materialized views and Unity Catalog "
        "metric views from its semantic model, reconcile every KPI against what the report shows, "
        "set up a Genie space, then keep it in sync. Use when the user wants to port, migrate or "
        "rebuild a Power BI report or semantic model on Databricks, turn DAX measures into metric "
        "views, check numbers against Power BI, or put Genie on top of a Power BI model."
    ),
    "body": _BODY,
    "files": _FILES,
}
