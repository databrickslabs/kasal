# From generated to production-ready metric views

The [migration guide](./ucmv-migration-guide.md) ends with generated, deployed metric views. This guide covers the steps after that. They take a generated draft to a set of metric views that people can rely on, and keep it in step with the Power BI model:

- decide what is in scope
- build a source layer under the metric views
- verify the deployed views
- reconcile the numbers without exposing values
- put the Genie space under version control
- sync the semantic model daily

- [Test status](#test-status)
- [The process at a glance](#the-process-at-a-glance)
- [Step 1: scope](#step-1-scope)
- [Step 2: convert](#step-2-convert)
- [Step 3: source layer and row access](#step-3-source-layer-and-row-access)
- [Step 4: deploy and verify](#step-4-deploy-and-verify)
- [Step 5: reconcile](#step-5-reconcile)
- [Step 6: Genie space as code](#step-6-genie-space-as-code)
- [Step 7: keep it in sync (scheduled)](#step-7-keep-it-in-sync-scheduled)
- [Confidential mode](#confidential-mode)
- [The skill for the human steps](#the-skill-for-the-human-steps)
- [Known limits](#known-limits)

## Test status

Read this before relying on a step.

| Part | Status |
|---|---|
| Extract and convert (Tools 90 → 86), measure formats, semantic snapshot | **Run live** against a production-size model (≈700 measures, 90+ Power Query tables): 36 metric views, about 73 % of real measures translated |
| Report scope (page reachability, filters, review buckets) | Unit tested. In the live run it produced nothing: the workspace returned no Fabric definitions (see [Known limits](#known-limits)) |
| Deploy verify / check, `preserve_grants` | Unit tested only |
| Reconciliation options (`statistics_only`, week grain, retry, lock) | Unit tested only |
| Source Layer Manager (Tool 100), Genie Space Sync (Tool 99), semantic sync in the Drift Monitor (Tool 98), confidential mode | Unit tested only |

"Unit tested only" means the logic is covered by tests but has not yet run against a live workspace. Use a sandbox schema the first time.

## The process at a glance

Each step is one crew with one agent and one task. The agent calls its tool with zero arguments; every value sits in the task's tool configuration. The crews marked ✋ end with a human decision before the next step.

```text
 1  Scope            Tool 90  ->  scope table (keep / snapshot / fold / genie / drop / n/a / review)   ✋ sign off
 2  Convert          Tool 86  ->  metric view YAML, formats, scope review buckets, source-layer DDL
 3  Source layer     Tool 100 ->  access objects, materialized views, snapshots, row-count check        ✋ counts OK
 4  Deploy + verify  Tool 88  ->  deploy (keep grants), verify definitions, check every measure runs
 5  Reconcile        Tool 86 (+ reconciliation) -> share of aligned cells per measure, no values      ✋ 100 % or explained
 6  Genie            Tools 93, 92, 99 -> create the space, export as YAML, check, eval                ✋ benchmarks pass
 7  Sync (scheduled) Tool 100 refresh, Tool 98 drift + semantic sync
```

Wire steps 1 → 2 → 4 as a flow; the flow hands each crew's output to the next. Steps 3, 6 and 7 run on their own or on the Kasal scheduler.

## Step 1: scope

**Crew:** Pipeline Config Generator (Tool 90), e.g. "1 · Extract Power BI Model".

**Set:** the usual Power BI credentials plus `report_id` (required for the scope). Optional: `tracked_tables`, a comma-separated list of small data tables to watch, e.g. status or mapping tables.

**Output, in addition to the config:**

- `report_scope`: one row per model object (table, column, measure, relationship, role) with a decision. It is built from what the report can actually reach:
  - pages reached through tabs, bookmarks, buttons, page navigators, tooltips and drill-through
  - filters and slicer selections per page
  - linked and sibling reports
  - an inventory of the model
- `semantic_snapshot`: the baseline for the daily semantic sync (Step 7).

**Decide:** in the UC Metric View Generator result, use **Download Scope** to get the CSV. Rows with decision `review` need a person. The vocabulary:

| Decision | Meaning |
|---|---|
| `keep` | Migrate it as a metric view object |
| `snapshot` | Copy it from the published model (it exists only inside Power BI) |
| `fold` | Fold it into the SQL of the measures that use it |
| `genie` | Leave it to Genie, e.g. date-intelligence variants |
| `drop` | Not migrated |
| `n/a` | Not applicable |
| `review` | Still open |

## Step 2: convert

**Crew:** UC Metric View Generator (Tool 86), e.g. "2 · Convert to Metric Views", after Step 1 in a flow.

New in the output:

| Output | What to do with it |
|---|---|
| `format` on every measure | Derived from the Power BI format string. Lint rules MV009 and MV010 flag measures without one |
| `report_scope.review` | Open scope rows grouped into buckets with one proposal each (L1 / L2 link a mirror, M1–M3 dropped but needed, B1–B5 building blocks, C1–C3 unused columns, G* gaps). Decide a bucket, not a row |
| `extra_filter` in the reconciliation mapping drafts | Page filters that apply to a measure; reconciliation uses them |
| `source_layer_ddl` | One materialized view per Power BI table, for Step 3 |

**Read mode:** with `"read_from_source_layer": true` in the pipeline config, every metric view reads from its `src_<table>` view instead of the raw source. Set `"source_layer_naming"` to `"src_prefix"` (default) or `"table_name"`. Use it once the source layer exists.

**SWITCH measures:** the extracted config carries `switch_join_alias` / `switch_join_col` as TODO markers when the model has SWITCH measures. Until they are filled in, those decomposed measures are reported as untranslatable with that reason. The rest of the run is not affected.

## Step 3: source layer and row access

**Tool:** [UCMV Source Layer Manager (Tool 100)](./tool-100-ucmv-source-layer-manager.md). Use one crew per action. Start with the dry runs.

| Action | Use it to | Key inputs |
|---|---|---|
| `setup_access` | Create the access schema, the row access map and the access functions. Dry run unless `confirm: true` | `access_schema`, `reader_groups`, `access_rows` |
| `emit` / `deploy` | Produce / create the materialized views with row filters | `view_specs_json`, `row_filters`, `report_scope` |
| `load_snapshots` | Copy tables that exist only inside Power BI into `pbi_<table>` | `snapshot_tables_json` |
| `count_check` | Compare a Power BI table with its UC object (total and per period) | `pbi_table`, `uc_object`, `by`, `cutoff_column` |

Row filters, shorthand form:

```json
{"fact_sales": {"value_access": {"field": "region", "column": "region_code"}}}
```

A fact table needs the column its row filter uses. `rls_warnings` names facts that a dimension filter does not reach.

**Only with customer approval:** `load_snapshots` copies data out of the published model. Run it only where the customer has agreed that the data may land in that workspace.

## Step 4: deploy and verify

**Tool:** Metric View Deployer (Tool 88). Use one crew per mode.

| Setting | Effect |
|---|---|
| `preserve_grants: true` (default) | Reads the grants before a replace and re-applies them. Fails closed if they cannot be read |
| `mode: verify` | Read-only. Compares each deployed definition with the generated YAML, field by field |
| `mode: check` | Read-only. Runs every measure once. Optional `group_by`, `timing`, `slow_threshold_s` |

## Step 5: reconcile

**Crew:** "2 · Convert to Metric Views + Verify Numbers" (Tool 86 with `enable_reconciliation: true`, `warehouse_id`). It deploys and reconciles every cycle, so no separate deployer is needed.

The reconciliation tool itself also supports these options:

- `statistics_only`: shares and counts, no values
- `scale_check`: percentage measures off by a factor of 100
- `cross_breakdown`: period × member per measure
- week grain, `pbi_year_column` / `pbi_month_column`
- retry and chunking against Power BI
- `lock`: one run per view at a time

The tool has no catalog entry of its own yet, so the generator runs it with defaults.

## Step 6: Genie space as code

Create the space with the crews "Prepare Genie Space" (Tool 93) and "Create Genie Space" (Tool 92). Then use [Genie Space Sync (Tool 99)](./tool-99-genie-space-sync.md):

| Action | Use it to |
|---|---|
| `extract` | Export the space as YAML files. Commit them after every change |
| `deploy` | Create or update a space from those files. Dry run unless `confirm_deploy: true` |
| `check` | Run every example, benchmark and snippet as a row count against the deployed views, and check the column settings and data sources |
| `eval` | Ask the space its benchmark questions (and `eval_questions`), grade the answers, optionally with an LLM judge. Use `statistics_only: true` for value-free results |

A metric view change belongs in the YAML, the Genie space and the reconciliation mappings together.

## Step 7: keep it in sync (scheduled)

Schedule two crews with the Kasal scheduler:

- **Refresh:** Tool 100 with `action: refresh`, `view_prefix: src_`, `skip_if_unchanged: true`. It reloads the snapshots and refreshes the materialized views, but only after Power BI has refreshed.
- **Drift + semantic sync:** the "UCMV Drift Monitor" crew (Tool 98) with these inputs:
  - `ucmv_names`: the deployed views
  - `semantic_snapshot_table`: e.g. `<catalog>.<schema>.pbi_semantic_object`. The first run stores the baseline
  - `report_scope`: the signed-off scope CSV
  - optional: `semantic_ignore`, `scope_review_choices` (e.g. `{"L1": "accept"}`), `tracked_tables`

  Every change found in tables, columns, relationships, Power Query, RLS, pages, page fields or tracked tables is routed through the scope decision and gets an action. Changes that need code also get an LLM proposal, which is never applied automatically. `scope_problems` lists signed-off rows that no longer match the model or the deployed views.

## Confidential mode

Set `confidential_values: true` in the tool configuration of Tools 90, 98, 99 and 100. What changes:

- Fractional numbers in tool outputs come back as `<withheld>`.
- Reconciliation and Genie eval switch to statistics only.

What it does not cover:

- Integers pass through.
- It covers tool outputs only, not what an agent writes on its own.

## The skill for the human steps

The built-in skill `powerbi-to-metric-views` (Configuration → Skills) describes the whole process, the decision vocabulary and the templates. Attach it to a chat agent and paste a tool's output at each ✋ step.

## Known limits

- **Report scope needs Fabric definitions.** The scope is built from the model's TMDL and the report definition (`getDefinition`). If the workspace returns no definition, `report_scope` is empty and so are the review buckets, Download Scope and `extra_filter`. That happens when the workspace is not Fabric-enabled or the service principal lacks item rights; look for "Fabric TMDL fallback returned no data" in the output. The conversion itself still runs.
- **`emit` / `deploy` of the source layer take `view_specs_json`.** The generator's `source_layer_ddl` is not handed to Tool 100 automatically yet. Until then, deploy the DDL in the SQL editor, or write the view specs.
- **Reconciliation options** beyond `statistics_only` are not settable from a crew yet (see Step 5).
- **Snapshot loads are not atomic.** A failed load can leave a partly loaded `pbi_<table>`; rerun it.

## See also

- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)
- [Tool 90 - pipeline config generator](./tool-90-pipeline-config-generator.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [Tool 88 - metric view deployer](./tool-88-metric-view-deployer.md)
- [Tool 99 - Genie Space Sync](./tool-99-genie-space-sync.md)
- [Tool 100 - UCMV Source Layer Manager](./tool-100-ucmv-source-layer-manager.md)

Back to the [Power BI integration hub](./README.md).
