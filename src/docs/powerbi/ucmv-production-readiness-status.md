# Production readiness: test status and open gaps

**Status: internal.** Not published with the app docs. The customer-facing guide is `ucmv-production-readiness.md`.

## Test status

Read this before relying on a step.

| Part | Status |
|---|---|
| Extract and convert (Tools 90 → 86), measure formats, semantic snapshot | **Run live** against a production-size model (≈700 measures, 90+ Power Query tables): 36 metric views, about 73 % of real measures translated |
| Report scope (page reachability, filters, review buckets) | Unit tested. In the live run it produced nothing: the workspace returned no Fabric definitions (see [Known limits](#known-limits)) |
| Deploy verify / check, `preserve_grants` | Unit tested only |
| Reconciliation options (`statistics_only`, week grain, retry, lock) | Unit tested only |
| Source Layer Manager (Tool 100), Genie Space Sync (Tool 99), UCMV Workbench (Tool 101), semantic sync in the Drift Monitor (Tool 98), confidential mode | Unit tested only |

"Unit tested only" means the logic is covered by tests but has not yet run against a live workspace. Use a sandbox schema the first time.

## Phase by phase: what covers what

A full migration of one report runs through the phases below. Each row names the Kasal crew or tool for the job, its status, and what is still missing. A ✋ marks a human decision.

| # | Phase | Result | In Kasal | Status | Not covered yet |
|---|---|---|---|---|---|
| 0a | Strategy ✋ | Models in scope, target schema, sibling / linked reports, access model | Crew "1 · Extract Power BI Model": `report_scope` with linked and sibling reports and the model inventory; the `powerbi-to-metric-views` skill for the decisions | Live (scope needs Fabric definitions) | Scope without Fabric definitions |
| 1 | Intake | Candidate lists from the generated output | Crew "2 · Convert to Metric Views": YAML, mapping drafts, untranslated measures with proposals | Live | — |
| 2 | Triage ✋ | Scope table: every table, measure and column kept or dropped, with the reason | `report_scope`, **Download Scope**, review buckets (`report_scope.review`); bucket choices in the Drift Monitor (`scope_review_choices`) | Unit tested | — |
| 3 | Sources ✋ | One materialized view per kept Power BI table, row counts equal to Power BI, snapshots of Power-BI-only data | [Tool 100](./tool-100-ucmv-source-layer-manager.md): `setup_access`, `emit` / `deploy`, `load_snapshots`, `count_check` | Unit tested | Generator `source_layer_ddl` → Tool 100 `view_specs_json` hand-off; no preseeded crew |
| 4 | Metric views | Thin metric views, a format on every measure, names Genie understands | Crew 2 (formats, `read_from_source_layer`); crew "4 · Finish Metric Views to 100 %" for the fixes | Live (generator); unit tested (crew 4) | — |
| 5 | Reconciliation | Every measure 100 % aligned with Power BI, or its gap classified | Crew "2 · Convert … + Verify Numbers" (iterative loop); crew 4 with the [UCMV Workbench](./tool-101-ucmv-workbench.md) (fix, deploy to sandbox, reconcile, classify) | Unit tested | A live run with data |
| 6 | Genie space ✋ | Examples, benchmarks, the space kept as files | Crews "9 · Prepare Genie Space" and "10 · Create Genie Space"; [Tool 99](./tool-99-genie-space-sync.md) `extract` / `check` / `eval` | Create: live before; Tool 99: unit tested | No preseeded crew for Tool 99 |
| 7 | Release ✋ | Deployed views with grants kept; a refresh that waits for Power BI | Crew "5 · Deploy Metric Views" (`preserve_grants`, `mode: verify` / `check`); Tool 100 `refresh` on the Kasal scheduler | Deploy path live; refresh unit tested | Pull request / git release of the YAML |
| 8 | Keep in sync | Daily change detection routed through the scope table, proposals for a person | "Continuous Drift Monitor" with `semantic_snapshot_table`, `report_scope`, `semantic_ignore` on the scheduler | Unit tested | Work items (e.g. Azure DevOps / Jira) for the changes |

**People still do these:**

- look at actual values when a reconciliation gap needs it (the tools show shares, not values);
- run the Genie benchmarks and judge the answers;
- create tokens and secrets.

**One agent vs. crews:** the reference process runs all phases with one coding agent. Kasal splits them into crews you chain in a flow, with an approval gate at each ✋. Crew 4's agent covers phases 4–5 only: it does not call the Source Layer Manager or Genie Space Sync.

