# Tool 101 - UCMV Workbench

**What it is:** The workbench an agent uses to finish generated metric views until they reconcile with Power BI at 100 %. It holds the UC Metric View Generator output as a working copy for the run and offers small, safe steps: look at the open measures, change one measure, deploy to a sandbox, reconcile, classify what cannot be fixed, and export.

**Crew:** "4 · Finish Metric Views to 100 %" in the BI Specialist workspace. Its agent carries the `powerbi-to-metric-views` skill.

---

## Why it exists

The generator translates most measures in one pass (about 73 % on a production-size model). The rest needs someone who reads the DAX, writes the SQL, deploys, compares the numbers with Power BI and tries again. Field migrations reached 100 % this way with a coding agent run locally. The workbench brings that loop into Kasal: the same steps, on a Databricks serving endpoint, inside a flow.

## How the agent works with it

1. `overview`: all views, with open gaps and reconciliation state.
2. Per view:
   - `deploy`, then `reconcile` for the baseline.
   - `gaps` lists the open measures. These are the untranslated ones (with DAX, reason and the generator's proposal) and the ones below 100 %.
   - Per measure: `show`, then `set_measure` (name, SQL `expr`, the `pbi_measure` it reproduces), then `deploy` and `reconcile` again.
   - What can't reach 100 % gets `classify`: `data_drift`, `pbi_defect`, `structural` or `out_of_scope`, always with a note.
3. `report` until `open_gaps` is 0 and every view is reconciled.
4. `export`: the final YAML and mappings. Crew "5 · Deploy Metric Views" deploys them to the target schema.

A gap is closed when its measure reconciles at 100 % or is classified with a note.

## Safety

- One measure per edit. A measure name must be an identifier, and every expression goes through the deployer's dangerous-SQL check.
- Every deploy goes to the **sandbox** catalog/schema of the task form, never to the target.
- Reconciliation runs **statistics only** by default: the agent sees shares and classifications, not data values. Confidential mode forces this.
- The agent passes only `action`, `view`, `measure`, `expr`, `pbi_measure`, `display_name`, `format_json`, `comment`, `classification` and `note`. Credentials, warehouse and sandbox come from the task form.
- The working copy belongs to one run (keyed by the job ID).
- The edited YAML does not keep the generator's review comments; the workbench keeps the original gaps and DAX instead.

## Configuration (task form)

| Parameter | Description |
|---|---|
| `ucmv_output` | Generator output (flow-injected). If it is empty, the tool reads this run's, or the latest, generator output from the database |
| `catalog`, `schema_name` | **Sandbox** target of every deploy |
| `warehouse_id`, `databricks_host` | SQL warehouse; workspace override |
| `workspace_id`, `dataset_id`, `tenant_id`, `client_id`, `client_secret` | Power BI side of the reconciliation |
| `reference_years` | Optional, e.g. `[2025, 2026]` |
| `statistics_only` | `true` (default): no values in reconciliation results |
| `confidential_values` | Confidential mode |

## See also

- [From generated to production-ready metric views](./ucmv-production-readiness.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [Tool 88 - metric view deployer](./tool-88-metric-view-deployer.md)

Back to the [Power BI integration hub](./README.md).
