# Tool 99 - Genie Space Sync

**What it is:** Treats a Genie space as code. It exports a space as YAML files, deploys a space from them, checks a space against the deployed metric views, and evaluates its answers.

**Status:** unit tested only; not yet run against a live space.

---

## Why it exists

A Genie space changes in the UI, and nobody can see what changed or roll it back. Its example SQL also breaks without notice when a metric view changes. Tool 99 makes the space a set of files you can commit, review and redeploy, and it checks those files against what is actually deployed.

## Actions

| Action | What it does | Writes? |
|---|---|---|
| `extract` | Reads a space and returns its configuration as YAML files: header, config, data sources, instructions, benchmarks | No |
| `deploy` | Creates or updates a space from those files | Only with `confirm_deploy: true` |
| `check` | Runs every example, benchmark and snippet SQL as a row count (never values) and checks the column settings and data sources against existing objects. Also flags percentage measures multiplied by 100 and unknown identifiers in instructions, and checks reconciliation mappings if given | No |
| `eval` | Asks the space its benchmark questions plus `eval_questions` and grades the answers, optionally with an LLM judge and against a Fabric data agent | No (optional `results_table`) |

## Configuration

| Parameter | Used by | Description |
|---|---|---|
| `action` | all | `extract`, `deploy`, `check` or `eval` |
| `space_id` | all but a first `deploy` | The Genie space |
| `space_files` | `deploy`, `check` | YAML files from `extract` |
| `warehouse_id` | `check`, `eval` | SQL warehouse for the row counts |
| `confirm_deploy` | `deploy` | `false` (default) = dry run |
| `check_sql` | `check` | Run the SQL (default `true`) |
| `reconciliation_mappings` | `check` | Optional mappings to check as well |
| `use_benchmarks`, `eval_questions`, `max_questions` | `eval` | Which questions to ask (default: benchmarks, at most 50) |
| `statistics_only` | `eval` | Report scores and counts, no values |
| `llm_judge`, `llm_model` | `eval` | Grade free-text answers with an LLM |
| `fabric_agent_url`, `fabric_token` | `eval` | Optional: ask a Fabric data agent the same questions |
| `price_per_1k_tokens`, `results_table` | `eval` | Optional cost estimate and a table to store results |
| `confidential_values` | all | See [confidential mode](./ucmv-production-readiness.md#confidential-mode) |

## Example task configuration

```json
{
  "Genie Space Sync": {
    "result_as_answer": true,
    "action": "check",
    "space_id": "<space id>",
    "warehouse_id": "<warehouse id>",
    "check_sql": true
  }
}
```

## Notes

- Run `extract` and commit the files after every change made in the UI.
- Run `check` after every metric view change. The view, the Genie space and the reconciliation mappings change together.
- `eval` runs questions one after another; keep `max_questions` small for the first run.

## See also

- [From generated to production-ready metric views](./ucmv-production-readiness.md)
- [Power BI integration hub](./README.md)

Back to the [Power BI integration hub](./README.md).
