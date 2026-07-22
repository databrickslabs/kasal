# Golden baseline outputs — PBI/UCMV/Genie pipeline (Phase 0 of the WP3 migration)

Captured **2026-06-11** from live-validated flow executions (post PR-52, all paths verified
end-to-end against real Databricks/PowerBI environments). These files are the parity reference
for the WP3 god-tool → domain-service extraction: after migrating a tool, re-run the matching
flow and compare the structured fields below against these baselines.

See `src/docs/powerbi/ucmv-pipeline-architecture.md` for the end-to-end stage map.

## Files and source runs

| File | Producer | Source run |
|---|---|---|
| `ucmv_output.json` | UC Metric View Generator (JSON mode) | UCMV pipeline flow, 2026-06-11 |
| `genie_space_config.json` | UCMV Genie Space Config Generator (Tool 93) | `flow_genie_space_gen`, 2026-06-11 |
| `visual_mappings.json` | PBI Visual-UCMV Mapper (Tool 94) | `flow_dashboard_deployer`, 2026-06-11 |
| `reduced_model_context.json` | Power BI Metadata Reducer (Tool 81, strategy=llm) | analyst crew, 2026-06-11 |

## Comparison rules

**Compare (must match after a migration):**
- `ucmv_output.json`: `yaml` table keys, per-table measure names, `sql` keys, `stats` counts,
  `measures_with_dax` entries.
- `genie_space_config.json`: presence + non-emptiness of `text_instructions`,
  `sample_questions`; parseability of `example_sqls_json` / `join_specs_json`; `catalog`,
  `schema_name`.
- `visual_mappings.json`: per-visual `visual_id`, `visual_type`, `ucmv_view`, `dimensions`,
  `measures`; every `sql` uses `MEASURE()` syntax against the mapped view.
- `reduced_model_context.json`: `status`, `reduction_summary` table/measure selections,
  `cache_saved: true`.

**Do NOT compare (environment-specific or LLM free-text — expected to vary):**
- Sanitized placeholders: `https://example.databricks.com`, `WAREHOUSE_ID_REDACTED`,
  `CUSTOMER` (customer names are scrubbed for the public repo).
- LLM prose wording (instruction phrasing, question wording) — compare presence/shape, not text.
- Timestamps, execution/job IDs, durations.

**Sanitization applied at capture:** workspace URLs → `example.databricks.com` /
`example.azuredatabricks.net`, warehouse IDs → `WAREHOUSE_ID_REDACTED`, customer names →
`CUSTOMER`, any PAT-shaped strings → `TOKEN_REDACTED`. Apply the same scrubbing to new
captures before diffing (see `tests/unit/golden/test_golden_baselines.py::SCRUB`).

## Refreshing baselines

Only refresh deliberately (intended behavior change), never to "make the diff green":
extract the new run's outputs from `execution_trace`, apply the sanitization above, replace
the file, and state the behavioral change in the PR description.
