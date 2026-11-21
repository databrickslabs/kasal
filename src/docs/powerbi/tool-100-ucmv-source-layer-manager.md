# Tool 100 - UCMV Source Layer Manager

**What it is:** Manages the materialized-view layer under the metric views: one view per Power BI table (its Power Query, applied steps and calculated columns), so the metric views stay thin. It also sets up row access, copies tables that exist only inside Power BI, refreshes the layer and compares row counts with Power BI.

---

## Why it exists

A metric view that carries a whole Power Query transformation in its `source:` is hard to read, slow and impossible to reuse. With one materialized view per Power BI table in between, the metric view only aggregates. Row-level security then has a single place to live, and refreshes happen once per table.

## Actions

| Action | What it does | Writes? |
|---|---|---|
| `emit` | Produces the source-layer DDL with Unity Catalog row filters (`WITH ROW FILTER`), plus a hint for every Power BI RLS role | No |
| `deploy` | Creates the views in dependency order | Yes |
| `setup_access` | Creates the access schema, the row access map and the access functions (`object_access`, `value_access`), and grants the reader groups | Only with `confirm: true` |
| `load_snapshots` | Copies tables that exist only inside Power BI into typed `pbi_<table>` tables, from the published model. Typed-in data, Excel/SharePoint and other models count. Large tables are paged, and the row filter is re-applied after every load | Yes |
| `refresh` | Reloads the snapshots, then refreshes the views. With `skip_if_unchanged`, only after Power BI has refreshed | Yes |
| `count_check` | Compares a Power BI table with its UC object: total and per-period row counts, members on one side only, numeric columns that differ. Separates drift after Power BI's last refresh from real differences | No |

Outputs carry verdicts and counts only, never values.

## Configuration

| Parameter | Used by | Description |
|---|---|---|
| `action` | all | See above (default `emit`) |
| `catalog`, `schema_name`, `warehouse_id` | all | Target and warehouse |
| `view_specs_json` | `emit`, `deploy` | `[{table, source_table or native_sql, m_expression, group_by_columns, calc_columns, target}]` |
| `row_filters` | `emit`, `deploy` | Per table, e.g. `{"fact_sales": {"value_access": {"field": "region", "column": "region_code"}}}` |
| `relationships_json`, `rls_dax_json` | `emit` | Lets the tool warn about facts a dimension filter does not reach and suggest filters from RLS DAX |
| `report_scope` | `emit` | Scope table: only `keep` / `snapshot` tables get views |
| `materialized` | `emit`, `deploy` | Materialized views (default) or plain views |
| `access_schema`, `reader_groups`, `access_rows`, `confirm` | `setup_access` | Where the access objects go, who may read, initial map rows |
| `snapshot_tables_json` | `load_snapshots` | `[{pbi_table, uc_table, page_by, types, column_map, tmdl_expression}]` |
| `views`, `view_prefix`, `skip_if_unchanged` | `refresh` | Which views, and whether to skip when nothing changed |
| `pbi_table`, `uc_object`, `by`, `uc_by`, `members`, `sums`, `column_map_json`, `cutoff_column` | `count_check` | The table pair, the period column and what to compare |
| `workspace_id`, `dataset_id`, `tenant_id`, `client_id`, `client_secret` | `load_snapshots`, `refresh`, `count_check` | Power BI access |
| `confidential_values` | all | See [confidential mode](./ucmv-production-readiness.md#confidential-mode) |

## Notes

- A fact table needs the column its row filter uses; `rls_warnings` names the ones where it is missing.
- `load_snapshots` copies data out of the published model. Run it only where that data may land in the target workspace.
- Snapshot loads are not atomic. If one fails, rerun it.
- The UC Metric View Generator's `source_layer_ddl` is not passed to this tool automatically yet. Deploy that DDL in the SQL editor, or write `view_specs_json`.

## See also

- [From generated to production-ready metric views](./ucmv-production-readiness.md)
- [Power BI integration hub](./README.md)

Back to the [Power BI integration hub](./README.md).
