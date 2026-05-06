# UC Metric View Migration Example

Generates Databricks UC Metric View YAML + deploy SQL from Power BI data — identical output to the original `generate_metric_views.py` monolith.

## Files

| File | Size | Description |
|------|------|-------------|
| `run_locally.py` | — | Run the pipeline locally (same output as original script) |
| `crew_ucmv_generator.json` | 4K | Kasal crew config for import |
| `pipeline_config.json` | 43K | Customer-specific config (join maps, filter sets, SWITCH decompositions) |
| `measure_table_mapping.json` | 569K | 470 DAX measures with table allocations |
| `mquery_transpilation.json` | 202K | 60 MQuery table transpilations |
| `pbi_relationships.json` | 64K | PBI relationships for auto-join detection |
| `scan_result_debug.json` | — | PBI scan data for inline SQL enrichment |

## Quick Run

```bash
cd src/backend
source .venv/bin/activate
python ../../examples/uc_metric_view_migration/run_locally.py
```

Output: `~/Downloads/ucmv_example_output/` — 26 YAML + 26 SQL files.

## Expected Output

- **23 metric views** matching the original `sc_reporting_project/output/`
- **3 bonus views** (C_Banner, C_Dim_calendar, FT_SKU_Performance — artifact-only, filtered by original)
- **~366 measures translated** across all tables
- Diffs vs original are cosmetic only (parenthesization, SQL formatting)

## Using via Kasal UI

1. **Import crew**: Load `crew_ucmv_generator.json` into Kasal
2. **Set execution inputs**: The crew expects 5 JSON inputs:
   - `measures_json` — contents of `measure_table_mapping.json`
   - `mquery_json` — contents of `mquery_transpilation.json`
   - `relationships_json` — contents of `pbi_relationships.json`
   - `scan_data_json` — contents of `scan_result_debug.json`
   - `config_json` — contents of `pipeline_config.json`
3. **Run** — the agent calls Tool 86 and returns YAML + SQL + stats

Note: The input files are large (900K+ total). For production use, chain tools 73→74→75→86 so the agent extracts data live from the Power BI API instead of pasting JSON.

## Live Pipeline (no pre-extracted data)

For a real migration, create a 4-task crew:

```
Task 1: Extract MQuery (Tool 74)
  → Config: workspace_id, dataset_id, PBI credentials

Task 2: Extract Measures (Tool 73)
  → Config: inbound_connector=powerbi, outbound_format=uc_metrics, PBI credentials

Task 3: Extract Relationships (Tool 75)
  → Config: PBI credentials

Task 4: Generate UC Metric Views (Tool 86)
  → Config: catalog, schema_name, config_json (pipeline_config.json contents)
  → Receives outputs from Tasks 1-3 automatically
```

## Pipeline Config Reference

The `pipeline_config.json` contains customer-specific configuration. Empty defaults work for basic tables; populated values are needed for complex patterns:

| Key | When Needed |
|-----|-------------|
| `join_key_map` | Dimension tables with non-obvious join keys |
| `fact_join_map` | Cross-table DIVIDE measures (fact-to-fact joins) |
| `enrichment_joins` | Extra dimension joins not auto-detected from DAX |
| `filter_sets` | Named filter value lists for SWITCH decomposition |
| `switch_decompositions` | SELECTEDVALUE+SWITCH measures broken into SQL |
| `measure_resolutions` | Static DAX measure name → SQL resolution map |
| `mapping_only_tables` | Tables with measures but no MQuery SQL entry |
| `column_overrides` | PBI column → physical column name overrides |
| `dimension_exclusions` | Per-table dimensions to hide |
| `measure_metadata` | Per-table display names, synonyms, comments |
| `comment_overrides` | Per-table metric view comment overrides |
