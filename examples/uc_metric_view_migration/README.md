# UC Metric View Migration

Migrate Power BI semantic models to Databricks Unity Catalog Metric Views. One tool, two modes: live API extraction or pre-extracted JSON.

**No commercial tool does this.** Microsoft's migration path keeps DAX (goes to Fabric). Consulting firms do manual rewrites. UC Metric Views (v1.1, DBR 17.2+) are too new for anyone else to have built migration tooling.

## How It Works

```
                    ┌─────────────────────────────────────────┐
                    │         Tool 86: UC Metric View         │
                    │              Generator                   │
                    ├─────────────────────────────────────────┤
                    │                                         │
   API MODE         │   JSON MODE                             │
   (automatic)      │   (pre-extracted)                       │
                    │                                         │
   PBI credentials ─┤── OR ── measures_json ──┐              │
   workspace_id     │         mquery_json     │              │
   dataset_id       │         relationships   │              │
                    │         scan_data        │              │
                    │         config_json ─────┘              │
                    │              │                          │
                    │              ▼                          │
                    │   ┌─────────────────────┐              │
                    │   │ MQuery Parser       │              │
                    │   │ DAX Translator (14p) │              │
                    │   │ Kahn's Dependency    │              │
                    │   │ Join Detector        │              │
                    │   │ YAML Emitter         │              │
                    │   │ Migration Report     │              │
                    │   └─────────────────────┘              │
                    │              │                          │
                    │              ▼                          │
                    │   YAML + SQL + Report per fact table    │
                    └─────────────────────────────────────────┘
```

## Workflow Options

### Option A: Fully Automatic (API Mode)

One tool call. Provide PBI credentials, get metric views back.

```
Tool 86 config:
  workspace_id:  bcb084ed-...
  dataset_id:    9340689f-...
  tenant_id:     9f37a392-...
  client_id:     7b597aac-...
  client_secret: ***
  auth_method:   service_principal
  catalog:       my_catalog
  schema_name:   my_schema
  config_json:   {pipeline_config.json contents}
```

Tool 86 automatically:
1. Authenticates via Service Principal
2. Extracts measures (Execute Queries API)
3. Extracts MQuery table definitions (Admin API scan)
4. Extracts relationships (Execute Queries API)
5. Extracts scan data (Admin API)
6. Runs the full pipeline (parse → translate → detect joins → emit YAML/SQL)
7. Returns YAML + SQL + migration report per fact table

### Option B: Two-Phase (Extract → Review → Migrate)

For customers who want to review metadata before migration:

**Phase 1: Extract metadata** (use tools 73, 74, 75 individually or as a crew)
```
Tool 74 (MQuery Converter)     → mquery_transpilation.json
Tool 73 (Measure Converter)    → measure_table_mapping.json
Tool 75 (Relationships Tool)   → pbi_relationships.json
```

**Review step**: Customer inspects the JSONs, adjusts allocations, builds `pipeline_config.json` with join maps and SWITCH decompositions.

**Phase 2: Generate metric views** (Tool 86 in JSON mode)
```
Tool 86 config:
  measures_json:      {Phase 1 output}
  mquery_json:        {Phase 1 output}
  relationships_json: {Phase 1 output}
  config_json:        {customer-reviewed config}
  catalog:            my_catalog
  schema_name:        my_schema
```

This is the workflow used in the example files below — the JSONs were extracted once, reviewed, then fed to the pipeline.

### Option C: Multi-Task Crew (Agent Orchestrated)

Create a Kasal crew where the agent chains tools automatically:

```
Task 1: "Extract MQuery table definitions"
  → Tool 74, PBI credentials in config

Task 2: "Extract DAX measures"
  → Tool 73, PBI credentials in config

Task 3: "Extract relationships"
  → Tool 75, PBI credentials in config

Task 4: "Generate UC Metric Views from the extracted data"
  → Tool 86, catalog + schema + config_json in config
  → Agent passes Task 1-3 outputs automatically
```

### Future: Option D (Planned)

An extraction agent that auto-exports metadata to a staging area for review:
```
Agent extracts → saves to workspace files → user reviews in UI → triggers migration
```
This separates the extraction from the migration, letting customers inspect and edit the metadata (especially `pipeline_config.json`) before committing to a migration run.

## Example Files

| File | Size | Description |
|------|------|-------------|
| `run_locally.py` | — | Run the pipeline locally (same output as original monolith) |
| `crew_ucmv_generator.json` | 4K | Importable Kasal crew config |
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

Output: `~/Downloads/ucmv_example_output/` — 26 YAML + 26 SQL + migration report.

## Expected Output

- **23 metric views** matching the verified reference output
- **3 bonus views** (C_Banner, C_Dim_calendar, FT_SKU_Performance)
- **~366 measures translated** across all tables
- **Migration report** (markdown) with executive summary, per-table stats, join map, untranslatable measures
- Diffs vs reference are cosmetic only (parenthesization, SQL formatting)

## Pipeline Config Reference

The `pipeline_config.json` drives all customer-specific behavior. Empty defaults work for basic models; populated values are needed for complex PBI patterns.

### Core Config (structure)

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

### Display Config (metadata)

| Key | When Needed |
|-----|-------------|
| `dimension_exclusions` | Per-table dimensions to hide from output |
| `measure_metadata` | Per-table display names, synonyms, comments |
| `comment_overrides` | Per-table metric view comment overrides |
| `name_prefixes_to_strip` | Prefixes to strip for display names (e.g. `["bic_", "khr"]`) |
| `percentage_multiplier_patterns` | Regex for measures needing 100* multiplier |

### Behavior Config (tuning)

| Key | Default | When Needed |
|-----|---------|-------------|
| `dim_alias_map` | `{}` | PBI dimension table → SQL join alias mapping |
| `parameter_defaults` | `{}` | PBI parameter values (`CurrencyFilter`, `RE_Version_ranges`) |
| `period_dim_priority` | `["fiscper", "fiscal_year_period", "date_key"]` | Custom period dimension ordering |
| `int_period_dims` | `["fiscper", "fiscal_year_period"]` | Period dimensions with integer type |
| `budget_suffix` | `"_bp"` | Suffix identifying budget measure variants |
| `cwc_filter_column` | `"bic_cwc_type"` | Physical column for CWC filter expansion |
| `switch_join_alias` | `"dim_wkctr"` | Default join alias for SWITCH measures |
| `switch_join_col` | `"bic_cwc_type"` | Default join column for SWITCH FILTER |

### Starting From Scratch

For a new customer with no config:
1. Run Tool 86 in API mode with `config_json: {}` — this produces a baseline with auto-detected joins and base measures only
2. Review the migration report — it shows what was translated and what was skipped
3. Build `pipeline_config.json` incrementally: add `join_key_map` entries for skipped dimensions, `switch_decompositions` for SELECTEDVALUE+SWITCH measures, `measure_resolutions` for CALCULATE([ref]) patterns
4. Re-run with the config — each iteration translates more measures
