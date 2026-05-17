# Tool 90 - Pipeline Config Generator

**What it is:** Calls 4 Power BI APIs directly - no LLM, no intermediation - and produces a complete `pipeline_config.json` with all 26 keys for Tool 86. This is the recommended starting point for a new migration.

---

## Why It Exists

The `pipeline_config.json` that Tool 86 needs is complex (26 keys, nested structures, business-domain inputs). Generating it by hand from scratch takes hours. Tool 90 calls the PBI APIs and auto-fills ~70% of the config with real data from the model, leaving only the genuinely ambiguous parts (SWITCH decompositions, filter sets) for the SA to fill manually.

## What Problem It Solves

- **Config authoring cold start:** Instead of a blank JSON with 26 empty keys, the SA gets a populated draft with real table names, relationship keys, column metadata, and TODO markers
- **Accuracy:** Data comes directly from PBI APIs - no guessing, no manual lookup of workspace/dataset IDs, column names, or relationship definitions
- **Efficiency:** Typical SA time for config authoring goes from 6+ hours to ~2-3 hours for a first migration

---

## Why Two Service Principals?

The 4 APIs Tool 90 calls have different permission requirements:

| API Call | SP Needed | What It Gets |
|----------|-----------|-------------|
| `INFO.VIEW.RELATIONSHIPS()` | Non-Admin SP | Table relationships → `join_key_map`, `enrichment_joins` |
| `$SYSTEM.MDSCHEMA_MEASURES` | Non-Admin SP | All measures + DAX → `switch_decompositions` skeletons, `filter_sets` |
| Admin Scanner (`/admin/workspaces/scanResult`) | Admin SP | Full schema → `column_metadata`, M-Query, hidden flags |
| Report Definition (optional, PBIR) | Non-Admin SP | Visual metadata → `measure_metadata`, `dimension_metadata` with synonyms |

You **must** configure both SPs. See [Authentication Setup](./01-authentication-setup.md) for full instructions.

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workspace_id` | Yes | Power BI Workspace GUID |
| `dataset_id` | Yes | Semantic Model / Dataset GUID |
| `tenant_id` | Yes | Azure AD Tenant ID (shared by both SPs) |
| `client_id` | Yes | Non-Admin SP Client ID |
| `client_secret` | Yes | Non-Admin SP Client Secret |
| `admin_client_id` | Yes | Admin SP Client ID |
| `admin_client_secret` | Yes | Admin SP Client Secret |
| `report_id` | No | PBIR Report GUID (adds synonym metadata) |
| `catalog` | No | Target UC catalog (default: `main`) |
| `schema_name` | No | Target UC schema (default: `default`) |

---

## Example Crew

```json
{
  "name": "Generate Pipeline Config",
  "tasks": [{
    "name": "Auto-generate UCMV pipeline config",
    "description": "Call Power BI APIs to generate the pipeline_config.json for the UC Metric View Generator",
    "tool_ids": [90],
    "tool_config": {
      "90": {
        "workspace_id": "{workspace_id}",
        "dataset_id": "{dataset_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}",
        "admin_client_id": "{admin_client_id}",
        "admin_client_secret": "{admin_client_secret}",
        "report_id": "{report_id}",
        "catalog": "my_catalog",
        "schema_name": "metrics"
      }
    }
  }]
}
```

---

## What Gets Auto-Filled vs Manual (Real Example: SC Reporting, 471 measures)

| Config Key | Auto-fill Status | SA Action |
|------------|-----------------|-----------|
| `join_key_map` | ✅ 26 entries | Review, trim extras, add composite keys |
| `enrichment_joins` | ✅ 32 entries | Review, pick which need enrichment |
| `column_metadata` | ✅ Complete | Review only |
| `parameter_defaults` | ✅ Complete | Review only |
| `name_prefixes_to_strip` | ✅ Auto-detected | Adjust if needed |
| `dimension_exclusions` | ✅ 1 entry | Verify correct table |
| `measure_metadata` | ✅ 42 tables with synonyms | Review synonyms (if report_id provided) |
| `fact_join_map` | ⚠️ Skeletons only | Fill `grain`, `pivot_col`, `value_col` per table |
| `switch_decompositions` | ❌ Empty | Write SQL per SWITCH branch - biggest manual effort |
| `filter_sets` | ❌ Empty | Extract filter value lists from DAX/domain knowledge |
| `column_overrides` | ❌ Empty | Run Tool 86 first, then fix mismatches |
| `measure_resolutions` | ❌ Empty | Map cross-table measure references |
| `mapping_only_tables` | ❌ Empty | Specify source tables for pure-mapping fact tables |

Typical result: **11 keys auto-filled, 4 need review, 11 need manual domain knowledge.**

---

## Workflow After Running Tool 90

1. Download the proposed config from Tool 90 output
2. Open `proposed_pipeline_config.json` in an editor
3. Search for `TODO` markers - these are the gaps
4. Fill `switch_decompositions` first (biggest unlock - each entry increases translation rate)
5. Run Tool 86 → check `migration_report` for translation rate
6. Iterate: use `gap_analyzer.py` or Tool 89 gap analysis to prioritize next fixes
7. When rate is acceptable → Tool 88 dry-run → approve → deploy

See [UCMV Migration Guide](./ucmv-migration-guide.md) for the full iterative workflow.

---

## Standalone Script Equivalent

Tool 90 wraps the `generate_config.py` script from the examples directory. If you prefer running it outside Kasal:

```bash
python examples/uc_metric_view_migration/generate_config.py \
  --workspace-id ac0fa11c-... \
  --dataset-id ecdd57ae-... \
  --tenant-id 9f37a392-... \
  --client-id 7b597aac-... \
  --client-secret "U5b8Q~..." \
  --admin-client-id 8d8aa6ee-... \
  --admin-client-secret "RXm8Q~..." \
  --catalog my_catalog \
  --schema metrics \
  --output proposed_pipeline_config.json
```

---

## Notes

- Run Tool 90 **before** Tools 73/74/75 - the config it produces is needed by Tool 86, but Tool 90 itself calls the PBI APIs independently
- If you've already run Tools 73/74/75, use Tool 89 instead - it proposes config from their JSON without additional API calls
- See [Pipeline Config Guide](../UCMV_PIPELINE_CONFIG_GUIDE.md) for detailed explanations of every config key
