# Tool 89 - config generator

**What it is:** Takes the JSON outputs already extracted by Tools 73, 74, and 75, and proposes a `pipeline_config.json` for Tool 86 - with auto-filled keys, gap analysis, and confidence scores.

---

## Why it exists

Tool 86 (UC Metric View Generator) accepts a `config_json` parameter with up to 26 keys that control join mappings, filter sets, column overrides, and more. Authoring this config manually from scratch is the biggest bottleneck in the migration. Tool 89 automates the first draft by analyzing the extracted data.

## What problem it solves

- **Config authoring from extracted JSON:** When Tools 73/74/75 have already run and their JSON is available, Tool 89 proposes the config without going back to the PBI API
- **Gap analysis:** Shows exactly which config keys are auto-filled, which need review, and which require domain knowledge - so the SA knows what to work on next
- **Confidence scoring:** Each proposed config key gets a confidence score (0.0-1.0)

---

## Tool 89 versus Tool 90

| Aspect | Tool 89 | Tool 90 |
|---|---------|---------|
| Input | Pre-extracted JSON from Tools 73/74/75 | Live PBI API calls |
| PBI credentials needed | No | Yes (two SPs) |
| Best for | JSON already extracted in Kasal | Start of migration with PBI access |
| Output | Same config format | Same config format |

Use Tool 89 when you already have the JSON. Use Tool 90 when you want to go directly from PBI API to config.

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `measures_json` | Yes (or cache) | JSON from Tool 73 |
| `mquery_json` | Yes | JSON from Tool 74 |
| `relationships_json` | No | JSON from Tool 75 |
| `scan_data_json` | No | Scan data from Tool 86 API mode |
| `workspace_id` | No | Used to auto-load Tool 79 cache if measures_json absent |
| `dataset_id` | No | Used to auto-load Tool 79 cache if measures_json absent |
| `catalog` | No | Target UC catalog (default: `main`) |
| `schema_name` | No | Target UC schema (default: `default`) |

---

## Example crew position

```text
Tool 73 + Tool 74 + Tool 75 (run first - extraction phase)
    ↓
Tool 89 (propose config from their JSON outputs)   ← this tool
    ↓
SA edits pipeline_config.json (fill TODOs, ~2-3h first time)
    ↓
Tool 86 (generate UC Metric Views with the config)
```

---

## Example output

```json
{
  "proposed_config": {
    "join_key_map": {
      "Dim_Customer": {"alias": "dim_customer", "join_key": "customer_id", "dim_columns": ["customer_name", "segment"]},
      "Dim_Date": {"alias": "dim_date", "join_key": "date_key", "dim_columns": ["year", "quarter", "month"]}
    },
    "enrichment_joins": {
      "Fact_Sales": ["Dim_Customer", "Dim_Date"]
    },
    "switch_decompositions": {},
    "filter_sets": {},
    "column_overrides": {},
    "measure_resolutions": {}
  },
  "gap_analysis": {
    "auto_filled": ["join_key_map", "enrichment_joins", "column_metadata", "parameter_defaults"],
    "needs_review": ["dimension_exclusions", "fact_join_map"],
    "manual_required": ["switch_decompositions", "filter_sets", "measure_resolutions", "mapping_only_tables"]
  },
  "confidence_scores": {
    "join_key_map": 0.85,
    "enrichment_joins": 0.78,
    "switch_decompositions": 0.0
  }
}
```

---

## Notes

- `switch_decompositions` and `filter_sets` always require manual input - they encode business domain knowledge that can't be extracted from code
- The `gap_analysis.manual_required` list is your SA's work queue for the Human Review phase
- After the SA fills in the TODOs, run Tool 86 - if translation rate is below target, re-run Tool 89 gap analysis on the improved config to see what's still missing
- See the [pipeline config guide](../UCMV_PIPELINE_CONFIG_GUIDE.md) for what each config key means

## See also

- [Power BI integration hub](./README.md)
- [Tool 90 - pipeline config generator](./tool-90-pipeline-config-generator.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [Pipeline config guide](../UCMV_PIPELINE_CONFIG_GUIDE.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

Back to the [Power BI integration hub](./README.md).
