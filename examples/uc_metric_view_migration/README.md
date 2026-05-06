# UC Metric View Migration Example

This example demonstrates how to use Kasal Tool 86 (UC Metric View Generator) to replicate
the output of the standalone `generate_metric_views.py` pipeline.

## Files

| File | Description |
|------|-------------|
| `pipeline_config.json` | Customer-specific config extracted from the monolith (join maps, filter sets, SWITCH decompositions, enrichment joins, metadata) |
| `measure_table_mapping.json` | 470 DAX measures with table allocations (from Tool 73 output) |
| `mquery_transpilation.json` | 60 MQuery table transpilations (from Tool 74 output) |
| `pbi_relationships.json` | PBI relationships for auto-join detection (from Tool 75 output) |
| `crew_ucmv_generator.json` | Example Kasal crew config for import |

## Quick Test (CLI)

From `src/backend/`:

```bash
source .venv/bin/activate
python3 -c "
import json
from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader

BASE = '../../examples/uc_metric_view_migration'
measures = json.load(open(f'{BASE}/measure_table_mapping.json'))
mquery = json.load(open(f'{BASE}/mquery_transpilation.json'))
rels = json.load(open(f'{BASE}/pbi_relationships.json'))
config = json.load(open(f'{BASE}/pipeline_config.json'))

# Resolve catalog/schema placeholders
for t in config.get('mapping_only_tables', {}).values():
    t['source_table'] = t['source_table'].format(catalog='my_catalog', schema='my_schema')

parser = MQueryParser()
tables = parser.parse_json(mquery)
facts = {k for k, v in tables.items() if v.is_fact}
rel_enrich = RelationshipsLoader().load(rels, tables, facts)

pipeline = MetricViewPipeline(mapping=measures, mquery_tables=tables, config=config, relationships_enrichment=rel_enrich)
specs = pipeline.run()
yaml_out = pipeline.emit_all_yaml(catalog='my_catalog', schema='my_schema')

print(f'{len(specs)} metric views, {sum(len(s.measures) for s in specs.values())} measures')
for k, y in yaml_out.items():
    open(f'/tmp/{k}_uc_metric_view.yml', 'w').write(y)
print('YAML files written to /tmp/')
"
```

## Expected Output

- **20 UC Metric View YAML files** (vs 23 in original — 3 use special union/extended view logic)
- **349/422 measures translated (82%)**
- SWITCH decompositions: 45 measures across pe002, FT_Planning, FT_BPC003, etc.

## Using via Kasal UI

1. Import `crew_ucmv_generator.json` into Kasal
2. The crew has one task using Tool 86 with pre-loaded input data
3. Set `catalog` and `schema_name` to your target
4. Run — the agent generates YAML + SQL and returns as JSON

## Pipeline Config Keys

| Key | Purpose |
|-----|---------|
| `join_key_map` | PBI dim table → physical table join definitions |
| `fact_join_map` | Cross-table fact-to-fact join configs |
| `enrichment_joins` | Extra dimension joins not auto-detected from DAX |
| `filter_sets` | Named filter value lists (CWC_FILTER, WCT_CORE, etc.) |
| `switch_decompositions` | SELECTEDVALUE+SWITCH measures broken into individual SQL |
| `measure_resolutions` | Static DAX measure name → SQL resolution map |
| `mapping_only_tables` | Tables with measures but no MQuery SQL entry |
| `column_overrides` | PBI column → physical column name overrides |
| `dimension_exclusions` | Per-table dimensions to hide from YAML |
| `measure_metadata` | Per-table measure display names, synonyms, comments |
| `dimension_metadata` | Per-table dimension display names, synonyms |
| `comment_overrides` | Per-table metric view comment overrides |
| `dimension_order` | Per-table dimension ordering |
