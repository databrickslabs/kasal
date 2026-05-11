#!/usr/bin/env python3
"""
Run the UC Metric View Generator locally against the SC Reporting dataset.

Produces the same output as the original generate_metric_views.py monolith.

Usage (from src/backend/):
    source .venv/bin/activate
    python ../../examples/uc_metric_view_migration/run_locally.py

Output goes to ~/Downloads/ucmv_example_output/
"""

import json
import os
import sys

# Add backend to path
script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(script_dir, "..", "..", "src", "backend")
sys.path.insert(0, backend_dir)
sys.path.insert(0, os.path.join(backend_dir, "src"))

from src.engines.crewai.tools.custom.metric_view_utils.pipeline import (
    MetricViewPipeline,
)
from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import (
    RelationshipsLoader,
)
from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import (
    ScanDataParser,
)
from src.engines.crewai.tools.custom.metric_view_validation_utils.pipeline import (
    MetricExpressionValidatorPipeline,
)

# ── Configuration ──
CATALOG = "david_test_metrics"
SCHEMA = "test_schema"
# OUTPUT_DIR = os.path.expanduser('~/Downloads/ucmv_example_output')
OUTPUT_DIR = os.path.join(script_dir, "output")

# ── Load inputs ──
print("Loading inputs...")
measures = json.load(open(os.path.join(script_dir, "measure_table_mapping.json")))
mquery_entries = json.load(open(os.path.join(script_dir, "mquery_transpilation.json")))
rels_raw = json.load(open(os.path.join(script_dir, "pbi_relationships.json")))
config = json.load(open(os.path.join(script_dir, "pipeline_config.json")))

# Resolve catalog/schema placeholders in mapping-only tables
for tbl_cfg in config.get("mapping_only_tables", {}).values():
    tbl_cfg["source_table"] = tbl_cfg["source_table"].format(
        catalog=CATALOG, schema=SCHEMA
    )

print(f"  Measures: {len(measures)}")
print(f"  MQuery entries: {len(mquery_entries)}")

# ── Parse MQuery ──
parser = MQueryParser()
mquery_tables = parser.parse_json(mquery_entries)
fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}
print(f"  Parsed: {len(mquery_tables)} tables, {len(fact_tables)} fact tables")

# ── Parse relationships ──
rel_enrich = RelationshipsLoader().load(rels_raw, mquery_tables, fact_tables)
total_auto = sum(len(v) for v in rel_enrich.values())
print(f"  Relationships: {total_auto} enrichment joins")

# ── Parse scan data ──
scan_path = os.path.join(script_dir, "scan_result_debug.json")
scan_data = {}
if os.path.exists(scan_path):
    scan_data = ScanDataParser().parse(scan_path)
    print(f"  Scan data: {len(scan_data)} tables")
else:
    print("  Scan data: not found (skipping)")

# ── Run pipeline ──
print("\nRunning pipeline...")
pipeline = MetricViewPipeline(
    mapping=measures,
    mquery_tables=mquery_tables,
    config=config,
    relationships_enrichment=rel_enrich,
    scan_data=scan_data,
    unflatten_tables=True,
)
specs = pipeline.run()

# ── Emit output ──
os.makedirs(OUTPUT_DIR, exist_ok=True)
yaml_out = pipeline.emit_all_yaml(catalog=CATALOG, schema=SCHEMA)
sql_out = pipeline.emit_all_sql(catalog=CATALOG, schema=SCHEMA)

for k, y in yaml_out.items():
    with open(os.path.join(OUTPUT_DIR, f"{k}_uc_metric_view.yml"), "w") as f:
        f.write(y)
for k, s in sql_out.items():
    with open(os.path.join(OUTPUT_DIR, f"deploy_{k}.sql"), "w") as f:
        f.write(s)

# ── Migration Report ──
results = pipeline.get_results()
report = results.get("migration_report", "")
if report:
    report_path = os.path.join(OUTPUT_DIR, "migration_report.md")
    with open(report_path, "w") as f:
        f.write(report)
    print(f"Migration report: {report_path}")

# ── Summary ──
print(f'\n{"="*60}')
print(f"RESULTS")
print(f'{"="*60}')
print(f"Metric views: {len(yaml_out)}")
print(f"SQL files: {len(sql_out)}")
print(f"Output: {OUTPUT_DIR}/")
print()

total_t = sum(
    s.get("translated", 0) for k, s in pipeline.stats.items() if k != "__unassigned__"
)
total_m = sum(
    s.get("total", 0) for k, s in pipeline.stats.items() if k != "__unassigned__"
)
print(
    f"Total: {total_t}/{total_m} measures translated ({total_t * 100 // total_m if total_m else 0}%)"
)
print()

for k in sorted(specs.keys()):
    s = pipeline.stats[k]
    base = s.get("base", 0)
    dax = s.get("dax", 0)
    sw = s.get("switch", 0)
    print(f'  {k}: {s["translated"]}/{s["total"]} (base={base} dax={dax} switch={sw})')

# ── Validation ──
for k, y in yaml_out.items():
    ucmv_path = os.path.join(OUTPUT_DIR, f"{k}_uc_metric_view.yml")

    # Always map the fact table name to "source" (Databricks alias)
    validator_pipeline = MetricExpressionValidatorPipeline(
        table_mappings={k: "source"},
    )
    result_dict = validator_pipeline.run(
        metrics_view_yaml_path=ucmv_path,
        table_mapping_json_path=os.path.join(script_dir, "measure_table_mapping.json"),
    )

    with open(os.path.join(OUTPUT_DIR, f"validation_{k}_uc_metric_view.json"), "w") as f:
        json.dump(result_dict, f, indent=2, default=str)

    print(f"==== Validation results for: {k} ====")
    for idx, measure_data in enumerate(result_dict.get("evaluated", []), 1):
        eval_result = measure_data.get("measure_eval_result", {})
        # Print differences and similarities
        differences = eval_result.get("differences", [])
        similarities = eval_result.get("similarities", [])
        recommendations = eval_result.get("recommendations", [])

        if similarities:
            print(f"      Similarities: {len(similarities)} match(es)")
            for sim in similarities:
                print(f"        • {sim.split('. ')[0]}")
                for s in sim.split(". ")[1:]:
                    print(f"            • {s}")

        if differences:
            print(f"      Differences:  {len(differences)} issue(s)")
            for diff in differences:
                print(f"        • {diff.split('. ')[0]}")
                for d in diff.split(". ")[1:]:
                    print(f"            • {d}")

        if recommendations:
            print(f"      Recommendations:")
            for rec in recommendations:
                print(f"        • {rec}")

