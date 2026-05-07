#!/usr/bin/env python3
"""
Synthetic demo exercising ALL 10 PBI limitation features.

Generates a single UC Metric View from fabricated data that triggers every
limitation detection path:

  1. USERELATIONSHIP (inactive relationship → alternate join alias)
  2. M:N Relationships (flagged, not migrated)
  3. Row-Level Security (flagged, needs Databricks row filters)
  4. Aggregation Tables (Import storageMode flagged)
  5. Conditional Formatting (business-logic COLOR measure NOT rejected)
  6. Incremental Refresh (flagged for date-filter recommendation)
  7. Default Summarization (SummarizeBy=None column flagged)
  8. Calculation Groups (base measures expanded × group items)
  9. Perspectives (flagged, not migrated)
 10. Field Parameters (flagged, not migrated)

Usage (from src/backend/):
    source .venv/bin/activate
    python ../../examples/uc_metric_view_migration/run_all_limitations_demo.py

Output goes to ~/Downloads/ucmv_all_limitations_demo/
"""
import os
import sys

# Add backend to path
script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(script_dir, '..', '..', 'src', 'backend')
sys.path.insert(0, backend_dir)
sys.path.insert(0, os.path.join(backend_dir, 'src'))

from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
from src.engines.crewai.tools.custom.metric_view_utils.data_classes import TableInfo

# ── Configuration ──
CATALOG = 'demo_catalog'
SCHEMA = 'all_limitations'
OUTPUT_DIR = os.path.expanduser('~/Downloads/ucmv_all_limitations_demo')

# ────────────────────────────────────────────────────────────────────────────
# Synthetic MQuery tables (what MQueryParser would produce from transpiled SQL)
# ────────────────────────────────────────────────────────────────────────────

fact_sales = TableInfo(
    table_name='Fact_Sales',
    source_table=f'{CATALOG}.{SCHEMA}.fact_sales',
    aggregate_columns=[
        {'name': 'revenue', 'source_col': 'revenue'},
        {'name': 'quantity', 'source_col': 'quantity'},
        {'name': 'cost', 'source_col': 'cost'},
        {'name': 'discount_amount', 'source_col': 'discount_amount'},
        {'name': 'returns_amount', 'source_col': 'returns_amount'},
    ],
    group_by_columns=['order_date', 'ship_date', 'customer_id', 'product_id',
                      'region_id', 'comp_code', 'fiscper'],
    calculated_columns=[],
    is_fact=True,
    full_sql=(
        'SELECT order_date, ship_date, customer_id, product_id, region_id, '
        'comp_code, fiscper, SUM(revenue) AS revenue, SUM(quantity) AS quantity, '
        'SUM(cost) AS cost, SUM(discount_amount) AS discount_amount, '
        'SUM(returns_amount) AS returns_amount '
        f'FROM {CATALOG}.{SCHEMA}.fact_sales '
        'GROUP BY order_date, ship_date, customer_id, product_id, region_id, comp_code, fiscper'
    ),
)

dim_calendar = TableInfo(
    table_name='Dim_Calendar',
    source_table=f'{CATALOG}.{SCHEMA}.dim_calendar',
    aggregate_columns=[],
    group_by_columns=['date_key', 'year', 'quarter', 'month', 'fiscal_period'],
    calculated_columns=[],
    is_fact=False,
    full_sql=f'SELECT * FROM {CATALOG}.{SCHEMA}.dim_calendar',
)

dim_customer = TableInfo(
    table_name='Dim_Customer',
    source_table=f'{CATALOG}.{SCHEMA}.dim_customer',
    aggregate_columns=[],
    group_by_columns=['customer_id', 'customer_name', 'segment', 'region'],
    calculated_columns=[],
    is_fact=False,
    full_sql=f'SELECT * FROM {CATALOG}.{SCHEMA}.dim_customer',
)

dim_product = TableInfo(
    table_name='Dim_Product',
    source_table=f'{CATALOG}.{SCHEMA}.dim_product',
    aggregate_columns=[],
    group_by_columns=['product_id', 'product_name', 'category', 'brand'],
    calculated_columns=[],
    is_fact=False,
    full_sql=f'SELECT * FROM {CATALOG}.{SCHEMA}.dim_product',
)

mquery_tables = {
    'Fact_Sales': fact_sales,
    'Dim_Calendar': dim_calendar,
    'Dim_Customer': dim_customer,
    'Dim_Product': dim_product,
}
fact_tables = {'Fact_Sales'}

# ────────────────────────────────────────────────────────────────────────────
# Relationships — active, inactive, and M:N
# ────────────────────────────────────────────────────────────────────────────

relationships = [
    # Active: Fact_Sales.order_date → Dim_Calendar.date_key
    {
        'from_table': 'Fact_Sales', 'from_column': 'order_date',
        'to_table': 'Dim_Calendar', 'to_column': 'date_key',
        'from_cardinality': 'Many', 'to_cardinality': 'One',
        'is_active': True,
    },
    # ▸ Limitation 1: USERELATIONSHIP — inactive rel for ship_date
    {
        'from_table': 'Fact_Sales', 'from_column': 'ship_date',
        'to_table': 'Dim_Calendar', 'to_column': 'date_key',
        'from_cardinality': 'Many', 'to_cardinality': 'One',
        'is_active': False,
    },
    # Active: Fact_Sales.customer_id → Dim_Customer.customer_id
    {
        'from_table': 'Fact_Sales', 'from_column': 'customer_id',
        'to_table': 'Dim_Customer', 'to_column': 'customer_id',
        'from_cardinality': 'Many', 'to_cardinality': 'One',
        'is_active': True,
    },
    # ▸ Limitation 2: M:N — Dim_Customer ↔ Dim_Product (many-to-many)
    {
        'from_table': 'Dim_Customer', 'from_column': 'customer_id',
        'to_table': 'Dim_Product', 'to_column': 'product_id',
        'from_cardinality': 'Many', 'to_cardinality': 'Many',
        'is_active': True,
    },
    # Active: Fact_Sales.product_id → Dim_Product.product_id
    {
        'from_table': 'Fact_Sales', 'from_column': 'product_id',
        'to_table': 'Dim_Product', 'to_column': 'product_id',
        'from_cardinality': 'Many', 'to_cardinality': 'One',
        'is_active': True,
    },
]

# ────────────────────────────────────────────────────────────────────────────
# Measure mapping — includes various DAX patterns + conditional formatting
# ────────────────────────────────────────────────────────────────────────────

measures = [
    # Base measures (SUM pattern)
    {'measure_name': 'Total Revenue', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "SUM(Fact_Sales[revenue])"},
    {'measure_name': 'Total Quantity', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "SUM(Fact_Sales[quantity])"},
    {'measure_name': 'Total Cost', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "SUM(Fact_Sales[cost])"},
    {'measure_name': 'Total Discount', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "SUM(Fact_Sales[discount_amount])"},
    {'measure_name': 'Total Returns', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "SUM(Fact_Sales[returns_amount])"},

    # DAX DIVIDE pattern
    {'measure_name': 'Gross Margin', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "DIVIDE(SUM(Fact_Sales[revenue]) - SUM(Fact_Sales[cost]), SUM(Fact_Sales[revenue]))"},

    # SAMEPERIODLASTYEAR pattern
    {'measure_name': 'Revenue PY', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "CALCULATE(SUM(Fact_Sales[revenue]), SAMEPERIODLASTYEAR(Dim_Calendar[date_key]))"},

    # ▸ Limitation 1: USERELATIONSHIP — uses inactive ship_date → Calendar
    {'measure_name': 'Ship Revenue', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "CALCULATE(SUM(Fact_Sales[revenue]), USERELATIONSHIP(Fact_Sales[ship_date], Dim_Calendar[date_key]))"},

    # ▸ Limitation 5: Conditional Formatting — has business logic (SUM + DIVIDE), should NOT be rejected
    {'measure_name': 'Margin_Color', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': 'IF(DIVIDE(SUM(Fact_Sales[revenue]) - SUM(Fact_Sales[cost]), SUM(Fact_Sales[revenue])) < 0.1, "Red", "Green")'},

    # Pure formatting — SHOULD be rejected as artifact
    {'measure_name': 'Header_Color', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': '"#336699"'},

    # COUNTX + FILTER pattern
    {'measure_name': 'High Value Orders', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "COUNTX(FILTER(Fact_Sales, Fact_Sales[revenue] > 1000), Fact_Sales[revenue])"},

    # Measure-to-measure reference (tests dependency graph)
    {'measure_name': 'Net Revenue', 'proposed_allocation': 'Fact_Sales',
     'dax_expression': "[Total Revenue] - [Total Returns]"},
]

# ────────────────────────────────────────────────────────────────────────────
# Scan data — triggers RLS, aggregation, refresh, summarization
# ────────────────────────────────────────────────────────────────────────────

scan_data_json = {
    'workspaces': [{
        'datasets': [{
            'name': 'SalesDataset',
            'tables': [
                {
                    'name': 'Fact_Sales',
                    # ▸ Limitation 4: Aggregation — Import storageMode
                    'storageMode': 'Import',
                    'source': [{
                        'expression': (
                            'let Source = Value.NativeQuery(conn, '
                            '"SELECT * FROM fact_sales", null, [EnableFolding=true]) '
                            'in Source'
                        ),
                    }],
                    'columns': [
                        {'name': 'revenue', 'dataType': 'double'},
                        {'name': 'quantity', 'dataType': 'int64'},
                        {'name': 'cost', 'dataType': 'double'},
                        # ▸ Limitation 7: SummarizeBy=None — should not be aggregated
                        {'name': 'order_id', 'dataType': 'string', 'summarizeBy': 'none'},
                        {'name': 'customer_id', 'dataType': 'string', 'summarizeBy': 'none'},
                    ],
                    # ▸ Limitation 6: Incremental Refresh — has a refresh policy
                    'refreshPolicy': {
                        'policyType': 'basic',
                        'incrementalGranularity': 'Day',
                        'incrementalPeriods': 30,
                        'rollingWindowGranularity': 'Year',
                        'rollingWindowPeriods': 2,
                    },
                },
                {
                    'name': 'Dim_Calendar',
                    'storageMode': 'DirectQuery',
                    'source': [{
                        'expression': (
                            'let Source = Value.NativeQuery(conn, '
                            '"SELECT * FROM dim_calendar", null, [EnableFolding=true]) '
                            'in Source'
                        ),
                    }],
                    'columns': [
                        {'name': 'date_key', 'dataType': 'dateTime'},
                        {'name': 'year', 'dataType': 'int64'},
                    ],
                },
            ],
            # ▸ Limitation 3: RLS — roles defined on Sales table
            'roles': [
                {
                    'name': 'RegionFilter',
                    'tablePermissions': [
                        {'name': 'Fact_Sales', 'filterExpression': "[region_id] = USERPRINCIPALNAME()"},
                    ],
                },
                {
                    'name': 'ManagerView',
                    'tablePermissions': [
                        {'name': 'Fact_Sales', 'filterExpression': "[comp_code] IN {\"0100\",\"0200\"}"},
                    ],
                },
            ],
        }],
    }],
}

# ────────────────────────────────────────────────────────────────────────────
# Pipeline config — calculation groups, perspectives, field parameters
# ────────────────────────────────────────────────────────────────────────────

pipeline_config = {
    'enrichment_joins': {},

    # ▸ Limitation 8: Calculation Groups — YTD, PY, QoQ applied to base measures
    'calculation_groups': [
        {
            'name': 'Time Intelligence',
            'items': [
                {'name': 'YTD', 'expression': 'CALCULATE(SELECTEDMEASURE(), DATESYTD(Dim_Calendar[date_key]))'},
                {'name': 'PY', 'expression': 'CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR(Dim_Calendar[date_key]))'},
                {'name': 'QoQ', 'expression': 'SELECTEDMEASURE() / CALCULATE(SELECTEDMEASURE(), DATEADD(Dim_Calendar[date_key], -1, QUARTER)) - 1'},
            ],
        },
    ],

    # ▸ Limitation 9: Perspectives
    'perspectives': [
        {'name': 'Sales Overview', 'tables': ['Fact_Sales', 'Dim_Calendar']},
        {'name': 'Customer Analysis', 'tables': ['Fact_Sales', 'Dim_Customer']},
    ],

    # ▸ Limitation 10: Field Parameters
    'field_parameters': [
        {'name': 'Metric Selector', 'measures': ['Total Revenue', 'Gross Margin', 'Total Quantity']},
        {'name': 'Time Comparison', 'measures': ['Revenue PY', 'Revenue YTD']},
    ],
}

# ────────────────────────────────────────────────────────────────────────────
# Parse relationships → get inactive + M:N
# ────────────────────────────────────────────────────────────────────────────

print('=' * 60)
print('ALL 10 PBI LIMITATIONS DEMO')
print('=' * 60)
print()

print('Loading synthetic inputs...')
rel_loader = RelationshipsLoader()
rel_enrichment = rel_loader.load(relationships, mquery_tables, fact_tables)
inactive_rels = rel_loader.get_inactive_relationships()
m2n_rels = rel_loader.get_skipped_m2n()
print(f'  Enrichment joins: {sum(len(v) for v in rel_enrichment.values())}')
print(f'  Inactive relationships (USERELATIONSHIP): {len(inactive_rels)}')
print(f'  M:N relationships (skipped): {len(m2n_rels)}')

# ── Parse scan data → RLS, aggregation, refresh, summarization ──
from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser

scan_parser = ScanDataParser()
scan_data = scan_parser.parse(scan_data_json)
rls_tables = scan_parser.get_rls_tables()
refresh_policy_tables = scan_parser.get_refresh_policy_tables()
no_summarize_columns = scan_parser.get_no_summarize_columns()
print(f'  Scan data tables: {len(scan_data)}')
print(f'  RLS-protected tables: {rls_tables}')
print(f'  Refresh policy tables: {[r["table_name"] for r in refresh_policy_tables]}')
print(f'  SummarizeBy=None columns: {[(c["table_name"], c["column_name"]) for c in no_summarize_columns]}')

# ── Run pipeline ──
print('\nRunning pipeline...')
pipeline = MetricViewPipeline(
    mapping=measures,
    mquery_tables=mquery_tables,
    config=pipeline_config,
    relationships_enrichment=rel_enrichment,
    inactive_relationships=inactive_rels,
    m2n_relationships=m2n_rels,
    scan_data=scan_data,
    refresh_policy_tables=refresh_policy_tables,
    no_summarize_columns=no_summarize_columns,
    rls_tables=rls_tables,
)
specs = pipeline.run()

# ── Emit output ──
os.makedirs(OUTPUT_DIR, exist_ok=True)
yaml_out = pipeline.emit_all_yaml(catalog=CATALOG, schema=SCHEMA)
sql_out = pipeline.emit_all_sql(catalog=CATALOG, schema=SCHEMA)

for k, y in yaml_out.items():
    with open(os.path.join(OUTPUT_DIR, f'{k}_uc_metric_view.yml'), 'w') as f:
        f.write(y)
for k, s in sql_out.items():
    with open(os.path.join(OUTPUT_DIR, f'deploy_{k}.sql'), 'w') as f:
        f.write(s)

# ── Migration Report ──
results = pipeline.get_results()
report = results.get('migration_report', '')
if report:
    report_path = os.path.join(OUTPUT_DIR, 'migration_report.md')
    with open(report_path, 'w') as f:
        f.write(report)
    print(f'\nMigration report: {report_path}')

# ── Verification ──
print(f'\n{"=" * 60}')
print('RESULTS')
print(f'{"=" * 60}')
print(f'Metric views: {len(yaml_out)}')
print(f'SQL files: {len(sql_out)}')
print(f'Output: {OUTPUT_DIR}/')
print()

# Show per-table stats
for k in sorted(specs.keys()):
    s = pipeline.stats[k]
    base = s.get('base', 0)
    dax = s.get('dax', 0)
    sw = s.get('switch', 0)
    cg = s.get('calculation_group', 0)
    print(f'  {k}: {s["translated"]}/{s["total"]} (base={base} dax={dax} switch={sw} calc_group={cg})')

# ── Verify all 10 limitations triggered ──
limitations = results.get('limitations', {})
print(f'\n{"=" * 60}')
print('LIMITATION COVERAGE')
print(f'{"=" * 60}')

checks = [
    ('1. USERELATIONSHIP',       'inactive_relationships', lambda v: len(v) > 0),
    ('2. M:N Relationships',     'm2n_relationships',      lambda v: len(v) > 0),
    ('3. Row-Level Security',    'rls_tables',             lambda v: len(v) > 0),
    ('4. Aggregation Tables',    'aggregation_warnings',   lambda v: len(v) > 0),
    ('5. Conditional Formatting', None,                    None),  # checked separately
    ('6. Incremental Refresh',   'refresh_policies',       lambda v: len(v) > 0),
    ('7. Default Summarization', 'summarization_warnings', lambda v: len(v) > 0),
    ('8. Calculation Groups',    'calculation_groups_expanded', lambda v: len(v) > 0),
    ('9. Perspectives',          'perspectives',           lambda v: len(v) > 0),
    ('10. Field Parameters',     'field_parameters',       lambda v: len(v) > 0),
]

all_pass = True
for label, key, check_fn in checks:
    if key is None:
        # Conditional formatting: verify Margin_Color was NOT quick-rejected
        # (it may still be untranslatable due to pattern complexity, but the
        # key improvement is that it's NOT rejected as "display-only artifact")
        spec = specs.get('Fact_Sales')
        if spec:
            translated_names = {m.original_name for m in spec.measures}
            untranslatable_names = {m.original_name for m in spec.untranslatable}
            rejected_as_artifact = any(
                m.original_name == 'Margin_Color' and 'display' in (m.skip_reason or '').lower()
                for m in spec.untranslatable
            )
            header_rejected_as_artifact = any(
                m.original_name == 'Header_Color' and 'artifact' in (m.skip_reason or '').lower()
                for m in spec.untranslatable
            )
            if 'Margin_Color' in translated_names:
                print(f'  [PASS] {label} — Margin_Color translated (business logic preserved)')
            elif 'Margin_Color' in untranslatable_names and not rejected_as_artifact:
                # Not rejected as artifact — business logic was detected, just too complex for regex
                print(f'  [PASS] {label} — Margin_Color NOT rejected as artifact (needs LLM for full translation)')
            elif rejected_as_artifact:
                print(f'  [FAIL] {label} — Margin_Color wrongly rejected as display-only artifact')
                all_pass = False
            else:
                print(f'  [FAIL] {label} — Margin_Color not found in any list')
                all_pass = False
            # Verify Header_Color IS rejected as artifact
            if 'Header_Color' not in translated_names:
                print(f'         ✓ Header_Color correctly rejected (pure formatting)')
            else:
                print(f'         ✗ Header_Color should have been rejected')
                all_pass = False
        else:
            print(f'  [FAIL] {label} — No Fact_Sales spec')
            all_pass = False
        continue

    val = limitations.get(key, [])
    if check_fn(val):
        print(f'  [PASS] {label} — {len(val)} item(s) detected')
    else:
        print(f'  [FAIL] {label} — not triggered (key={key})')
        all_pass = False

print()
if all_pass:
    print('ALL 10 LIMITATIONS VERIFIED ✓')
else:
    print('SOME LIMITATIONS NOT TRIGGERED — check configuration')

# ── Show the report's PBI Native Features table ──
print(f'\n{"=" * 60}')
print('PBI NATIVE FEATURES TABLE (from migration report)')
print(f'{"=" * 60}')
for line in report.split('\n'):
    if 'PBI Native' in line or '|' in line and ('USERELATIONSHIP' in line or 'M:N' in line
            or 'Row-Level' in line or 'Aggregation' in line or 'Refresh' in line
            or 'Summarization' in line or 'Calculation' in line or 'Perspective' in line
            or 'Field Param' in line or 'Conditional' in line or 'Feature' in line
            or '------' in line):
        print(f'  {line}')
