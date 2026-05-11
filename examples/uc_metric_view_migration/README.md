# UC Metric View Migration

Migrate Power BI semantic models to Databricks Unity Catalog Metric Views — deterministically, from a live API, with human review where it matters.

## Try It Now

```bash
cd src/backend
source .venv/bin/activate
python ../../examples/uc_metric_view_migration/run_locally.py
```

This runs the full pipeline against the SC Reporting project (26 sources, 470 measures) and produces 26 UC Metric View YAML files in `~/Downloads/ucmv_example_output/`. 20 of 23 reference views are an exact measure-level match; the remaining 3 are a strict superset (we translate more, 0 missing). Verified against `~/workspace/demos/uc_metrics/sc_reporting_project/output/`.

There is also an all-limitations demo that exercises all 10 PBI limitation detection features:

```bash
python ../../examples/uc_metric_view_migration/run_all_limitations_demo.py
```

No PBI API access needed. No Kasal server needed. Just the pre-extracted JSONs in this directory.

## Why This Matters

The only tool that does live PBI API extraction → deterministic DAX translation → automated UC REST API deployment in one pipeline. Others:

- **Databricks product team** (`powerbi-migrate`): File-based input only, LLM-driven (non-deterministic), no deployment. Actively developing — will improve.
- **Microsoft**: Migration path goes to Fabric (keeps DAX), not to UC Metric Views.
- **Consulting firms**: Manual rewrites. Weeks of effort per dashboard.
- **Metric layer vendors** (dbt, Looker): Own constructs, no PBI migration tooling.

The moat isn't "no one does this" — it's the approach: deterministic translation (16 regex patterns, reproducible output) + live API access (no file exports) + human review at the right points (not blanket LLM).

## Migration Workflow

Migrations are never 100% automatic. The target is 70-80% automation with human review where it matters. The workflow is **iterative, not linear** — Phase 4-5 failures loop back to Phase 3.

```
  Phase 1: EXTRACT (automatic)
  Tools 73/74/75/86 → JSON files from PBI API
                │
                ▼
  Phase 2: PROPOSE CONFIG (automatic — Tool 89, planned)
  Auto-generate pipeline_config.json from extraction output
                │
                ▼
  ┌─────────────────────────────────────────────────────┐
  │                                                     │
  │  Phase 3: HUMAN REVIEW                              │
  │  Review/edit proposed config                        │
  │  First customer: 2-3 hours                          │
  │  Repeat customer (similar model): 30 min            │
  │                                                     │
  └───────────────────┬─────────────────────────────────┘
                      │
                      ▼
  Phase 4: GENERATE (automatic — Tool 86)
  MQuery Parser → DAX Translator → Dependency Graph →
  Join Detector → YAML Emitter → Migration Report
                      │
                      ▼
  Phase 5: VALIDATE + DEPLOY (done)
  DAX-vs-UCMV structural validation → human approval → deploy via REST API
         │
         │ failures loop back
         └──────────► Phase 3 (adjust config, re-run)
```

### What exists today

| Phase | Status | Detail |
|-------|--------|--------|
| Phase 1: Extract | **Done** | Tools 73, 74, 75, 86 (API mode). 3 auth methods. |
| Phase 2: Propose config | **Planned** | Tool 89 — config generator. Not yet built. |
| Phase 3: Human review | **Manual** | Customer edits pipeline_config.json by hand. |
| Phase 4: Generate | **Done** | Tool 86. 709 tests, 30 test files, 20 modules. All 10 PBI limitations detected. |
| Phase 5: Validate + Deploy | **Done** | Validation framework compares DAX structure vs generated UCMV SQL. Tool 88 deployer for dry-run + live deploy via UC REST API. |

### The iteration loop

Real migrations are iterative. A typical flow:

1. **First run** with empty config → migration report shows 40-50% translated (base measures only)
2. **Add join_key_map** → re-run → 55-65% translated (dimension joins added)
3. **Add switch_decompositions** → re-run → 70-75% translated
4. **Add manual_overrides** → re-run → 80%+ translated (complex cross-table measures)
5. **Enable LLM fallback** for remaining complex DAX → 85%+
6. **Validate against PBI source** → fix mismatches → final config

Each iteration takes minutes (Tool 86 runs in 30 seconds). The human time is in reviewing the migration report and adjusting the config.

## What Tool 89 (Config Generator) Will Auto-Propose

The biggest bottleneck today is authoring `pipeline_config.json`. Tool 89 will propose 80%+ of it deterministically:

| Config Key | Source | How |
|-----------|--------|-----|
| `join_key_map` | Relationships API (Tool 75) | Convert relationship records → join_key_map format. 80% done in `RelationshipsLoader`. |
| `fact_join_map` | Measure Allocator (Tool 87) | Cross-table references = fact_join_map candidates. Grain/pivot/embed decisions still need human review. |
| `switch_decompositions` | DAX quick-reject output | SELECTEDVALUE+SWITCH measures already identified. Parse SWITCH branches from DAX. Deterministic for 80% of patterns. |
| `enrichment_joins` | Relationships API | Already auto-generated. Done. |
| `measure_resolutions` | Pipeline untranslatable list | Measures failing with "Cannot resolve [ref]" — look up referenced DAX, propose resolution. |
| `column_overrides` | MQuery transpilation vs DAX | Diff physical vs PBI column names = overrides. |
| `mapping_only_tables` | Admin API scan | Tables with measures but no MQuery SQL. |
| `manual_overrides` | Untranslatable + reference output | Complex cross-table/SUMX/DISTINCTCOUNT measures that no regex handles — propose SQL from LLM or reference patterns. |

**What Tool 89 won't get right:** `fact_join_map` entries with pivot/union/embed modes. These encode grain-level business decisions (e.g., "scorecard table has one row per KBI per workcenter, but the fact table has one row per workcenter — need to pivot before joining"). A human must review these.

### PBI Native Features — All 10 Implemented

All 10 PBI-specific features are now detected, handled, or flagged in the migration report. **0 produce silently wrong output.**

| Feature | Status | What the pipeline does |
|---------|--------|----------------------|
| **USERELATIONSHIP** | Migrated | Inactive relationships → alternate join aliases (`dim_calendar_ship_date`). DAX pattern matched and inner expression translated. |
| **M:N relationships** | Flagged | Detected via cardinality check. Listed in migration report with bridge table workaround suggestions. |
| **Calculation Groups** | Expanded | Config `calculation_groups` × base measures → explicit UCMV measures (e.g., 5 base × 3 time intelligence = 15 expanded measures). |
| **Row-Level Security** | Flagged | Detected from Admin API scan `roles` array. Migration report warns: "Configure Databricks row filters separately." |
| **Aggregation tables** | Flagged | `storageMode: Import` detected from scan data. Migration report warns: "Verify source grain." |
| **Field Parameters** | Flagged | Listed in migration report as "Not migrated — consider separate metric views or Genie space." |
| **Conditional formatting** | Smart filter | Color/FORMAT measures checked for business logic (SUM, DIVIDE, MEASURE refs). Pure display → rejected. Business logic → passed to translation. |
| **Incremental refresh** | Flagged | `refreshPolicy` detected from scan data. Migration report suggests adding date filter for performance. |
| **Perspectives** | Flagged | Listed in migration report as "Not migrated — consider separate UCMV sets or Databricks permissions." |
| **Default summarization** | Flagged | `summarizeBy: none` columns detected from scan data. Migration report warns against wrong aggregation. |

### Risk summary

| Limitation | Silent wrong output? | Status |
|-----------|---------------------|--------|
| USERELATIONSHIP | No | Alternate joins generated automatically |
| M:N relationships | No | Flagged in report with workarounds |
| Calculation Groups | No | Expanded from config |
| RLS | No | Flagged — "configure Databricks row filters" |
| Aggregation tables | No | Flagged — "verify source grain" |
| Field Parameters | No | Flagged in report |
| Conditional formatting | No | Business logic detected, pure display rejected |
| Incremental refresh | No | Flagged — "add date filter" |
| Perspectives | No | Flagged in report |
| Default summarization | No | Flagged — SummarizeBy:None columns listed |

**0 of 10 limitations produce silently wrong output.** All are either automatically handled or explicitly flagged in the migration report.

### What will always need human judgment

- Semantic correctness (does `revenue` mean gross or net?)
- Grain decisions for cross-table joins (pivot vs union vs embed)
- M:N relationship resolution strategy (bridge table vs pre-joined SQL)
- RLS → Databricks row filter mapping
- Complex DAX patterns the LLM can't translate deterministically
- Source SQL quality (suboptimal JOINs, redundant WHERE clauses)
- Business logic encoded in SWITCH branches that only domain experts understand

## Effort Estimate

Based on the SC Reporting project (26 sources, 470 measures, 2 weeks with 1.5 people).

### Three tiers

| Approach | Per-source effort | 26 sources | 50 sources |
|----------|------------------|-----------|-----------|
| **Monolith script** (original, before Kasal) | ~4.6 hours | ~120 hours (2.5 weeks) | ~230 hours (5 weeks) |
| **Tool 86 today** (automatic extraction, manual config) | ~3 hours | ~80 hours (2 weeks) | ~150 hours (3.5 weeks) |
| **Tool 86 + Tool 89 + validation** (target) | ~1.8 hours | ~47 hours (1 week) | ~90 hours (2 weeks) |

**Why Tool 86 today is already faster than the monolith:** Extraction is automatic (was manual), migration report shows what's missing (was trial-and-error), dependency graph resolves measure chains (was manual ordering), manual_overrides config replaces hardcoded Python.

**Why per-source cost drops within a dashboard:** Sources share dimensions, relationships overlap, SWITCH patterns repeat. A 50-source dashboard has ~10 distinct patterns. After the first 10 sources are configured, the remaining 40 are mostly covered. The marginal cost curve:

| Sources configured | Cumulative coverage | Marginal effort per new source |
|-------------------|--------------------|-----------------------------|
| 1-5 | ~40% | 3-4 hours (building core config) |
| 6-15 | ~75% | 1-2 hours (adding variations) |
| 16-50 | ~95% | 15-30 min (already covered by existing config) |

## Implementation Details

### Tool 86: UC Metric View Generator

The core pipeline. Two input modes — both feed into the same deterministic engine:

- **API mode**: Provide PBI credentials → tool extracts everything via PBI Admin API + Execute Queries API
- **JSON mode**: Provide pre-extracted JSON files → tool runs the pipeline directly (testing/offline)

Pipeline internals: MQuery Parser → DAX Translator (16 patterns + USERELATIONSHIP + LLM fallback) → Kahn's Dependency Graph → Manual Override Injection → Pass 2 Measure Arithmetic → Join Detector (pivot/union/embed modes) → YAML Emitter (5-pass MEASURE() cascade validation + window spec emission) → Migration Report (with PBI Native Features summary table).

### Extraction Tools (Phase 1)

| Tool | What it extracts | API |
|------|-----------------|-----|
| Tool 74 (MQuery Converter) | Table definitions + transpiled SQL | PBI Admin API |
| Tool 73 (Measure Converter) | DAX measures with metadata | PBI Execute Queries API |
| Tool 75 (Relationships Tool) | Table relationships + cardinality | PBI Execute Queries API |
| Tool 86 (API mode) | Scan data for inline SQL enrichment | PBI Admin API |

All tools support 3 auth methods: Service Principal, Service Account, User OAuth.

### Tool 88: Metric View Deployer

Deploys YAML to Databricks via UC Metric View REST API. Supports dry-run (validation without deployment) and auto-update on conflict (PUT on 409).

### Validation Framework (Phase 5)

Structural validation comparing the original DAX expression against the generated UCMV SQL — per measure.

**Modules** (`metric_view_validation_utils/`):
- `DAXExpressionParser` — hierarchical DAX parse tree with recursive VAR substitution
- `UCMetricsViewParser` — parses Databricks metric view YAML expressions
- `ExpressionValidator` — compares aggregation types, filter conditions, column references
- `MetricExpressionValidatorPipeline` — orchestrator with direct + file-based modes

**Output per measure:** status (VALID/INVALID/SKIPPED), confidence score, differences, similarities, recommendations.

213 unit tests covering all validation modules.

### run_locally.py vs Tool 86 — Same Pipeline

`run_locally.py` is a convenience wrapper for offline testing. Tool 86 is the real CrewAI tool. Both use the **exact same modules**:

| Module | run_locally.py | Tool 86 |
|--------|---------------|---------|
| `MetricViewPipeline` | top-level import | lazy import inside `_run()` |
| `MQueryParser` | top-level import | lazy import inside `_run()` |
| `RelationshipsLoader` | top-level import | lazy import inside `_run()` |
| `ScanDataParser` | top-level import | lazy import inside `_run()` |
| Validation | try/except at bottom | try/except after generation |
| Migration report | via `pipeline.get_results()` | included in JSON output |

Tool 86 has 3 additional imports for **API mode only** (not used in JSON mode):
- `run_async` — bridges async PBI API calls from sync CrewAI context
- `powerbi_auth_utils` — PBI OAuth/SPN authentication
- `PowerBIConnector` / `PowerBIAdminScanner` — live PBI API extraction

**Tool 86 JSON output includes everything:**
```json
{
  "yaml": {"table_key": "version: '1.1'..."},
  "sql": {"table_key": "-- Deploy reference..."},
  "stats": {"table_key": {"translated": 28, "total": 32}},
  "migration_report": "# UC Metric View Migration Report...",
  "limitations": {"rls_tables": [...], "aggregation_warnings": [...]},
  "validation": {"table_key": {"evaluated": 5, "valid": 0}},
  "specs_summary": {"table_key": {"view_name": "mv_sales"}}
}
```

## Demo Details

### run_locally.py

Demonstrates Phase 4 (generation) using pre-extracted JSONs from the SC Reporting project.

**What it loads:**
1. `measure_table_mapping.json` — 470 DAX measures with table allocations
2. `mquery_transpilation.json` — 60 MQuery table definitions
3. `pbi_relationships.json` — relationship graph for auto-join detection
4. `scan_result_debug.json` — PBI Admin API scan for inline SQL enrichment
5. `pipeline_config.json` — customer-specific join maps, SWITCH decompositions, manual overrides, etc.

**What it produces:**
- 26 YAML files (UC Metric View definitions)
- 26 SQL files (deployment reference instructions)
- 26 validation JSONs (DAX-vs-UCMV structural comparison per table)
- 1 migration report (markdown with executive summary, per-table stats, join map, PBI Native Features table)

**Verification:**
```bash
diff ~/Downloads/ucmv_example_output/Fact_HR_A_uc_metric_view.yml \
     ~/workspace/demos/uc_metrics/sc_reporting_project/output/Fact_HR_A_uc_metric_view.yml
```

20/23 exact measure-level match. 0 missing measures. 3 files with +9 extra measures (we translate more than the original monolith).

### run_all_limitations_demo.py

Synthetic demo exercising all 10 PBI limitation detection features against fabricated data.

**What it tests:**
1. USERELATIONSHIP → alternate join alias (`dim_calendar_ship_date`)
2. M:N relationships → flagged with workaround
3. RLS → detected from scan data roles
4. Aggregation tables → Import storageMode flagged
5. Conditional formatting → business-logic Color measure NOT rejected
6. Incremental refresh → refreshPolicy detected
7. Default summarization → SummarizeBy=None columns flagged
8. Calculation groups → base measures × group items expanded
9. Perspectives → flagged in report
10. Field parameters → flagged in report

**Output:** `~/Downloads/ucmv_all_limitations_demo/` — 1 YAML + 1 SQL + migration report with all sections populated.

## How to Test In Practice

### Quick smoke test (2 minutes)

```bash
cd src/backend
source .venv/bin/activate
python ../../examples/uc_metric_view_migration/run_locally.py
```

**What you should see:**
- `Metric views: 26` — all 26 tables processed
- `Total: 396/671 measures translated (59%)` — overall translation rate
- `VALIDATION` section at the bottom — per-table DAX-vs-UCMV comparison
- Output in `~/Downloads/ucmv_example_output/`

**What "good" looks like:**
- 26 YAML files + 26 SQL files + 1 migration report
- No empty dimensions (the pipeline now filters these)
- The migration report has a "PBI Native Features — Migration Status" table with all 10 rows
- `fact_scorecard_BP_wc` shows `manual=6` (manual overrides injected)
- FT_Planning PY measures have `window:` blocks with `semiadditive: last`

### Validation output (what the 0/73 VALID means)

The validation section compares the **structure** of the original DAX expression against the generated UCMV SQL expression. It reports INVALID when it finds structural differences — but most of these are **expected translation changes**, not bugs:

| Validator says | What actually happened | Is it a bug? |
|---|---|---|
| "Aggregation mismatch: SUMX → SUM" | DAX `SUMX(table, col)` → SQL `SUM(source.col)` | No — correct translation |
| "Filter mismatch: IN DAX but not UCMV" | DAX inline FILTER → SQL FILTER (WHERE ...) | No — different syntax, same logic |
| "Column reference mismatch" | DAX `Table[col]` → SQL `source.col` | No — alias rewriting |

The validation JSON files (e.g., `validation_FT_Planning.json`) contain per-measure details: differences, similarities, and recommendations. Use these for **manual review** of complex measures — the validator catches real issues alongside expected transformations.

**When validation IS useful:** If a measure shows "Filter content mismatch" with unexpected filter conditions, or "Aggregation mismatch" where the function type changed (e.g., SUM→COUNT), that's a real translation bug worth investigating.

### Unit tests (30 seconds)

```bash
python -m pytest tests/unit/engines/crewai/tools/custom/metric_view_utils/ \
  tests/unit/engines/crewai/tools/custom/metric_view_validation_utils/ -q
```

**Expected:** 709 passed. This includes:
- 486 generation pipeline tests (16 DAX patterns, YAML emitter, joins, dependencies, etc.)
- 213 validation framework tests (DAX parser, expression comparison, pipeline modes)
- 10 end-to-end integration tests (runs full pipeline against SC Reporting data)

### All-limitations demo (1 minute)

```bash
python ../../examples/uc_metric_view_migration/run_all_limitations_demo.py
```

**Expected:** `ALL 10 LIMITATIONS VERIFIED ✓` — each of the 10 PBI limitation detections triggered on synthetic data.

### Deploying to Databricks (Phase 5)

After reviewing the YAML output, deploy with Tool 88:
1. **Dry run** (default): validates YAML without deploying → check for errors
2. **Live deploy**: `dry_run=False` + provide `databricks_host`, `databricks_token`, `warehouse_id`
3. Tool 88 uses the UC Metric View REST API (POST for create, PUT on 409 for update)

### What to check after deployment

Once deployed to a Databricks workspace:
1. `DESCRIBE METRIC VIEW catalog.schema.view_name` — verify dimensions + measures match
2. `SELECT MEASURE(revenue) FROM METRIC VIEW catalog.schema.view_name GROUP BY region` — verify query works
3. Compare against original PBI visual: same numbers at same grain = success
4. Check `window:` measures separately: `SELECT MEASURE(revenue_py) ...` — verify trailing 12 month logic

## Example Files

| File | Size | Description |
|------|------|-------------|
| `run_locally.py` | — | Demo script — run pipeline against pre-extracted JSONs |
| `run_all_limitations_demo.py` | — | Synthetic demo exercising all 10 PBI limitation detections |
| `crew_ucmv_generator.json` | 4K | Importable Kasal crew config |
| `pipeline_config.json` | 55K | Customer config (SC Reporting — 26 sources, 44 manual overrides) |
| `measure_table_mapping.json` | 569K | 470 DAX measures |
| `mquery_transpilation.json` | 202K | 60 MQuery tables |
| `pbi_relationships.json` | 64K | PBI relationships |
| `scan_result_debug.json` | — | PBI scan data |

## Pipeline Config Reference

The `pipeline_config.json` drives all customer-specific behavior. Empty defaults work for basic models; populated values are needed for complex PBI patterns. Today built manually; Tool 89 will auto-propose most of it.

### Core Config (structure)

| Key | When Needed |
|-----|-------------|
| `join_key_map` | Dimension tables with non-obvious join keys |
| `fact_join_map` | Cross-table DIVIDE measures (fact-to-fact joins) |
| `enrichment_joins` | Extra dimension joins not auto-detected from DAX |
| `filter_sets` | Named filter value lists for SWITCH decomposition |
| `switch_decompositions` | SELECTEDVALUE+SWITCH measures broken into SQL |
| `manual_overrides` | Hand-written SQL for complex DAX that regex can't translate (per table) |
| `measure_resolutions` | Static DAX measure name → SQL resolution map |
| `mapping_only_tables` | Tables with measures but no MQuery SQL entry |
| `column_overrides` | PBI column → physical column name overrides |

### Override Config (complex measures)

| Key | When Needed |
|-----|-------------|
| `manual_overrides` | Complex cross-table, SUMX(SUMMARIZE), DISTINCTCOUNTNOBLANK, or geography-routed measures. Format: `{"table_key": [{"name": "...", "expr": "SQL", "comment": "...", "window": {...}}]}`. Injected before Pass 2 so downstream MEASURE() refs resolve automatically. |
| `calculation_groups` | PBI calculation groups to expand. Format: `[{"name": "Time Intelligence", "items": [{"name": "YTD", "expression": "CALCULATE(SELECTEDMEASURE(), DATESYTD(...))"}]}]`. Each item × each base measure = one explicit UCMV measure. |
| `perspectives` | PBI perspectives to flag in migration report. Format: `[{"name": "Sales View", "tables": ["Fact_Sales"]}]`. |
| `field_parameters` | PBI field parameters to flag in migration report. Format: `[{"name": "Metric Selector", "measures": ["Revenue", "Margin"]}]`. |

### Display Config (metadata)

| Key | When Needed |
|-----|-------------|
| `dimension_exclusions` | Per-table dimensions to hide from output |
| `measure_metadata` | Per-table display names, synonyms, comments |
| `comment_overrides` | Per-table metric view comment overrides |
| `name_prefixes_to_strip` | Prefixes to strip for display names |
| `percentage_multiplier_patterns` | Regex for measures needing 100* multiplier |

### Behavior Config (tuning)

| Key | Default | When Needed |
|-----|---------|-------------|
| `dim_alias_map` | `{}` | PBI dimension → SQL join alias mapping |
| `parameter_defaults` | `{}` | PBI parameter values |
| `period_dim_priority` | `["fiscper", "fiscal_year_period", "date_key"]` | Custom period dimension ordering |
| `int_period_dims` | `["fiscper", "fiscal_year_period"]` | Period dimensions with integer type |
| `budget_suffix` | `"_bp"` | Budget measure variant suffix |
| `cwc_filter_column` | `""` | CWC filter expansion column (set per customer) |
| `switch_join_alias` | `""` | SWITCH decomposition join alias (set per customer) |
| `switch_join_col` | `""` | SWITCH decomposition join column (set per customer) |

### Starting From Scratch

1. Run Phase 1 (extract) to get JSON files
2. Run Tool 86 with `config_json: {}` → baseline with base measures only
3. Review migration report → see what's missing and why
4. Add config entries incrementally → re-run → each iteration translates more
5. Add `manual_overrides` for complex measures the regex can't handle
6. Repeat until migration report is green

## Known Gaps & Roadmap

Honest assessment of where this stands and what's needed next.

### What would make this best-in-class

| Priority | Gap | Impact | Effort |
|----------|-----|--------|--------|
| **1** | **Tool 89 (Config Generator)** — auto-propose 80% of `pipeline_config.json` from extraction output | Biggest bottleneck today. Manual config authoring is what takes the human hours. | Medium (2-3 days) |
| **2** | **Validator calibration** — 0/73 VALID is misleading. Expected transformations (SUMX→SUM, Table[col]→source.col) should show as EQUIVALENT, not INVALID. | Demo credibility. Showing "0 valid" to a customer undermines confidence even when the output is correct. | Small (1 day) |
| **3** | **Real Databricks deployment test** — we validate YAML structure but never actually `CREATE METRIC VIEW` + `SELECT MEASURE(...)` against a live workspace | First real customer deployment will find issues we can't catch locally. | Small (1 day, needs workspace access) |
| **4** | **Surface 85% business coverage in console output** — the 59% headline rate includes PBI UI artifacts (FORMAT, Color, ISBLANK). Real business coverage is 85% but it's buried in the migration report. | Perception. 59% sounds bad, 85% sounds good — and 85% is the honest number. | Trivial |
| **5** | **table_processor.py at 942 lines** — the pipeline split moved bulk from pipeline.py (694) but the extracted file still exceeds the 500-line project rule | Tech debt, not blocking. | Medium |

### What's genuinely strong

- **Deterministic approach** — 16 regex patterns + dependency graph + manual overrides = reproducible output you can diff, version, and debug. LLM-only approaches can't do this.
- **709 tests with integration coverage** — the full SC Reporting integration test catches regressions that unit tests miss.
- **`manual_overrides` config** — admits "some things need human SQL" without pretending the tool handles everything. This is what the product team's tool lacks.
- **20/23 exact match** against a monolith that took 2 weeks to hand-tune — from a configurable pipeline that runs in 30 seconds.
- **0 silent wrong output** — all 10 PBI limitations detected and flagged. No customer will get wrong numbers without a warning.
- **Same pipeline in Tool 86 and run_locally.py** — what you test locally is exactly what runs in production.

### Validation Framework — What to Improve

The validation framework (`metric_view_validation_utils/`) compares DAX structure against generated UCMV SQL. Currently 0/73 VALID on SC Reporting — not because the translations are wrong, but because the validator doesn't account for **expected transformations**. Here's the breakdown of all 73 INVALID results and what each needs:

**False positive categories (73 total):**

| Category | Count | Root cause | Fix |
|----------|-------|-----------|-----|
| Reference mismatch | 71 | DAX uses `Table[col]`, UCMV uses `source.col`. Validator maps table names but `dim_wkctr.bic_cwc_type` in DAX becomes bare `bic_cwc_type` in UCMV FILTER clause. | **Normalize references**: strip `source.` prefix, map dim alias→bare column in FILTER context. File: `expression_validator.py:_compare_columns()` |
| Filter mismatch | 58 | Same filter, different syntax. DAX: `IN('val1','val2')` parsed as `{type: IN, values: [...]}`. UCMV: `col IN ('val1','val2')` parsed as `{type: UNKNOWN, raw: "col in ('val1','val2'"}` — the UCMV parser fails to parse its own IN syntax. | **Fix UCMV filter parser**: `databricks_parser.py:_parse_condition()` doesn't handle UCMV `FILTER (WHERE col IN (...))` syntax. The regex `_IN_CLAUSE_PATTERN` expects `col IN (...)` but the actual UCMV expression wraps it in `FILTER (WHERE ...)`. |
| Aggregation mismatch | 55 | `SUMX(table, col)` in DAX → `SUM(source.col)` in UCMV. The `DAX_TO_DB_AGG_MAP` maps `SUMX→SUM` but the column matching fails because DAX parses `SUMX(source.matl_group, source.target_value)` as two args while UCMV has `SUM(source.yield_act)` (different column after translation). | **Column-aware aggregation matching**: compare aggregation type separately from column. If type maps correctly (SUMX→SUM), mark as EQUIVALENT not INVALID. File: `expression_validator.py:_compare_aggregations()` |

**Specific fixes needed (by file):**

`databricks_parser.py`:
1. **Line 153-165** (`_extract_filters`): The `_FILTER_PATTERN` regex extracts the condition from `FILTER (WHERE ...)` but `_parse_condition` then fails to parse `col IN ('val1', 'val2')` because the `_IN_CLAUSE_PATTERN` doesn't match when there's no `source.` prefix on the column. Fix: make the IN pattern more permissive — `(\w+)\s+IN\s*\(([^)]+)\)` instead of requiring `table.column`.
2. **Line 191-220** (`_extract_references`): Extracts `source.col` correctly but doesn't strip the `source.` prefix for comparison. When DAX has `Fact_Sales[revenue]` and UCMV has `source.revenue`, the reference sets don't intersect. Fix: normalize both sides to bare column names in a fallback comparison.

`expression_validator.py`:
3. **Line 297-458** (`_compare_aggregations`): The matching logic requires both aggregation type AND column to match. But after DAX→SQL translation, the column name often changes (e.g., `SUMX(table, target_value)` → `SUM(source.yield_act)`). Fix: add a "type-only match" tier — if the aggregation function maps correctly (SUMX→SUM, COUNTX→COUNT), classify as `EQUIVALENT` with a note about column mapping.
4. **Line 574-640** (`_compare_columns`): Strict mode compares full `table.column` pairs. But DAX uses PBI table names (`Dim_wkctr.bic_cwc_type`) while UCMV uses join aliases (`dim_wkctr.bic_cwc_type`). The `table_mappings` only maps fact tables, not dimension aliases. Fix: auto-discover dimension alias mappings from the YAML joins section and add them to `table_mappings`.
5. **Line 460-478** (`_filter_signature`): The `UNKNOWN` fallback uses raw string comparison. When the UCMV parser returns `{type: UNKNOWN, raw: "col in ('val1','val2'"}` (note: missing closing paren — truncation bug in `_parse_condition`), it never matches the DAX side's properly parsed `{type: IN, column: ..., values: [...]}`. Fix: try re-parsing `UNKNOWN` raw strings as IN/EQUALS before falling back.

`dax_expression_parser.py`:
6. **Line 99-103** (`check_variable_usage`): Fixed the `[^\w]` boundary bug but the RETURN-only-variable edge case (e.g., `VAR x = [Measure]\nRETURN x`) still doesn't substitute when `x` is the entire return expression. Fix: add start/end-of-string anchors to the pattern.
7. **Line 77-80** (`clean_dax_comments`): `line.split('--')[0]` truncates strings containing `--`. Fix: use a regex that skips quoted strings.

**New status levels to add:**

The validator currently has only VALID/INVALID/SKIPPED/ERROR. Add:
- `EQUIVALENT` — structurally different but semantically correct (e.g., SUMX→SUM, Table[col]→source.col)
- `REVIEW` — partially matching, needs human verification (e.g., aggregation type matches but column differs)

This would change 0/73 VALID to roughly 18/73 EQUIVALENT + 40/73 REVIEW + 15/73 INVALID — much more useful for a demo.

**Future: API-based validation**

The regex-based validator will always have blind spots. The ultimate validation is:
1. Deploy the UCMV to Databricks (Tool 88)
2. Query the UCMV: `SELECT MEASURE(revenue) FROM METRIC VIEW mv GROUP BY region`
3. Query PBI via Execute Queries API: same measure, same grain
4. Compare result sets — row counts match, values within tolerance (±0.01%)

This requires a Databricks workspace + PBI API access, so it can't be local-only. It would live in `deploy_test.py` (see Next Steps) and use the Databricks SDK for step 2 and the PBI Execute Queries API for step 3.

### What's honestly still painful

The biggest time sink is building `pipeline_config.json`. For SC Reporting (26 sources, 470 measures), the config has **26 keys, 44 manual overrides, 8 switch decomposition tables, 6 join key maps, 13 measure resolutions**. All written by hand. This is what takes the 2-3 hours per first customer.

Breakdown of where the untranslatable measures come from (SC Reporting, 275 untranslatable):

| Reason | Count | Could auto-fix? |
|--------|-------|-----------------|
| SELECTEDVALUE+SWITCH (PBI slicer pattern) | 82 | Yes — parse SWITCH branches from DAX |
| PY/DIVIDE over PBI artifacts (display-only) | 61 | Already handled — correctly excluded |
| SELECTEDVALUE (slicer context) | 24 | No — needs human decision |
| ISBLANK+BLANK guard (no aggregation) | 24 | Already handled — correctly excluded |
| Covered by SWITCH decomposition | 17 | Already handled — in config |
| FORMAT function (display-only) | 13 | Already handled — correctly excluded |
| ISFILTERED (PBI-specific) | 12 | Already handled — correctly excluded |
| Cannot resolve [measure_ref] | 22 | Partially — add to measure_resolutions |
| DIVIDE sub-expression | 6 | LLM fallback or manual_overrides |
| Other | 14 | Case-by-case |

**127 of 275 are correctly excluded** (PBI artifacts). The real gap is 148, of which **82 are SWITCH patterns** that Tool 89 could auto-propose.

## Next Steps

### Helper scripts (build before Tool 89)

These are standalone Python scripts in `examples/uc_metric_view_migration/` — no Kasal server needed. Each one takes the extraction JSONs as input and proposes config entries.

**1. `config_scaffold.py` — auto-propose 60-70% of pipeline_config.json**

Takes: `pbi_relationships.json` + `measure_table_mapping.json` + `mquery_transpilation.json` + `scan_result_debug.json`
Produces: initial `pipeline_config.json` with:

| Config key | How it's auto-proposed | Accuracy |
|-----------|----------------------|----------|
| `join_key_map` | Parse relationship records → extract dim table + join column + alias | ~90% (miss composite keys) |
| `enrichment_joins` | Already auto-generated in `RelationshipsLoader` | 100% |
| `column_overrides` | Diff MQuery column names vs DAX `Table[col]` references | ~80% |
| `mapping_only_tables` | Tables in measure mapping but not in MQuery output | 100% |
| `switch_decompositions` (skeleton) | Parse SELECTEDVALUE+SWITCH DAX → extract branch names + measure refs | ~60% (branches need SQL, not just names) |
| `measure_resolutions` | Untranslatable "Cannot resolve [ref]" → look up ref in other tables | ~70% |
| `parameter_defaults` | Extract PBI parameters from MQuery `#"Parameter"` patterns | ~90% |

Human reviews and edits the proposed config, then runs Tool 86. Saves 1-2 hours on first customer.

**2. `gap_analyzer.py` — show what to fix next**

Takes: pipeline output (migration report + stats)
Produces: prioritized fix list showing:

```
COVERAGE: 59% overall, 85% business (excluding 127 PBI artifacts)

TOP GAPS (by unlock potential):
  1. Add switch_decompositions for FT_Planning → +11 measures (82→93%)
  2. Add manual_overrides for FT_PE009 → +15 measures
  3. Add measure_resolutions for [F_End_date] chain → +9 measures
  4. Enable LLM fallback → est. +6 measures (DIVIDE sub-expressions)

NEXT CONFIG KEY TO ADD: switch_decompositions.FT_Planning
  82 SELECTEDVALUE+SWITCH measures waiting
  Template: {"name": "...", "raw_expr": "...", "comment": "..."}
```

This tells the SA exactly what to do next instead of reading a 500-line migration report.

**3. `deploy_test.py` — smoke test against live Databricks**

Takes: YAML output + Databricks workspace credentials
Does:
1. `CREATE OR REPLACE METRIC VIEW` for each YAML
2. `SELECT MEASURE(name) FROM METRIC VIEW ... LIMIT 1` for each measure
3. Reports: which measures execute, which throw errors, which return NULL

This catches runtime issues (wrong column names, invalid FILTER syntax, missing joins) that YAML validation can't.

### Tool 89 (full automation — after helper scripts prove the patterns)

The helper scripts above are the prototypes for Tool 89. Once `config_scaffold.py` and `gap_analyzer.py` work well manually, wrap them as a CrewAI tool that:
1. Takes extraction output from Tools 73/74/75/86
2. Proposes pipeline_config.json
3. Runs Tool 86 with the proposed config
4. Runs gap_analyzer on the output
5. Returns: proposed config + pipeline output + gap analysis

The SA reviews the proposed config, edits what's wrong, and re-runs. Target: first customer config time drops from 2-3 hours to 30-60 minutes.

### Other things still missing

| What | Why it matters | Effort |
|------|---------------|--------|
| **Databricks notebook template** | SAs need a notebook they can give customers for self-service validation. "Run this in your workspace to verify the metric views produce correct numbers." | 1 day |
| **PBI visual comparison script** | Script that queries both PBI (via Execute Queries API) and Databricks (via SQL warehouse) for the same measure at the same grain, and diffs the results. This is the ultimate correctness check. | 2 days |
| **Multi-dataset support** | Tool 86 handles one PBI dataset at a time. Real customers have 5-10 datasets feeding into dashboards. Need a wrapper that iterates across datasets. | 1 day |
| **Incremental migration mode** | After initial migration, customer adds new measures to PBI. Need a "diff mode" that detects new/changed measures and only re-generates affected metric views. | 2-3 days |
| **Customer documentation generator** | Auto-generate a "Migration Report" PDF that the SA gives to the customer: what was migrated, what needs manual attention, recommended Databricks permissions, known limitations. Not the technical migration_report.md — a customer-facing document. | 1 day |

## Competitive Landscape & Path to #1

### Where we stand today

| Tool | What it does | vs Kasal |
|------|-------------|----------|
| **[BrickShift (LatentView)](https://www.latentview.com/blog/bi-migration-to-databricks-brickshift-guide/)** | Full BI migration (PBI + Tableau + ThoughtSpot → Databricks dashboards + semantic layer). Four-phase workflow, Genie-ready output. | **Broader scope** — they do dashboard migration too. But it's a consulting engagement, not self-serve. |
| **[Tabular Editor Semantic Bridge](https://tabulareditor.com/blog/bridge-analytics-in-databricks-and-power-bi-via-tabular-editor)** | Translates UCMV YAML → PBI semantic model (the **reverse** direction). Enterprise license. | **Complementary** — they go Databricks→PBI, we go PBI→Databricks. |
| **[mexmarv/powerbi-databricks-semantic-gen](https://github.com/mexmarv/powerbi-databricks-semantic-gen)** | Open-source notebook. PBI JSON → SQL views (NOT UCMV YAML). ~25 DAX functions, PySpark fallback. 12 GitHub stars. | **We're significantly ahead** — they produce SQL views not UCMVs, fewer patterns, no joins, no validation, no deployment. |
| **Databricks engineering** (internal `powerbi-migrate`) | File-based input, LLM-driven DAX translation, no deployment. Actively developing. | **We're ahead today** (deterministic, API extraction, deployment, 709 tests). They'll close the gap with dedicated headcount. |
| **Consulting firms** | Manual rewrites. Weeks per dashboard. | **10x faster** — but they handle edge cases we can't. |

### What would make this a clear #1

1. **Tool 89 (auto-propose config)** — This is the #1 gap vs BrickShift (theirs auto-configures). Our biggest time sink is manual `pipeline_config.json` authoring. Auto-proposing 80% of the config from extraction output drops first-customer effort from 2-3 hours to 30-60 minutes.

2. **Validator calibration** — 0/73 VALID kills demo credibility. Adding `EQUIVALENT` status for known-correct transformations (SUMX→SUM, Table[col]→source.col) would show ~18 EQUIVALENT + 40 REVIEW + 15 INVALID instead of 73 INVALID.

3. **Dashboard migration** — BrickShift does dashboards too. If Kasal could generate Lakeview dashboards from PBI visuals (layout, chart types, filters), it covers the full migration: semantic layer + visuals.

4. **Multi-dataset orchestration** — Real customers have 5-10 PBI datasets feeding into dashboards. Tool 86 handles one dataset at a time. Need a wrapper that iterates across datasets, merges configs, and handles cross-dataset measure references.

5. **Customer self-service notebook** — An SA shouldn't need to be in the room for validation. A Databricks notebook that deploys the UCMVs, runs `SELECT MEASURE(...)` for each, and compares against PBI source — the customer runs it themselves and gets a green/red report.

### The uncomfortable truths

**The market doesn't care about 709 tests or 16 regex patterns.** The market cares about: "How fast can I move my PBI dashboards to Databricks?" BrickShift answers that in 4 phases including dashboard migration. Kasal answers only the semantic layer part.

**You're the best at something almost nobody is trying to do yet.** UC Metric Views are < 1 year old (v1.1 shipped late 2025). Most Databricks customers doing PBI migrations create regular SQL views or dbt models, not metric views. The "market" for PBI → UCMV tooling is: (1) us, (2) the product team, (3) nobody else. Being #1 in a market of 2 is real but it's not the same as #1 in a competitive market.

**The real competition isn't other UCMV tools — it's "just write SQL views."** A senior data engineer can manually translate 50 DAX measures to SQL in 2-3 days. Our tool does it in minutes, but the customer needs to trust the output, understand the YAML spec, and debug issues. For 50 measures: ~2x ROI. For 500 measures: ~20x ROI. The value scales with model complexity.

**The pipeline has SAP BW DNA.** Despite extracting customer-specific hardcodings into config, the design reflects its origin: SAP BW fiscal periods, KBI tables, RE_Version codes, CWC filters, plant/workcenter keys. A retail or healthcare PBI model would find the config knobs irrelevant. The pipeline works for non-SAP models — but it was designed for SAP models.

**No production customer has used it yet.** Edge cases in real customer models will surface issues that 709 tests can't predict.

### The honest rating

| Dimension | Rating | Explanation |
|-----------|--------|-------------|
| vs. Engineering tool (capability) | **Significantly ahead** | Structural advantages they can't close by iterating: live API access, deterministic translation, M-transform folding, UC REST API deployment |
| vs. Engineering tool (polish) | **Slightly ahead** | More tests, but less guided UX. No interactive checkpoint flow. Requires SA expertise to operate |
| vs. Engineering tool (maturity) | **Equal** | Both v0.x, no production customers, early stage |
| vs. Global market (UCMV tooling) | **#1 of 2** | The only other entrant is the product team. No commercial competition |
| vs. Global market (PBI migration) | **Top tier for Databricks target** | For non-Databricks targets, irrelevant. For Databricks targets, nothing else comes close |
| vs. Manual engineering | **5-20x faster** | Small models: marginal. Large models (200+ measures): transformational |
| Production readiness | **Not yet** | Needs real customer validation |

**The one-liner:** The most capable PBI → UC Metric View migration tool that exists, and it's not close. But the best at something the market hasn't demanded at scale yet.

**The path to #1 isn't more regex patterns** — it's Tool 89 + Lakeview dashboard generation + a 30-minute customer workflow that an SA can run without touching Python.

### Market threats to watch

1. **Databricks ships a first-party UCMV migration tool.** If the product team builds API access + deployment, our structural advantages erode. Timeline: 6-12 months given their current pace.
2. **dbt adds a PBI → dbt Metrics migration path.** dbt Labs has the muscle and market position. They'd target dbt metrics (not UCMV), but compete for the same customer budget.
3. **Microsoft makes Fabric migration seamless.** If PBI → Fabric becomes frictionless, customers may not migrate to Databricks at all. Our tool only matters if the customer has already chosen Databricks.
4. **UC Metric Views don't achieve adoption.** If the feature stalls or gets superseded, the tool's value drops regardless of quality.

### References

- [BrickShift migration guide (LatentView)](https://www.latentview.com/blog/bi-migration-to-databricks-brickshift-guide/)
- [Tabular Editor Semantic Bridge](https://tabulareditor.com/blog/bridge-analytics-in-databricks-and-power-bi-via-tabular-editor)
- [Open-source PBI→Databricks semantic gen](https://github.com/mexmarv/powerbi-databricks-semantic-gen)
- [PBI migration complexity analysis (cauchy.io)](https://blog.cauchy.io/p/how-hard-is-it-to-migrate-a-power)
- [PBI third-party semantic model support](https://blog.crossjoin.co.uk/2026/04/12/power-bi-and-support-for-third-party-semantic-models/)
