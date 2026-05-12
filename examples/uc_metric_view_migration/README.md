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
| Phase 2: Propose config | **Done** | Tool 89 + `config_scaffold.py`. Auto-proposes 7/8 config keys. 58% translation rate without manual edits. |
| Phase 3: Human review | **Manual** | Customer edits pipeline_config.json by hand. |
| Phase 4: Generate | **Done** | Tool 86. 823 tests, 20+ modules. All 10 PBI limitations detected. 93% business coverage. |
| Phase 5: Validate + Deploy | **Done** | Validation: 4 VALID + 27 EQUIVALENT + 3 REVIEW. Tool 88 deployer. `deploy_test.py` for smoke testing. |

### The iteration loop

Real migrations are iterative. A typical flow:

1. **Run `config_scaffold.py`** → auto-proposes 7/8 config keys → **58% translation rate immediately**
2. **Run `gap_analyzer.py`** → shows top gaps by unlock potential → SA knows what to fix next
3. **Fill in switch_decomposition SQL** → re-run → 70-75% translated
4. **Add manual_overrides** → re-run → 80%+ translated (complex cross-table measures)
5. **Enable LLM fallback** for remaining complex DAX → 85%+
6. **Run `deploy_test.py --dry-run`** → validates YAML structure
7. **Run `deploy_test.py` live** → CREATE METRIC VIEW + SELECT MEASURE() smoke test

Each iteration takes minutes (Tool 86 runs in 30 seconds). The config scaffold eliminates 60-70% of manual config work. The gap analyzer tells the SA exactly what to add next.

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

**Expected:** 823 passed. This includes:
- 497 generation pipeline tests (16 DAX patterns, YAML emitter, joins, dependencies, integration)
- 234 validation framework tests (DAX parser, expression comparison, EQUIVALENT/REVIEW statuses)
- 45 config scaffold tests (auto-proposal of 8 config keys)
- 29 gap analyzer tests (categorization, unlock potential, end-to-end)
- 8 deploy test mocks (YAML parsing, deploy, measure testing)
- 10 Tool 89 tests (schema, instantiation, SWITCH detection, error handling)

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
| `config_scaffold.py` | — | Auto-propose pipeline_config.json from extraction JSONs |
| `gap_analyzer.py` | — | Prioritized gap analysis after pipeline run |
| `deploy_test.py` | — | Smoke test YAML against live Databricks (or dry-run) |
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
2. Run `config_scaffold.py` → auto-propose 7/8 config keys (58% translation rate immediately)
3. Run Tool 86 with proposed config → review migration report
4. Run `gap_analyzer.py` → see prioritized gaps and what to add next
5. Fill in switch_decomposition SQL + manual_overrides → re-run → each iteration translates more
6. Run `deploy_test.py --dry-run` → validate YAML structure
7. Repeat steps 3-6 until migration report is green

## What's Been Automated (formerly manual)

The config authoring bottleneck has been addressed with a semi-automated workflow. Here's the before/after:

### Before: Fully manual config (2-3 hours per first customer)

The SA had to hand-write all 26 config keys in `pipeline_config.json` by reading the migration report, understanding the PBI model, and manually crafting join maps, switch decompositions, and measure resolutions. This required deep knowledge of both PBI and UCMV.

### After: Semi-automated config proposal (30-60 minutes)

```
  Step 1: Extract (automatic — Tools 73/74/75/86)
  PBI API → 4 JSON files
                │
                ▼
  Step 2: Scaffold (automatic — config_scaffold.py / Tool 89)
  4 JSONs → proposed_pipeline_config.json (7/8 keys auto-proposed)
                │
                ▼
  Step 3: Gap analysis (automatic — gap_analyzer.py)
  Shows: 93% business coverage, top gaps by unlock potential,
         "next config key to add" recommendation
                │
                ▼
  Step 4: Human review (30-60 min)
  SA reviews proposed config, fills in TODO items for:
  - switch_decompositions SQL (skeletons auto-generated, SQL needs human)
  - manual_overrides for complex cross-table measures
  - fact_join_map grain decisions
                │
                ▼
  Step 5: Generate + Validate (automatic — Tool 86 + validation)
  Pipeline → 26 YAMLs + validation (4 VALID, 27 EQUIVALENT, 3 REVIEW)
                │
                ▼
  Step 6: Deploy test (deploy_test.py)
  Dry-run: validates YAML structure (no credentials needed)
  Live: CREATE METRIC VIEW + SELECT MEASURE() smoke test
```

### What each tool does

| Tool | What it auto-proposes | Verified accuracy |
|------|----------------------|-------------------|
| **`config_scaffold.py`** | 7/8 config keys: join_key_map (5 dims), enrichment_joins (52), switch_decompositions (111 skeletons), mapping_only_tables (2), parameter_defaults (3), filter_sets (17), measure_resolutions (8) | 58% translation rate with proposed config alone (vs 59% with full manual config) |
| **`gap_analyzer.py`** | Prioritized "what to fix next" with unlock potential per gap. Top gap: SWITCH patterns (89 measures, +307 downstream refs) | Correctly identifies SWITCH as #1 gap |
| **`deploy_test.py`** | Dry-run validates YAML structure (23/26 valid, 3 empty tables correctly flagged). Live mode deploys + tests measures | 371 measures parsed, 0 parse errors |
| **Tool 89** | CrewAI wrapper of config_scaffold + gap analysis. Returns proposed config JSON + confidence scores + gap summary | 10 tests, registered as tool ID 89 |

### What still needs human judgment

| Config key | Why it can't be auto-proposed |
|-----------|------------------------------|
| `switch_decompositions` SQL | Skeletons auto-generated (branch names + case values) but the actual SQL numerator/denominator expressions require understanding the business logic |
| `manual_overrides` | Complex cross-table measures (SUMX+SUMMARIZE, geography-routed DISTINCTCOUNT). LLM fallback can help but deterministic auto-proposal is impossible |
| `fact_join_map` | Grain/pivot/embed decisions are business logic. "Scorecard has 1 row per KBI per workcenter, fact has 1 per workcenter — pivot before joining" — only domain experts know |

### What's genuinely strong

- **Deterministic approach** — 16 regex patterns + dependency graph + manual overrides = reproducible output you can diff, version, and debug. LLM-only approaches can't do this.
- **823 tests** — generation (497), validation (234), gap analysis (29), deploy test (8), config scaffold (45), Tool 89 (10).
- **Semi-automated config** — `config_scaffold.py` proposes 7/8 keys achieving 58% translation rate before any human edits. Gap analyzer tells the SA exactly what to add next.
- **Calibrated validation** — 4 VALID + 27 EQUIVALENT + 3 REVIEW (was 0/73 VALID). EQUIVALENT means "correct translation, different syntax."
- **93% business coverage** — the real number after excluding 246 PBI artifacts (FORMAT, Color, ISBLANK, SELECTEDVALUE).
- **20/23 exact match** against a monolith that took 2 weeks to hand-tune — from a configurable pipeline that runs in 30 seconds.
- **0 silent wrong output** — all 10 PBI limitations detected and flagged.
- **Same pipeline everywhere** — Tool 86, run_locally.py, Tool 89 all use identical modules.

### Remaining gaps

| What | Why it matters | Effort |
|------|---------------|--------|
| **table_processor.py at 942 lines** | Exceeds 500-line project rule. Tech debt, not blocking. | Medium |
| **Databricks notebook template** | Customer self-service validation notebook. SA shouldn't need to be in the room. | 1 day |
| **PBI visual comparison script** | Query both PBI + Databricks for same measure, diff results. The ultimate correctness check. | 2 days |
| **Multi-dataset support** | Real customers have 5-10 datasets. Tool 86 handles one at a time. | 1 day |
| **Incremental migration mode** | Detect new/changed measures after initial migration, re-gen only affected UCMVs. | 2-3 days |
| **Customer documentation generator** | Customer-facing PDF migration report (not the technical markdown). | 1 day |

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
