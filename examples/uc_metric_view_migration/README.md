# UC Metric View Migration

Migrate Power BI semantic models to Databricks Unity Catalog Metric Views — deterministically, from a live API, with human review where it matters.

## Try It Now

```bash
cd src/backend
source .venv/bin/activate
python ../../examples/uc_metric_view_migration/run_locally.py
```

This runs the full pipeline against the SC Reporting project (26 sources, 470 measures) and produces 26 UC Metric View YAML files in `~/Downloads/ucmv_example_output/`. The 23 core views match the verified reference output at `~/workspace/demos/uc_metrics/sc_reporting_project/output/` with cosmetic-only differences.

No PBI API access needed. No Kasal server needed. Just the pre-extracted JSONs in this directory.

## Why This Matters

The only tool that does live PBI API extraction → deterministic DAX translation → automated UC REST API deployment in one pipeline. Others:

- **Databricks product team** (`powerbi-migrate`): File-based input only, LLM-driven (non-deterministic), no deployment. Actively developing — will improve.
- **Microsoft**: Migration path goes to Fabric (keeps DAX), not to UC Metric Views.
- **Consulting firms**: Manual rewrites. Weeks of effort per dashboard.
- **Metric layer vendors** (dbt, Looker): Own constructs, no PBI migration tooling.

The moat isn't "no one does this" — it's the approach: deterministic translation (14 regex patterns, reproducible output) + live API access (no file exports) + human review at the right points (not blanket LLM).

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
  Phase 5: VALIDATE + DEPLOY (planned)
  Deterministic checks → compare vs PBI → human approval → deploy
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
| Phase 4: Generate | **Done** | Tool 86. 459 tests, 21/21 module coverage. |
| Phase 5: Validate | **Planned** | Deterministic check framework (separate project, no timeline yet). Tool 88 deployer exists for dry-run + live deploy. |

### The iteration loop

Real migrations are iterative. A typical flow:

1. **First run** with empty config → migration report shows 40-50% translated (base measures only)
2. **Add join_key_map** → re-run → 55-65% translated (dimension joins added)
3. **Add switch_decompositions** → re-run → 70-75% translated
4. **Add measure_resolutions** → re-run → 80%+ translated
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

**What Tool 89 won't get right:** `fact_join_map` entries with pivot/union/embed modes. These encode grain-level business decisions (e.g., "scorecard table has one row per KBI per workcenter, but the fact table has one row per workcenter — need to pivot before joining"). A human must review these.

### Known Limitations

**USERELATIONSHIP (inactive relationships)**

DAX uses `USERELATIONSHIP(Table[Column], Dim[Column])` to activate an inactive relationship for a specific calculation (e.g., using ShipDate instead of the default OrderDate join to a calendar dimension). Today:
- The Relationships API (Tool 75) extracts inactive relationships (`is_active: false`)
- `RelationshipsLoader` skips them — they're extracted but discarded

The fix: UC Metric Views support multiple joins to the same dimension with different aliases. A `USERELATIONSHIP` measure becomes a second join entry:
```yaml
joins:
  - name: dim_calendar           # default (OrderDate)
    source: calendar_table
    on: source.order_date = dim_calendar.date
  - name: dim_calendar_ship      # USERELATIONSHIP alternative
    source: calendar_table
    on: source.ship_date = dim_calendar_ship.date
```
Then the measure references `dim_calendar_ship` instead of `dim_calendar`. This can be auto-detected from the inactive relationships we already extract — planned for Tool 89.

**M:N relationships (many-to-many)**

PBI supports many-to-many through bridging tables or bidirectional cross-filtering. Today:
- `RelationshipsLoader` explicitly skips them (`from_card == 'Many' and to_card == 'Many': continue`)
- UC Metric Views don't natively support M:N joins

Workarounds (require human decision):
1. **Bridge table** — create a Databricks view that resolves the M:N into two 1:N joins, reference it in `enrichment_joins` config
2. **Pre-joined source SQL** — use inline `source: |-` SQL in the YAML that bakes in the M:N resolution
3. **Skip and flag** — document in migration report for manual handling

Both limitations are flagged in the migration report when detected. Neither produces silently wrong output — the measures are marked untranslatable with specific reasons.

### What will always need human judgment

- Semantic correctness (does `revenue` mean gross or net?)
- Grain decisions for cross-table joins (pivot vs union vs embed)
- USERELATIONSHIP join alias decisions (which alternative join to use)
- M:N relationship resolution strategy (bridge table vs pre-joined SQL)
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

**Why Tool 86 today is already faster than the monolith:** Extraction is automatic (was manual), migration report shows what's missing (was trial-and-error), dependency graph resolves measure chains (was manual ordering).

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

Pipeline internals: MQuery Parser → DAX Translator (14 patterns + LLM fallback) → Kahn's Dependency Graph → Join Detector (pivot/union/embed modes) → YAML Emitter (5-pass MEASURE() cascade validation) → Migration Report.

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

## Demo Details: run_locally.py

Demonstrates Phase 4 (generation) using pre-extracted JSONs from the SC Reporting project.

**What it loads:**
1. `measure_table_mapping.json` — 470 DAX measures with table allocations
2. `mquery_transpilation.json` — 60 MQuery table definitions
3. `pbi_relationships.json` — relationship graph for auto-join detection
4. `scan_result_debug.json` — PBI Admin API scan for inline SQL enrichment
5. `pipeline_config.json` — customer-specific join maps, SWITCH decompositions, etc.

**What it produces:**
- 26 YAML files (UC Metric View definitions)
- 26 SQL files (deployment reference instructions)
- 1 migration report (markdown with executive summary, per-table stats, join map)

**Verification:**
```bash
diff ~/Downloads/ucmv_example_output/Fact_HR_A_uc_metric_view.yml \
     ~/workspace/demos/uc_metrics/sc_reporting_project/output/Fact_HR_A_uc_metric_view.yml
```

23/23 original views present, 0 missing, cosmetic diffs only.

## Example Files

| File | Size | Description |
|------|------|-------------|
| `run_locally.py` | — | Demo script — run pipeline against pre-extracted JSONs |
| `crew_ucmv_generator.json` | 4K | Importable Kasal crew config |
| `pipeline_config.json` | 43K | Customer config (SC Reporting — 26 sources) |
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
| `measure_resolutions` | Static DAX measure name → SQL resolution map |
| `mapping_only_tables` | Tables with measures but no MQuery SQL entry |
| `column_overrides` | PBI column → physical column name overrides |

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
| `cwc_filter_column` | `"bic_cwc_type"` | CWC filter expansion column |
| `switch_join_alias` | `"dim_wkctr"` | SWITCH decomposition join alias |
| `switch_join_col` | `"bic_cwc_type"` | SWITCH decomposition join column |

### Starting From Scratch

1. Run Phase 1 (extract) to get JSON files
2. Run Tool 86 with `config_json: {}` → baseline with base measures only
3. Review migration report → see what's missing and why
4. Add config entries incrementally → re-run → each iteration translates more
5. Repeat until migration report is green
