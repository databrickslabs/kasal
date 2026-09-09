# PBI → UCMV: coverage evaluation and best-effort roadmap (SC / PAAT / DCC)

Consolidated summary of three CCHBC dataset runs (2026-07-28), each analysed
against the current `feat/pbi-ucmv-fixes-v2` behaviour, plus the concrete repo
changes they imply. Source analyses live with the run artifacts
(`EVALUATION_README.md` / `DIAGNOSTIC_README.md` per dataset); this doc is the
in-repo, actionable digest.

## TL;DR

| Dataset | Model type | Source tables (M-Query) | Measures resolved to SQL | UCMVs | Best-effort ceiling |
|---------|-----------|:-----------------------:|:------------------------:|:-----:|:-------------------:|
| **SC** | dimensional data model | ✅ 86/86 transpiled + validated | full | **30 views, 476/476 measures, 100%** | n/a (already works) |
| **PAAT** | report skin on external warehouse | ❌ 0/50 | **46** (ready) | 0 (fallback) | ~10–19% |
| **DCC** | KPI / data-quality report | ❌ 0/17 | **0** | 0 (fallback) | ~0–5% |

- **SC is the success baseline.** With real transpiled source tables, the pipeline
  emits perfect measures (100% exact SQL match to ground truth) and improved on the
  prior run (91% → 100%; +41 measures recovered). Coverage of *all* source measures
  = 74% (476 emitted / 642); the other 166 are honestly documented as not-emitted.
- **PAAT / DCC produce no UCMVs — correctly, not as a bug.** Both are Power BI
  *report* layers, not data models: no M-Query source tables, no physical column
  refs. The pipeline reaches its designed fallback (`_build_fallback_extract`) and
  emits raw material instead of silently-wrong views.

## Why PAAT and DCC are hard (and what's fixable)

Both lack the three things SC had: M-Query source tables, physical `table_refs`, and
measures that reduce to single-table aggregates.

- **Fixable with customer input:** supply the physical source-table names
  (`fact_source_map`). This unlocks PAAT's 46 already-resolved measures. It is a
  *prerequisite* for DCC but not sufficient.
- **Fixable with real engineering (DCC-specific):** measure-on-measure composition
  (40% of DCC measures reference other measures) + a `COUNTROWS`/`SUMMARIZE`/
  `CALCULATETABLE` → SQL translator.
- **Never fixable:** slicer/selector measures (`SELECTEDVALUE`/`SWITCH`/`ALLSELECTED`
  — 20% PAAT, 32% DCC), disconnected/parameter tables, and text/format measures.
  These are report interaction state, not data — no static SQL equivalent exists.

## The better fix first: resolve the upstream semantic model

PAAT/DCC are **thin reports on an upstream/external semantic model** — the model
that holds the M-Queries and physical tables is a *separate dataset*, not the one
these runs extracted. So the highest-value fix is **not** to manually re-supply the
missing sources; it is to **extract the underlying model's `dataset_id`** (which has
everything, like SC did). Kasal already extracts by `dataset_id` via `executeQueries`
(`powerbi_semantic_model_dax_tool.py`), so this is largely an *input* choice today
and an *automation* opportunity tomorrow:

- **Today (operational):** point the run at the upstream model's `dataset_id`
  (found via Power BI lineage / `GET /reports/{id}` → `datasetId`). Full case-A
  extraction, no code change.
- **Feature (automate):** thin-report detection + upstream resolution — when
  extraction finds no M-Queries, read the report's `datasetId`, check whether it is a
  live-connection / DirectQuery-to-dataset, and **follow it to the base model** before
  extracting. The REST surface exists (`/reports/{id}`, `/datasets/{id}`); Kasal does
  not chase the upstream link yet — that is the gap.

`fact_source_map` (below) is the **fallback-of-the-fallback**: only for when the
upstream model is genuinely unreachable (external AAS/SSAS cube, or no access to the
dataset). See the customer-facing guide **`thin-report-and-source-resolution.md`**
(exposed in the in-app Documentation → Power BI migration).

## What to adapt in the repo

Ordered by leverage. Phases 1–3 are the shared "best-effort UCMV" feature that
turns PAAT's "0 views + JSON dump" into "a few real fact views + an honest gap".
All gated behind an explicit `allow_best_effort` flag so today's default contract
("never emit silently-wrong SQL") is unchanged when off. **Prefer upstream-model
resolution (above) over best-effort whenever the base model is reachable.**

### 1. `fact_source_map` supply mechanism — THE unlock (no code path works without it)
- **`pipeline_config_generator_tool.py`** — add `fact_source_map` and
  `allow_best_effort` to the `config_keys` tuple (~L110–121); read via `_get(...)`.
  Flows through `tool_configs`, no API route change.
- **`generate_config.py`** — allow `source_table` to come from `fact_source_map`
  when M-Query extraction yielded nothing (today `mquery_expression` is empty for
  these datasets, so `_enrich_source_tables_from_mquery` has nothing to parse).
- **UI** (`PipelineConfigGeneratorConfigSelector.tsx`) — `allow_best_effort` Switch +
  a key→value editor (PBI table → physical table); mirror the warehouse-enrichment
  toggle wiring.

### 2. Best-effort view generation
- **`uc_metric_view_generator_tool.py`** — before building `output` (~L369), when
  `yaml_output` is empty AND `allow_best_effort` AND `fact_source_map` present, call
  a new `_build_best_effort_views(...)`. It emits one thin UCMV per supplied source
  from measures that ALREADY resolved to real aggregatable SQL (reuse
  `measure_resolutions`; reuse the `base_expr` + `FILTER (WHERE …)` assembly from
  `generate_config.py` L708–712). Every emitted measure carries a `TODO: verify`
  comment — it is a draft produced without a validated transpiled source.
- Never emit a `TODO`/selector measure as SQL — those go to not-emitted (step 3).

### 3. Honest not-emitted documentation (reuse SC machinery)
- Extend `_build_fallback_extract(...)` to also render the SC-style
  `# ─── Not emitted as measures (N) — grouped by reason ───` block per source, so
  the existing `evaluate_ucmv.py` coverage counter scores PAAT/DCC for free. Reason
  buckets: `selector logic`, `unresolved ref`, `measure-on-measure`,
  `disconnected/no source`, `text/format`.

### 4. (DCC-only, larger, defer) measure composition + table-expression translation
- **Phase 5 — measure composition:** inline `[Measure]` references by resolving each
  referenced measure's own SQL (dependency graph). Extend the existing SC
  `MEASURE(...)` composition. Only helps once leaf measures resolve to real columns.
- **Phase 6 — table-expression translator:** `COUNTROWS`/`SUMMARIZE`/`CALCULATETABLE`
  → SQL `COUNT`/`GROUP BY`/subquery. Substantial DAX-engine work; gated, best-effort,
  `TODO: verify`. `ALLSELECTED`/slicer-context measures stay out of scope.
- **ROI note:** Phases 5–6 buy DCC only ~0–5% coverage. Build only if
  measure-on-measure KPI models prove a common customer pattern.

### 5. Automated evaluation harness (adopt as CI, no product code)
- `evaluate_ucmv.py` (measure-vs-measure vs a validated set) is the eval. Gates:
  **correctness** `recall==100 && mismatch==0` (never emit wrong SQL) and
  **coverage never-regress** `coverage_pct >= previous`. Coverage <100% is expected
  (untranslatable DAX); the gate is that it must not drop.

## Recommendation

Build **1–3** first (the PAAT best-effort feature) behind `allow_best_effort`;
it is high-leverage, low-risk, and reuses existing machinery. Defer **4** (DCC
composition/table-expr) until there's demand — its ceiling is low. Set customer
expectations up front: for report-layer models, UCMV coverage is inherently bounded,
and physical source-table names are a required input Kasal cannot invent.

---

_Digest of: SC `EVALUATION_README.md` (30 views, 100% measure match, 74% coverage,
+41 vs prior run); PAAT `DIAGNOSTIC_README.md` (46 resolvable, ~10–19% ceiling);
DCC `DIAGNOSTIC_README.md` (0 resolvable, measure-on-measure web, ~0–5% ceiling).
Analyses run 2026-07-28/29 on CCHBC crew outputs._
