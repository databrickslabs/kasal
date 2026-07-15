# Tenant-Scale DAX Coverage — Status & Evidence

**Date:** 2026-07-15 · **Branch:** `feat/pbi-ucmv-fixes-v2`

**What this document is:** a status record of the DAX→UCMV transpiler's coverage,
backed by measurement against a full tenant corpus. It started as a plan; the
Tier 1–3 work below is now **shipped**, so this reads as "what we did and what
the evidence says," followed by the genuinely-open items.

## TL;DR

- **Tiers 1–3 shipped** (see §3): two silently-wrong bug classes fixed, a verified
  share-of-total handler added, and the table-valued constructs sorted honestly
  into translate-vs-skip. All guarded and tested.
- **Evidence — two complementary measurements:**
  1. **Full tenant deterministic pass** (585,955 measures, local, 41s): **43
     silently-suspect (0.01%)** — the guards hold at planet scale. This is a
     *safety floor* for the regex tier, **not** a quality score (77% route to the
     LLM under `--no-llm` and show as TODO — expected).
  2. **Inline Opus sample** (48 hard-bucket measures, §2): **0 silently-wrong**,
     50% faithful, 40% honestly-declined. The corpus is sound; remaining gaps are
     model-tier or genuine static-metric-view limitations, not instruction gaps.
- **At-scale LLM-tier measurement is not currently runnable here** — the Databricks
  path is concurrency/budget-bound, and the Anthropic Batches path needs an API
  key this (company-licensed, PyPI-firewalled) machine doesn't have. The two
  measurements above are the best available composite.
- **Highest-value open item:** the extraction **truncation guard** (§4) — protects
  against silent degraded runs; independent of all the DAX-translation work.

**Inputs used:**
- Tenant export — `New_Query_2026_07_15_16_05_52.csv` (585,955 measures, 142,270
  unique expressions, 5,001 datasets, 4,649 tables; 98.8% carry full DAX).
- Reference single-report comparison — 226 emitted measures vs. original DAX (ground
  truth from the run's `execution_trace` output).
- Transpiler surface: `metric_view_utils/dax_translator.py` (fast-path registry),
  `skills/dax/PATTERNS.md` + `UNSUPPORTED.md` (LLM skill corpus),
  `sql_measure_sanitizer.py::detect_lost_dax_component` (silently-wrong guard).

> **Scope note:** frequency counts are *structural* fingerprints (regex over the
> expression text), not a semantic classifier — one-significant-figure sizing,
> good for prioritization, not exact. "Faithful"/"safe" numbers are *structural
> fidelity* (filters/terms preserved), **not** numerical proof — no warehouse
> execution was done.

---

## 1. What the tenant corpus actually contains

DAX construct frequency (share of the 585,948 non-empty measures whose
expression contains the construct; a measure can hit several):

| Construct | Count | Share |
|-----------|------:|------:|
| CALCULATE | 245,317 | 41.9% |
| VAR / RETURN | ~177,000 | 30.2% |
| DIVIDE | 130,380 | 22.3% |
| SELECTEDVALUE | 110,962 | 18.9% |
| IF( | 109,947 | 18.8% |
| FILTER | 99,299 | 16.9% |
| SWITCH | 74,464 | 12.7% |
| ALL( | 53,724 | 9.2% |
| SUMX | 30,485 | 5.2% |
| DISTINCTCOUNT | 25,440 | 4.3% |
| SUMMARIZE | 24,949 | 4.3% |
| COUNTROWS | 21,492 | 3.7% |
| ALLSELECTED | 14,644 | 2.5% |
| CALCULATETABLE | 12,577 | 2.1% |
| FORMAT( | 11,196 | 1.9% |
| DATEADD | 7,826 | 1.3% |
| USERELATIONSHIP | 5,899 | 1.0% |
| SAMEPERIODLASTYEAR | 5,093 | 0.9% |
| TREATAS | 4,977 | 0.8% |
| RANKX | 3,412 | 0.6% |
| TOTALYTD | 3,023 | 0.5% |
| TOPN | 2,961 | 0.5% |
| LOOKUPVALUE | 2,641 | 0.5% |

**Headline:** the corpus is dominated by `CALCULATE` (42%), `VAR/RETURN`
composition (30%), and `DIVIDE` ratios (22%) — the exact family the reference bugs
live in. `SELECTEDVALUE`/`SWITCH` (19%/13%) are largely display-layer
(skip-by-design). The long tail (`RANKX`, `TOPN`, `SUMMARIZE`, `TREATAS`,
`LOOKUPVALUE`, `EARLIER`) is individually <5% and mostly not expressible in a
static metric view.

---

## 1a. Construct-by-construct coverage verdict

What the transpiler *actually* does per construct today, cross-checked against
the fast-path matcher registry (`dax_translator.py::_register_patterns`) and the
LLM skill corpus (`skills/dax/*.md`). Status legend: 🟢 handled well ·
🟡 partial/risky · 🔴 gap · ⚪ correctly skipped by design.

| Construct | Share | Status | Reality |
|-----------|------:|:------:|---------|
| CALCULATE | 41.9% | 🟢 | Workhorse. `calculate_equality_filter`, `calculate_measure_ref`, sumx/countx/averagex-filter matchers + corpus §3. Simple `CALCULATE(agg, FILTER)` solid. **Nested/multi-filter CALCULATE is the risk edge** (EDGE_CASES §3 says "flag for review" — not always enforced). |
| VAR / RETURN | 30.2% | 🟢 | **Bug A fixed** (Tier 1): §2b now covers var-chain arithmetic (`a − b`/`a + b`, no DIVIDE) + guard demotes collapses. |
| DIVIDE | 22.3% | 🟢 | Multiple matchers + corpus §2, ANSI-safe `/NULLIF`. **Bug B fixed** (Tier 1): measure-ref num/denom filters preserved + `num==denom` guard. |
| SELECTEDVALUE | 18.9% | ⚪ | Slicer/display-layer — skip or decompose. Correct. |
| IF( | 18.8% | 🟢 | → `CASE WHEN` (corpus §8). |
| FILTER | 16.9% | 🟢 | → `FILTER (WHERE …)`. Core strength. |
| SWITCH | 12.7% | ⚪ | Slicer dispatch → per-branch measures (corpus §5/6). Verify decomposition emits branches vs. dropping. |
| ALL( ) | 9.2% | 🟢 | **Tier 2 shipped**: §4 rewritten (window-based share-of-total + ALLSELECTED caveat + "not-a-share" cases) + collapse guard (100% on a 4k sample). |
| SUMX | 5.2% | 🟢 | `simple_sumx` + filter variants; `SUMX(t, a*b)` → `SUM(a*b)`. |
| DISTINCTCOUNT | 4.3% | 🟢 | `distinctcountnoblank` matcher + corpus. |
| SUMMARIZE | 4.3% | 🟡 | **Tier 3**: `PATTERNS.md` §13 teaches the fixed-LOD `GROUP BY` source-view translation (LLM path). |
| COUNTROWS | 3.7% | 🟢 | → `COUNT(1)` / `COUNT(DISTINCT)` (corpus §1/§12). |
| ALLSELECTED | 2.5% | 🟡 | **Tier 2**: share-of-total variant translated as a `range: all` window **approximation** (no visual scope in metric views) — flagged, not silent. |
| CALCULATETABLE | 2.1% | 🟡 | **Tier 3**: grouped-virtual-table case folded into the §13 SUMMARIZE source-view pattern. |
| FORMAT( | 1.9% | 🟢 | → semantic `format:` metadata (corpus §9). |
| DATEADD | 1.3% | 🟡 | Time-intel: calendar `_py` join / window workarounds, guarded vs. fabrication. Handled *if* calendar column exists, else honest TODO. |
| USERELATIONSHIP | 1.0% | 🟡 | Matcher exists, deliberately routed to LLM+join-context. Partial. |
| SAMEPERIODLASTYEAR | 0.9% | 🟡 | Matcher + corpus workaround; "needs calendar column" caveat. |
| TREATAS | 0.8% | ⚪ | **Tier 3**: `UNSUPPORTED.md` — disconnected-slicer dispatch, display-layer (like SWITCH); honest skip, no unlock. |
| RANKX | 0.6% | 🟢 | `rankx` matcher → window function. |
| TOTALYTD | 0.5% | 🟡 | Corpus window-measure workaround; conditional on date dim. |
| TOPN | 0.5% | ⚪ | **Tier 3**: `UNSUPPORTED.md` — row ranking; needs source-view `ROW_NUMBER()`/`QUALIFY`, never MAX. Honest skip. |
| LOOKUPVALUE | 0.5% | ⚪ | **Tier 3**: `UNSUPPORTED.md` — display label / parameter table; a real attribute lookup is a join, not this. Honest skip. |

Status cells above reflect the post-Tier-1/2/3 state; §3 has the detail on what
each fix did and its evidence. The former 🔴 gaps (VAR arithmetic, ALL
share-of-total, SUMMARIZE, TREATAS/TOPN/LOOKUPVALUE) are now either fixed,
handled via the LLM corpus, or honestly categorized as display-layer skips.

**On regex vs. LLM (adoptability):** the split is right — regex fast-path for the
deterministic tier, LLM + skill-corpus for the flexible tail. The lever for
adoptability was **not** "more regex"; it was better LLM instructions (a corpus
rule generalizes across the whole construct family, not just one report) plus guards
that convert silent-wrong into honest-TODO. The remaining lever is **model tier**
(§2c) and **warehouse-verified measurement** (§4), not more corpus.

**High-frequency constructs (CALCULATE/DIVIDE/FILTER/SUMX/IF/COUNTROWS/
DISTINCTCOUNT) are genuinely handled**; the gaps were concentrated in the
now-addressed tail, not diffuse.

---

## 2. Evidence — three measurements

Coverage is measured, not asserted. Three passes, each answering a different
question. All "faithful/safe" numbers are **structural fidelity** (filters and
terms preserved), not numerical proof — no SQL was executed against a warehouse.

### 2a. Reference single-report comparison (the original quality baseline)

226 emitted measures vs. original DAX (of 256 emitted; 30 had no full DAX to diff):

| Bucket | Count | Meaning |
|--------|------:|---------|
| BASE | 114 | `SUM(source.col)` — faithful by construction |
| FAITHFUL | 66 | filtered aggregates; filter literal-sets match DAX |
| RATIO_OK | 20 | ratios whose num/den differ, as the DAX does |
| SUSPECT (triaged) | 17 → **8 real bugs** | 9 were false positives (inherited `bic_creg_type='Plant'` from a base measure) |
| NO_DAX | 9 | hidden weighted-avg ratios / raw `CASE WHEN` from another path |

**~218 / 226 (~96%) faithful or correct** at the time. The 8 failures clustered
into exactly **two root-cause bug classes** — both now fixed (Tier 1, §3).

### 2b. Full tenant deterministic pass (safety floor across all 585,955)

Ran every tenant measure through the regex fast-path + all guards, **local, no
LLM, 41 seconds**:

| Verdict | Count | Note |
|---------|------:|------|
| demoted_todo | 451,051 (77%) | **Not a quality figure** — these route to the LLM in production; `--no-llm` shows them as TODO by design |
| skipped_by_design | 109,084 (19%) | display-layer / correctly-skipped (SWITCH/SELECTEDVALUE/FORMAT/slicer scalars) |
| base | 18,457 | plain source-column aggregates |
| faithful | 7,121 | filtered aggregates the fast-path translates cleanly |
| ratio_ok | 199 | ratios |
| **silently_suspect** | **43 (0.01%)** | guard-flagged emitted SQL — the number that matters |

**Reading:** at full tenant scale the guards keep silently-wrong output to
**0.01%**. This is a *safety floor for the deterministic tier* — it proves the
fast-path + guards don't ship wrong SQL en masse. It does **not** measure the LLM
tier (that's the 77% demoted here); 2c covers that.

### 2c. Inline Opus sample (the LLM-tier read)

Because at-scale LLM measurement isn't runnable on this machine (see TL;DR), the
LLM tier was sampled directly: 48 measures drawn from the *hard* buckets
(var_return, share_of_total, allexcept, summarize, divide, calculate_filter,
allselected, time_intel), translated by Opus applying the skill corpus, then
scored through the same production guard.

- **0 / 48 silently-wrong** (every emitted translation passed the guard).
- **24 faithful + 5 ratio_ok** (~60% emitted and clean).
- **19 honestly-declined** (TODO/review) — almost all *genuine* static-metric-view
  limits (time-intel needing a calendar column, multi-col ALLEXCEPT, SUMMARIZE
  needing a source-view precompute, disconnected-slicer filters), not corpus gaps.

**Reading:** with a strong model + our corpus, nothing ships wrong and the
declines are legitimate. The remaining lever for production quality is **model
tier** (does the production model match Opus here?), not more corpus work. This
is the *ceiling*; the production number depends on the deployed model.

> **Caveat carried by all three:** structural fidelity ≠ numerical correctness. A
> wrong-operator translation that preserves every literal would score faithful.
> Executing against a warehouse is the only thing that closes that gap, and we
> have not done it.

---

## 3. Shipped — the fixes and their evidence

Tiers 1–3 are committed on `feat/pbi-ucmv-fixes-v2`. Each is a corpus rule and/or
a `detect_lost_dax_component` guard check, with regression tests.

### Tier 1 — two confirmed bug classes (correctness on already-handled volume)

**Bug A — multi-block arithmetic collapse (`a − b`, `a + b` outside DIVIDE).**
DAX bound ≥2 `CALCULATE`/`SUMX` blocks to vars and combined them with `+`/`-`
(no `DIVIDE`); the transpiler emitted only the first term. Casualties (6):
`cost_to_supply_{actuals,bp,re}` (dropped `-b`), `total_nsr_{actuals,bp,re}`
(dropped `+b`). Blast radius ≈ **10,955 (1.9%)** of the tenant. **Shipped:**
`PATTERNS.md` §2b generalized from var-chain-DIVIDE to var-chain arithmetic
(`return a - b`, `a + b`, `(a+b)/c`) with a worked no-DIVIDE example; the guard's
additive-drop check flags "additive term dropped" → demotes to TODO instead of
shipping wrong SQL.

**Bug B — measure-ref ratio num/denom filter-set loss.**
`DIVIDE(CALCULATE([M], pred_num), CALCULATE([M], pred_den))` emitted `num == denom`
→ constant 1.0. Casualty: `consumer_complaints_actual` (lost the `bic_csubkbi`
code sets on both sides). Population at risk ≈ **6,017 (1.0%)**. **Shipped:** two
resolver sub-fixes (strip wrapping parens on `(table[col] in {…})` predicates;
parse single-line `var … return DIVIDE(…)`) so per-side filters survive; guard
check #5 flags `num==denom` collapse. Verified on run 47916: **16 QSE ratios → 14
distinct num/denom, 0 collapsed** (was shipping 1.0).

### Tier 2 — ALL()/ALLSELECTED share-of-total (biggest translatable gap)

Corpus sampling of the 68,087 ALL-family measures showed the clean translatable
target is the share-of-total ratio `DIVIDE([M], CALCULATE([M], ALL(dim)))` —
`divide_with_all` (32.7%) + `allselected_share` (21.3%) ≈ 54% of the family.
**Shipped:** `PATTERNS.md` §4 rewritten — window-based translation (§4a), the
ALLSELECTED approximation caveat (§4b: metric views have no visual scope), and
explicit "NOT a share-of-total" cases (§4c: ALLEXCEPT, ALL-as-slicer-reset,
ALL(Dates)+time-intel); guard check catches a collapsed share-of-total (DAX has
DIVIDE+ALL, SQL has no window and num==denom) on **100% of a 4,000-measure
sample**.

### Tier 3 — table-valued constructs sorted honestly (translate vs skip)

These do **not** share one fate. Sorted each by whether a UCMV equivalent exists:
- **Translatable → `PATTERNS.md`:** `SUMX(SUMMARIZE(fact, cols), expr)` → §13
  fixed-LOD `GROUP BY` in the source SELECT + identity dimension; `ALLEXCEPT(table,
  one_col)` → §14 fixed-LOD window (~63% of ALLEXCEPT keeps one col; 2+ →
  source-view PARTITION BY).
- **Display-layer, honest skip → `UNSUPPORTED.md`:** TREATAS (disconnected-slicer
  dispatch), LOOKUPVALUE (display label / parameter table), TOPN (row ranking —
  needs source-view `ROW_NUMBER()`/`QUALIFY`). These have **no source-view
  unlock** — advertising one would be wrong.
- **Emitter guidance:** `_categorize_untranslatable` emits a construct-specific
  category + the actual next step per construct, so the "not emitted" comment is
  actionable rather than a generic "needs manual translation."

Decision: **no new "special PBI patterns" skillfile** — the two existing corpus
files were enriched instead, because the split is translate-vs-skip, not one bucket.

---

## 4. What's open

| Item | Why it matters | Notes |
|------|----------------|-------|
| **Extraction truncation guard** | 47821-style silent degraded run: when XMLA/Execute-Queries extraction fails, `kpi.formula` returns bare column tokens (`bic_csubkbi`) that the transpiler emits as clean-looking `source.bic_csubkbi` with **no error surfaced**. A whole run can ship ~80% wrong measures silently. | Detect the degraded fingerprint at the extraction boundary (high fraction of single-token "DAX" + all-snake names + missing `all_allocations`) → reject/retry via the TMDL/SP fallback. **Independent of all DAX-translation work; highest-value demo-safety item.** |
| **Warehouse execution ("did the number match?")** | Every measurement here is *structural* fidelity — it can't catch a wrong-operator translation that preserves all literals. | The only way to close the gap. Needs a Databricks warehouse + the reference data; run emitted SQL and the DAX side-by-side and compare results. |
| **At-scale LLM-tier coverage %** | 2c is a 48-measure sample; a real per-construct LLM number needs the full corpus through the production model. | Blocked on this machine (Databricks concurrency/budget; no Anthropic key + PyPI firewalled). Runnable where either the Batches API or an un-throttled endpoint is reachable. The deterministic harness + structural scorer are recoverable from git history (`4ad8cca5`). |
| **Second-order generalization** (lower priority) | Nested-CALCULATE sub-patterns beyond Bug A (~14% touch ≥2 CALCULATE), DISTINCTCOUNT `noblank` verification, confirming SWITCH decomposition emits per-branch measures. | Sample from the corpus per construct once a real measurement loop exists; don't invest ahead of the data. |

## 5. Honest caveats

- **Structural fidelity ≠ correctness.** Every "faithful/safe/96%/0.01%" figure
  measures whether filters and terms were preserved, not whether the number is
  right. No SQL was executed against a warehouse. This is the single biggest
  caveat and it applies to the whole document.
- **The 0.01% deterministic figure is a safety floor, not a quality score.** 77%
  of measures route to the LLM (shown as TODO under `--no-llm`); the deterministic
  pass only proves the regex tier + guards don't ship wrong SQL en masse.
- **Frequency counts are regex fingerprints** — one-significant-figure sizing.
- **Denominator matters.** The tenant export mixes hidden/helper measures (23,589
  hidden) and display artifacts; a real coverage % should exclude the
  skip-by-design population (SWITCH/SELECTEDVALUE, FORMAT, slicer scalars).
- **The inline Opus sample is a ceiling.** Production quality depends on the
  deployed model matching Opus on these buckets — unmeasured here.
