# Tenant-Scale DAX Coverage Roadmap

**Date:** 2026-07-15
**Author:** analysis session (Claude)
**Inputs:**
- CCHBC full-tenant PBI measure export — `New_Query_2026_07_15_16_05_52.csv`
  (585,955 measures, 142,270 unique expressions, 5,001 datasets, 4,649 tables).
- CCHBC single-report comparison — 226 comparable emitted measures vs. original
  DAX (ground truth from the run's `execution_trace` output).
- Current transpiler: `metric_view_utils/dax_translator.py` (fast-path registry),
  `skills/dax/PATTERNS.md` (LLM skill corpus), `sql_measure_sanitizer.py`
  (`detect_lost_dax_component` guard).

**Purpose:** Turn "works for CCHBC" into "works for the general PBI tenant." The
tenant export is a planet-scale sample of real DAX; its construct-frequency
distribution tells us exactly where coverage effort pays off, and the CCHBC
comparison tells us which bugs are already costing us faithful translations.

> **Scope note:** counts below are *structural* fingerprints (regex over the
> expression text), not a semantic classifier. They size blast radius to one
> significant figure — good enough to prioritize, not exact.

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
composition (30%), and `DIVIDE` ratios (22%) — the exact family our CCHBC bugs
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
| VAR / RETURN | 30.2% | 🟡 | Corpus §2b teaches var-chain inline **only inside DIVIDE**. `return a − b` / `a + b` → **Bug A collapse**. Biggest single quality gap because VAR touches 30% of the corpus. |
| DIVIDE | 22.3% | 🟢 | Multiple matchers + corpus §2, ANSI-safe `/NULLIF`. Except measure-ref num/denom filter loss (**Bug B**). |
| SELECTEDVALUE | 18.9% | ⚪ | Slicer/display-layer — skip or decompose. Correct. |
| IF( | 18.8% | 🟢 | → `CASE WHEN` (corpus §8). |
| FILTER | 16.9% | 🟢 | → `FILTER (WHERE …)`. Core strength. |
| SWITCH | 12.7% | ⚪ | Slicer dispatch → per-branch measures (corpus §5/6). Verify decomposition emits branches vs. dropping. |
| ALL( ) | 9.2% | 🔴 | **Biggest real gap.** Corpus §4 documents `window: range: all` share-of-total but it's LLM-only, no matcher, unverified at scale (~54k measures). |
| SUMX | 5.2% | 🟢 | `simple_sumx` + filter variants; `SUMX(t, a*b)` → `SUM(a*b)`. |
| DISTINCTCOUNT | 4.3% | 🟢 | `distinctcountnoblank` matcher + corpus. |
| SUMMARIZE | 4.3% | 🔴 | **Zero coverage** — no matcher, not in corpus. Table-valued; needs pre-agg source view. |
| COUNTROWS | 3.7% | 🟢 | → `COUNT(1)` / `COUNT(DISTINCT)` (corpus §1/§12). |
| ALLSELECTED | 2.5% | ⚪🔴 | Documented unsupported (no visual-context equivalent). Honest skip; 15k measures untranslated. |
| CALCULATETABLE | 2.1% | 🔴 | **Zero coverage.** Table-valued filter context. |
| FORMAT( | 1.9% | 🟢 | → semantic `format:` metadata (corpus §9). |
| DATEADD | 1.3% | 🟡 | Time-intel: calendar `_py` join / window workarounds, guarded vs. fabrication. Handled *if* calendar column exists, else honest TODO. |
| USERELATIONSHIP | 1.0% | 🟡 | Matcher exists, deliberately routed to LLM+join-context. Partial. |
| SAMEPERIODLASTYEAR | 0.9% | 🟡 | Matcher + corpus workaround; "needs calendar column" caveat. |
| TREATAS | 0.8% | 🔴 | **Zero coverage.** Virtual relationship; hard. |
| RANKX | 0.6% | 🟢 | `rankx` matcher → window function. |
| TOTALYTD | 0.5% | 🟡 | Corpus window-measure workaround; conditional on date dim. |
| TOPN | 0.5% | 🔴 | Zero coverage. |
| LOOKUPVALUE | 0.5% | 🟡 | Corpus mention (→ join); no matcher. Row-context, often needs source column. |

**Reading of the table — three tiers:**

- **Tier 1 — fix (correctness on already-"handled" volume):** Bug A (VAR
  arithmetic collapse — the VAR family is 30% of the corpus; the broken subset
  is ~2% but silently wrong) and Bug B (measure-ref num/denom). These make the
  *common* patterns trustworthy. Bug A is an LLM-instruction + guard fix (cheap,
  high impact); Bug B is a resolver fix (riskier).
- **Tier 2 — build (real coverage gap) — DONE:** `ALL()`/`ALLSELECTED`
  share-of-total. Corpus sampling (68,087 ALL-family measures) showed the clean
  translatable target is the **share-of-total ratio** `DIVIDE([M], CALCULATE([M],
  ALL(dim)))` (`divide_with_all` 32.7% + `allselected_share` 21.3% ≈ 54% of the
  ALL family). Shipped: (1) rewritten skill corpus §4 — the window-based
  translation (4a), the ALLSELECTED approximation caveat (4b), and the explicit
  "NOT a share-of-total" cases (4c: ALLEXCEPT, ALL-as-slicer-reset, ALL(Dates)+
  time-intel); (2) a `detect_lost_dax_component` check #5 that catches a collapsed
  share-of-total (DAX has DIVIDE+ALL/ALLSELECTED, SQL has no window and num==denom
  → constant 1.0) and demotes to TODO. Guard catches the collapsed form on
  100% of a 4,000-measure real-corpus sample. Regression tests added.
- **Tier 3 — categorize, don't chase (~8% combined) — DONE:** SUMMARIZE,
  CALCULATETABLE, TREATAS, TOPN, LOOKUPVALUE, ALLEXCEPT. Corpus sampling showed
  these do NOT share one fate — the honest split is:
  - **Translatable (→ PATTERNS.md):** `SUMX(SUMMARIZE(fact, cols), expr)` →
    §13 fixed-LOD `GROUP BY` in the source SELECT + identity dimension;
    `ALLEXCEPT(table, one_col)` → §14 fixed-LOD window (`range: all`) — this is
    ~63% of ALLEXCEPT (single kept col); 2+ kept cols → source-view PARTITION BY.
  - **Display-layer, honest skip (→ UNSUPPORTED.md):** TREATAS (disconnected-
    slicer KPI dispatch — same family as SWITCH), LOOKUPVALUE (builds a display
    string / reads a parameter table — a real attribute lookup is a join, not
    this), TOPN (row ranking — needs source-view ROW_NUMBER()/QUALIFY, do not
    approximate with MAX). These have **no source-view unlock** — advertising one
    would be wrong.
  - **Emitter guidance:** `_categorize_untranslatable` now emits a
    construct-specific category + the actual next step for each (SUMMARIZE →
    "materialize GROUP BY as identity dimension"; TREATAS → "define each KPI as
    its own measure, no unlock"; etc.), so the "not emitted" comment is
    actionable, not a generic "needs manual translation". These construct checks
    run BEFORE the generic SELECTEDVALUE/scalar catch (they co-occur with a
    slicer arg; the construct is the actionable signal).
  Key correction from the original plan: the "for every skip, state the unlock"
  framing was too optimistic — half of these (TREATAS/LOOKUPVALUE/TOPN) have no
  unlock and are correctly display-layer skips. A new "special PBI patterns"
  skillfile was NOT created; the two existing corpus files (PATTERNS/UNSUPPORTED)
  were enriched instead.

**On regex vs. LLM (adoptability):** the split is right — regex fast-path for the
deterministic ~80%, LLM+skill-corpus for the flexible tail. The lever for
adoptability is NOT "more regex"; it is (1) better LLM instructions (generalizing
the VAR-arithmetic rule fixes Bug A across the whole 30% VAR family, not just
CCHBC) and (2) a **regression harness** that scores each construct bucket against
the tenant corpus so coverage is a measured number per release, not an anecdote.

**~85% of measures by volume are genuinely handled today** (the high-frequency
CALCULATE/DIVIDE/FILTER/SUMX/IF/COUNTROWS/DISTINCTCOUNT constructs). The gaps are
concentrated, not diffuse.

---

## 2. CCHBC comparison result (the quality baseline)

226 comparable emitted measures (of 256 emitted; 30 had no full DAX to diff):

| Bucket | Count | Meaning |
|--------|------:|---------|
| BASE | 114 | `SUM(source.col)` — faithful by construction |
| FAITHFUL | 66 | filtered aggregates; filter literal-sets match DAX |
| RATIO_OK | 20 | ratios whose num/den differ, as the DAX does |
| SUSPECT (triaged) | 17 → **8 real bugs** | see §3; 9 were false positives (inherited `bic_creg_type='Plant'` from a base measure) |
| NO_DAX | 9 | hidden weighted-avg ratios / raw `CASE WHEN` from another path |

**~218 / 226 (~96%) faithful or correct.** Strong — but the 8 failures cluster
into **two root-cause bug classes**, not noise.

---

## 3. The two confirmed bug classes (fix these first)

### Bug A — multi-block arithmetic collapse (`a − b`, `a + b` outside DIVIDE)

**Symptom:** DAX binds ≥2 `CALCULATE`/`SUMX` blocks to vars and the `RETURN`
combines them with `+`/`-` (no `DIVIDE`). The transpiler emits **only the first
term**.

CCHBC casualties (6): `cost_to_supply_actuals`, `cost_to_supply_bp`,
`cost_to_supply_re` (each `a - b`, dropped the `-b` on `{DHHX}`), `total_nsr_actuals`,
`total_nsr_bp`, `total_nsr_re` (each `a + b`, dropped the `+b`).

**Why it slips through:**
- Skill corpus `PATTERNS.md` §2b teaches var-chain inlining **only inside a
  `DIVIDE`**. A plain `return a - b` has *no* taught pattern, so the LLM falls
  back to emitting the leading filtered SUM.
- The guard `detect_lost_dax_component` has checks for prior-year, DIVIDE-
  denominator, and exclusion — **but no check for dropped additive/subtractive
  terms outside a DIVIDE.** So the collapse ships silently.

**Tenant blast radius:** `VAR + ≥2 CALCULATE + a±b arithmetic` ≈ **10,955 (1.9%)**;
`return combines vars with +/- outside DIVIDE` ≈ **9,658 (1.6%)**. ~2% of the
planet, and every one is a silently-wrong number.

**Fix:**
1. **Skill corpus** — generalize `PATTERNS.md` §2b from "var-chain DIVIDE" to
   "var-chain arithmetic": the same inline rule applies to `return a - b`,
   `a + b`, `(a+b)/c`, etc. Add a worked `a - b` (no DIVIDE) example.
2. **Guard** — add a check to `detect_lost_dax_component`: if the DAX `RETURN`
   combines ≥2 aggregate blocks with `+`/`-` and the SQL has a single aggregate
   term (one `FILTER`, no matching `+`/`-`), flag `additive term dropped`. This
   demotes to untranslatable-with-TODO instead of shipping a wrong number.

### Bug B — measure-ref ratio num/denom filter-set loss

**Symptom:** `DIVIDE(CALCULATE([BaseMeasure], <pred_num>), CALCULATE([BaseMeasure], <pred_den>))`
where num and denom filter the SAME base measure by DIFFERENT predicates. The
transpiler emitted **num == denom** (both filters identical) → ratio always 1.0.

CCHBC casualty (1 clear): `consumer_complaints_actual` — DAX divides
`bic_csubkbi IN {KEMAA0011,041720560}` by `{KEMAA0012,041720579}`; both code
sets were lost and replaced with the base measure's `bic_chversion='0000' AND
bic_creg_type='Plant'` on both sides.

**Why it slips through:** the resolver expands `[BaseMeasure]` to its own
filters but drops the per-side `CALCULATE` override predicates. The guard's
DIVIDE check only fires when the denominator is *entirely* absent, not when it's
present-but-wrong (num==denom).

**Tenant blast radius:** `DIVIDE(CALCULATE([Measure]…),CALCULATE([Measure]…))` ≈
**6,017 (1.0%)**. Not all lose their filters, but this is the population at risk.

**Fix:**
1. **Resolver** — when expanding a measure-ref inside `CALCULATE`, the per-call
   filter predicate must be AND-ed onto the base measure's filters *per side*,
   not discarded.
2. **Guard** — add a `num==denom` structural check: if a ratio's numerator and
   denominator normalize to identical text, flag `ratio collapses to 1.0`.
3. **Validator** — the expression-validator (separate stage) should also catch
   num==denom, since this produces well-formed SQL that passes a structural
   compare. (See `expression_validator.py`.)

---

## 4. Coverage opportunities beyond the two bugs (generalization)

Ranked by tenant frequency × feasibility in a static metric view:

| Rank | Pattern | Tenant share | Current handling | Recommendation |
|------|---------|-------------:|------------------|----------------|
| 1 | **ALL() / ALLSELECTED share-of-total** | 12.8% (74,876) | Skill corpus §4 teaches the `window: range: all` pattern, but it's LLM-only and unverified at scale | Add fast-path/verified handler + regression corpus; this is the single biggest *translatable* population we don't systematically cover |
| 2 | **≥2 nested CALCULATE (multi-block)** | 14.4% (84,255) | Partially — the `a±b` subset is Bug A; the rest (nested filter contexts) is LLM best-effort | Once Bug A is fixed, sample this bucket to find the next sub-pattern |
| 3 | **DISTINCTCOUNT** | 4.3% (25,440) | `distinctcountnoblank` fast-path + `COUNT(DISTINCT)` in corpus | Verify the `noblank` variant against tenant samples; likely mostly covered |
| 4 | **Time intelligence (PY/YTD/DATEADD)** | 2.9% (17,023) | Correctly categorized as untranslatable (needs a calendar/`_py` column) | Keep skipping, but improve the emitted TODO: tell the user *exactly* what calendar column to add. Consider a `window`-based YTD where a date dim exists |
| 5 | **SWITCH + SELECTEDVALUE slicer dispatch** | 9.8% (57,191) | Skip-by-design (display layer) — decompose to per-branch measures | Correct. Ensure the decomposition (corpus §5/§6) actually emits the per-branch measures rather than dropping the whole thing |
| 6 | **USERELATIONSHIP** | 1.0% (5,899) | Deliberately LLM+join-context (excluded from fast-path) | Leave as-is; low volume |
| 7 | Long tail: RANKX, TOPN, SUMMARIZE, TREATAS, LOOKUPVALUE, EARLIER | each <5% | Mostly untranslatable | Categorize honestly in the "not emitted" block; don't over-invest |

---

## 5. Proposed sequence

1. **Bug A fix** (skill corpus §2b generalization + guard additive-drop check) —
   ~2% of tenant, all silently wrong. Highest correctness impact.
2. **Bug B fix** (resolver per-side filter AND + guard num==denom + validator
   num==denom) — ~1% of tenant, silently wrong, and closes a validator blind
   spot.
3. **Build a tenant regression harness** — sample N measures per construct
   bucket from the corpus, translate, and score with the same literal/structure
   diff used in the CCHBC comparison (`/tmp/ucmv_full_compare.py` is the
   prototype). This makes "coverage" measurable per release instead of anecdotal.
4. **ALL()/ALLSELECTED verified handler** — the biggest translatable population
   (12.8%) we don't systematically cover.
5. Iterate down the §4 table, re-scoring against the harness each time.

## 6. Honest caveats

- The 96% CCHBC number is **structural fidelity**, not semantic proof. A
  wrong-operator translation that preserves all literals would pass it.
- Tenant blast-radius counts are regex fingerprints; treat them as
  one-significant-figure sizing, not exact classification.
- The tenant export mixes hidden/helper measures (23,589 hidden) and display
  artifacts; a real coverage % should exclude the skip-by-design population
  (SWITCH/SELECTEDVALUE, FORMAT, slicer scalars) from the denominator.
