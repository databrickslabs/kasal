# UCMV Ground-Truth Comparison — Kasal run 150726

**Date:** 2026-07-15 · **Method:** coverage + spot-check (structural, not executed)

Compares the Kasal output `ucmvs_150726/` against the customer's 1:1-with-PowerBI
ground truth (`tsc_ucm (1)/tsc_ucm/`). Supersedes the 2026-07-14 gap analysis,
which ran against the earlier `ucmvs_from_kasal/` output.

> **Structural, not numerical.** YAML was diffed (measure presence + shape); views
> were not executed, so numerical tie-out is not verified. Source-table mismatch
> (a customer-input item) gates tie-out regardless.

## Headline — two clear results

1. **The all-or-nothing collapse is gone, and the structural/hygiene failures are
   fixed.** The prior run dropped *all* complex KPIs on 11/12 tables (base
   measures only) and shipped non-runnable YAML (`{catalog}`/`{schema}`
   placeholders, leaked PBI filter-params `& RE_Version &`, duplicate dimensions).
   **None of those recur** across the 13 comparable tables (one lone
   `local_date_table` leak). The tables that produced *zero* complex measures now
   produce many: `ft_bpc003` 1→45, `ft_qse` 1→14, `ft_bpc003_losses` 1→7.

2. **Complex-measure coverage is real but partial (~70% of GT measure count), and
   the misses are systematic, not random** — they cluster into three named
   causes (below), which makes them fixable rather than mysterious.

## Coverage table (13 comparable tables)

`nm✓` = GT measures matched by name in Kasal · `GTc`/`KSc` = complex (non-base)
measures each side · miss-cause = shape of the GT complex measures Kasal omitted.

| Table | GT | Kasal | nm✓ | GT complex | KS complex | Dominant miss-cause |
|-------|---:|------:|----:|-----------:|-----------:|---------------------|
| fact_hr_a | 17 | 11 | 11 | 10 | 4 | src-filter ×6 |
| fact_iom05 | 32 | 26 | 25 | 7 | 1 | src-filter ×7 |
| fact_iom06 | 22 | 8 | 8 | 10 | 0 | MEASURE-ratio ×7 |
| fact_iom35 | 22 | 159 | 3 | 22 | 67 | **over-generation** (159≫22) |
| fact_pe002 | 44 | 21 | 21 | 23 | 0 | join-alias-filter ×23 |
| fact_pe004 | 16 | 11 | 11 | 7 | 0 | mixed |
| fact_pe005 | 12 | 5 | 5 | 7 | 0 | src-filter ×7 |
| fact_sc | 11 | 1 | 0 | 10 | 0 | src-filter ×10 (table barely translated) |
| fact_scorecard_bp_wc | 3 | 1 | 1 | 2 | 0 | src-filter ×2 |
| ft_bpc003 | 151 | 45 | 31 | 150 | 44 | MEASURE-ratio ×99 |
| ft_bpc003_losses | 57 | 7 | 6 | 56 | 6 | src-filter ×49 |
| ft_hr_b | 7 | 5 | 5 | 6 | 5 | src-filter ×1 |
| ft_qse | 51 | 14 | 3 | 50 | 13 | src-filter ×48 |
| **Total** | **445** | **314** | — | — | — | |

## The three miss-causes (systematic, fixable)

1. **Source-column FILTER measures dropped** — the largest bucket. GT measures
   like `SUM(source.value) FILTER (WHERE bic_chversion='0000')` or
   `… bic_creg_type='Plant'` — a plain source-column predicate — are missing on
   `ft_qse` (×48), `ft_bpc003_losses` (×49), `fact_sc` (×10), `fact_iom05` (×7),
   `fact_pe005` (×7). These are *exactly* the shape the transpiler handles
   elsewhere (and Bug B / share-of-total fixes target). The gap here is that many
   of these measures **never reached the generator with their DAX** (name-match is
   also low on ft_qse: 3/51) — pointing back at the config-gen → generator handoff
   for these tables, not the transpiler.

2. **`MEASURE()`-composed ratios not emitted** — `MEASURE(a)/NULLIF(MEASURE(b),0)`
   composite KPIs where the *base* measures exist but the ratio wasn't composed:
   `ft_bpc003` (×99!), `fact_iom06` (×7). The base measures translate; the
   composition layer on top is missing.

3. **Join-alias FILTER measures dropped** — `fact_pe002`'s 23 misses all filter on
   a *joined dimension* column (`FILTER (WHERE dim_wkctr.bic_cwc_type IN (...))`).
   Kasal emitted the 21 base measures but every measure whose filter references a
   join alias was dropped. Distinct from cause #1 (source-column filters).

**Anomaly resolved — `fact_iom35` over-generation is NOT noise.** 159 Kasal vs 22
GT, but investigated: no duplicate names, only 20 same-body redundancies. The 159
= **92 base measures** (every aggregatable source column — uncurated dumping, vs
GT's curated 22) + **67 complex**, and the complex set is a **variant
cross-product**: `_py` ×22, `_pct` ×21, `_pct_py` ×20 (each KPI emitted in
base/PY/%/%+PY forms). The strict "3 name-matched" was a **naming artifact** —
GT `case_fill_rate_pct_new` vs Kasal `kbi_case_fill_rate_pct`. On a fuzzy
KPI-stem match, **16 of 21 GT KPIs are present** under different names; the 5
missing (`OTIF %`, `OT %`, `DIFOT+ %`, on-time/total deliveries) are the same
MEASURE()-composition gap. So iom35 is the **inverse failure mode**: over-broad
(correct KPIs buried in ~130 extra variants + raw columns), not under-emitting.
Fix is **curation** (emit the used variants, not the full cross-product) —
distinct from the coverage fixes below, and lower priority (correct-but-noisy
beats missing).

## Assessment

- **This is a large, genuine improvement** over the "devastating" prior run. The
  output is now *runnable* (hygiene fixed) and the collapse is gone.
- **But it is not yet at parity.** ~70% of GT measures by count; the complex-KPI
  layer — the reason the dashboards exist — is still partially dropped on most
  tables. Do not represent this as "matches PowerBI."
- **The misses are systematic and point mostly upstream of the transpiler:** the
  dominant cause (source-column FILTER measures missing, with low name-match on
  the worst tables) looks like the **config-gen → generator handoff** not
  delivering those measures' DAX per table — the same handoff path the prior fix
  plan's P0 flagged. The transpiler-side causes (#2 MEASURE-composition, #3
  join-alias filters) are narrower and cleanly fixable.

## Handoff diagnostic (run 47916 = source of this export) — RESULT

Queried the run's `execution_trace`: per fact table, measures received vs. those
carrying full DAX, vs. those emitted.

- **The DAX arrived — the handoff is NOT the bottleneck.** Every table received
  its measures with full DAX (FT_QSE 28/28, FT_BPC003 60/60, ft_bpc003_losses
  9/9). The prior run's root cause (DAX never reached the generator) is **fixed**.
- **The gap moved downstream.** FT_QSE received 28 → emitted 13; FT_BPC003
  60 → 48. The dropped measures are lost in translation/emission, not handoff.
- **Every dropped measure references another `[measure]`** (FT_BPC003 13/13,
  ft_bpc003_losses 3/3, FT_QSE 15/15). Two sub-causes:
  1. **Prior-Year time-intelligence (largest bucket):** `*_PY` / `*_BP`-on-PY
     measures built on `CALCULATE([PY_Start_date])` / `[PY_End_date]` date-window
     scaffolding. Period-shift isn't expressible without a calendar `date_py`
     column → correctly skipped, but it's a large share of the miss count. Needs
     the calendar-column workaround (already documented in `UNSUPPORTED.md`),
     which is partly a **[customer-input]** item (supply/confirm the PY calendar).
  2. **Dependency cascade:** a measure whose referenced base measure was itself
     dropped (e.g. every FT_QSE `*_BP` composes on `[Plant_Comp KBI_Value_BP]`, a
     `SWITCH(TRUE())` display-layer measure that's correctly skipped — so all 12
     `_BP` dependents cascade out). The `_Actual` twins survived because their
     base resolved.
- **Separate upstream item:** FT_QSE has 51 measures in GT but the generator
  only *received* 28 — ~23 GT measures never entered the pipeline (extraction /
  config-gen selected a subset). That is upstream of the generator, distinct from
  both the handoff and the transpiler.

## Next actions (by leverage) — REVISED after the diagnostic

1. **Prior-Year time-intelligence** (largest single bucket, spans FT_BPC003,
   ft_bpc003_losses, and the `_BP`/`_PY` families). Two parts: (a) implement the
   calendar-`date_py` self-join / window translation the corpus already
   documents; (b) flag the calendar-column dependency as a customer input.
2. **Dependency-cascade handling.** When a referenced base measure is dropped
   (display-layer `SWITCH`, or unresolved), decide per case: resolve the base
   (e.g. decompose `Plant_Comp KBI_Value_*` KBI-selectors) so dependents survive,
   or drop dependents with an explicit "base measure not emitted" reason instead
   of silently. Recovering `Plant_Comp KBI_Value_BP` alone recovers ~12 FT_QSE.
3. **MEASURE()-composition** — `ft_bpc003` ×99, `fact_iom06` ×7: base measures
   exist, the ratio-on-top isn't assembled.
4. **Join-alias FILTER support** — `fact_pe002`'s 23 (`FILTER (WHERE dim.col …)`).
5. **Extraction subset** (FT_QSE 51→28 received) — why fewer measures than GT
   entered the pipeline; upstream of the generator.
6. **Numerical tie-out** — after coverage; gated by the customer source-table map.

> **Correction to the pre-diagnostic plan:** the earlier top item ("diagnose the
> handoff — DAX may not be arriving") is **resolved** — the DAX arrives. The real
> top lever is Prior-Year time-intelligence + the dependency cascade, both
> downstream.

## SECOND diagnostic (deeper) — most of the gap is EXTRACTION, not translation

Fixing the dependency cascade (Fix #2, shipped) recovered the "received but
dropped" measures. But a per-measure received-vs-GT reconciliation shows that is
the *small* slice. The dominant gap is measures that **never reached the
generator at all** (verified with fuzzy KPI-stem matching, so it is not a naming
artifact):

| Table | GT total | never received | received (anywhere) |
|-------|---------:|---------------:|--------------------:|
| ft_qse | 51 | **48** | 3 |
| ft_bpc003 | 151 | **109** | 42 |
| ft_bpc003_losses | 57 | **52** | 5 |
| fact_pe002 | 44 | **37** | 7 |
| fact_sc | 11 | **11** | 0 |

The generator *did* receive measures for these tables (FT_QSE got 28 with full
DAX), but they are **largely different measures than GT's** — extraction/config-gen
pulled a smaller, different subset than the dashboard actually uses. `fact_iom06`
isn't even a distinct allocation; its composite ratios (`DIF %`, `OTD %`) never
arrived. `FT_BPC003` received 60 measures, **0** of them the `DIVIDE([measure])`
composites GT has.

**Reframing (corrects the priority order above):**
- **The dominant lever is EXTRACTION (was ranked #6, lowest).** ~90% of the gap
  is measures never extracted — no transpiler fix can recover them. This is the
  next thing to chase.
- The transpiler-side fixes (#1 PY time-intel, #3 MEASURE-composition, #4
  join-alias filter) only address the ~10% "received but dropped" slice. Still
  worth doing, but they are not what closes the coverage gap.
- Fix #2 (dependency cascade, shipped) was the largest *transpiler-side* win.

## THIRD diagnostic — the "extraction gap" is mostly SWITCH-decomposition, not missing measures

Traced FT_QSE end-to-end (config-gen 47859 → generator 47916). Config-gen
extracted **28** FT_QSE measures; only **3 of GT's 51 names** appear anywhere in
config-gen's full 470-measure set. But comparing the actual measures shows this
is **not** measures being dropped — it is a **modeling difference**:

- **Kasal extracted the PBI report's real measures** — e.g.
  `Plant_Comp KBI_Value_Actual`, a **single measure** whose body is
  `SWITCH(TRUE(), Or(ISFILTERED/HASONEVALUE plant), <plant branch>, <company branch>)`
  — one measure that dynamically returns the plant *or* company value by slicer
  context.
- **GT split each into two static measures** — `plant_kbi_value_actual` **and**
  `company_kbi_value_actual`. A UC metric view has no slicer context, so the human
  **materialized both branches of the SWITCH as separate measures.**

So the ~51-vs-28 gap is largely **one PBI SWITCH measure → two GT measures**
(plant + company), ×~28 QSE measures ≈ 51. Kasal has the *source* measures; it
just isn't **decomposing the plant/company SWITCH selector into its two branches**.

**This is a real, fixable Kasal capability** (SWITCH-geo-selector decomposition),
same family as the Fix #2 dependency work — NOT a customer-data problem and NOT
lost extraction. It reclassifies the bulk of the FT_QSE / QSE-family gap from
"[customer-input]" to **"[fix-in-kasal]: decompose the plant/company selector."**
Likely applies wherever a `Plant_Comp`/geo `SWITCH(ISFILTERED…)` selector exists.

> **Net:** the coverage gap is smaller than the raw counts implied — much of it is
> 1 source measure → 2 emitted measures, recoverable by decomposition rather than
> by re-extraction.

**Update — Fix #2 already recovers the plant branch.** Re-running the resolver
on FT_QSE's 28 extracted measures with the Fix #2 code: **all 4 base measure-refs
now resolve** (`Plant_Comp KBI_Value_Actual/BP`, `Total_KBI_ Actual/BP`) where
before they were 0 (all TODO). So on the next run the ~15 cascade-dropped QSE
dependents come back, resolved to the **plant branch** (the default GT itself
picks). That is the bulk of the QSE recovery — shipped.

The remaining piece is emitting the **company-branch variant** as a second
measure (`*_company_*`). That is a genuine but larger feature (it doubles measure
count for geo-selector measures and needs a naming/allocation convention) —
scoped as **follow-up**, not bundled into these fixes:
`derive_switch_decompositions` currently fires only on `SELECTEDVALUE`+SWITCH; it
would need a `SWITCH(TRUE(), Or(ISFILTERED|HASONEVALUE …), plantBranch,
companyBranch)` geo-selector case that emits both branches.
