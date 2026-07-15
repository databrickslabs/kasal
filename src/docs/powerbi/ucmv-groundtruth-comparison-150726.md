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

**One anomaly:** `fact_iom35` is *inverted* — 159 Kasal measures vs 22 GT, only 3
name-matched. Kasal massively over-generated here (or GT is a curated subset).
Not a coverage gap — a precision/naming question worth a look (are the 159 valid,
or is it emitting noise?).

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

## Next actions (by leverage)

1. **Diagnose the handoff per table** (P0 from the prior plan, still the top item):
   for `ft_qse`, `ft_bpc003_losses`, `fact_sc`, count how many measures reached
   the generator carrying DAX. Low name-match + missing source-filter measures ⇒
   the DAX didn't arrive, so no transpiler fix would recover them.
2. **MEASURE()-composition** — teach/verify the ratio-composition layer
   (`ft_bpc003` ×99 alone would move the total materially).
3. **Join-alias FILTER support** — `fact_pe002`'s 23 misses; a bounded transpiler
   gap (filters on join columns, not just `source.`).
4. **Numerical tie-out** — once coverage is closed, execute against the warehouse;
   still gated by the customer-input source-table mapping.
