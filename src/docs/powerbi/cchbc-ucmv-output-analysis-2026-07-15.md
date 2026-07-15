# CCHBC UCMV Output Analysis — post-fix run (2026-07-15)

> ## ⚠️ SUPERSEDED — read this first (update 2026-07-15, later run)
>
> **The root cause below ("~90% upstream extraction gap / needs CCHBC domain
> input") was WRONG. The real cause was a missing `report_id`.**
>
> The run analysed in the body of this doc was executed **without a `report_id`**.
> Without it, config-gen falls back to a degraded measure source that returns
> **bare column names** (`bic_csubkbi`) instead of the measure bodies — which is
> why so many measures looked like un-translatable "bare columns."
>
> Re-running **with `report_id` set** (Pipeline Config Generator → "Optional:
> Report Metadata" accordion; for the local test model the report is
> `SC - Total Supply Chain No RLS`) changed everything, verified from the run's
> own `measures_with_dax` + `resolved_measures_by_table` artifact:
>
> | metric | without report_id | **with report_id** |
> |---|---|---|
> | measures with real-formula DAX | ~93 / 470 | **454 / 470** |
> | DAX-translated (26 views) | 82 | **228** |
> | real-aggregate measures emitted | ~16 | **~298** |
> | untranslatable | 109 | **32** |
>
> Crucially, KPIs I had labelled **"needs CCHBC domain input, unfixable by
> tooling" — the `fis_code` cost-code lists, KBI codes — were in the DAX all
> along.** Example now generated correctly (matches ground truth):
> `supply_chain_cost_excl_env_fees_taxes_bpc = SUM(kbi_value) FILTER (WHERE
> bic_chversion='0000' AND fis_code IN ('DCC3','DCC1','DCCE',...))`.
> `FT_BPC003` went from 1 measure → **61 correct filtered KPIs**.
>
> **Action items from this correction:**
> 1. `report_id` must be standard/required in the flow — running without it
>    silently halves output quality (it's currently an optional hidden accordion).
> 2. Residual real bugs (small, well-defined) in the good run: ~10 malformed
>    (`/ NULLIF(, 0)` empty ratio on pe002 CIP/EPL/OPL; leaked cross-table DAX
>    ref `EDGE_Measure[...]` on QSE DPMO), plus 43 TODO (mostly the
>    `F_Start_date`/`F_End_date` date-window family + prior-year + distinctcount).
> 3. The per-measure semantic-correctness verdict (good run vs ground truth) is
>    appended at the bottom of this doc under "Good-run verification".
>
> Everything BELOW this box describes the earlier (no-report_id) run and is kept
> for history — treat its "upstream/unfixable" conclusions as OUTDATED.

---

_Detailed cross-check of the new `~/Downloads/export/` UCMVs (26 files) against the
original 470 PBI measures (`original_dax_measures.json`) and the human ground-truth
(`tsc_ucm (1)/`, 16 comparable tables). Four parallel reviewers, one per table
cluster, then consolidated. Verdict-oriented: what works, what's a tool bug, what's
upstream, what needs CCHBC input._

## TL;DR

The P0 re-homing fix **worked** — measures now reach the fact tables (before: 11/12
facts had 0 complex measures; now measures flow through everywhere). **But** the
output is **not yet production-quality**, for one dominant reason that is **not** a
transpiler bug:

> **~90% of the measure gap is an UPSTREAM PBI-extraction problem.** For **374 of
> 470** measures, the extracted `dax_expression` is a **single bare column name**
> (`bic_chversion`, `bic_csubkbi`, `atp_a_phc`, `matl_group`, …) instead of the real
> DAX formula. The transpiler faithfully translated what it was given — a bare
> column → `SUM(col)` or a passthrough — because the ratio/FILTER/exclusion logic
> **never reached it**. The real formulas exist only in the human ground truth.

On top of that upstream gap sit a **handful of genuine tool bugs** (some fixed this
session, some still open) and a set of **structural join defects** that would stop
views running even where measures are correct.

**Bottom line for the customer:** this is a real step forward (measures now route
correctly), but do **not** present it as "done". The headline blocker is the DAX
extraction, not translation. See §5 for the go-forward split.

## 1. Headline numbers

| | old broken run | this run |
|---|---|---|
| Facts with complex measures | 1/12 | most tables |
| Total base measures | ~143 | 143 |
| Total "DAX-translated" | ~few | 82 |
| Total untranslatable (documented) | ~0 on failing tables | 109 |

The jump in *untranslatable* from ~0 to 109 is itself proof P0 worked: measures now
**arrive** at the facts (before, they were silently absent). Of the 82
"DAX-translated": **~16 valid aggregates, ~48 bare-column passthroughs, ~18
MEASURE() refs**. Of those, only a small number are semantically correct vs GT.

## 2. Root-cause classification (the deliverable)

Every failing measure was bucketed. The proportions are strikingly consistent
across all four clusters:

### A. UPSTREAM extraction gap — **dominant (~90%)**, NOT our bug
The `dax_expression` fed to the pipeline is a bare column token, not a formula.
Examples (generated → what GT proves it should be):
- QSE `consumer_complaints_actual`: `source.bic_csubkbi` → GT:
  `SUM(kbi_value) FILTER (WHERE bic_chversion='0000' AND bic_creg_type='Plant' AND bic_csubkbi IN ('KEMAA0011','041720560')) / NULLIF(…,0)`
- HR_A `sc_fte_bp`: absent → GT:
  `SUM(COALESCE(khr004001_fte_total,0)) FILTER (WHERE bic_chversion='B000' AND func_area IN ('Z320','Z110','Z120','Z310'))`
- iom05 `cfr_atpa_actual`: `SUM(source.atp_a_phc)` (numerator only) → GT:
  `SUM(atp_a_phc) FILTER (comp_code<>'0307') / NULLIF(SUM(ordered_phc) FILTER(...),0)` with `×-1` and `format: percentage`.

**No transpiler change fixes these** — the input was already degraded. The fix is in
the PBI **measure-extraction** step (Execute Queries / Admin Scan / TMDL) that
populates `dax_expression`. **This is the #1 thing to investigate next.**

> ⚠️ **Caveat — the `original_dax_measures.json` I compared against is from Jul 1
> and may be stale.** Today's export shows *richer* DAX in at least one place
> (`C_Banner` f_start_date has the full `SELECTEDVALUE…SWITCH` body today vs just
> `Index` in the Jul-1 file). So I cannot 100% confirm the *current* extraction is
> still returning bare columns for the fact-table measures. **This is the single
> most important thing to confirm** — see §4 (need the current run's `measures_json`
> / `conversion_history`).

### B. GENUINE TOOL BUGS
Fixed this session (with tests):
- **P3 scan-SQL param leak** — `& CurrencyFilter &` / `& RE_Version &` survived into
  `source:` SQL on 7 tables (CO012, Fxn, BPC003). The resolver ran but didn't *flag*
  survivors in the scan-data path. Now flagged as TODO. *(Note: it flags, doesn't
  invent the value — that's CCHBC input.)*
- **Dimension/measure name collision** — `kbi_value`, `ebit`, `total_taxesfees_in_nsr`,
  `error2`, `error_uc` emitted as BOTH a dimension and a measure (invalid UCMV) in 4
  files. Now the colliding dimension is dropped (measure wins).
- **`#(lf)` / `#(tab)` / doubled-quote M-token leak** — Power Query escape tokens
  leaked verbatim into `source:` SQL and a dimension name/expr (C_Dim_Calendar_daily).
  Now normalized at MQuery parse time.

Still OPEN (found in this analysis, not yet fixed):
- **Phantom `dim_calendar` join on `source.date_key` — affects 18 files.** The source
  projects `fiscper`, not `date_key`; GT joins on `fiscper`. The calendar join
  silently matches nothing. **Highest-impact open structural bug.** Origin: config
  `join_key_map` derived from a PBI relationship whose key column the aggregated
  source doesn't carry.
- **Phantom `dim_wkctr` join on `source.plant_workcenter_key`** (scorecard, pe005) —
  source has `plant`+`workcenter` separately; GT joins on `CONCAT(plant,'/',workcenter)`.
- **Bare column emitted AS a measure** (`expr: source.bic_chversion`) instead of
  routing to untranslatable — iom06_detailed (3), CO012 (2), HR_B (2). A measure expr
  must aggregate; a raw column is invalid. Inconsistent with iom05 which wraps in SUM.
- **Measures referencing columns not in the emitted source** — commented-out
  (`total_depreciation`, `total_taxesfees_in_nsr` in CO012_Actuals) or aggregated-away
  (`fis_code`, `kbi_value` in PC_RE_All). Will fail at view creation.
- **Leaked PBI GUID auto-date-table joins** (`local_date_table_<guid>`) emitted as
  real dimensions (Fxn_REBP_extended, PC_RE_All, C_Dim_Calendar_daily).
- **Unaliased aggregate** `SUM(gross_profit_total),` (no `AS`) in CO012_REBP source.
- **`SUM()` over percentage columns** (pe009 `*_pct_hidden`) — summing a percentage
  is meaningless; GT only uses them inside weighted ratios.
- **Wrong-column value**: pe002 `opl_actual` = `MEASURE(epl)` (should be `opl`) — but
  the wrong column was fed upstream, so borderline upstream.
- **Junk/test measure shipped**: iom06_detailed `ot_actual_test`.
- **Stylistic incoherence**: identical bare inputs randomly become `SUM(source.x)` vs
  `MEASURE(x)`.

### C. ALLOCATION over-eager (a P0 side-effect to tighten)
The 5 `C_Dim_*` / `C_Banner` **dimension** tables should **not** be metric views at
all (0/5 legitimate). Bare-column "measures" and scalar slicer calcs
(`SELECTEDVALUE…SWITCH` date pickers) were routed onto dimension tables. P0 correctly
re-homes measures to referenced facts, but it (and the upstream allocator) also let
non-aggregatable columns/scalars land on dim tables. **Guard needed:** don't emit a
UCMV for a table whose only "measures" are bare columns / non-aggregates.

### D. NEEDS CCHBC DOMAIN INPUT — unfixable by tooling
Even with perfect extraction, the FILTER *contents* are business definitions:
- KBI-code IN-lists (`KEMAA0011` = consumer complaints, `KIOM006xx`, `KPE00xxx`)
- `fis_code` cost-code lists (Sugar=`DCAD`, 25-code input-cost list, …)
- `func_area` SC membership (`Z110/Z120/Z310/Z320`)
- `bic_cwc_type` line-type allow-lists; version codes `0000`/`B000`/`RE`
- comp_code exclusions (`0403/0550/0307`), plant exclusions, PY semantics
- the plant-vs-company split (GT splits one PBI measure into 2+ variants)

## 3. Structural sweep (all 26 files)

| Check | Result |
|---|---|
| `{catalog}`/`{schema}` placeholders | ✅ NONE (P1 holds) |
| `& Param &` tokens | ⚠️ 7 files (P3 now flags them; value is CCHBC input) |
| `/ NULLIF(1,0)` no-ops | ✅ NONE (P5 holds) |
| duplicate dimension names | ✅ NONE (P2 holds) |
| duplicate join names | ✅ NONE (P2 holds) |
| dim/measure name collision | ⚠️ 4 files (FIXED this session) |
| `#(lf)` M-token leak | ⚠️ 1 file (FIXED this session) |
| phantom `date_key` calendar join | ❌ 18 files (OPEN — highest impact) |
| base measures COALESCE-wrapped | ✅ yes (P5 holds) |

## 4. What we need from the customer to finish the diagnosis

1. **The CURRENT run's `measures_json` / config-gen output** (or the
   `conversion_history` row, `source_format=powerbi_config`, from today's run). This
   settles the one open question: is today's extraction STILL returning bare-column
   DAX for the fact measures, or was that a stale-file artifact? Everything downstream
   hinges on it. Not a new run — just the stored extraction record.
2. **Domain code lists** (§2.D) — needed for the FILTER contents regardless of tool
   fixes. These gate numerical correctness.
3. Confirmation of the real target catalog/schema (this run mixed
   `david_test_metrics.test_schema`, `udm_datamart_*`, and `main.default` — looks
   like a test deploy target).

## 5. Recommendation

**Not done — but the right kind of not-done.** The architecture is working (measures
route to facts, base measures are clean and correct, structural hygiene is largely
solid). The blocker is data quality upstream of the transpiler, plus a short list of
mechanical join/packaging bugs.

Suggested order:
1. **Confirm the extraction question (§4.1)** — this reclassifies most of the 90%.
   If today's extraction IS still bare-column, fixing the PBI measure extraction is
   the single highest-leverage next step and dwarfs everything else.
2. **Fix the open structural bugs (§2.B):** phantom `date_key`/`wkctr` joins
   (18 files), bare-column-as-measure routing, source-column binding, GUID date-table
   join drop, dim-table UCMV guard. These are mechanical and unit-testable.
3. **Hand CCHBC the domain-input list (§2.D)** — parallel track; they supply code
   lists, we wire them into config.
4. Only after 1–3 is a re-run worth tie-out validation.

**Do not tell the customer "it works" yet.** Tell them: measure routing is fixed and
verified, base measures are correct, and we've found the real remaining blocker is
the DAX extraction fidelity — for which we need their current-run extraction record
to confirm and then fix.

## Fixes landed this session (all with tests, suite green)
P3 scan-path flag · dim/measure collision drop · `#(lf)`/doubled-quote normalization.
(These are in addition to P0–P6 from the prior session.)

## Still-open tool bugs (prioritised)
1. Phantom `date_key` calendar join (18 files) — remap to the period dim the source
   projects (`fiscper`) when `date_key` is absent.
2. Bare-column-as-measure → route to untranslatable instead of emitting invalid expr.
3. Measure exprs must bind to columns present in the emitted source (drop refs to
   commented-out / aggregated-away columns).
4. Drop leaked `local_date_table_<guid>` joins.
5. Dimension-table UCMV guard (don't emit a view whose only measures are bare cols).
6. `dim_wkctr` CONCAT key; unaliased-aggregate guard; percentage-column SUM guard;
   drop `*_test` junk measures.

---

# Good-run verification (report_id set) — per-measure correctness

_Added 2026-07-15. Two parallel reviewers compared the good run's generated SQL
against the original PBI DAX (now full, via report_id) and the human ground truth.
265 measures reviewed across 14 fact tables._

## Combined verdict

| Verdict | Count | % |
|---|---|---|
| CORRECT | 188 | 71% |
| CLOSE (minor: NULLIF/COALESCE/×100/sign) | 8 | 3% |
| WRONG | 37 | 14% |
| TODO / untranslatable | 32 | 12% |

**Trust is bimodal and the boundary is crisp:**
- ✅ **Self-contained `SUM(FILTER(version + code-list))` measures translate faithfully.**
  Every base additive measure (~90 `SUM(COALESCE(col,0))`) is perfect, and the
  filtered cost KPIs reproduce their `fis_code` / `func_area` / KBI code-lists
  **exactly** vs ground truth (FT_BPC003 core, both losses tables, CO012_REBP,
  iom05 base, pe004). This is the big win the report_id unlocked.
- ❌ **Anything requiring cross-measure resolution, prior-year, percentages, or a
  compound DIVIDE denominator is unreliable** (~45% failure rate on non-trivial
  derived measures).

Per-table: iom05 STRONG, iom06 STRONG(shallow), pe004 STRONG, CO012_REBP STRONG,
losses/losses_RE STRONG, BPC003 MIXED(strong core), CO012_Actuals MIXED, HR_A MIXED,
HR_B MIXED, pe002 MIXED, pe005 WEAK, scorecard_BP WEAK, FT_QSE WEAK.

## Residual defects — ranked by blast radius (all fixable, well-defined)

1. **Unresolved DAX measure-to-measure references → `TODO: fill SQL expression`
   (30 measures — the single highest-value fix).** The engine doesn't resolve
   `[MeasureName]` refs to other measures. Two culprits dominate:
   `[Plant_Comp KBI_Value_Actual]/[_BP]` (23 QSE measures) and
   `[AVG_KBI_Div_Factor]` (6 HR_A + 1 HR_B). GT shows both are trivially
   resolvable — the KBI base is `SUM(kbi_value) FILTER (version + creg_type)`, and
   the AVG factor is the constant `1` (`/ NULLIF(1,0)`). Fixing these two moves
   FT_QSE from WEAK to strong and clears most of the 32 TODOs.
2. **Prior-year (`SAMEPERIODLASTYEAR`) silently dropped (16 measures WRONG).** PY
   measures emit SQL byte-identical to their current-period sibling — a dangerous
   *silent* error (runs, wrong number). BPC003 (13) + losses (3). Must either model
   the time-shift or emit a TODO instead of a wrong value.
3. **Dropped ratio denominator → silent bare numerator (pe005 all 7 yields, +others).**
   `DIVIDE(a, b-c)` emits only numerator `a`; the `(issued-received)` denominator +
   plant/comp exclusions vanish. Most dangerous class — valid SQL, wrong number.
4. **Dangling DAX var tokens / empty `/ NULLIF(, 0)` (pe002 CIP/EPL/OPL, scorecard,
   QSE).** Multi-var DAX whose RETURN combines vars (`a+a1`, `res1+…`) emits bare
   `a`/`b` as undefined identifiers, or collapses to an empty ratio — invalid SQL.
5. **UNRESOLVED `[val]` + scrambled DIVIDE→NULLIF arg-mapping (all 4 scorecard_BP
   ratios).** Quoted-table+bracket refs (`'fact_x'[val]`) not recognized; raw
   SUMX/FILTER left in place; DIVIDE's 3 args mis-slotted into NULLIF.
6. **Malformed 3-arg NULLIF (QSE ratios).** DAX `DIVIDE(n,d,0)`'s alternate-result
   3rd arg leaks in as `NULLIF(d, 0, 0)` — invalid SQL. The alt_result must be
   consumed, not forwarded to NULLIF.
7. **Missing ×100 on percentage measures (HR turnover/absence-rate, 3 CLOSE).** GT
   multiplies by 100 + sets `format: percentage`; both omitted.
8. **View-level exclusion filters not emitted.** The comp_code exclusions
   (`0403/0550/0307`) that GT applies are ABSENT from generated iom05 (no `filter:`
   block at all). CFR ratios therefore include companies GT excludes → wrong totals.
   (Reviewer initially over-credited these as correct; verified false.)
9. **Minor:** DAX comment leakage (`-- Denominator` into SQL), `BLANK()` emitted
   literally, missing COALESCE guards on subtractions, cross-fact refs to un-joined
   tables (HR_A → CO012), dropped subtraction terms (BPC003 Cost-to-Supply PY).

## Bottom line
The report_id fix took the tool from "mostly broken" to **"~71% correct, with a
sharply-defined ~30% residual of well-understood, fixable defects."** The residual
is NOT domain-knowledge gaps (those turned out to be in the DAX) — it's a short list
of transpiler features: measure-ref resolution (#1), prior-year (#2), compound-DIVIDE
denominators (#3-4), and a few SQL-syntax bugs (#5-6). #1 alone is the highest
leverage. This is a tractable engineering backlog, not a fundamental limitation.
