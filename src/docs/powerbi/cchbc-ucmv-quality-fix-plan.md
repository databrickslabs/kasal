# CCHBC UCMV Quality — Concrete Fix & Action Plan

_Grounded in the code, not just the report. Companion to
`~/Downloads/CCHBC_UCMV_Quality_Report.docx` and its two markdown appendices
(`CCHBC_UCMV_gap_analysis.md`, `CCHBC_kasal_fix_plan.md`). Written 2026-07-15 on
`feat/pbi-ucmv-fixes-v2`._

## 1. What the report found (recap)

Kasal was run on the CCHBC PBI model with the pipeline config **unedited**, to
measure raw out-of-the-box quality. Result: base `SUM` measures + a plausible
join topology are emitted reliably, but **11 of 12 fact tables dropped nearly all
the business KPIs** (ratios, FILTER-measures, `MEASURE()`-composed waterfalls).
One table — `fact_iom35` — transpiled 128 complex DAX measures correctly.

## 2. Root cause of the headline gap — now confirmed from code + output

The report's P0 hypothesis ("the transpiler works; something upstream starves the
other tables") is **correct, and we can be more precise**. This is an
**allocation/handoff problem, not a transpiler problem.**

### The decisive evidence (from the actual generated YAML)

The per-table header counts (`N base / N DAX-translated / N untranslatable`) are:

| Table | base | dax | **untranslatable** |
|---|---|---|---|
| fact_iom35 | 50 | **128** | 3 |
| fact_iom36 | 4 | 6 | 0 |
| C_Banner | 0 | 4 | 2 |
| fact_pe005 | 4 | 1 | 0 |
| fact_pe002 | 21 | **0** | **0** |
| fact_iom05 | 25 | **0** | **0** |
| fact_hr_a | 7 | **0** | **0** |
| …every other fact | n | **0** | **0** |

The transpiler **always records a failure** as `untranslatable` (see
`table_processor.py:264-276` — every measure that reaches `translate()` lands in
either `translated` or `untranslatable`). The failing tables report
**`untranslatable = 0`**. That is only possible if **zero complex DAX measures
ever reached `process_table` for those tables** — they were never in the group.
If it were a transpiler failure, `untranslatable` would be non-zero (as on iom35 =
3, C_Banner = 2). It is not a gradient of translation quality; it is a gradient of
**how many measures were allocated to each fact table**.

### Why measures don't reach the fact table

The chain (verified in code):

1. `pipeline.py:189` → `measure_groups = self._group_by_table()`.
2. `artifact_cascade.py:group_by_table()` (line 117) buckets each measure by
   `all_allocations` if present, else by **`proposed_allocation`** (line 134),
   else `__unassigned__`.
3. `pipeline.py:206` pulls `measure_groups.get(table_key, [])` — a fact table only
   sees measures whose allocation names **that fact**.
4. In JSON/flow mode (the customer's path), `proposed_allocation` is set by
   config-gen's `_build_ucmv_measures()`
   (`pipeline_config_generator_tool.py:462-468`):
   ```python
   allocation = (m.get("proposed_allocation")
                 or m.get("table_name")   # ← PBI *holder* table, not the fact
                 or m.get("table")
                 or "__unassigned__")
   ```
   PBI measures carry `table_name` = the table the measure is **defined on**. In
   CCHBC's model that is overwhelmingly a **measure-holder table**
   (`C_Measure_Table_*`), not the fact — the same measure-table pattern already
   recorded for this customer (model `c915ac4f`: ~434/470 measures live on dummy
   `C_Measure_Table_CL/SL/PE/ET` holders; real data is in ~20 `FT_*`/`fact_*`
   tables). See `ucmv-flow-handoff-json-mode` memory.
5. Holder tables have no source SQL → `is_fact = False` → skipped
   (`pipeline.py:195`). Their measures never reach any fact's `process_table`.
   → **0 DAX measures in the fact's group → 0 translated, 0 untranslatable.**

**`fact_iom35` is the exception because its 128 measures are defined directly on
`iom35`** (`table_name == fact`), so `proposed_allocation` already points at the
fact. `fact_iom36`/`C_Banner`/`pe005` got their handful the same way. Everyone
else's KPIs are stranded on holder tables.

### The missing component: nobody re-homes holder-table measures to facts

`all_allocations` — the field that would place a measure onto **every fact it
references in its DAX** (`artifact_cascade.py:126`, "placed into every table they
reference") — is **read but never written** anywhere in the backend:

```
$ grep -rn "all_allocations" src/backend/src --include=*.py | grep -v test | grep -v ".get("
artifact_cascade.py:126:  # (docstring)          ← only a reader exists
```

Tool 87 (the measure-allocator, `tool-87-measure-allocator.md`) is documented to
do exactly this — parse `Table[Column]` DAX refs, match against known fact
tables, and emit `proposed_allocation`/allocations — but the **config-gen → UCMV
JSON handoff does not run an allocation pass**; it passes `table_name` straight
through as the allocation. So on any measure-holder-pattern model, complex
measures are silently orphaned.

**This single gap explains the 11/12 failure.** Fixing it is P0 and almost
certainly recovers the bulk of the missing measures — the transpiler is already
proven (iom35).

## 3. Do we need anything from the customer?

**For the P0 diagnosis: no.** The root cause is established from the emitted YAML
+ the code path. To *confirm* it on the exact CCHBC run before coding (30-min
sanity check, no customer action) we can self-serve from what's already persisted:

- `conversion_history` (`source_format = powerbi_config`) stores
  `input_data.measures` — every measure with its `expression` (DAX) and
  `table_name` (`pipeline_config_generator_tool.py:540`). Query it and **count
  measures-with-DAX grouped by `table_name`**. Expectation: the vast majority sit
  on `C_Measure_Table_*` holders; only iom35/iom36/pe005 have DAX on the fact
  itself. That is the proof. If we have read access to their Lakebase
  `conversion_history` we need nothing from them.
- If we do **not** have DB access, ask the customer for **one** artifact: the
  `conversion_history` row for this run (or the config-gen tool output JSON with
  the `measures[]` array). Not a new run — just the stored record.

**For the `[CCHBC-ACTION]` items (Section 5): yes, we need input** — but these are
data/semantics no tool can infer, and they are *not* what's blocking measure
coverage. They gate numerical tie-out, which is a later milestone.

## 4. Fixes we own — prioritised, with exact locations

Effort tags are rough. Each has a concrete test.

### P0 — Re-home holder-table measures onto the facts they reference _(highest leverage)_
- **Symptom:** 11/12 facts get 0 complex measures; holder-pattern models orphan
  every KPI.
- **Fix:** add a DAX-reference allocation pass in the config-gen → UCMV handoff
  (implement Tool 87's contract inline, or call it). For each measure, parse the
  `Table[Column]` / `MEASURE()` refs in its DAX, resolve them through
  `relationships`/`fact_join_map` to the fact table(s), and emit
  **`all_allocations`** (primary = the fact carrying the base columns; secondary =
  other referenced facts) instead of leaving `proposed_allocation = table_name`.
- **Where:** `pipeline_config_generator_tool.py::_build_ucmv_measures` (line 444)
  — it currently hard-passes `table_name`. Add the allocation resolver here (it
  already has `relationships`/`admin_tables` in scope in `_run`). `group_by_table`
  (`artifact_cascade.py:117`) already consumes `all_allocations` correctly, so the
  emit side needs no change.
- **Fallback for measures with no resolvable fact ref:** keep current behaviour
  (base-only) but log the count, so orphaned measures are visible not silent.
- **Test:** feed a measure `DIVIDE(SUM(Fact_A[x]), SUM(Fact_B[y]))` defined on a
  `C_Measure_Table` holder → asserts it lands in both `Fact_A` and `Fact_B`
  groups, not on the holder.
- **Effort:** the diagnostic is 0.5d; the resolver is days, and overlaps the
  handoff-hardening already on this branch.

### P1 — Resolve `{catalog}`/`{schema}` placeholders in join sources _(makes output runnable)_
- **Symptom:** literal `"{catalog}.{schema}.c_dim_calendar"` in join `source:`
  across nearly every file → the view won't run as generated.
- **Where:** `join_detector.py:58-101` (`detect`) — the join `source` falls back
  through `dim_table_info.source_table` → `fact_info.dim_source_tables` →
  `jk.get('source_table')`, and when all are empty a `{catalog}.{schema}.<dim>`
  template survives. Catalog/schema are known at generation time (on the tool
  config). Substitute them, or resolve the dim against the real scan/mquery source
  table instead of the placeholder.
- **Test:** no emitted join `source:` contains `{catalog}` or `{schema}`.
- **Effort:** small–medium. High value.

### P2 — Dimension dedup (correctness) + curation (quality)
- **Symptom:** calendar emitted 2–3× (`dim_calendar` + `dim_calendar_dummy` +
  `C_Dim_calendar`) → **duplicate dimension names → schema-invalid YAML**; 48–130
  raw ETL columns dumped; load-bearing dims (`region`, `KBI Code`, `Version`,
  `is_live`) missing.
- **Where:** dimension-emission in `table_processor.py` (Steps 2 + 4, lines
  147-247) and `yaml_emitter.py`.
  - **Dedup (must-fix, correctness):** collapse duplicate dimension `name`s; drop
    the phantom `dim_calendar_dummy`; don't emit a join's columns twice.
  - **Curate (quality):** prefer columns referenced by measures/filters + a
    conservative allow-list (keys, dates, geo/plant/BU); demote pure ETL columns
    (`objvers`, `logsys`, `postal_cd`, `process_run_id`, `year_card`, `past_flag`).
- **Test:** no duplicate dimension names; calendar emitted once.
- **Effort:** dedup = small (do first); curation = medium.

### P3 — PBI filter-parameter resolve-or-flag
- **Symptom:** `"& FiscperFilter &"`, `& RE_Version &` pasted verbatim into the
  SQL `filter:` (invalid); real business filters dropped. (Seen in
  `fact_pe002` filter block.)
- **Where:** the M-query filter path (`generate_config.py` M parsing) +
  `pbi_parameter_resolver.py` (already exists — extend it). Detect unresolved
  `" & <Param> & "` interpolation and either resolve from `parameter_defaults` or
  **surface it as an explicit `TODO`/needs-input** rather than emitting broken SQL.
- **Test:** no `& <Param> &` tokens survive into any emitted `filter:`; unresolved
  ones become a documented TODO.
- **Effort:** medium. (The *values* are `[CCHBC-ACTION]`; the tool's job is
  resolve-or-flag, not invent.)

### P4 — Rich join SQL: QUALIFY dedup + key padding
- **Symptom:** GT joins use `SELECT DISTINCT … QUALIFY ROW_NUMBER() PARTITION BY
  comp_code = 1` dedup, `LPAD(co_code_bw,4,'0')` padding, region-exclusion lists;
  Kasal emits flat equijoins → keys don't match, dims fan out.
- **Where:** join emission in `join_detector.py` / `table_processor.py`.
  (a) auto-dedup a dim join on its key via QUALIFY when the key isn't unique;
  (b) detect the `co_code_bw`↔`comp_code` length mismatch and pad;
  (c) leave business exclusion lists to config (`[CCHBC-ACTION]`).
- **Validation:** the customer hand-wrote exactly this in `edge_output` — real
  evidence it's the right pattern.
- **Test:** a dim with a non-unique key emits a QUALIFY-dedup subquery; the geo
  join uses the padded key.
- **Effort:** medium–large.

### P5 — Transpiler correctness bug batch _(small each, unit-testable)_
Found even in iom35's good output; fix once, benefits every table after P0:
- **Unparenthesized DIVIDE numerator** — `SUM(a)+SUM(b)/NULLIF(x,0)` must be
  `(SUM(a)+SUM(b))/NULLIF(x,0)`. Wrap the full numerator.
  (`dax_translator.py` divide / measure-ref paths.)
- **`x / NULLIF(1,0)` no-op** and **`x / x = 1` self-division** — detect + resolve
  the real divisor or emit numerator alone with a note.
- **Broken cross-refs** — a measure referencing a `_pct` measure that was never
  generated; the `MEASURE()`-ref validation in `yaml_emitter.py` should
  drop/flag consistently.
- **Base measures not `COALESCE(...,0)`-wrapped** — GT wraps all; match it.
- **Validation:** the customer hand-wrote the NULLIF fix in `ft_qse`.
- **Effort:** small each; batch with a unit test per bug.

### P6 — Time-intelligence (Prior-Year) + DISTINCTCOUNT _(largest, lowest urgency)_
- **Symptom:** 21 PY measures became `TODO` stubs
  (`CALCULATE(..., ALL(C_Dim_calendar), date_id IN VALUES(date_PY))`), plus
  `DISTINCTCOUNT + NOT(ISBLANK)`.
- **Where:** the LLM-first skill corpus
  (`metric_view_utils/skills/dax/`) + `dax_translator`. PY offset → window /
  self-join on the calendar; needs the calendar `date_py` column (exists in dims).
- **Effort:** large. A clean `TODO` stub is an acceptable interim; consider a
  `WINDOW`/PY skill-corpus addition over deterministic code.

## 5. Not ours — belongs in the CCHBC customer summary

These are inputs no tool can infer; they gate **numerical tie-out**, not measure
coverage:

1. **Correct physical source tables.** GT uses curated
   `` `dc_adb-landing-zone-002`.idor.tsc_fact_* ``; Kasal picked raw upstream
   datamart tables. Some GT facts are **UNIONs** (Fact_SC blends 4 KBI tables;
   Kasal used 1) — cannot tie out until the real sources are supplied.
2. **Real business filter values** — comp_code exclusions (`0403/0550/0307`), the
   `FiscperFilter` / `RE_Version` parameter values.
3. **Domain code semantics** — `fis_code` cost-code lists, `KIOM006xx`/`KPE00xxx`
   KBI codes, `func_area` SC membership (`Z110/Z120/Z310/Z320`), the `is_live`
   go-live rule.
4. **Which dimensions are business-relevant** + correct join keys
   (`LPAD(co_code_bw,4)`).

> The customer's own hand-edits (`tsc_ucm (1)/`) fixed exactly P4 (QUALIFY-dedup +
> comp_code padding, in `edge_output`) and P5 (NULLIF safety, in `ft_qse`) — real
> evidence these are the right generic fixes — layered over the `[CCHBC-ACTION]`
> business rules above.

## 6. Recommended sequence

1. **P0 diagnosis** (0.5d, no code): query `conversion_history` for this run,
   count DAX-measures-per-`table_name`, confirm the holder-table orphaning.
2. **P0 fix** (allocation pass) — the biggest single lever; unblocks measure
   coverage on every holder-pattern table.
3. **P2-dedup + P1 placeholders** — cheap correctness fixes; make output *runnable
   and valid* (today it is neither).
4. **P3 filter-param flag-or-resolve** — stop emitting invalid SQL.
5. **P5 transpiler bug batch** — small, high-confidence, unit-testable.
6. **P4 join SQL + P2-curation** — medium, quality-improving.
7. **P6 time-intelligence** — largest, lowest urgency; TODO is an acceptable
   interim.

## 7. Honest caveats
- Everything above is **structural** — it confirms measures/dims/joins are present
  and shaped; it does **not** confirm the numbers tie out. Source-table mismatch
  (Section 5.1) gates that regardless of these fixes.
- 3 GT tables (`fact_pe009`, `ft_hr_b`, `ft_planning`) need a manual name-mapping
  pass before the final measure-level tie-out.
