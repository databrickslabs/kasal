# PBI→UCMV: otc_management session handoff (2026-09-22)

Handoff for whoever picks up this branch next. Covers a single continuous
debugging + feature session on `feat/pbi-migration-visual-referance-and-improved-translation-and-mapping`,
building on work already on the branch before this session started (visual
usage extraction, SWITCH decomposition, the PBI↔UCMV mapping generator,
fx_OTCKPI custom-function resolution — see that earlier handoff summary in
this branch's git history / conversation record if you need the full
pre-session context).

## The goal

Migrate Power BI reports (proven ground: `otc_management`) to Unity Catalog
Metric Views via Kasal's PBI Specialist flow — two chained crews, **Pipeline
Config Generator** → **UC Metric View Generator** — while keeping every fix
**general** (works for any report with the same shape), never hardcoded to
one customer's report. Secondary goal, explicit ask this session: recover
report **visual usage** (which page/visual references each measure, and how
many) so a downstream reconciliation step can prioritize which measures
matter and decide what to keep in the UCMV draft.

## What was actually broken, in the order it was found

Each of these looked like "the translation quality is bad" from the outside,
but the underlying causes were almost all **plumbing between the two crews**,
not the DAX-to-SQL logic itself.

### 1. The CrewAI harness silently paraphrased every tool's output
`services/execution/harnesses/crewai/tools.py`'s `adapt_tool()` wraps every
Kasal tool in a `KasalToolAdapter` for the CrewAI harness (the harness this
platform defaults to — `selection.py`'s `DEFAULT_HARNESS`). It copied `name`/
`description`/`args_schema` onto the adapter but **not** `result_as_answer` —
so CrewAI's own executor never short-circuited the agent loop, and every
`result_as_answer=True` tool (all the PBI/UCMV tools, plus ~15 others
platform-wide) had its raw JSON output paraphrased into prose by the LLM
before it ever reached the next step. **Fixed**: carry `result_as_answer`
onto the adapter. This affects every tool marked `result_as_answer` under the
CrewAI harness, not just this flow.

### 2. The flow's cross-crew data injection silently no-op'd under CrewAI
`flow_builder/modules/flow_methods.py`'s listener method injects the
previous crew's JSON output into the next crew's tool `_default_config` —
but it read `agent.tools` expecting raw Kasal tool objects. Under the CrewAI
harness those are the SAME `KasalToolAdapter` wrappers from #1, which don't
expose `_default_config` directly (it's one level down, on
`adapter.kasal_tool`). Every check for `hasattr(tool, "_default_config")`
came back `False`, so **zero** cross-crew injection ever happened —
`config_json`/`measures_json`/`mquery_json`/`visual_usage_index` all reached
UC Metric View Generator empty, which silently fell back to its own
independent (much weaker) direct-Fabric-API re-extraction. **Fixed**: unwrap
`tool.kasal_tool` when it's present, but only when the inner object has a
*real* dict `_default_config` (a plain `MagicMock()` test fixture without a
declared `.kasal_tool` also auto-fabricates a truthy child mock — the check
guards against treating that as a real unwrap target).

Together, #1 and #2 explain essentially everything that looked wrong before
this session: missing exports, near-empty measure counts, no mapping output.

### 3. SWITCH-decomposed measures were silently orphaned
`services/powerbi/switch_decomposition.py` correctly *detects and resolves*
genuine PBI `SWITCH()` measures (e.g. `ACT vs Target`), but attributed each
one to the **referenced measure's raw PBI home table** — which in a
holder-pattern model (most business measures defined on one shared
`Measures_Table`) is never a real fact table.
`metric_view_utils/table_processor.py`'s Step 6 only looks up
`switch_decompositions[table_key]` for the table it's currently processing,
and a holder table is never itself processed (no warehouse source) — so
**every one of these measures was silently dropped**, with no error, no
warning. **Fixed**: re-home via the same DAX-`Table[Column]`-scan technique
already used for ordinary measure allocation
(`PipelineConfigGeneratorTool._resolve_measure_allocations`), applied to
both `derive_switch_decompositions` and `derive_geo_switch_decompositions`.
Fully general — operates on whatever `fact_tables` set the current run
computes, no report-specific names anywhere in the logic.

### 4. `discover_report_id` used an API this platform's SA account can't call
Visual usage extraction needs a `report_id`; when the tool-task-form leaves
it blank (as otc_management's does — and as most reports will, since filling
it in isn't part of the "call the tool with zero arguments" design), it
auto-discovers one via the classic `GET .../groups/{ws}/reports` REST
endpoint. That endpoint needs delegated PBI workspace access the
Service-Account-only auth tier doesn't have — confirmed live, a bare 401.
Meanwhile the **Admin Scanner** (`trigger_admin_scan`, already called one
step earlier via SP fallback for `admin_tables`, at no extra cost) returns a
full `GetInfo` payload that includes a `workspaces[].reports[]` array with
`datasetId` bindings for every report in the workspace — verified live, it
resolves to the exact right report. **Fixed**: `discover_report_id` now
checks that already-fetched payload first, falling back to the classic
endpoint only when no scan result is available (e.g. standalone CLI use).

### 5. Two smaller coverage gaps closed
- **External/unscanned source tables** (`mapping_only_tables.py`): a table
  Fabric scanned but whose M-Query is a non-warehouse connector (Excel /
  SharePoint / Dataflow / AAS) previously got a hard skip with no further
  coverage, even when its measures are fully known. Now gets a draft
  `mapping_only_tables` entry (a proposed `{catalog}.{schema}.<table>`
  landing name) — land the data there and the draft view/measures need no
  further changes. (The pre-existing "table not in the admin scan at all"
  case worked before this session but emitted non-functional TODO-string
  placeholders for `dimensions`/`aggregate_columns` instead of real empty
  lists; fixed as part of the same change.)
- **Raw columns used in visuals with no named measure** (new capability,
  described in detail below).

## New capability this session: aggregated-column-as-measure

The ask: a PBI report author can drag a raw numeric **column** straight into
a visual's Values well — PBI applies the column's own default aggregation
(`SummarizeBy`: Sum/Average/Count/Min/Max/DistinctCount) with **no DAX
measure ever defined**. Confirmed as a real gap on a different customer
report. Scoped exactly as requested: promote a column **only** when it's (a)
aggregatable (`SummarizeBy != "None"`) **and** (b) actually present in
`visual_usage_index` — never just because it exists on a fact table.

New modules (`services/powerbi/`):
- `column_metadata.py` — `extract_column_summarize_by`, via
  `EVALUATE INFO.VIEW.COLUMNS()` (same SA-token Execute Queries path
  `extract_measures` already uses — no new auth tier).
- `implicit_column_measures.py` — pure matching: for each visual-usage field
  with no matching PBI measure, resolves `(table, column, summarize_by)`,
  maps to the right SQL aggregate, prefers a fact-table match when the same
  column name exists on several tables.
- `metric_view_utils/implicit_column_builder.py` — turns those dict entries
  into `TranslationResult`s (kept out of `table_processor.py` to avoid
  pushing that file over the file-size ceiling — see below).

Threaded through the exact same flow-handoff path as `visual_usage_index`
(new `implicit_column_measures` field: Pipeline Config Generator output →
`flow_methods.py` injection loop → `UCMetricViewGeneratorTool` input →
`table_processor.py` Step 6d → `yaml_emitter.py`).

**Flagging, as explicitly requested**: these measures get their own
`implicit_visual_column` category and their own **"Aggregated Column
Measures"** YAML section (never folded into "DAX-Translated"), with a
comment that always states the reason *and* the visual reference:

```yaml
 ─── Aggregated Column Measures (1) ───

  - name: test_raw_value
    expr: SUM(source.fltp)
    comment: "Aggregated column as measure (Sum) - no named PBI measure
      exists; included because this column is drawn/filtered directly in a
      report visual · Used on: OTC Scorecard"
```

Also wired into `pbi_ucmv_mapping.py` as a proper `raw_column` pbi_kind
(`pbi_table`/`pbi_column`, matching the reconciliation framework's own
`mapping_schema.py` taxonomy) — this was **completely unhandled** before this
session (any top-level `pbi_kind` outside `direct`/`composite`/
`dimension_conditional` silently fell through to being mis-reported as
`direct` against a measure name that doesn't exist). Fixed as part of the
same change.

**Verified two ways** (otc_management genuinely has zero qualifying columns —
confirmed by direct cross-reference of all 62 visual-usage fields against
every aggregatable column; the two name collisions found, `Show as` and `DCC
Domain`, both have `SummarizeBy: "Default"` on text/label columns, correctly
excluded):
1. Every pure function unit-tested (44 new tests across all the files above).
2. A synthetic entry forced through the **real** `UCMetricViewGeneratorTool`
   using otc_management's actual config/measures/mquery data, confirming the
   full JSON round-trip (serialize → merge into config → Step 6d → YAML
   emission) produces exactly the section shown above.

If you need to see this feature actually fire (not just verified-correct on
data with no candidates), you need a report where a raw numeric column is
genuinely dragged into a visual with no measure behind it — otc_management
isn't that report.

## What's confirmed working now, on a real redeploy

Compared against the `old_kasal` baseline (a prior implementation, files in
`otc_management_dashboard (3).zip` if you still have it):

| Table | old_kasal measures | current |
|---|---|---|
| Fact_OTC | 6 | 34 |
| Fact_NPS | 2 | 6 |
| Fact_CustomerExp | 1 | 1 |
| AI_Invoice-DataBricks SQL | 3 | 3 |
| Calendar445 | — | 1 |
| SKU Active Or NotActive | — | 2 |
| **Total** | **12** | **47** |

(Counted from the `measures:` YAML section only, not dimensions — an earlier
in-session claim of an "old baseline of 39" was a mistake, conflating
dimension count with measure count; corrected here.)

Also confirmed on a real redeploy, via `execution-logs-*.txt`'s
`_diagnostics` block on the UC Metric View Generator's own output (grep for
`preinject_` / `visual_usage_annotated_count`):
- `preinject_config_json_chars` / `measures_json` / `mquery_json` /
  `visual_usage_index`: all non-trivial character counts — the flow handoff
  is carrying real data end to end.
- `visual_usage_annotated_count: 15` — visual usage tags ARE reaching
  measures. Grep any emitted YAML for `Used on:` to see them (e.g.
  `Fact_OTC_uc_metric_view.yml` has `fx_OTCKPI(...) resolved against
  Fact_OTC.bic_csubkbi/fltp · Used on: OTC Scorecard`), and the
  `mappings/*.mapping_candidates.yml` files carry a `used_in_visuals:` block
  per measure.
- The external-source draft feature fired for real:
  `SKU Active Or NotActive_uc_metric_view.yml` exists with
  `source: main.default.sku_active_or_not_active` and real measures.
- `implicit_column_measures` correctly stayed empty this run (see above —
  not a bug, otc_management has no qualifying columns).

**Important UX note, not a bug**: the downloadable `pipeline_config.json` is
**only** Pipeline Config Generator's `proposed_config` slice (join_key_map,
fact_join_map, etc.) — it has never included `visual_usage_index`,
`measures_json`, `mquery_json`, or `implicit_column_measures`, which are
sibling keys in that tool's full output, not part of `config`. Looking for
visual usage there will always come up empty; it shows up in the emitted
YAML's `Used on:` comments and the mapping files' `used_in_visuals:` blocks
instead. Worth a UI rename/clarification if this trips people up again.

## What's NOT done — flagged, not fixed

- **`custom_function_resolution.py`'s `fx_OTCKPI` handling is tenant-specific
  by name** (predates this session). It pattern-matches the literal DAX
  function name `fx_OTCKPI`, a shared corporate DAX library function this
  customer reuses across ~150 of their own datasets — general across *this
  tenant's* reports, but a documented no-op for any other customer (matches
  nothing, does nothing there). Not touched this session; flagging per an
  explicit "no report-specific code" concern raised mid-session.
- **`NPS Driver Contribution`-style measures**: a clean, real aggregate
  measure that fails only because it references another measure through
  `CALCULATE([Ref], REMOVEFILTERS(...))` rather than a bare `[Ref]` — the
  measure-reference resolver only handles the bare case. Diagnosed, not
  implemented.
- **`SELECTEDVALUE(Table[Column])`-shaped "measures"** (e.g. `Placeholder NPS
  Label Primary Driver`) are correctly excluded as measures, but could
  become a UCMV **dimension** instead of a pure skip — the DAX-side
  skip_reason for this one literally suggests it ("Define primary_drivers as
  a dimension in the metric view"). Diagnosed, not implemented.
- **Execution-log capture doesn't reliably carry Pipeline Config Generator's
  own console output** — every downloaded log this session had zero
  `[PipelineConfigGen]`-prefixed lines even though `UC Metric View
  Generator`'s own diagnostics survived. Looks like a log-capture/truncation
  limitation upstream of both tools, not something either tool controls.
  Worth checking `execution/logs/` if Pipeline Config Generator's own
  warnings ever need to be inspected post-hoc again (this session had to
  reproduce runs live instead, each time it mattered).

## File-size ratchet notes (CLAUDE.md's 800-target/1500-ceiling rule)

- New sibling modules split out of already-oversized files, following the
  pattern already established by `switch_decomposition.py` /
  `calculation_groups.py` / `custom_function_resolution.py`:
  `mapping_only_tables.py`, `report_discovery.py`, `column_metadata.py`,
  `implicit_column_measures.py` (all in `services/powerbi/`), plus
  `metric_view_utils/implicit_column_builder.py`. All small, all with their
  own tests, all respect the CLI-standalone-loading constraint documented in
  `pipeline_config.py`'s own docstring (no cross-import back to it).
- `table_processor.py` briefly crossed 1500 mid-session from the Step 6d
  addition; extracted into `implicit_column_builder.py` and trimmed comments
  to land at 1498.
- Three files were **already** over the ceiling before this session and are
  explicitly flagged in `services/execution/CLAUDE.md` as high-risk to split
  without subprocess-level testing (`pipeline_config_generator_tool.py` and
  `uc_metric_view_generator_tool.py` run inside the crew/flow subprocess;
  `flow_methods.py` is core flow-runtime code). Each grew by a small,
  call-site-only amount this session (wiring new fields through, not new
  business logic) rather than risk a drive-by split. Flagging this as a
  known, deliberate exception — a real shrink pass on these three is still
  owed whenever there's room to do it carefully with subprocess-level
  testing.

## Suggested next steps

1. **Redeploy and rerun** with a real `report_id` supplied explicitly (the
   user's own stated plan going forward) — removes any dependency on
   discovery working at all, belt-and-suspenders on top of the fix in §4.
2. Decide whether `pipeline_config.json`'s download scope should be widened,
   or the UI should surface `visual_usage_index` / the mapping / implicit
   columns as their own explicit sections — the data is all there; it's a
   presentation question now, not an extraction one.
3. If you get access to a report with the "raw column dragged into a
   visual, no measure behind it" pattern, that's the one that will actually
   exercise the new aggregated-column-as-measure feature end to end (see
   above for why otc_management can't).
4. The two "flagged, not fixed" measure-pattern gaps above
   (`CALCULATE(MEASURE_REF, REMOVEFILTERS(...))` resolution,
   `SELECTEDVALUE` → dimension) are the next-highest-value, well-scoped,
   fully general wins if you want to keep pushing coverage up.
