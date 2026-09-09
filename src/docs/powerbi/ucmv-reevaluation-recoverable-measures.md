# UCMV Re-evaluation — surfacing newly-recoverable measures

## Why

Kasal's DAX→UC-Metric-View capability keeps improving: new patterns in
`dax_translator.py`, a better LLM-first path in `dax_llm_fallback.py`, and an
evolving skill corpus under `metric_view_utils/skills/`. But those gains only ever
helped **new** conversions. A model converted last month still shows the measures
that failed *back then*, even though the transpiler can handle several of them today.

This feature closes that loop: for each already-converted model, re-try **only the
measures that previously failed** with today's transpiler, and surface the ones that
are now translatable so a human can choose to re-transpile them.

Credit: idea from Kyle Hale.

## What makes it feasible

Everything needed is already persisted (no new extraction round-trip, no PowerBI API
calls):

| Stored where | What |
|---|---|
| `PowerBIExtraction` (`src/models/powerbi_extraction.py`) | `measures` (with DAX), `admin_tables` (M-queries), `relationships`, **`proposed_config`** (the pipeline config), keyed by `workspace_id`/`dataset_id`/`report_id`, `group_id`, `created_at` |
| `ConversionHistory` (`src/models/conversion.py`) | `input_data`, `output_data`, `configuration` per conversion |
| UCMV generator output | `untranslatable_items[]` — per measure: `original_name`, `dax_expression`, `skip_reason`, `category`, `dax_class`, `referenced_by` |
| `PowerBIExtractionRepository` | `get_latest_for_dataset()`, `find_by_group()` already exist |

## Design decisions (and one rejected idea)

**Re-try only previously-FAILED measures.** Never re-transpile a measure that already
succeeded — that risks silently changing a validated measure. Hard rule.

**Trigger on a capability fingerprint + actual re-run, NOT on a skill-file diff.**
The tempting version ("what's new in the skill files?") was rejected:
1. Most capability gains are in **code** (`dax_translator` patterns, the LLM-first
   path, emitters) — not the `.md` skill files. Diffing skills alone misses them.
2. Mapping "this skill paragraph changed" → "measure X now works" is guesswork; it
   would propose measures that still fail and miss ones that now work.

Instead: hash the whole capability surface (skill corpus + translator/fallback source
+ registered pattern names) into a **fingerprint**. If it hasn't changed since the
stored run, there is nothing to gain — skip. If it has, **just re-run the transpiler
and compare**. Ground truth beats inference, and the deterministic fast-path makes it
cheap.

**Note:** `ConversionHistory.converter_version` exists as a column but is never
written anywhere in the codebase, so it cannot be the gate today. The fingerprint
replaces it (and we record it going forward).

**Cost + noise controls:**
- **Category gate** — `_categorize_untranslatable` (yaml_emitter) already separates
  *impossible* (display artifact, slicer/scalar helper, prior-year time-intelligence,
  disconnected-slicer dispatch) from *expressible* (fixed-LOD/ALLEXCEPT,
  group-then-aggregate/SUMMARIZE, top-N/TOPN, distinct-count, complex-DAX). Retry the
  expressible buckets; skip the impossible ones by default (`include_impossible` opt-in).
- **Deterministic-first** — run `translate(..., trivial_only=True)` first; only escalate
  to the LLM path when explicitly enabled (`use_llm`), so a scheduled sweep across many
  datasets doesn't silently burn tokens.
- **Impact ordering** — sort by `referenced_by` desc so high-dependency measures surface
  first.
- **Suppression via review annotations** — the "Not transpiled" review panel already
  persists `untranslatable_review` (status `wont_fix` / `hand_written`). Those are
  skipped, so the sweep stops proposing what a human already dismissed.

**Proposes only — never auto-applies.** The tool produces a report. Re-transpiling
happens through the existing UCMV routes, human-triggered.

## Implementation

### 1. `metric_view_utils/capability_version.py` (new)
- `pattern_names()` — registered translator pattern names (capability surface).
- `capability_fingerprint()` — stable short hash over: every skill-corpus file's
  content, the source of `dax_translator.py` + `dax_llm_fallback.py`, and the pattern
  names. Cached per process. Fail-open (returns a sentinel on error — never blocks).
- `capability_summary()` — `{fingerprint, pattern_count, skill_files}` for the report.

### 2. `metric_view_utils/reevaluate.py` (new)
- `RETRYABLE_CATEGORIES` / `IMPOSSIBLE_CATEGORIES` — the category gate, aligned with
  `_categorize_untranslatable`'s buckets.
- `is_retryable(item, include_impossible=False)` — category + annotation suppression.
- `reevaluate_measures(untranslatable_items, config, *, review=None,
  include_impossible=False, use_llm=False, limit=None)` →
  `{newly_translatable: [...], still_failing: [...], skipped: [...], counts: {...}}`.
  Builds one `DAXTranslator(config)` from the stored `proposed_config` and calls
  `translate()` per previously-failed measure; a non-empty `sql_expr` = recovered.
  Each recovered row carries `new_sql`, the `previous_skip_reason`, and `referenced_by`.

### 3. `UCMVReevaluationTool` (new custom tool)
Loads the latest `PowerBIExtraction` per dataset (group-scoped), pulls its stored
untranslatable items + `proposed_config`, runs `reevaluate_measures`, and emits:
```json
{"capability": {...}, "datasets": [{"dataset_id", "workspace_id", "view_names",
 "fingerprint_changed", "newly_translatable": [...], "still_failing_count",
 "skipped_count"}], "summary": {"datasets_scanned", "measures_recovered", ...}}
```
Schedulable or manually triggered by a crew. Read-only w.r.t. UCMV state.

### 4. Frontend `ReevaluationResultViewer.tsx` (new)
Detection helper + per-dataset accordions listing recovered measures: **Measure |
Previously failed because | New SQL | Used by**. Wired into `ShowResult` alongside the
existing UCMV/Validator viewers. Read-only proposal.

## Critical files

- `src/backend/src/engines/crewai/tools/custom/metric_view_utils/capability_version.py` (new)
- `src/backend/src/engines/crewai/tools/custom/metric_view_utils/reevaluate.py` (new)
- `src/backend/src/engines/crewai/tools/custom/ucmv_reevaluation_tool.py` (new)
- `src/frontend/src/components/Jobs/ReevaluationResultViewer.tsx` (new)
- `src/frontend/src/components/Jobs/ShowResult.tsx` (wire the viewer)
- Reused: `metric_view_utils/dax_translator.py`, `yaml_emitter._categorize_untranslatable`,
  `repositories/powerbi_extraction_repository.py`

## Verification

- Backend: `.venv/bin/python -m pytest tests/unit/engines/crewai/tools/custom/ -k "reevaluat or capability" -q`
- Frontend: `npm run test:run -- src/components/Jobs/ReevaluationResultViewer.test.tsx`, then `npm run tsc` && `npm run lint`
- End-to-end: run the re-evaluation tool against a group that has prior extractions →
  report lists datasets with recovered measures + their new SQL; a dataset whose
  fingerprint is unchanged reports `fingerprint_changed: false` with nothing proposed.

## Known limitations

- **Cost**: escalating to the LLM path per retried measure costs tokens; that is why
  `use_llm` is opt-in and the category gate exists.
- **Stale `proposed_config`**: if the underlying UC tables changed since the original
  run, recovered SQL may not validate — the report is a *proposal*, and re-transpiling
  through the normal route re-validates.
- **"Latest per dataset"** is `created_at` desc; a dataset converted from multiple
  reports may need `report_id` disambiguation.
