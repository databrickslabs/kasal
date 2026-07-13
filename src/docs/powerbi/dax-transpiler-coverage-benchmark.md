# DAX → UC Metric View Transpiler — Coverage Benchmark

_Last updated: 2026-07-13. All numbers are code-verified from real runs on this
branch (`feat/pbi-ucmv-fixes-v2`), not estimates._

## TL;DR

Kasal converts DAX measures to UC Metric View SQL through a **deterministic-first,
LLM-tail** pipeline:

1. A pattern registry of deterministic converters runs first (no LLM — cheap,
   reproducible, auditable).
2. Whatever the rules don't handle falls through to a skill-corpus-grounded LLM.
3. Genuinely unsupported patterns are surfaced as documented TODOs (with usage
   counts so reviewers prioritize), never silently dropped.

On coverage of the pure DAX→SQL task, Kasal **ties** a best-in-class rules-only
transpiler (field-eng) at 23/25 on a shared micro-benchmark. Kasal's advantage is
not "we convert more" — it is *how* the same coverage is reached (skill-grounded
LLM tail + a governed migration platform around it).

## Benchmark A — 25-measure head-to-head (small, controlled)

| Engine | Total coverage | Mechanism |
|---|---|---|
| Field-eng (rules only) | 23/25 (92%) | per-function deterministic converters |
| **Kasal (deterministic + skill-LLM)** | **23/25 (92%)** | 15 deterministic + 8 skill-corpus LLM |

Kasal's 23 breaks down as:
- **15 deterministic** — no LLM, reproducible.
- **8 skill-corpus LLM** — the tail field-eng flags "requires manual review"
  (e.g. `SWITCH → CASE WHEN`, `SUMX → SUM(qty*price)`, `ISBLANK → COALESCE`, and
  measure-ref → `MEASURE(total_sales)/MEASURE(total_cost)` composition — a
  UC-Metric-View-specific pattern grounded in the skill files).
- **2 still failed** — `SAMEPERIODLASTYEAR`, nested `CALCULATE`. Kasal's LLM
  *attempted* these and correctly self-classified them as unsupported
  (`success: false`) rather than emitting wrong SQL. Field-eng flags the same
  two for manual work.

**Honest read:** on raw coverage this is a **tie**, not "way ahead." Do not claim
Kasal transpiles more on this set — it doesn't. The defensible claim is coverage
parity *plus* the platform (below).

## Benchmark B — real ~300-measure model (scale, deterministic-only)

From a real customer semantic model run through the full pipeline
(run `49fdf657`, 2026-07-13 ~08:12; HITL approval #399). De-duplicated by unique
measure name (a measure covered in any view counts once):

| Metric | Value |
|---|---|
| Unique measures | 303 |
| Deterministically translated | 250 |
| Untranslated ("no matching pattern") | 53 |
| **De-duplicated coverage** | **250 / 303 = 82.5%** |
| Rate-limit errors during run | 0 |

**Important caveat — this run's LLM tail did NOT execute.** `use_llm_fallback` was
`true` in config but `llm_token` / `llm_workspace_url` were empty, so zero
`DAX_LLM` calls fired (verified: no `Attempting LLM fallback` in the logs). So
**82.5% is the deterministic-only floor** on a real model at scale — the 53
"no matching pattern" measures are exactly the tail the skill-corpus LLM would
attempt. With the LLM tail active (valid credentials), coverage is expected to be
meaningfully higher (cf. Benchmark A's deterministic-15 → total-23 lift).

> Note on raw vs. de-duplicated: summing per-view "DAX-translated" counts in the
> logs gives ~552 translated / ~89% — but that **double-counts** measures that
> appear in multiple views. **82.5% de-duplicated is the honest number.**

## What makes coverage robust (not just high)

- **Deterministic-first shrinks LLM exposure.** The more patterns handled by
  rules, the fewer measures depend on the LLM — which is what fails under the
  workspace FMAPI rate limit (`REQUEST_LIMIT_EXCEEDED` on
  `databricks-claude-sonnet-4`). A throttled run (07:33) dropped to ~253 exprs;
  a clean-window run (08:12) completed fully. Same code — the difference was the
  rate limit. Promoting deterministic converters directly reduces this exposure.
- **21 of 24 registered converters run deterministically** in `llm_first` mode.
  Only 3 stay LLM-routed by design: `userelationship` (deterministic output
  drops alt-relationship join semantics), `selectedvalue_switch` (parameterized
  SWITCH — no single SQL), `sameperiodlastyear` (narrow conditional window).

## The claim that survives scrutiny

- **Transpiler alone:** coverage parity with a best-in-class rules engine (23/23
  on the shared set). Be ready for someone to run field-eng and see 23/25 — it's
  a tie, and that's fine.
- **Platform:** this is the unchallengeable part. Field-eng stops at a SQL string
  in a file. Kasal delivers a **governed migration**: live semantic-model
  connection, HITL approve-before-deploy, structural validation, deploy, Genie
  config, multi-tenant isolation, scheduling — and a skill-grounded LLM that
  *attempts* (and safely declines) the tail field-eng punts to a human.

## Reproducing these numbers

- **Deterministic coverage (unit-level):** the converter registry and its
  `_TRIVIAL_FAST_PATH` live in
  `src/backend/src/engines/crewai/tools/custom/metric_view_utils/dax_translator.py`;
  tests in `tests/unit/engines/crewai/tools/custom/metric_view_utils/test_dax_translator_extended.py`.
- **End-to-end coverage:** run the BI migration flow on a model; read the
  UCMV generator's HITL approval payload (`hitl_approvals.previous_crew_output`,
  the `yaml` map) and de-duplicate translated vs. TODO measures by name.
- **For a firmer LLM-tail number:** re-run with valid `llm_token` /
  `llm_workspace_url` so the fallback fires, and confirm `DAX_LLM: N/M measures
  translated` appears in the logs.

## Honest caveats

- Benchmark A (25 measures) is a small sample — 92% is real but not
  statistically robust. Benchmark B (303 measures) is the firmer figure, but
  measures the deterministic floor only (LLM tail didn't run).
- The de-duplicated 82.5% counts a measure as covered if it translates in any
  view; a measure untranslatable on its home table but resolved cross-table
  could nudge the true number slightly higher.
- The 25-measure LLM run in Benchmark A bypassed Kasal's multi-tenant DB auth
  wrapper to call the endpoint directly, but used the real skill-corpus system
  prompt and parsing/validation logic — so the LLM behavior is genuine; only the
  auth path differed.
