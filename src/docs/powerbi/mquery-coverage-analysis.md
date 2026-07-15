# M-Query Source Coverage Analysis

**Date:** 2026-07-15 · **Input:** `pbi_metadata_reports.csv` (1,862 table `source_expression` M-queries across the tenant)

Purpose: check which PBI table sources our MQuery pipeline can turn into a UCMV
`source:` SELECT, and — per the ask — whether the **no-SQL** ones are a gap or
correctly skipped. Focus on the tables that "should not have SQL at all."

## Categorization of all 1,862 source expressions

| Category | Count | % | Extractable to SQL? | Correct handling |
|----------|------:|--:|:-------------------:|------------------|
| **Inline constant table** — `Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("…Base64"))))` | 677 | 36% | ❌ | **Skip** — hardcoded slicer/selector helper tables (`Value_Type_Selector`, `Time_Period_Selector`, `RE/BP/CY_Selector`). No DB source exists. |
| **DAX calculated tables** — `SUMMARIZECOLUMNS`, `GENERATESERIES`, `VALUES(...)`, `Row(...)`, parameter queries | 440 | 24% | ❌ | **Skip** — computed in-model (e.g. `GENERATESERIES(-5,20,5)` for a "Top N" slicer, `Dim_cal = GENERATESERIES(date(...))`). No source SQL exists. |
| **Other pure-M** — `Table.Combine`, `List.Numbers`, `Access.Database`, M-to-M refs (`Source = <another M table>`) | 327 | 18% | ⚠️ mostly ❌ | Derived/local — mostly skip; a few `Access.Database`/dataflow are external non-warehouse sources. |
| **Native SQL** — `Value.NativeQuery(…, "SELECT …")` | 187 | 10% | ✅ | **Extract** — core MQuery-parser path. |
| **Databricks connector** — `Databricks.Catalogs(...){[Name=…]}[Data]` chains | 152 | 8% | ✅ | **Extract** — resolve the catalog/schema/table navigation to a 3-level name. |
| **Dataflow / AAS** — `PowerBI.Dataflows`, `AnalysisServices.Database` | 66 | 4% | ⚠️ | External source; needs the upstream mapping (customer input). |
| **SQL database (non-native)** — `Sql.Database(...)` table nav | 13 | 1% | ✅ | **Extract**. |

## Answer to the question

**The "no SQL" tables are overwhelmingly things that *correctly* have no SQL —
they are not warehouse facts/dims.** ~60% of all sources (inline-const 36% +
DAX-calc 24%) are display scaffolding or in-model computed tables. For these the
right behavior is a **clean skip with a clear reason**, not SQL recovery — the
same family as the SWITCH/SELECTEDVALUE display-layer skips.

So the risk is **not** "we're missing SQL we should have extracted." It is the
inverse: **are we cleanly recognizing these as non-facts and skipping them, or
do we try to recover SQL and emit a broken/empty view?**

**Genuinely extractable = ~19%** (native SQL 10% + Databricks 8% + SQL-db 1% ≈
352 tables). That is the MQuery-parser's real job.

## Do we need new skillfile examples?

- **No new DAX-corpus examples needed** for the no-SQL tables — they are not DAX
  measures; they are table sources, handled (or skipped) by the MQuery parser,
  not the DAX translator's skill corpus.
- **The one thing worth verifying in code** (not corpus): that
  `looks_like_raw_mquery()` / the MQuery parser **classify inline-const and
  DAX-calc sources as skip-with-reason**, rather than routing them to the raw-M →
  SQL LLM recovery (which would waste tokens and can emit an empty/degenerate
  `source:`). Spot-check showed `looks_like_raw_mquery("…Table.FromRows(Binary.
  Decompress…)")` returns **True** — i.e. it *would* attempt recovery on a
  constant table. That is the actionable item: add a guard that short-circuits
  `Table.FromRows(Json.Document(Binary.…))` and pure DAX-calc (`GENERATESERIES`/
  `SUMMARIZECOLUMNS`/`VALUES`/`Row(`) sources to "skip — inline/computed table,
  not a warehouse source."

## Recommended follow-up (bounded)

Add an early skip-classifier to the MQuery path:
- `Table.FromRows(...Binary.Decompress(...Base64...))` → skip: "inline constant
  table (slicer/selector helper) — no warehouse source."
- Source body is `GENERATESERIES(` / `SUMMARIZECOLUMNS(` / `VALUES(` / `Row(` /
  a parameter query (`meta [IsParameterQuery=true]`) → skip: "DAX calculated
  table — computed in-model, no source SQL."

This prevents wasted LLM recovery + empty-view emission on ~60% of sources and
makes the skip reason explicit in the output, consistent with how the DAX side
now categorizes its skips.
