# Power BI: thin reports, source tables, and what Kasal can convert

When Kasal converts a Power BI model into UC Metric Views, the single most
important factor for how much converts is **whether the model carries its source
tables**. This guide explains why some models convert fully, why others produce few
or no metric views, how to tell which case you are in, and what to do about it.

> **The short version:** a UC Metric View must be built on a physical table
> (`catalog.schema.table`). If your Power BI model contains the queries that point at
> those tables (its "M-Queries"), Kasal finds them automatically and conversion works
> well. If your report is a **thin layer on a separate/upstream semantic model**, the
> source tables are not in what Kasal extracted — so point Kasal at the **underlying
> model** instead of the thin report. If the underlying model is unreachable, you can
> supply the table names manually, but **some tables/measures may be missing**.

---

## 1. Three kinds of Power BI model (and how well each converts)

| Your model is… | Contains source tables (M-Queries)? | Kasal result |
|---|---|---|
| **A. Report on a real data connection** (Get Data → Databricks / SQL / etc.) | ✅ yes — Power BI wrote the M-Query for you | **Converts well, automatically.** Source tables auto-detected. |
| **B. Thin report on a separate/upstream semantic model** (live connection to a published dataset, an Analysis Services / lakehouse model) | ❌ no — the model with the queries lives elsewhere | **Few or no metric views** unless you point Kasal at the upstream model. |
| **C. Hand-entered / calculated tables** ("Enter Data", DAX calculated tables) | ❌ no physical table exists at all | **Not convertible** — the data lives only inside the report. |

Case A is the common, happy path. Case B is the one this guide is really about — it
looks like a failure but usually isn't: the data *is* convertible, you were just
pointed at the wrong artifact. Case C genuinely cannot be converted (there is no
warehouse table behind it).

---

## 2. Why a thin report (case B) produces few/no metric views

A thin report contains **measures and slicers**, but **not** the physical tables —
those live in the upstream model it connects to. Kasal extracts what the report
carries, finds no source tables (no M-Queries) and no physical column references,
and therefore cannot build metric views on them. It falls back to emitting a raw
extract of the measures instead of inventing tables it can't see.

This is Kasal behaving correctly — it will **not** guess a table name and emit SQL
that might be silently wrong. The fix is to give it the model that actually has the
tables.

---

## 3. How to check which case you are in

**Fastest — run the conversion and read the signature.** If Kasal produces **0
metric views** and the extract shows **no M-Queries / no source tables** and mostly
slicer-style measures (`SELECTEDVALUE`, `SWITCH`), you are in **case B** (thin
report). A healthy case-A model produces metric views with real `source:` tables.

**In the Power BI Service (UI, no code):**
- Open the workspace → select the report → **Lineage view**. A thin report shows an
  arrow to a **separate dataset** (often in another workspace) — that dataset is your
  underlying model.
- Or report → **Settings** → it names the dataset it is built on. If that dataset is
  not the one you pointed Kasal at, that is the model to use.

**Via the Power BI REST API (what a future Kasal version will automate):**
```
GET /v1.0/myorg/groups/{workspace_id}/reports/{report_id}
      → returns "datasetId"  (the dataset THIS report uses)
GET /v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}
      → indicates whether it is a live connection / DirectQuery to another dataset
```
If the report's `datasetId` lives in another workspace, or the dataset is a
DirectQuery-to-dataset, that **target** is the model to convert.

---

## 4. What to do about a thin report

**Preferred — point Kasal at the underlying model.** A thin report and its upstream
model are two different datasets with two different `dataset_id`s. Run the
conversion against the **upstream model's** `dataset_id` (the one lineage points to),
not the thin report's. That model contains the M-Queries, physical tables, and
relationships — i.e. the full case-A experience. **No manual mapping needed.**

**Fallback — supply the source tables manually.** If you cannot reach the upstream
model (e.g. it is an external Analysis Services cube, or you lack access to that
dataset), you can provide a **source-table override map** in the Config Editor:
each report table → its physical `catalog.schema.table`. Kasal then builds
best-effort metric views for the measures that are real aggregations.

> ⚠️ **Best-effort has limits — tables and measures may be missing.** When Kasal
> works from a thin report (with or without a manual override map) rather than the
> full underlying model, it can only convert what it can resolve:
> - Tables you don't map are **skipped**.
> - Measures that are slicer/selector logic (`SELECTEDVALUE`, `SWITCH`,
>   `ALLSELECTED`), or that reference other measures, or that use table operations
>   (`COUNTROWS(SUMMARIZE(...))`) may **not convert** — they have no static SQL
>   equivalent.
> - Every best-effort measure is flagged `TODO: verify` and must be reviewed before
>   deployment.
> This path is a head start, not a complete migration. For completeness, use the
> underlying model.

---

## 5. Summary decision guide

```
Ran conversion → got metric views with real source tables?
├─ Yes → Case A. You're done; review measures normally.
└─ No / very few, no M-Queries in the extract → Case B or C.
     ├─ Does lineage show an upstream dataset you can access?
     │    ├─ Yes → re-run against that dataset_id (best result).  ← do this
     │    └─ No  → supply a source-table override map (best-effort;
     │             expect missing tables/measures, all flagged TODO: verify).
     └─ Tables are hand-entered / calculated only → Case C, not convertible.
```

**Bottom line:** most "no metric views" outcomes are a thin report pointed at the
wrong artifact, not a conversion failure. Point Kasal at the underlying semantic
model and it converts like any other; only fall back to manual source mapping when
that model is genuinely out of reach — and expect gaps when you do.
