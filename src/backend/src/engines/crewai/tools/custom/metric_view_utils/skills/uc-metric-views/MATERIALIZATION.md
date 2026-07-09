# Materialization for Metric Views (Experimental)

Source: Databricks official documentation (`docs/web/docs/metric-views/materialization.md`).

## Overview

Materialization pre-computes aggregations using Lakeflow Spark Declarative Pipelines. At query time, the optimizer routes queries to the best materialized view via automatic aggregate-aware query rewriting.

## Requirements

- Serverless compute enabled.
- DBR 17.2 or above.

## Configuration

All materialization config is in a top-level `materialization` field:

```yaml
materialization:
  schedule: every 6 hours       # Same syntax as MV schedule clause
  mode: relaxed                 # Only supported mode during experimental
  materialized_views:
    - name: baseline
      type: unaggregated        # Materializes entire data model

    - name: revenue_breakdown
      type: aggregated           # Pre-computes specific dim/measure combos
      dimensions:
        - category
        - color
      measures:
        - total_revenue

    - name: suppliers_by_category
      type: aggregated
      dimensions:
        - category
      measures:
        - number_of_suppliers
```

## Materialization Types

| Type | What it does | When to use |
|---|---|---|
| `aggregated` | Pre-computes specific dimension+measure combos. | Targeting common query patterns. Must have at least one dimension or measure. |
| `unaggregated` | Materializes the entire data model (source + joins + filter). | Expensive source views/queries or expensive joins. |

## Query Rewrite Strategy

1. **Exact match**: Query grouping expressions match MV dimensions exactly, and aggregations are a subset of MV measures.
2. **Unaggregated match**: If an unaggregated MV exists, always eligible.
3. **Fallback**: Reads directly from source tables.

Verify with `EXPLAIN EXTENDED` or query profiles.

## Lifecycle

- `CREATE` / `ALTER` is synchronous; MVs materialize asynchronously.
- Manual refresh: `REFRESH MATERIALIZED VIEW <metric-view-name>`.
- Incremental refresh when possible.
- `TRIGGER ON UPDATE` is NOT supported.

## Restrictions

- `relaxed` mode only (experimental).
- No unaggregated MV when source is another metric view.
- No RLS/CLM in MVs (falls back to source).
- No non-deterministic functions in MVs.
