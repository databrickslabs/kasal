# Level of Detail (LOD) Expressions in Metric Views

Source: Databricks official documentation (`docs/web/docs/metric-views/data-modeling/level-of-detail.md`).

## Overview

LOD expressions control aggregation granularity independently of query dimensions. Two types:

- **Fixed LOD**: Aggregate over pre-defined dimensions, ignoring query groupings.
- **Coarser LOD**: Aggregate at a coarser granularity by excluding specific dimensions.

## Fixed LOD

Implemented using SQL window functions in the `source` query, then exposed as identity dimensions.

```yaml
version: 1.1

source: |
  SELECT
    o_orderkey, o_orderpriority, o_totalprice, o_orderdate,
    SUM(o_totalprice) OVER (PARTITION BY o_orderpriority) AS priority_total_price
  FROM samples.tpch.orders

dimensions:
  - name: order_priority
    expr: o_orderpriority
  - name: priority_total_price
    expr: priority_total_price          # Identity dimension

measures:
  - name: total_sales
    expr: SUM(o_totalprice)
  - name: pct_of_priority_total
    expr: SUM(o_totalprice) / ANY_VALUE(priority_total_price)
```

Use `ANY_VALUE()` when the fixed LOD value is constant within a group.

## Coarser LOD (Experimental)

Uses window measures with `range: all` to exclude dimensions:

```yaml
measures:
  - name: total_sales
    expr: SUM(o_totalprice)

  - name: all_priorities_sales
    expr: SUM(o_totalprice)
    window:
      - order: order_priority
        range: all
        semiadditive: last

  - name: pct_of_total_sales
    expr: SUM(o_totalprice) / MEASURE(all_priorities_sales)
```

## When to Use

- Percentages of total (e.g., category's share of total sales).
- Comparing individual values to dataset-wide aggregates.
- Segment-level metrics constant across different groupings.
