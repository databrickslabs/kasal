# Composability in Metric Views

Source: Databricks official documentation (`docs/web/docs/metric-views/data-modeling/composability.md`).

## Overview

Metric views are composable — you can build new definitions on top of existing ones:

- Reference previously defined dimensions in new dimensions.
- Reference any dimension or previously defined measures in new measures.
- Reference columns from joins defined in the metric view.

## Measure Types

| Type | Description | Example |
|---|---|---|
| **Atomic** | Direct aggregation on a source column. | `SUM(o_totalprice)` |
| **Composed** | Combines other measures via `MEASURE()`. | `MEASURE(Total Revenue) / MEASURE(Order Count)` |

## Example: Average Order Value (AOV)

```yaml
source: samples.tpch.orders

measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
    display_name: 'Total Revenue'

  - name: order_count
    expr: COUNT(1)
    display_name: 'Order Count'

  - name: avg_order_value
    expr: MEASURE(total_revenue) / MEASURE(order_count)
    display_name: 'Avg Order Value'
```

If `total_revenue` changes (e.g., a tax-exclusion filter is added), `avg_order_value` automatically inherits the change.

## Example: Fulfillment Rate (conditional logic)

```yaml
measures:
  - name: total_orders
    expr: COUNT(1)

  - name: fulfilled_orders
    expr: COUNT(1) FILTER (WHERE o_orderstatus = 'F')

  - name: fulfillment_rate
    expr: MEASURE(fulfilled_orders) / MEASURE(total_orders)
    display_name: 'Order Fulfillment Rate'
    format:
      type: percentage
```

## Best Practices

1. **Define atomic measures first.** Establish SUM, COUNT, AVG before derived measures.
2. **Use `MEASURE()` for consistency.** Don't repeat aggregation logic — reference existing measures.
3. **Prioritize readability.** `MEASURE(Gross Profit) / MEASURE(Total Revenue)` is clearer than inline SQL.
4. **Combine with semantic metadata.** Use `format: percentage` on ratios for downstream tools.
