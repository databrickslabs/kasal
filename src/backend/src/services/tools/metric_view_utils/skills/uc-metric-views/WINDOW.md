# Window Measures in Metric Views (Experimental)

Source: Databricks official documentation (`docs/web/docs/metric-views/data-modeling/window-measures.md`).

## Definition

Window measures enable windowed, cumulative, or semiadditive aggregations. Required fields:

- **`order`**: The dimension that determines window ordering.
- **`range`**: Extent of the window:
  - `current` — rows where ordering value equals current row.
  - `cumulative` — all rows where ordering value <= current row.
  - `trailing <N> <unit>` — rows going backward N units (excludes current unit).
  - `leading <N> <unit>` — rows going forward N units.
  - `all` — all rows regardless of ordering value.
- **`semiadditive`**: How to summarize when the order field is not in GROUP BY: `first` or `last`.

## Trailing / Moving Window (7-day)

```yaml
measures:
  - name: t7d_customers
    expr: COUNT(DISTINCT o_custkey)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
```

## Period-over-Period Growth

```yaml
measures:
  - name: previous_day_sales
    expr: SUM(o_totalprice)
    window:
      - order: date
        range: trailing 1 day
        semiadditive: last

  - name: current_day_sales
    expr: SUM(o_totalprice)
    window:
      - order: date
        range: current
        semiadditive: last

  - name: day_over_day_growth
    expr: (MEASURE(current_day_sales) - MEASURE(previous_day_sales)) / MEASURE(previous_day_sales) * 100
```

## Cumulative (Running) Total

```yaml
measures:
  - name: running_total_sales
    expr: SUM(o_totalprice)
    window:
      - order: date
        range: cumulative
        semiadditive: last
```

## Year-to-Date (Period-to-Date)

```yaml
dimensions:
  - name: date
    expr: o_orderdate
  - name: year
    expr: DATE_TRUNC('year', o_orderdate)

measures:
  - name: ytd_sales
    expr: SUM(o_totalprice)
    window:
      - order: date
        range: cumulative
        semiadditive: last
      - order: year
        range: current
        semiadditive: last
```

## Semiadditive Measure (Bank Balance)

```yaml
measures:
  - name: semiadditive_balance
    expr: SUM(balance)
    window:
      - order: date
        range: current
        semiadditive: last
```

Still sums over all customers within a single day — `semiadditive: last` only applies when aggregating across days.

## MEASURE()-Based Window Variants

Define a base atomic measure, then create windowed variants that reference it.
This avoids duplicating the aggregation expression across multiple window measures:

```yaml
measures:
  - name: active_users
    expr: COUNT(DISTINCT user_id)

  - name: t7d_active_users
    expr: MEASURE(active_users)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last

  - name: t28d_active_users
    expr: MEASURE(active_users)
    window:
      - order: date
        range: trailing 28 day
        semiadditive: last
```

**Important**: Percentage-change or growth measures must NOT be window measures
themselves. They should be non-window composed measures that reference window
measures via `MEASURE()`:

```yaml
  - name: user_growth_pct
    expr: |
      CASE WHEN MEASURE(t28d_active_users) = 0 THEN NULL
           ELSE (MEASURE(t7d_active_users) - MEASURE(t28d_active_users))
                / MEASURE(t28d_active_users) * 100
      END
```

## Caveats

- Window measures are **Experimental** — behavior may change.
- `range: all` (coarser LOD) is experimental and may not be fully functional
  on all DBR versions. Test before depending on it in production.
- Use `version: 1.1` for metric views with window measures to access semantic
  metadata fields (`display_name`, `format`).
