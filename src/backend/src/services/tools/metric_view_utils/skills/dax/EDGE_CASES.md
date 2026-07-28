# DAX Translation Edge Cases and Gotchas

## 1. BLANK() vs NULL Semantics

DAX `BLANK()` propagates differently than SQL `NULL`:
- In DAX: `BLANK() + 1 = 1` (BLANK is treated as 0 in arithmetic).
- In SQL: `NULL + 1 = NULL`.

**Impact**: Measures that rely on BLANK arithmetic may produce different results in SQL. If the DAX uses `BLANK()` as a sentinel value in arithmetic, add `COALESCE(expr, 0)` in the SQL translation.

## 2. DIVIDE vs SQL Division

- `DIVIDE(a, b)` returns `BLANK()` when `b = 0`.
- `DIVIDE(a, b, alt)` returns `alt` when `b = 0`.
- SQL `a / b` throws an error or returns NULL when `b = 0`.

Always translate `DIVIDE` to `CASE WHEN ... = 0 THEN ... ELSE ... / ... END`.

## 3. Filter Context vs SQL WHERE

DAX `CALCULATE` modifies the filter context for a single measure evaluation. The closest SQL equivalent is `FILTER (WHERE ...)` on aggregate expressions. However:

- `CALCULATE` with `ALL()` removes ALL existing filters — not just one column.
- `CALCULATE` with multiple filter arguments applies them as AND.
- Nested `CALCULATE` expressions replace (not AND) filters on the same column.

For complex `CALCULATE` chains, translate conservatively and flag for review.

## 4. Row Context vs No Row Context

DAX measures always evaluate in a filter context (aggregation). Calculated columns evaluate in a row context (per-row). Metric view measures are aggregations only — they have no row-context equivalent.

If a DAX measure uses `EARLIER()`, `LOOKUPVALUE()`, or iterator functions like `SUMX` with row-level expressions, the translation may require a pre-computed column in the source view.

## 5. SUMX / AVERAGEX / MINX / MAXX (Iterator Functions)

```
DAX:  SUMX(table, table[price] * table[quantity])
SQL:  SUM(source.price * source.quantity)
```

Simple iterator functions translate directly because the inner expression is per-row. Complex iterators with nested `CALCULATE` or `FILTER` require careful decomposition.

## 6. YTD/non-YTD Table Pairs

A common PBI pattern: separate fact tables for full-year and year-to-date data (`cst_c8` vs `cst_c8_ytd`). The DAX uses a flag measure to switch between them.

**Migration strategy**: UNION both tables into a single source view with a discriminator column (`is_ytd`), then expose as a dimension. Eliminates the switching logic.

## 7. Exchange Rate / Scaling Factor Scalars

PBI models often read slicer-controlled scalars:
```
DAX:  MAX(setup_scaling[Scale])    -- returns 1000 or 1000000
DAX:  MAX(dim_exchange_rates[currency_rate])
```

These are single-value reads from embedded config tables controlled by slicers.

**Migration strategy**: Express measures in raw units in the metric view. Handle unit conversion (thousands, millions, currency) in the dashboard layer. If a fixed exchange rate is needed, bake it into the source view.

## 8. Commented-Out DAX

PBI models sometimes contain `--IF(...)` commented-out guards within measure expressions. Do NOT translate the commented-out code — verify with stakeholders whether the disabled logic is intentional.

## 9. Magic Numbers

DAX expressions sometimes contain hardcoded constants:
```
DAX:  SUM('FACT OM DATA'[Turnover]) * 7.44989942
```

These are typically fixed exchange rates (e.g., EUR to DKK). Document the constant and its likely meaning. Flag for stakeholder verification.

## 10. Measure Name Collisions

PBI allows the same measure name across different tables. UC metric views have a flat namespace — all dimensions and measures must have unique names within a single metric view.

When merging measures from multiple PBI tables into one metric view, prefix with the domain (e.g., `cs_value_cy`, `rcs_value_cy`).

## 11. Circular Measure Dependencies

DAX allows measures to reference each other in ways that appear circular but resolve through filter context evaluation order. UC metric views require a strict DAG — no circular references.

If the dependency graph tool detects cycles, the measures likely use implicit filter-context resolution that needs manual decomposition.
