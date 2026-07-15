# DAX to SQL Pattern Catalog

Each entry shows the DAX pattern, its UC metric view equivalent, and migration notes.

## 1. Direct Aggregations (Leaf Measures)

### SUM

```
DAX:  SUM(table[column])
SQL:  SUM(source.column)
YAML: expr: SUM(source.column)
```

### COUNT / COUNTROWS

```
DAX:  COUNTROWS(table)      or  COUNT(table[column])
SQL:  COUNT(1)               or  COUNT(source.column)
```

### DISTINCTCOUNT

```
DAX:  DISTINCTCOUNT(table[column])
SQL:  COUNT(DISTINCT source.column)
```

### AVERAGE / MIN / MAX

```
DAX:  AVERAGE(table[column])
SQL:  AVG(source.column)
```

Direct 1:1 mapping for all standard aggregation functions.

## 2. DIVIDE (Safe Division)

```
DAX:  DIVIDE([Numerator], [Denominator], 0)
YAML: expr: |
        CASE WHEN MEASURE(Denominator) = 0 THEN 0
             ELSE MEASURE(Numerator) / MEASURE(Denominator)
        END
```

`DIVIDE` returns the alternate result (3rd argument, default BLANK) when the denominator is zero. Map to `CASE WHEN`.

### 2b. Var-chain DIVIDE with filtered aggregates (INLINE every var)

The most common real-world ratio: `var`s each bind a `CALCULATE(SUMX(FILTER(fact, <pred>), fact[col]))`, and the `RETURN` is `DIVIDE(<num expr>, <den expr>)` where the operands do arithmetic on those vars (e.g. `b - c`). **Inline each var's aggregate into the DIVIDE — never emit a bare `a`/`b`/`c` identifier, and never drop the denominator.**

```
DAX:  var a = CALCULATE(SUMX(FILTER(fact_pe005, fact_pe005[matl_group] IN {"1003005","1003014"}), fact_pe005[target_value]))
      var b = CALCULATE(SUMX(FILTER(fact_pe005, fact_pe005[matl_group] IN {"1003005","1003014"}), fact_pe005[issued_value]))
      var c = CALCULATE(SUMX(FILTER(fact_pe005, fact_pe005[matl_group] IN {"1003005","1003014"}), fact_pe005[received_value]))
      return DIVIDE(a, b - c)

YAML: expr: |
        SUM(source.target_value)   FILTER (WHERE source.matl_group IN ('1003005','1003014'))
        / NULLIF(
            SUM(source.issued_value)   FILTER (WHERE source.matl_group IN ('1003005','1003014'))
          - SUM(source.received_value) FILTER (WHERE source.matl_group IN ('1003005','1003014')),
          0)
```

Rules for this shape:
- Each `var = CALCULATE(SUMX(FILTER(fact, <pred>), fact[col]))` → `SUM(source.col) FILTER (WHERE <pred>)`.
- Substitute every var into the `DIVIDE` numerator/denominator expression, preserving the arithmetic (`b - c` stays a subtraction of two filtered SUMs).
- `DIVIDE(num, den)` → `num / NULLIF(den, 0)`. Wrap a multi-term numerator/denominator in parentheses.
- **The denominator (`b - c` here) MUST appear** — a bare numerator (`SUM(target_value) FILTER(...)`) with no `/ NULLIF(...)` is WRONG and will be rejected.
- Discard slicer-scalar vars that don't feed the result (e.g. `var std = CALCULATE([F_Start_date])`).

## 3. CALCULATE with FILTER

```
DAX:  CALCULATE(SUM(table[revenue]), FILTER(table, table[status] = "Active"))
YAML: expr: SUM(source.revenue) FILTER (WHERE source.status = 'Active')
```

UC metric views support `FILTER (WHERE ...)` on individual aggregate expressions. This is the direct equivalent of `CALCULATE` with a simple `FILTER`.

## 4. CALCULATE with ALL / REMOVEFILTERS

```
DAX:  CALCULATE([Total Revenue], ALL(dim_table))
```

This removes all filter context from `dim_table`. In a metric view, this pattern is typically used for "share of total" calculations. Use **coarser LOD** with window measures:

```yaml
measures:
  - name: total_revenue
    expr: SUM(source.revenue)
  - name: total_revenue_all
    expr: SUM(source.revenue)
    window:
      - order: <dimension_to_remove>
        range: all
        semiadditive: last
  - name: share_of_total
    expr: MEASURE(total_revenue) / MEASURE(total_revenue_all)
```

## 5. SWITCH / SELECTEDVALUE (Slicer Dispatch)

```
DAX:  SWITCH(
        SELECTEDVALUE(dim_switch[value]),
        "Option A", [Measure_A],
        "Option B", [Measure_B],
        [Measure_A]
      )
```

**NOT a metric view measure.** This is display-layer logic — the user picks a slicer value and the visual changes which measure to show.

**Migration strategy**: Define each option as a separate measure in the metric view. Let the dashboard/BI tool handle the toggle.

```yaml
measures:
  - name: Measure_A
    expr: SUM(source.col_a)
  - name: Measure_B
    expr: SUM(source.col_b)
```

## 6. SWITCH(TRUE(), ...) Context Routing

```
DAX:  SWITCH(TRUE(),
        [flag] = "Current", CALCULATE(SUM(t[val]), FILTER(t, t[period] = "CY")),
        [flag] = "Prior",   CALCULATE(SUM(t[val]), FILTER(t, t[period] = "PY"))
      )
```

**Migration strategy**: Replace the flag measure with a dimension. Each branch becomes a filtered measure or the user filters by the dimension at query time.

```yaml
dimensions:
  - name: period
    expr: source.period    # "CY" or "PY"

measures:
  - name: value
    expr: SUM(source.val)
  # User queries: WHERE period = 'CY'
```

## 7. RELATED (Cross-Table Column Reference)

```
DAX:  RELATED(dim_table[attribute])
```

In PBI, `RELATED` follows a relationship to pull a column from a related table. In UC metric views, this is handled by **joins**:

```yaml
joins:
  - name: dim
    source: catalog.schema.dim_table
    'on': source.fk = dim.pk

dimensions:
  - name: attribute
    expr: dim.attribute
```

## 8. IF / IIF (Conditional)

```
DAX:  IF([Revenue] > 1000, "High", "Low")
SQL:  CASE WHEN MEASURE(Revenue) > 1000 THEN 'High' ELSE 'Low' END
```

Direct mapping to SQL `CASE WHEN`.

## 9. FORMAT (Display Formatting)

```
DAX:  FORMAT([Revenue], "$#,##0.00")
```

**Not a measure.** Use semantic metadata instead:

```yaml
format:
  type: currency
  currency_code: USD
  decimal_places:
    type: exact
    places: 2
```

## 10. Measure-to-Measure Reference

```
DAX:  [Gross Profit] / [Revenue]
YAML: expr: MEASURE(Gross_Profit) / MEASURE(Revenue)
```

Bracket references `[MeasureName]` in DAX become `MEASURE(measure_name)` in UC. The composability model is nearly identical.

## 11. BLANK() / Null Handling

```
DAX:  IF(ISBLANK([value]), 0, [value])
SQL:  COALESCE(MEASURE(value), 0)
```

DAX `BLANK()` maps to SQL `NULL`. Use `COALESCE` or `CASE WHEN ... IS NULL`.

## 12. VALUES / DISTINCT (Table Functions)

```
DAX:  COUNTROWS(VALUES(table[column]))
SQL:  COUNT(DISTINCT source.column)
```

`VALUES` returns distinct values of a column. In aggregation context, `COUNTROWS(VALUES(...))` is equivalent to `COUNT(DISTINCT ...)`.
