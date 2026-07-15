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

### 2b. Var-chain filtered aggregates — INLINE every var (ratio OR plain arithmetic)

The most common real-world shape: `var`s each bind a `CALCULATE(SUMX(FILTER(fact, <pred>), fact[col]))` (or `CALCULATE([Measure], <pred>)`), and the `RETURN` combines those vars with arithmetic. This covers **both** a `DIVIDE(...)` ratio **and** a plain `a - b` / `a + b` / `(a + b) / c` expression with no DIVIDE at all. **Inline each var's aggregate into the RETURN expression — never emit a bare `a`/`b`/`c` identifier, and never drop a term.**

**Ratio form (`return DIVIDE(a, b - c)`):**

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

**Plain-arithmetic form (`return a - b`, NO DIVIDE) — this is just as common and MUST keep every term:**

```
DAX:  var std = CALCULATE([F_Start_date])
      var a = CALCULATE(SUMX(FILTER(FT_BPC003, FT_BPC003[bic_chversion]="0000" && FT_BPC003[fis_code_parent] IN {"DCD2","DHF2"}), FT_BPC003[value]))
      var b = CALCULATE(SUMX(FILTER(FT_BPC003, FT_BPC003[bic_chversion]="0000" && FT_BPC003[fis_code_parent] IN {"DHHX"}), FT_BPC003[value]))
      return a - b

YAML: expr: |
        SUM(source.value) FILTER (WHERE bic_chversion = '0000' AND fis_code_parent IN ('DCD2','DHF2'))
      - SUM(source.value) FILTER (WHERE bic_chversion = '0000' AND fis_code_parent IN ('DHHX'))
```

Rules for this shape (apply to BOTH forms):
- Each `var = CALCULATE(SUMX(FILTER(fact, <pred>), fact[col]))` → `SUM(source.col) FILTER (WHERE <pred>)`.
- Substitute **every** var into the `RETURN` expression, preserving **all** arithmetic operators (`a - b` stays a subtraction of two filtered SUMs; `a + b` a sum; `(a+b)/c` the full ratio).
- `DIVIDE(num, den)` → `num / NULLIF(den, 0)`. Wrap a multi-term numerator/denominator in parentheses.
- **Every term MUST appear.** Emitting only the first block (`SUM(...) FILTER(...)` for `a` alone) when the DAX says `a - b` is WRONG and will be rejected — the `- b` term is silently missing and the number is wrong. This applies whether or not there is a DIVIDE.
- Discard slicer-scalar vars that don't feed the result (e.g. `var std = CALCULATE([F_Start_date])`, `var etd = CALCULATE([F_End_date])`).

## 3. CALCULATE with FILTER

```
DAX:  CALCULATE(SUM(table[revenue]), FILTER(table, table[status] = "Active"))
YAML: expr: SUM(source.revenue) FILTER (WHERE source.status = 'Active')
```

UC metric views support `FILTER (WHERE ...)` on individual aggregate expressions. This is the direct equivalent of `CALCULATE` with a simple `FILTER`.

## 4. CALCULATE with ALL / ALLSELECTED / REMOVEFILTERS (share-of-total)

`ALL(dim)` / `ALLSELECTED(dim)` inside a `CALCULATE` removes filter context from
`dim`, producing a "total across dim" value. By far the most common real use is a
**share-of-total ratio**: a measure divided by that same measure evaluated with
the dimension's filter removed.

### 4a. Share-of-total ratio — `DIVIDE([M], CALCULATE([M], ALL(dim)))`

This is the #1 ALL-family shape in the wild. Translate the "all" side to a
**coarser-LOD window measure** (`range: all` over the removed dimension), then
compose the ratio as a NON-window measure via `MEASURE()`.

```
DAX:  Category Share := DIVIDE([Sales], CALCULATE([Sales], ALL(dim_product[category])))

YAML: version: '1.1'
      measures:
        - name: sales
          expr: SUM(source.amount)
        - name: sales_all_category           # the ALL(...) side
          expr: MEASURE(sales)
          window:
            - order: category                # the dimension ALL() removed
              range: all
              semiadditive: last
        - name: category_share
          expr: |
            CASE WHEN MEASURE(sales_all_category) = 0 THEN NULL
                 ELSE MEASURE(sales) / MEASURE(sales_all_category) END
```

Rules:
- The dimension inside `ALL(...)` becomes the window's `order:` (it must exist as
  a dimension in the view). `ALL(table)` (whole table, no column) → use the
  primary grain dimension of that table.
- The ratio measure MUST be a plain (non-window) measure referencing the base and
  the `_all_*` window measure via `MEASURE()` — never put `window:` on the ratio.
- `DIVIDE` → the `CASE WHEN … = 0 THEN NULL ELSE … END` guard (§2), not bare `/`.

### 4b. ALLSELECTED — same shape, with a fidelity caveat

`ALLSELECTED(dim)` removes the dimension's filter **but respects the outer
slicer/visual selection** — a concept metric views do not have. Translate it the
SAME way as `ALL` (a `range: all` window), but this is an **approximation**: it
computes the total over ALL rows, not "all rows within the current slicer scope."

```
DAX:  SS_MS_Actual := DIVIDE([KBI_Actual], CALCULATE([KBI_Actual], ALLSELECTED(Serve[Single/Multi Serve])))
```

→ same YAML shape as 4a. Add a comment noting the approximation so a reviewer
knows the slicer-scope nuance was flattened. If the visual scope matters for
correctness, flag as REVIEW rather than emitting silently.

### 4c. What is NOT a clean share-of-total (do NOT force it)

- **`ALLEXCEPT(table, keep_col, …)`** — keeps some filters, drops the rest. This
  is a coarser-LOD over *multiple* remaining dimensions; not a single `range: all`
  window. Leave as a documented TODO unless it reduces to exactly one removed dim.
- **`ALL(dim)` as a slicer-context reset** next to an equality filter
  (`CALCULATE([M], Dim_KBI[kbi]="Volume", ALL(Dim_KBI))`) — the `ALL` is undoing a
  slicer, not building a total. Translate the equality filter as a normal
  `FILTER (WHERE …)` and drop the `ALL(Dim_KBI)` reset (it has no metric-view
  meaning). Do NOT emit a window here.
- **`ALL(Dates)` + DATESINPERIOD / time-intelligence** — that is time-intel
  (§ time intelligence rules), not share-of-total.

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

## 13. SUMX(SUMMARIZE(...)) — group-then-aggregate (fixed LOD in the source view)

`SUMX(SUMMARIZE(fact, colA, colB), <row expr>)` groups the fact to the
(`colA`, `colB`) grain, evaluates `<row expr>` **once per group**, then sums the
results. A metric view cannot build a virtual grouped table inline — but the
grouped pre-aggregate can be **materialized in the `source:` SELECT** with
`GROUP BY`, exposed as an identity dimension, and then summed.

```
DAX:  Mat Contribution :=
        DIVIDE(
          SUMX(SUMMARIZE(FT_PE009, FT_PE009[comp_code], FT_PE009[material]),
               [Mat contr per pack] * CALCULATE(SUM(FT_PE009[sales_metal_hidden]))),
          1000000)

YAML: version: '1.1'
      source: |
        SELECT
          comp_code, material,
          -- per-(comp_code, material) group value, computed once per group:
          SUM(mat_contr_per_pack * sales_metal_hidden) AS grp_mat_contribution
        FROM <catalog>.<schema>.ft_pe009
        GROUP BY comp_code, material
      dimensions:
        - name: comp_code
          expr: comp_code
        - name: material
          expr: material
      measures:
        - name: mat_contribution
          expr: SUM(source.grp_mat_contribution) / 1000000
```

Rules:
- The `SUMMARIZE(fact, colA, colB)` grain → the source `GROUP BY colA, colB`.
- The per-row expression inside `SUMX` → the aggregate inside the grouped SELECT.
- The outer `SUMX(...)` → `SUM(source.<grouped_col>)` in the measure.
- If the row expression itself references OTHER measures with their own filter
  context (nested CALCULATE beyond a simple SUM), it may not reduce to a single
  GROUP BY — then treat as UNSUPPORTED (source-view precompute needed, flag it).

## 14. ALLEXCEPT(table, keep_col) — fixed LOD at one grain

`ALLEXCEPT(table, keep_col)` removes ALL filter context on `table` **except**
`keep_col` — i.e. "aggregate at the `keep_col` grain, ignoring every other
filter." When exactly ONE column is kept, this is a coarser-LOD window measure
ordered on the NON-kept dimensions (or, equivalently, a fixed LOD at `keep_col`).

```
DAX:  Year Weight := CALCULATE(SUM('Face Time'[Weight]), ALLEXCEPT('Face Time', 'Face Time'[Year]))

YAML: version: '1.1'
      measures:
        - name: weight
          expr: SUM(source.weight)
        - name: weight_by_year        # fixed at the Year grain
          expr: MEASURE(weight)
          window:
            - order: year
              range: all              # remove all dims EXCEPT the ordering grain
              semiadditive: last
```

Rules:
- **ONE kept column** → the fixed-LOD/window pattern above (the kept col is the
  retained grain). This is ~63% of real ALLEXCEPT usage.
- **TWO OR MORE kept columns** → a multi-dimension fixed LOD; not a single
  window. Precompute in the source view with
  `SUM(...) OVER (PARTITION BY keep_col1, keep_col2)` as an identity dimension, or
  flag UNSUPPORTED. Do NOT approximate with a single `range: all` window — it
  would collapse the wrong dimensions.
