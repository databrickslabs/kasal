# DAX to SQL Pattern Catalog

Each entry shows the DAX pattern, its UC metric view equivalent, and migration notes.

## 0. Output Conventions (MUST follow for EVERY `sql_expr`)

These apply on top of the per-pattern notes below.

### 0.1 Qualify source columns with `source.` — including inside FILTER

Write source-table columns as `source.<col>` **everywhere they appear**, including
inside `FILTER (WHERE ...)` clauses and `CASE WHEN` predicates. Unqualified names
are technically valid (they default to `source`), but this pipeline standardises
on the explicit `source.` prefix for consistency and review.

```
DAX:  CALCULATE(COUNTROWS(fact), fact[check_status] = 0)
SQL:  COUNT(1) FILTER (WHERE source.check_status = 0)     -- ✅ source. in FILTER
NOT:  COUNT(1) FILTER (WHERE check_status = 0)            -- ❌ unqualified
```

Columns from a **declared join** use that join's name (`<join_name>.<col>`, see
§7). Backtick names with spaces/punctuation: `` source.`Check Status` ``.

### 0.2 A metric view is SINGLE-SOURCE — never emit cross-table subqueries

The only valid column namespaces are `source` and the **declared join names**.
Do **not** emit a subquery that selects `FROM` another table, e.g.
`... IN (SELECT key FROM tech_rules_exceptions)` or
`... = (SELECT MAX(week_445_sequential) FROM param_calendar445)`. Such SQL
references a table that is neither `source` nor a declared join and will not
deploy.

If a measure genuinely needs a table that is neither `source` nor a declared
join — e.g. a separate calendar for a max-date / time-intelligence lookup, or an
exceptions list for a `NOT IN` filter — it is **not expressible as-is**. Return
`success=false` with `dax_class="architecture_change"` and say what the source
view would need (e.g. "precompute max_available_date per week grain as a column",
"materialise the exceptions flag on the fact"). Never fabricate a cross-table
subquery to force a translation.

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
- **Filters on a JOINED dimension keep the join alias — do NOT rewrite to `source.`.**
  When the `FILTER` targets a *different table* than the aggregated fact —
  `SUMX(fact_pe002, fact_pe002[epl]), FILTER(Dim_wkctr, Dim_wkctr[bic_cwc_type] IN {…})`
  — the aggregate is on `source` but the predicate is on the joined dim, so the
  filter column stays qualified by the dim's join alias:

  ```
  DAX:  var a = CALCULATE(SUMX(fact_pe002, fact_pe002[epl]),
                          FILTER(Dim_wkctr, Dim_wkctr[bic_cwc_type] IN {"APET","CAN","PET"}))
        var b = CALCULATE(SUMX(fact_pe002, fact_pe002[paid_hours]),
                          FILTER(Dim_wkctr, Dim_wkctr[bic_cwc_type] IN {"APET","CAN","PET"}))
        return DIVIDE(a, b)

  YAML: expr: |
          SUM(source.epl)        FILTER (WHERE dim_wkctr.bic_cwc_type IN ('APET','CAN','PET'))
          / NULLIF(
              SUM(source.paid_hours) FILTER (WHERE dim_wkctr.bic_cwc_type IN ('APET','CAN','PET')),
              0)
  ```

  The join to `dim_wkctr` must exist in the view's `joins:`. `Dim_wkctr[CWC_Filter]=1`
  → `dim_wkctr.cwc_filter = 1` likewise. Only fact-column filters map to `source.`.

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

---

# Measure-shape recipes (daxpatterns.com)

The sections above are the shapes seen most in the wild. The following are the
standard analytical *patterns* from daxpatterns.com — each is a whole measure
shape rather than a single function. Most reduce to a **source-view precompute**
(a `GROUP BY` / `OVER (...)` in the `source:` SELECT, exposed as a dimension or a
pre-aggregated column) that a plain measure then sums. That precompute is the
`dax_class="architecture_change"` route: emittable, but it reshapes the source.
For per-function mappings, see `FUNCTION_REFERENCE.md`.

## 15. Static segmentation (fixed bands)

Fixed price/age/size bands off a single row value → a `CASE` **dimension** (not a
measure). The band is known without aggregating.

```
DAX:  Price Band := SWITCH(TRUE(), [Price]<10,"Low", [Price]<50,"Mid", "High")

YAML: dimensions:
        - name: price_band
          expr: |
            CASE WHEN source.price < 10 THEN 'Low'
                 WHEN source.price < 50 THEN 'Mid'
                 ELSE 'High' END
```

This is the ONE segmentation case that is a clean `translatable_direct` — it's a
row-level bucket. Contrast with §16.

## 16. Dynamic segmentation (band on an aggregated value)

Count/measure entities whose **aggregated** value falls in a band (e.g. "customers
whose total sales > 1000"). The band depends on an aggregate, so the row-level
`CASE` of §15 is wrong — you must pre-aggregate to the entity grain first, then
bucket. That is a source-view precompute.

```
DAX:  Big Customers :=
        CALCULATE(DISTINCTCOUNT(Cust[Id]), FILTER(VALUES(Cust[Id]), [Sales] > 1000))

YAML: version: '1.1'
      source: |
        SELECT cust_id, SUM(amount) AS cust_sales
        FROM <catalog>.<schema>.sales
        GROUP BY cust_id
      dimensions:
        - name: sales_band
          expr: CASE WHEN source.cust_sales > 1000 THEN 'Big' ELSE 'Small' END
      measures:
        - name: customers
          expr: COUNT(DISTINCT source.cust_id)          -- filter band at query time
```

Rule: the entity you count (`cust_id`) becomes the `GROUP BY` grain of the source
precompute; the threshold measure (`[Sales]`) becomes the aggregate in that SELECT.
Never approximate with a row-level `CASE` on the raw fact — it buckets rows, not
entities.

## 17. New & returning customers (first-purchase logic)

"New in period" = entities whose FIRST-ever event falls in the period. `MIN() OVER`
per entity in the source view exposes the first-event date; the measure then
counts by comparing it to the row's period.

```
DAX:  New Customers :=
        CALCULATE(DISTINCTCOUNT(Sales[Cust]),
          FILTER(VALUES(Sales[Cust]), [First Order Date] IN <current period>))

YAML: source: |
        SELECT *, MIN(order_date) OVER (PARTITION BY cust_id) AS first_order_date
        FROM <catalog>.<schema>.sales
      measures:
        - name: new_customers
          expr: |
            COUNT(DISTINCT source.cust_id)
            FILTER (WHERE date_trunc('month', source.order_date)
                      = date_trunc('month', source.first_order_date))
        - name: returning_customers
          expr: |
            COUNT(DISTINCT source.cust_id)
            FILTER (WHERE date_trunc('month', source.order_date)
                      > date_trunc('month', source.first_order_date))
```

The `MIN(...) OVER (PARTITION BY entity)` first-event column is the reusable
building block for new/returning/reactivated/churn variants.

## 18. Events in progress (active-at-a-point-in-time)

Count events open at each date: `start <= d AND (end >= d OR end IS NULL)`. This is
a **range-join to a date spine**, not a fact aggregate — the fact has one row per
event, but you count it once per active day. Requires a `dim_date` join.

```
DAX:  Open := CALCULATE(COUNTROWS(Ev),
        FILTER(Ev, Ev[Start] <= MAX('Date'[Date]) && Ev[End] >= MIN('Date'[Date])))

YAML: joins:
        - name: cal
          source: <catalog>.<schema>.dim_date
          'on': source.start_date <= cal.d AND (source.end_date >= cal.d OR source.end_date IS NULL)
      measures:
        - name: events_in_progress
          expr: COUNT(1)          # grouped by cal.d at query time
```

If the range-join is not acceptable (fan-out concerns), route to
`architecture_change` and describe a source-view spine explode instead.

## 19. Like-for-like / same-store

Compare only entities present in **both** the current and prior period. The
"present in both" set is an intersection — expressible as a `FILTER (WHERE …)` only
if a "present-in-prior" flag is precomputed on the fact; otherwise it needs a
source-view self-join.

```
DAX:  Same-Store Sales := CALCULATE([Sales],
        FILTER(VALUES(Store[Id]), <store active in both periods>))

YAML: source: |
        SELECT s.*,
          MAX(CASE WHEN period = :prev THEN 1 ELSE 0 END) OVER (PARTITION BY store_id) AS in_prev,
          MAX(CASE WHEN period = :cur  THEN 1 ELSE 0 END) OVER (PARTITION BY store_id) AS in_cur
        FROM <catalog>.<schema>.sales s
      measures:
        - name: like_for_like_sales
          expr: SUM(source.amount) FILTER (WHERE source.in_prev = 1 AND source.in_cur = 1)
```

`dax_class="architecture_change"` — the `in_prev`/`in_cur` flags are the source
reshape. Pair with the period-offset measure (§ time intelligence) for the
comparison itself.

## 20. Ranking / TOPN

`RANKX` / `TOPN` rank-and-slice; metric views aggregate. See
`FUNCTION_REFERENCE.md §2` (RANKX) and `UNSUPPORTED.md` (TOPN). Simple "rank by a
measure" belongs in the **dashboard/visual** layer; a stored top-N needs a
source-view `ROW_NUMBER() OVER (ORDER BY … DESC)` (or `QUALIFY`) exposed as a
column. Do not emit a bare `MAX`/aggregate in place of a rank — it changes the
semantics.
