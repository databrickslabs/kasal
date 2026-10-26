# DAX Function Reference → UC Metric View SQL

Function-by-function map of the Microsoft DAX function reference to a **UC-metric-view-legal**
Spark SQL expression. This is the *vocabulary* layer — `PATTERNS.md` teaches the common measure
*shapes* (CALCULATE/DIVIDE/ALL/SWITCH/time-intel); this file tells you what any individual DAX
function becomes inside a measure `expr`, and — just as important — **which functions are NOT a
measure at all** and must be declined with the right `dax_class`.

> **These SQL forms are already rewritten for this pipeline.** The generic "DAX→SQL" tables you may
> have seen elsewhere emit unqualified columns and cross-table subqueries (e.g. `LOOKUPVALUE` →
> `(SELECT … FROM other_table)`). Those **do not deploy here.** Every form below obeys §0.

## 0. The four rules that override any per-function mapping

1. **`source.`-qualify every fact column**, including inside `FILTER (WHERE …)` and `CASE WHEN`
   predicates. Joined-dimension columns use the join alias (`dim.col`). (See `PATTERNS.md §0.1`.)
2. **Single-source only.** A measure `expr` may reference `source` and declared join aliases —
   never `SELECT … FROM another_table`. If a function's only faithful form needs another table, a
   recursive CTE, a self-join, or `ROW_NUMBER`-then-slice, it is **not expressible in a measure
   expr**: return `success=false`, `dax_class="architecture_change"`, and name the source-view
   precompute needed. (See `PATTERNS.md §0.2`.)
3. **A measure aggregates; it does not rank, slice, or format.** Functions that pick rows
   (`TOPN`, `FIRST`, `INDEX`), rank inline (`RANKX`), or build display text/labels
   (`FORMAT`, `CONCATENATEX`, `NAMEOF`) are not aggregatable measures — decline them, do not
   force an aggregate that changes the semantics.
4. **You cannot create UDFs or tables here.** When a function's only equivalent is "a SQL/Python
   UDF" (most `Financial`, statistical *distributions*), you may call one **only if it already
   exists** in UC (`my_udf(source.col)`); otherwise return `architecture_change` naming the UDF, or
   inline the closed-form arithmetic when one exists (below). Never invent a UDF name.

**Feasibility → `dax_class` cheat sheet** (how the buckets map to the output contract):

| Function feasibility | Typical `dax_class` | Meaning |
|---|---|---|
| Direct scalar / aggregate | `translatable_direct` | 1:1 or simple arithmetic on `source.` columns |
| Ratio / references a measure | `composed` | uses `MEASURE(...)` |
| Needs a `FILTER (WHERE …)` | `filtered` | CALCULATE/FILTER context |
| Needs window / recursive CTE / self-join / source precompute | `architecture_change` | still emittable once the source view is reshaped |
| FORMAT / label / color / slicer-dispatch | `display_layer` | not a metric |
| Visual-calc / model-only, no equivalent | `unsupported` | flag, do not fabricate |

---

## 1. Aggregation

Direct 1:1 — `expr: <SQL>(source.<col>)`:

| DAX | SQL | `dax_class` |
|---|---|---|
| `SUM` / `AVERAGE` / `MIN` / `MAX` / `COUNT` | `SUM` / `AVG` / `MIN` / `MAX` / `COUNT` | `translatable_direct` |
| `COUNTA(col)` / `COUNTROWS(t)` | `COUNT(source.col)` / `COUNT(1)` | `translatable_direct` |
| `COUNTBLANK(col)` | `COUNT(1) - COUNT(source.col)` | `translatable_direct` |
| `DISTINCTCOUNT` / `DISTINCTCOUNTNOBLANK` | `COUNT(DISTINCT source.col)` | `translatable_direct` |

**X-iterators** (`SUMX`/`AVERAGEX`/`MINX`/`MAXX`/`COUNTX`): the inner expression is per-row, so it
collapses into the aggregate — **no subquery**:

```
DAX:  SUMX(sales, sales[price] * sales[qty])
SQL:  SUM(source.price * source.qty)
```

With a `FILTER`, the predicate becomes a `FILTER (WHERE …)` (this is the bread-and-butter shape,
see `PATTERNS.md §2b`):

```
DAX:  AVERAGEX(FILTER(sales, sales[region]="US"), sales[margin])
SQL:  AVG(source.margin) FILTER (WHERE source.region = 'US')
```

**`PRODUCT` / `PRODUCTX`** — no native product aggregate; use the log-sum-exp identity (valid only
for strictly-positive values, else `architecture_change`):

```
DAX:  PRODUCTX(sales, sales[factor])
SQL:  EXP(SUM(LN(source.factor)))          -- factor > 0 only
```

**`APPROXIMATEDISTINCTCOUNT`** → `approx_count_distinct(source.col)` (`translatable_direct`).

---

## 2. Statistical aggregates  (these DO appear in real measures — translate them)

These are genuine aggregates and belong in a measure `expr` directly.

| DAX | SQL (measure `expr`) | `dax_class` |
|---|---|---|
| `MEDIAN` / `MEDIANX` | `median(source.col)` | `translatable_direct` |
| `PERCENTILE.INC` / `PERCENTILEX.INC` | `percentile(source.col, 0.75)` | `translatable_direct` |
| `PERCENTILE.EXC` / `PERCENTILEX.EXC` | `percentile_disc(0.75) WITHIN GROUP (ORDER BY source.col)` | `translatable_direct` |
| `STDEV.S` / `STDEVX.S` | `stddev_samp(source.col)` | `translatable_direct` |
| `STDEV.P` / `STDEVX.P` | `stddev_pop(source.col)` | `translatable_direct` |
| `VAR.S` / `VARX.S` | `var_samp(source.col)` | `translatable_direct` |
| `VAR.P` / `VARX.P` | `var_pop(source.col)` | `translatable_direct` |
| `GEOMEAN` / `GEOMEANX` | `EXP(AVG(LN(source.col)))` (col > 0) | `translatable_direct` |

Large columns: `approx_percentile(source.col, 0.5)` is a cheaper `MEDIAN`.

**`RANKX` / `RANK.EQ` — NOT a plain measure.** Ranking a measure across a dimension is a window
op, and metric views aggregate rather than rank-and-slice inline. Route it:

```
DAX:  RANKX(ALL(Product[Name]), [Sales])
→ dax_class="architecture_change"
  explanation: "Ranking across a dimension. Either rank in the dashboard/visual layer, or
   precompute in the source view: RANK() OVER (ORDER BY SUM(amount) DESC) at the product grain,
   then expose the rank as a dimension."
```

**Distributions** (`NORM.DIST`, `BETA.INV`, `CHISQ.*`, `T.*`, `POISSON.DIST`, `CONFIDENCE.*`,
`LINEST`, `PERMUT`, `COMBIN`): no native SQL. Call an existing UC UDF if one is registered
(`normdist(source.x, …)`); otherwise `architecture_change` naming the UDF. These almost never
appear as BI measures.

---

## 3. Math & trig

Same-named, direct 1:1 → `translatable_direct` (write `source.` on any column arg):

`ABS ACOS ACOSH ASIN ASINH ATAN ATANH COS COSH SIN SINH TAN TANH EXP LN LOG10 PI POWER SQRT SIGN
DEGREES RADIANS RAND ROUND TRUNC FLOOR CEILING MOD` → identical names.

Non-obvious mappings:

| DAX | SQL | Note |
|---|---|---|
| `DIVIDE(a, b)` / `DIVIDE(a,b,alt)` | `a / NULLIF(b, 0)` / `CASE WHEN b=0 THEN alt ELSE a/b END` | never bare `/`; see `EDGE_CASES §2` |
| `QUOTIENT(a, b)` | `DIV(source.a, source.b)` | integer division |
| `LOG(n, base)` | `LOG(base, n)` | **argument order flips** |
| `POWER(x, y)` / `x ^ y` | `POWER(source.x, source.y)` | |
| `INT` / `ROUNDDOWN` | `FLOOR` | |
| `ROUNDUP` | `CEIL` | |
| `CEILING(x, sig)` / `FLOOR(x, sig)` | `CEIL(source.x/sig)*sig` / `FLOOR(source.x/sig)*sig` | DAX takes a significance arg |
| `ACOT(x)` / `COT(x)` | `ATAN(1.0/source.x)` / `1.0/TAN(source.x)` | no native cot |
| `FACT(n)` | `FACTORIAL(source.n)` | |
| `CURRENCY(x)` | `CAST(source.x AS DECIMAL(19,4))` | |
| `CONVERT(x, TYPE)` | `CAST(source.x AS <type>)` | |

No native equivalent → arithmetic UDF or `architecture_change`: `GCD`, `LCM`, `MROUND`, `EVEN`,
`ODD`, `SQRTPI` (`SQRT(source.x*PI())` works), `RANDBETWEEN` (`FLOOR(RAND()*(hi-lo+1))+lo`).

---

## 4. Text

Direct 1:1 → `translatable_direct`:

| DAX | SQL | | DAX | SQL |
|---|---|---|---|---|
| `LEN` | `LENGTH(source.col)` | | `UPPER`/`LOWER` | `UPPER`/`LOWER` |
| `LEFT(c,n)` | `LEFT(source.c, n)` | | `RIGHT(c,n)` | `RIGHT(source.c, n)` |
| `MID(c,s,n)` | `SUBSTR(source.c, s, n)` | | `TRIM` | `TRIM(source.col)` |
| `SUBSTITUTE(c,o,n)` | `REPLACE(source.c,'o','n')` | | `REPT(s,n)` | `REPEAT('s', n)` |
| `EXACT(a,b)` | `source.a = source.b` | | `CONCATENATE(a,b)` | `CONCAT(source.a, source.b)` |
| `UNICHAR` / `UNICODE` | `CHR` / `ASCII` | | `VALUE(s)` | `TRY_CAST(source.s AS DOUBLE)` |
| `COMBINEVALUES(d,a,b)` | `CONCAT_WS('d', source.a, source.b)` | | | |

Traps (get these right):

- **`FIND(needle, hay)` / `SEARCH(needle, hay)`** → `INSTR(source.hay, 'needle')` — note the
  **argument order swaps** (DAX is needle-first, `INSTR` is haystack-first). `FIND` is
  case-sensitive; `SEARCH` is case-insensitive → wrap both sides in `LOWER(...)`.
- **`REPLACE(text, start, len, new)`** (positional) → `OVERLAY(source.text PLACING 'new' FROM start FOR len)`.
  Do **not** confuse with DAX `SUBSTITUTE` (value-based) → SQL `REPLACE`.
- **`FORMAT(x, "pattern")`** → this is display formatting, **not a measure**. Emit semantic
  metadata, not a `FORMAT` call (see `PATTERNS.md §9`). `dax_class="display_layer"`.
- **`FIXED(x, d)`** → `format_number(source.x, d)` returns a *string*; if the intent is a rounded
  number use `ROUND(source.x, d)` and flag the ambiguity.

**`CONCATENATEX(t, expr, delim)`** — string aggregation across rows. This builds a label, not a
metric. `dax_class="display_layer"`, skip (see `UNSUPPORTED.md`). Only if a genuine delimited-list
*value* is required: `array_join(collect_list(source.col), 'delim')` — but confirm it's a real
metric first.

---

## 5. Logical & Information

**Logical** — direct → `translatable_direct`:

| DAX | SQL |
|---|---|
| `IF(c, t, f)` / `IF.EAGER` | `CASE WHEN c THEN t ELSE f END` |
| `SWITCH(x, v1, r1, …, else)` | `CASE x WHEN v1 THEN r1 … ELSE else END` (value form) |
| `AND`/`OR`/`NOT` | `AND`/`OR`/`NOT` |
| `COALESCE` | `COALESCE` |
| `TRUE`/`FALSE` | `true`/`false` |
| `BITAND/BITOR/BITXOR` | `&` / `\|` / `^` |
| `BITLSHIFT/BITRSHIFT` | `shiftleft` / `shiftright` |
| `IFERROR(a, alt)` | `COALESCE(try(a), alt)` |

> `SWITCH(TRUE(), …)` and `SWITCH` over a slicer selection are **display-layer dispatch**, not a
> value form — see `PATTERNS.md §5/§6`. Only the value form (`SWITCH(col, …)`) maps to `CASE`.

**Information** — value tests are direct; context tests are not:

| DAX | SQL / action | `dax_class` |
|---|---|---|
| `ISBLANK(x)` | `source.x IS NULL` | `translatable_direct` |
| `ISERROR(a)` | `try(a) IS NULL` | `translatable_direct` |
| `ISEVEN/ISODD` | `source.n % 2 = 0` / `!= 0` | `translatable_direct` |
| `ISNUMBER/ISTEXT/ISLOGICAL/…` (type tests) | `typeof(source.col) = 'int'` etc. | `translatable_direct` |
| `CONTAINSSTRING(h, n)` | `source.h ILIKE '%n%'` | `translatable_direct` |
| `CONTAINSSTRINGEXACT(h, n)` | `source.h LIKE '%n%'` | `translatable_direct` |
| `USERNAME/USERPRINCIPALNAME` | `current_user()` | `translatable_direct` (RLS: prefer UC row policies) |
| `HASONEVALUE/HASONEFILTER/ISFILTERED/ISINSCOPE/ISCROSSFILTER` | filter-context tests — no metric-view meaning | `unsupported` / skip guard |
| `SELECTEDMEASURE*`, `ISSELECTEDMEASURE` | calculation-item context | `display_layer` |
| `NAMEOF`, `USEROBJECTID`, `CUSTOMDATA` | metadata / session | `unsupported` |

`CONTAINS`/`CONTAINSROW` (table membership tests) → an `EXISTS (SELECT … FROM other_table)` is
**forbidden** (§0.2). If the target is `source`/a join it's a `FILTER (WHERE …)`; otherwise
`architecture_change`.

---

## 6. Date & time

Direct 1:1 → `translatable_direct`: `YEAR MONTH DAY HOUR MINUTE SECOND QUARTER` (same names on
`source.col`), `NOW`/`UTCNOW` → `NOW()`, `TODAY` → `CURRENT_DATE()`, `WEEKDAY` → `DAYOFWEEK`,
`WEEKNUM` → `WEEKOFYEAR`, `DATE(y,m,d)` → `MAKE_DATE`, `DATEVALUE` → `TO_DATE`.

Non-obvious:

| DAX | SQL | Note |
|---|---|---|
| `DATEDIFF(a, b, DAY)` | `DATEDIFF(source.b, source.a)` | **arg order swaps**; DAY unit |
| `DATEDIFF(a, b, MONTH)` | `months_between(source.b, source.a)` | for MONTH/YEAR/QUARTER units |
| `EDATE(d, n)` | `add_months(source.d, n)` | |
| `EOMONTH(d, n)` | `last_day(add_months(source.d, n))` | |
| `YEARFRAC(a, b)` | `datediff(source.b, source.a) / 365.25` | approximation; note basis ignored |
| `NETWORKDAYS(a, b)` | business-day count | no native fn → `architecture_change` (source-view / UDF) |

---

## 7. Time intelligence

**All of this is covered in depth by `PATTERNS.md` (share-of-total, offsets) and `UNSUPPORTED.md`
(SAMEPERIODLASTYEAR, TOTALYTD, DATESINPERIOD, PARALLELPERIOD) plus `WINDOW.md` — read those.** The
one-line routing:

| DAX family | UCMV form | `dax_class` |
|---|---|---|
| `TOTALYTD/QTD/MTD`, `DATESYTD/QTD/MTD` | window `range: cumulative` + period `range: current` | `architecture_change` |
| `SAMEPERIODLASTYEAR`, `PREVIOUSYEAR/MONTH`, `PARALLELPERIOD` | calendar `date_py` self-join **or** window `offset`/`trailing` | `architecture_change` |
| `DATESINPERIOD`, `DATESBETWEEN` | window `trailing <N> <unit>` | `architecture_change` |
| `OPENING/CLOSINGBALANCE*`, `FIRSTDATE/LASTDATE` | window `semiadditive: first/last` | `architecture_change` |
| `STARTOF*`/`ENDOF*` | `date_trunc(...)` / `last_day(...)` on a date dimension | `translatable_direct` (as a dimension) |

Never fabricate a fixed day-offset (e.g. `LAG(…, 365)`) when the period dimension is sparse — if
no `date_py` column and non-dense periods, leave a documented TODO (`UNSUPPORTED.md`).

---

## 8. Filter / context functions  (mostly ⚪ — they are clauses, not scalars)

These are **filter-context modifiers**, not functions that return a scalar. Their translation is a
SQL *clause*, and the common shapes are already deep in `PATTERNS.md`:

| DAX | Route | See |
|---|---|---|
| `CALCULATE(expr, pred)` | `expr FILTER (WHERE pred)` → `filtered` | `PATTERNS.md §3` |
| `FILTER(t, pred)` inside an iterator | `FILTER (WHERE pred)` → `filtered` | `PATTERNS.md §2b` |
| `ALL/ALLSELECTED/REMOVEFILTERS(dim)` | share-of-total window `range: all` → `architecture_change` | `PATTERNS.md §4` |
| `ALLEXCEPT(t, keep)` | fixed-LOD window at `keep` grain → `architecture_change` | `PATTERNS.md §14` |
| `KEEPFILTERS(pred)` | intersect predicates with `AND` in the `WHERE` | `PATTERNS.md §3` |
| `SELECTEDVALUE(dim, alt)` | slicer read — usually `display_layer`; as a guard, skip it | `UNSUPPORTED.md` |

**Visual-calculation functions — always `unsupported` (❌), never force an aggregate:**
`FIRST`, `LAST`, `NEXT`, `PREVIOUS`, `INDEX`, `OFFSET` (visual), `RANK`, `ROWNUMBER`,
`RUNNINGSUM`, `MOVINGAVERAGE`, `WINDOW`, `LOOKUP`. These operate on a visual's row order, which a
metric view has no concept of. If a running/moving figure is genuinely wanted, that's a window
measure (`WINDOW.md`) — but confirm intent; do not silently convert a visual calc.

`EARLIER`/`EARLIEST` (row-context self-reference) → needs a self-join / precomputed column →
`architecture_change` (see `EDGE_CASES §4`).

---

## 9. Relationship

| DAX | Route | `dax_class` |
|---|---|---|
| `RELATED(dim[col])` | a declared `join` + `dim.col` reference | `filtered`/`composed` (see `PATTERNS.md §7`) |
| `RELATEDTABLE(t)` inside `SUMX` | aggregate over the join, **not** a subquery | `architecture_change` if it needs a grain change |
| `USERELATIONSHIP(a, b)` | inactive relationship — no UCMV equivalent | `unsupported` (`UNSUPPORTED.md`) |
| `CROSSFILTER(...)` | bidirectional filter direction | `unsupported` |

---

## 10. Parent-child (hierarchies)

`PATH`, `PATHITEM`, `PATHITEMREVERSE`, `PATHLENGTH`, `PATHCONTAINS` build/query an ancestor path
from a self-referencing (`id`, `parent_id`) table. This is a **recursive CTE** — not expressible in
a single measure expr (§0.2). Route to `architecture_change`:

```
DAX:  PATH(Emp[ID], Emp[ParentID])
→ dax_class="architecture_change"
  explanation: "Parent-child path. Precompute in the source view with a recursive CTE
   (WITH RECURSIVE anc AS (... UNION ALL ...)) exposing a `path` column, then PATHITEM →
   element_at(split(path,'|'), n), PATHCONTAINS → array_contains(split(path,'|'), x),
   PATHLENGTH → size(split(path,'|'))."
```

Once `path` exists as a source column, the `PATHITEM`/`PATHCONTAINS`/`PATHLENGTH` readers become
direct scalar expressions on `source.path`.

---

## 11. Financial

No native SQL. Two routes, in order:

1. **Closed-form → inline arithmetic on `source.` columns** (`translatable_direct`): `SLN` →
   `(source.cost - source.salvage) / source.life`; `EFFECT` → `POWER(1 + source.rate/source.n, source.n) - 1`;
   `NOMINAL`, `RRI` → `POWER(source.fv/source.pv, 1.0/source.n) - 1`; `PDURATION`, `DOLLARDE/FR`,
   `TBILLYIELD/PRICE/EQ`, `INTRATE`, `RECEIVED`, `DISC`, `ACCRINTM`.
2. **Iterative / no closed form → existing UC UDF or `architecture_change`**: `XIRR`, `RATE`,
   `NPER`, `YIELD*`, `IRR`, `VDB`, `DURATION`, `MDURATION`, `ODDF*`, `COUP*`.

Financial functions almost never appear as BI *measures*; if one does, prefer the closed form and
flag for review. Never invent a UDF name.

---

## 12. Table manipulation

| DAX | Route | `dax_class` |
|---|---|---|
| `SUMMARIZE(t, cols, "m", agg)` | `GROUP BY` — usually the metric-view grain itself | see `PATTERNS.md §13` |
| `SUMX(SUMMARIZE(...))` | source-view `GROUP BY` precompute | `architecture_change` (`PATTERNS.md §13`) |
| `ADDCOLUMNS/SELECTCOLUMNS/ROW` | projection — a dimension/measure expr, not a table | `translatable_direct` |
| `VALUES/DISTINCT(col)` in `COUNTROWS` | `COUNT(DISTINCT source.col)` | `translatable_direct` (`PATTERNS.md §12`) |
| `TREATAS(list, col)` | disconnected-slicer dispatch | `unsupported` (`UNSUPPORTED.md`) |
| `TOPN(n, t, order)` | row-selection → source `ROW_NUMBER()`/`QUALIFY` precompute | `architecture_change` (`UNSUPPORTED.md`) |
| `GENERATE/GENERATEALL` | lateral join → source view | `architecture_change` |
| `CROSSJOIN/UNION/INTERSECT/EXCEPT` | set ops belong in the **source** view, not a measure | `architecture_change` |
| `LOOKUPVALUE(res, key, val)` | label/param lookup — usually a join or display text | `display_layer` / a `join` (`UNSUPPORTED.md`) |

---

## 13. Quick decline list (⚪/❌ — return `success=false`)

When the measure's top-level intent is one of these and no source-view reshape is in scope, decline
cleanly with the class shown — **do not fabricate a translation**:

- **`display_layer`**: `FORMAT`, `CONCATENATEX`, `SELECTEDVALUE`-guard, `SWITCH`-on-slicer,
  `SELECTEDMEASURE*`, `LOOKUPVALUE`-as-label, `ISSELECTEDMEASURE`.
- **`unsupported`**: `ALLSELECTED` (scope-aware), `USERELATIONSHIP`, `CROSSFILTER`, `TREATAS`,
  `FIRST/LAST/INDEX/NEXT/PREVIOUS/RANK/ROWNUMBER/RUNNINGSUM/MOVINGAVERAGE/WINDOW` (visual calcs),
  `NAMEOF`, `CUSTOMDATA`, `USEROBJECTID`, `EXTERNALMEASURE`, `ISFILTERED/ISCROSSFILTERED`.
- **`architecture_change`** (emittable after a source-view reshape): `PATH*`, `TOPN`, `RANKX`,
  time-intelligence, `ALL/ALLEXCEPT` share-of-total/LOD, `SUMX(SUMMARIZE)`, recursive/self-join
  shapes, set operations, iterative financial.

The honest talk-track: **every DAX function reduces to SQL** — but roughly half are direct
metric-view expressions, some need the source view reshaped first (that's `architecture_change`,
not failure), and a genuine minority are Power-BI-visual conveniences with no metric meaning. Your
job is the *correct verdict*, not a forced translation.
