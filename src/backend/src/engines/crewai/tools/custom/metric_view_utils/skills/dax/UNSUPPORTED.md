# Unsupported DAX Patterns

These DAX patterns have no direct UC metric view equivalent. Flag for manual review.

## ALLSELECTED()

```
DAX:  CALCULATE([Revenue], ALLSELECTED())
```

`ALLSELECTED` computes a value across all items visible in the current visual context (the slicer/filter scope). There is no UC metric view equivalent because metric views don't have a concept of "visual context."

**Workaround**: Use coarser LOD with window measures (`range: all`) for simple cases. For complex cases, implement as dashboard-layer calculations or SQL window functions in the source view.

## USERELATIONSHIP()

```
DAX:  CALCULATE([Revenue], USERELATIONSHIP(orders[ship_date], calendar[date]))
```

PBI supports inactive relationships that can be activated per-measure via `USERELATIONSHIP`. UC metric views have a single join graph — no concept of active/inactive relationships.

**Workaround**: Define separate joins for each relationship, or create separate metric views for each relationship path.

## Time Intelligence Functions

### SAMEPERIODLASTYEAR

```
DAX:  CALCULATE([Revenue], SAMEPERIODLASTYEAR('Calendar'[Date]))
```

**Workaround (preferred order):**

1. **Calendar `date_py` self-join** — when the calendar dimension carries a
   prior-year column (`date_py`, `fiscper_py`, etc.), join the fact to the
   calendar on that column and SUM against it. This is exact (no offset guessing):

   ```yaml
   joins:
     - name: cal_py
       source: <catalog>.<schema>.c_dim_calendar
       on: source.fiscper = cal_py.date_py   # prior-year period lookup
   measures:
     - name: revenue_py
       expr: SUM(source.revenue)             # scoped to cal_py's PY period
   ```

2. **LAG window** — for a fixed 1-period offset on an ordered period dim:
   `LAG(SUM(source.revenue)) OVER (ORDER BY fiscper)`. Only valid when the period
   dim is dense (no gaps).

3. **`trailing 1 year` window measure**, or compute in the source view using
   `DATE_ADD(date, -1, 'YEAR')`.

If none of the inputs above are available (no `date_py` column, non-dense period),
leave the measure as a documented TODO — do NOT fabricate an offset.

### DATESINPERIOD / DATESBETWEEN

```
DAX:  CALCULATE([Revenue], DATESINPERIOD('Calendar'[Date], MAX('Calendar'[Date]), -30, DAY))
```

**Workaround**: Use window measures with `trailing 30 day` range.

### TOTALYTD / TOTALQTD / TOTALMTD

```
DAX:  TOTALYTD([Revenue], 'Calendar'[Date])
```

**Workaround**: Use window measures with cumulative range + period boundary:

```yaml
measures:
  - name: ytd_revenue
    expr: SUM(source.revenue)
    window:
      - order: date
        range: cumulative
        semiadditive: last
      - order: year
        range: current
        semiadditive: last
```

### PARALLELPERIOD / PREVIOUSMONTH / PREVIOUSYEAR

```
DAX:  CALCULATE([Revenue], PARALLELPERIOD('Calendar'[Date], -1, MONTH))
```

**Workaround**: Use window measures with `trailing 1 month` range.

## EXTERNALMEASURE

```
DAX:  EXTERNALMEASURE(...)
```

References a measure in an external Analysis Services model. Completely out of scope — requires migrating the AS source to Databricks.

## USERPRINCIPALNAME()

```
DAX:  USERPRINCIPALNAME()
```

Returns the email of the current PBI user for row-level security. No metric view equivalent.

**Workaround**: Use UC row access policies with `CURRENT_USER()`.

## ISFILTERED() / ISCROSSFILTERED()

```
DAX:  IF(ISFILTERED(table[column]), [filtered_measure], [unfiltered_measure])
```

Tests whether a specific column has an active filter in the visual context. Pure display-layer logic — no metric view equivalent.

**Action**: Skip entirely. Not a metric definition.

## CONCATENATEX / NAMEOF

```
DAX:  CONCATENATEX(VALUES(table[col]), table[col], ", ")
```

String aggregation across rows. Display-layer logic for building filter labels.

**Action**: Skip entirely. Not a metric definition.

## SELECTEDVALUE (as Guard)

```
DAX:  IF(ISBLANK(SELECTEDVALUE(dim[col])), BLANK(), [measure])
```

Returns BLANK when no single value is selected in a slicer. Display-layer guard logic.

**Action**: Skip the guard. The measure itself is still valid.
