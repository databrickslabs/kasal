# YAML Syntax Reference

Source: Databricks official documentation (`docs/web/docs/metric-views/data-modeling/syntax.md`).

## Column namespaces

A metric view defines exactly two column-reference namespaces:

1. **`source`** — the metric view's `source:` table. Reference its columns as `source.<col>` or unqualified `<col>` (unqualified defaults to `source` in measure and dimension expressions).
2. **Each join's declared `name`** — reference a joined table's columns as `<join_name>.<col>`. For nested joins (snowflake), use the parent-chained path: `<parent_join>.<child_join>.<col>`.

The source table's original or sanitized name is **not** a namespace. Do not emit `<source_table_name>.<col>`; use `source.<col>` (or the unqualified form) instead.

## Column Name References

Backtick a column name when it contains any of the following:

- spaces (e.g. `` `First Name` ``)
- punctuation (e.g. `` `order-date` ``, `` `total$amount` ``)
- a leading digit (e.g. `` `1-30days` ``, `` `1st_priority` ``)
- non-ASCII characters

Plain ASCII identifiers (letters, digits, underscore; not leading with a digit) do not require backticks.

| Case | Source Column | YAML Expression |
|---|---|---|
| No spaces | `revenue` | `expr: revenue` |
| With spaces | `First Name` | ``expr: "`First Name`"`` |
| Spaces in SQL expr | `First Name`, `Last Name` | ``expr: CONCAT(`First Name`, ' ', `Last Name`)`` |
| Quotes in name | `"name"` | ``expr: '`"name"`'`` |

## Expressions with Colons

YAML interprets unquoted colons as key-value separators. Wrap in double quotes:

```yaml
expr: "CASE WHEN `Customer Tier` = 'Enterprise: Premium' THEN 1 ELSE 0 END"
```

## Multi-Line Expressions

Use `|` block scalar. All lines must be indented at least two spaces beyond the `expr` key:

```yaml
expr: |
  CASE WHEN
    revenue > 100 THEN 'High'
  ELSE 'Low'
  END
```

## Dimension Definition

```yaml
dimensions:
  - name: Order date            # Column name
    expr: o_orderdate

  - name: Order month           # SQL expression
    expr: DATE_TRUNC('MONTH', `Order date`)

  - name: Month of order        # Referencing dimension with space
    expr: "`Order month`"

  - name: Order status          # Multi-line CASE
    expr: CASE
            WHEN o_orderstatus = 'O' THEN 'Open'
            WHEN o_orderstatus = 'P' THEN 'Processing'
            WHEN o_orderstatus = 'F' THEN 'Fulfilled'
          END
```

## Measure Definition

```yaml
measures:
  - name: Total revenue                       # Basic aggregation
    expr: SUM(o_totalprice)

  - name: Total revenue per customer          # Ratio
    expr: SUM(`Total revenue`) / COUNT(DISTINCT o_custkey)

  - name: Total revenue for open orders       # Measure-level filter
    expr: COUNT(o_totalprice) FILTER (WHERE o_orderstatus='O')

  - name: Revenue per customer for open       # Multi-filter
    expr: >
      SUM(o_totalprice) FILTER (WHERE o_orderstatus='O')
      / COUNT(DISTINCT o_custkey) FILTER (WHERE o_orderstatus='O')
```

## Column Name Mapping in CREATE VIEW

Column names in the `column_list` map to YAML dimensions/measures **by position**, not by name:

```sql
CREATE VIEW v (col1, col2) AS SELECT a, b FROM table;
-- a -> col1, b -> col2
```

## Version Changelog

### Version 1.1 (DBR 17.2+)

- Added: semantic metadata (display_name, format, synonyms, comment).
- Added: optional `comment` field for metric view, dimensions, and measures.
- Note: Single-line YAML comments (#) are removed when the definition is saved.

### Version 0.1 (DBR 16.4–17.1)

- Initial release.

## Semantic Metadata Fields

Each dimension and measure supports (v1.1+):

```yaml
- name: total_revenue
  expr: SUM(o_totalprice)
  comment: Total revenue from all orders
  display_name: Total Revenue           # Max 255 chars
  synonyms:                             # Up to 10, max 255 chars each
    - revenue
    - total sales
  format:
    type: currency                      # number | currency | percentage | byte | date | date_time
    currency_code: USD                  # ISO-4217 (for currency type)
    decimal_places:
      type: exact                       # max | exact | all
      places: 2                         # 0-10 (for max/exact)
    hide_group_separator: false
    abbreviation: compact               # none | compact | scientific
```

### Format Types

**Numeric**: `number`, `currency`, `percentage`, `byte`
  - `decimal_places`: `{type: max|exact|all, places: 0-10}` (`places` required for `max`/`exact`, not used with `all`)
  - `hide_group_separator`: `true|false`
  - `abbreviation` (number/currency only): `none|compact|scientific`
  - `currency_code` (**required** for currency type): ISO-4217 code

**Date**: `date`
  - `date_format`: `locale_short_month|locale_long_month|year_month_day|locale_number_month|year_week`
  - `leading_zeros`: `true|false`

**Date/Time**: `date_time`
  - `date_format`: `locale_short_month|locale_long_month|year_month_day|locale_number_month|year_week|no_date`
  - `time_format`: `no_time|locale_hour_minute|locale_hour_minute_second`
  - `leading_zeros`: `true|false`
