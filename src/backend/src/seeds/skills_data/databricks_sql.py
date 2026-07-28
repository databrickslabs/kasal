"""Writing SQL for Databricks and Unity Catalog.

Carries bundled reference files, following ``internal-comms``: the body says
which file to read for which situation, and only that file is loaded. It is also
the seed data that exercises tier 3 in a real install rather than only in tests.
"""

_METRIC_VIEWS = """# Unity Catalog metric views

A metric view defines measures once so every consumer computes them the same
way. Aggregating around it defeats the point.

## Aggregate through MEASURE()

```sql
SELECT region, MEASURE(total_revenue)
FROM catalog.schema.sales_metrics
GROUP BY region;
```

`SUM(revenue)` against a metric view either fails or silently bypasses the
definition — which is worse, because it returns a number.

## Discovering what a metric view offers

```sql
DESCRIBE TABLE EXTENDED catalog.schema.sales_metrics;
```

The output names the dimensions you may group by and the measures you may wrap
in `MEASURE()`. Anything not listed is not available: a metric view is not a
table with extra columns.

## Rules that catch people out

- You cannot mix `MEASURE()` with a raw aggregate over the same view.
- Filters apply to dimensions, not to measures. Filter before aggregating.
- Joining a metric view to a fact table usually means the metric view was the
  wrong object to start from — the join changes the grain the measure assumes.
"""

_COST = """# Why a Databricks query is slow or expensive

Ordered by how often it is the real cause.

1. **No partition filter.** If the table is partitioned by date and the query
   does not filter on it, every partition is read. Check with
   `DESCRIBE TABLE EXTENDED` and filter on the partition column.
2. **`SELECT *` on a wide table.** Delta is columnar; naming five columns reads
   five columns. `SELECT *` reads all two hundred.
3. **A sort you did not need.** `LIMIT` does not save you from `ORDER BY` — the
   sort happens first, over the whole result.
4. **A join that fans out.** Check the grain of the right-hand side before
   blaming the cluster; a join producing ten times the rows is ten times the
   work downstream.
5. **Many small files.** If a table has thousands of tiny files, `OPTIMIZE` is
   the fix, not a bigger cluster.

## Exploring safely

Three cheap queries beat one expensive wrong one:

```sql
DESCRIBE TABLE EXTENDED catalog.schema.table;
SELECT * FROM catalog.schema.table LIMIT 20;
SELECT COUNT(*) FROM catalog.schema.table;
```
"""

SKILL = {
    "name": "databricks-sql-conventions",
    "description": (
        "Conventions for writing SQL against Databricks and Unity Catalog: "
        "three-level catalog.schema.table names, aggregating metric views "
        "through MEASURE(), exploring an unfamiliar table safely, and what "
        "makes a query expensive. Use whenever writing, reviewing or debugging "
        "SQL for Databricks, querying a Unity Catalog table or metric view, or "
        "investigating a query that is slow or returns the wrong numbers. "
        "Trigger when the user mentions Databricks, Unity Catalog, a metric "
        "view, Delta tables, or SQL that is slow or wrong."
    ),
    "body": """# Writing SQL for Databricks

## When to use this skill

Any time SQL is being written or reviewed against Databricks or Unity Catalog.

## Always use three-level names

`catalog.schema.table`. A two-level name resolves against whatever catalog
happens to be current, which is how a query that worked in testing reads the
wrong data in production.

## Explore before you aggregate

```sql
DESCRIBE TABLE EXTENDED catalog.schema.table;
SELECT * FROM catalog.schema.table LIMIT 20;
SELECT COUNT(*) FROM catalog.schema.table;
```

Establish the grain — one row per what? — before grouping by anything.

## Reading a result

- Compare `COUNT(*)` with `COUNT(column)` before trusting an average: the gap is
  nulls, and nulls are excluded from aggregates silently.
- A join that increases the row count means the right side is not unique on the
  key. Check before reporting the numbers.
- `SUM` over a large `INT` column is worth casting to `BIGINT`.

## Reference files

Read the one that fits the situation — do not read both:

- **Working with a metric view** → `references/metric-views.md`
- **A query that is slow or expensive** → `references/query-cost.md`
""",
    "files": [
        {"path": "references/metric-views.md", "content": _METRIC_VIEWS},
        {"path": "references/query-cost.md", "content": _COST},
    ],
}
