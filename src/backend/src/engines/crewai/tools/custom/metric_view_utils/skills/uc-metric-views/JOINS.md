# Join Patterns in Metric Views

Source: Databricks official documentation (`docs/web/docs/metric-views/data-modeling/joins.md`).

## Star Schema

The `source` is the fact table. Joins use `LEFT OUTER JOIN` to dimension tables.

```yaml
source: catalog.schema.fact_table

joins:
  # ON clause (boolean expression)
  - name: dimension_table_1
    source: catalog.schema.dimension_table_1
    'on': source.dimension_table_1_fk = dimension_table_1.pk

  # USING clause (shared column names)
  - name: dimension_table_2
    source: catalog.schema.dimension_table_2
    using:
      - dimension_table_2_key_a
      - dimension_table_2_key_b

dimensions:
  - name: Dimension table 1 key
    expr: dimension_table_1.pk          # Dot notation to reference join columns

measures:
  - name: Count of dim1 keys
    expr: COUNT(dimension_table_1.pk)   # Join columns in measures too
```

### Namespacing

- `source.*` references the metric view's source table.
- `<join_name>.*` references columns from the joined table.
- If no prefix in an `on` clause, defaults to the join table.

### YAML Reserved Words

Quote `on`, `off`, `yes`, `no`, `NO` — PyYAML interprets them as booleans:

```yaml
'on': source.fk = dim.pk    # Correct
on: source.fk = dim.pk      # WRONG — parsed as True
```

## Snowflake Schema (DBR 17.1+)

Nested `joins:` within a join for multi-hop normalized dimensions:

```yaml
source: samples.tpch.orders

joins:
  - name: customer
    source: samples.tpch.customer
    'on': source.o_custkey = customer.c_custkey
    joins:
      - name: nation
        source: samples.tpch.nation
        'on': customer.c_nationkey = nation.n_nationkey
        joins:
          - name: region
            source: samples.tpch.region
            'on': nation.n_regionkey = region.r_regionkey

dimensions:
  - name: customer_name
    expr: customer.c_name
  - name: nation_name
    expr: customer.nation.n_name    # Traverse through nested join
```

## Struct-Returning Dimensions

A joined table can be returned as a whole-row struct dimension:

```yaml
dimensions:
  - name: customer
    expr: customer                    # Returns the full customer row as a struct
  - name: nation
    expr: customer.nation             # Returns nested join row as a struct
  - name: customer_name
    expr: customer.c_name             # Returns a scalar column (normal usage)
```

This is useful for downstream tools that consume the struct directly, but most
metric view queries will reference individual columns via dot notation.

## Constraints

- Joined tables cannot include `MAP` type columns.
- Join should follow many-to-one relationship. Many-to-many takes first matching row.
- Snowflake joins require DBR 17.1+.
