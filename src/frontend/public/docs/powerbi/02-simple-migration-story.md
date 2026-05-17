# The Power BI → Databricks Migration Story

A practical guide to what migration actually means, what you get at the end, and which path to take for your customer.

---

## What Problem Are We Solving?

Power BI semantic models contain years of business logic encoded in DAX measures, M-Query table transformations, and relationship definitions. When a customer moves from Power BI to Databricks for their analytics layer, they face a painful choice:

1. **Rewrite everything by hand** - weeks of consulting effort, loss of institutional knowledge, high error rate
2. **Keep Power BI as the semantic layer** - technical debt, two platforms to maintain, no path to UC Metric Views
3. **Use Kasal's migration toolkit** - deterministic, validated, 70-93% automated

The goal is not to replace Power BI dashboards (customers keep those). The goal is to move the **business logic** - the measure definitions and table transformations - into Unity Catalog Metric Views, so Databricks becomes the governed semantic layer that any downstream tool (including Power BI) can query.

---

## What Does the Customer Get?

After migration, each Power BI fact table becomes a **Unity Catalog Metric View** - a governed, queryable metric definition stored natively in Unity Catalog:

```yaml
# Example: fact_sales_uc_metric_view
name: fact_sales_uc_metric_view
catalog: my_catalog
schema: sales_metrics
source: my_catalog.raw.fact_sales
dimensions:
  - name: Region
    expr: "`region`"
  - name: Product Category
    expr: "`product_category`"
measures:
  - name: Total Revenue
    expr: "SUM(`amount`)"
  - name: YoY Growth
    expr: "SUM(`amount`) / NULLIF(LAG(SUM(`amount`), 12) OVER (ORDER BY month), 0) - 1"
```

This can then be queried with:
```sql
SELECT MEASURE(Total_Revenue), MEASURE(YoY_Growth)
FROM my_catalog.sales_metrics.fact_sales_uc_metric_view
GROUP BY Region
```

---

## The Two-Speed Story

### Fast Path: "Show Me It Works" (1-2 hours with an SA)

Use the standalone pipeline (no Kasal UI needed):

```bash
cd src/backend
source .venv/bin/activate
python ../../examples/uc_metric_view_migration/run_locally.py
```

This runs the full 5-phase pipeline against pre-extracted demo data and produces YAML files in `~/Downloads/ucmv_example_output/`. No PBI credentials needed for the demo.

Use this to demo the capability to a customer before they commit to setting up SP access.

### Full Path: Customer's Own Data

1. Set up Service Principals ([Authentication Guide](./01-authentication-setup.md))
2. Run Tool 90 to generate a proposed config from their live PBI APIs
3. SA reviews and fills in the ~30% that requires domain knowledge (~2-3 hours)
4. Run Tool 86 to generate YAML + SQL
5. Run Tool 88 (dry-run) to validate
6. Customer approves → Tool 88 deploys to Databricks

Total SA time: **~4-6 hours per semantic model** (first time). Repeat migrations of similar models: **~1-2 hours**.

---

## What Gets Automated vs What Needs Human Input

| Phase | Automation | Manual Effort |
|-------|-----------|---------------|
| Extract measures from PBI | ✅ 100% automated (Tool 73) | None |
| Extract M-Query sources | ✅ 100% automated (Tool 74) | None |
| Extract relationships | ✅ 100% automated (Tool 75) | None |
| Propose config (26 keys) | ✅ ~70% auto-filled (Tool 90) | Fill TODOs (~30%) |
| DAX → SQL translation | ✅ 93% deterministic (Tool 86) | Complex SWITCH/cross-table ~7% |
| YAML generation | ✅ 100% automated (Tool 86) | Review output |
| Deployment | ✅ Automated (Tool 88) | Approve dry-run first |

The **~30% human input** is unavoidable because it encodes business domain knowledge that cannot be extracted from code: Which filters apply globally? What does this SWITCH branch mean? Which fact tables share a grain?

---

## Limitations We're Honest About

Kasal's tooling is **not** a magic "one click and done" migration. We are transparent about what it handles and what it doesn't:

| Limitation | What Happens |
|-----------|-------------|
| Complex SWITCH decompositions | Skeleton generated; SA fills SQL per branch |
| Cross-table measure references | `measure_resolutions` config key; SA maps each one |
| Time intelligence (YTD, LY) | Detected and flagged; converted to SQL window functions where possible |
| PBI-specific visuals (e.g. KPI cards, scatter charts) | Not migrated - display layer stays in PBI |
| RLS (Row-Level Security) | Detected and warned; UC security model handles this differently |
| M-Query custom functions | LLM fallback available; accuracy varies |
| Calculation groups (Fabric only) | Tool 77 handles; requires Fabric workspace |

**Target coverage: 70-93% automation** depending on model complexity. The more SWITCH statements and cross-table measures, the lower the automatic rate.

---

## Competitive Landscape

| Tool | Approach | Deployment | Our Position |
|------|----------|-----------|--------------|
| **Databricks `powerbi-migrate`** | File-based input, LLM-driven (non-deterministic) | No deployment | We: live API + deterministic + deploy |
| **Microsoft** | Migration path → Fabric (keeps DAX) | No UC Metric Views | We: UC-native output |
| **Consulting firms** | Manual rewrites | Customer-managed | We: 70-93% automated, weeks → hours |
| **dbt / Looker** | Own metric constructs | Own deployment | No PBI migration tooling |

Our moat: **deterministic translation** (reproducible output, no LLM surprises) + **live API extraction** (no file exports needed) + **human review at the right points** (SA-guided, not blanket automation).

---

## Kasal's Upcoming Release

The next Kasal release (PR #51) brings the full 5-phase UCMV pipeline into the Kasal UI as a **semi-automatic guided workflow** - no Python setup, no command line. The SA and customer see:

1. Phase 1-2 run automatically (extraction + config proposal)
2. Config review screen in the UI
3. Phase 4-5 run automatically (generation + validation)
4. One-click deploy

This is what makes the tooling **customer-self-service ready**. Until that ships, it requires SA hands-on-keyboard using the Kasal workflow designer or the standalone pipeline.

---

## Related

- [Authentication Setup](./01-authentication-setup.md)
- [End-to-End UCMV Migration Guide](./ucmv-migration-guide.md)
- [Tool 86 - UC Metric View Generator](./tool-86-uc-metric-view-generator.md)
- [Tool 90 - Pipeline Config Generator](./tool-90-pipeline-config-generator.md)
