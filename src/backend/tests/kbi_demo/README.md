# KBI Dependency Demo

This directory contains a working demonstration of nested KBI (Key Business Indicator) dependency resolution across different converter formats.

## Files

### 1. `excise_tax_kbis.yaml`
Sample YAML definition with 4 KPIs demonstrating nested dependencies:
- **3 Leaf Measures**: Base aggregations from fact tables with filters
  - `excise_tax_actual` - SUM from FactTax with display_sign: -1
  - `excise_tax_plan_new_method` - SUM from FactTaxPlan (NEW)
  - `excise_tax_plan_old_method` - SUM from FactTaxPlan (OLD)
- **1 Parent Measure**: Calculated measure that references the 3 leaf measures
  - `excise_tax_total` - Formula combines all 3 leaf measures

### 2. `test_excise_tax_demo.py`
Python script that demonstrates the converters in action:
- Parses the YAML definition
- Generates DAX measures (Power BI / Tabular Model)
- Generates UC Metrics YAML (Databricks)
- Writes results to `demo.md`

**Usage:**
```bash
cd src/backend
python3 tests/kbi_demo/test_excise_tax_demo.py
```

### 3. `demo.md`
Generated output showing:
- Source YAML definition
- DAX measures with CALCULATE and filters
- UC Metrics YAML with Spark SQL expressions
- Architecture validation summary

## What This Proves

✅ **All converters handle nested KBI dependencies**

The shared logic in `common/transformers/formula.py` provides:
- `KbiFormulaParser` - Extracts `[KBI references]` from formulas
- `KBIDependencyResolver` - Builds dependency trees

This enables:
- **DAX**: Parent measures reference child measures using `[Measure Name]` syntax
- **SQL**: Parent CTEs JOIN child CTEs (not shown in current demo)
- **UC Metrics**: Parent metrics reference child metrics by name

## Key Features Demonstrated

| Feature | Implementation |
|---------|----------------|
| **KBI Dependency Resolution** | Parent formula: `excise_tax_actual + excise_tax_plan_new_method + ...` |
| **Filter Application** | WHERE clauses applied to each leaf measure |
| **Display Sign** | `(-1) *` wrapper for negative values |
| **Multi-source Aggregation** | Different source tables (FactTax, FactTaxPlan) |
| **Calculated Measures** | Parent measure with aggregation_type: CALCULATED |

## Architecture Notes

This demo validates the architectural decision to use shared logic for KBI dependency resolution while keeping format-specific code (DAX syntax, SQL query generation, UC Metrics YAML) in separate converters.

The confusion about "tree parsing" is clarified:
- **KBI Dependency Trees** (shared) - Resolving references between KPIs
- **DAX Function Trees** (DAX-specific) - Parsing nested DAX functions like `CALCULATE(SUMX(FILTER(...)))`

SQL and UC Metrics don't need DAX-specific function tree parsing, but they fully support KBI dependency trees!
