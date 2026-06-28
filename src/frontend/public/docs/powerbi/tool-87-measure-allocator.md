# Tool 87 - PBI measure allocator

**What it is:** Groups Power BI measures into their correct fact tables based on DAX expression analysis. A pre-processing step for Tool 86 when measures don't already have allocation metadata.

---

## Why it exists

Tool 86 (UC Metric View Generator) organizes measures per fact table - one UC Metric View per fact table. To do this, it needs to know which measures belong to which fact table. When measures come from Tool 73 with a `proposed_allocation` field already populated, Tool 86 uses that directly. When they don't have allocations (raw output from the PBI API), Tool 87 fills that gap.

## What problem it solves

- **Pre-flight for Tool 86:** Without measure allocations, Tool 86 may generate a single merged YAML or miss measures entirely
- **Automated grouping:** Parses DAX `Table[Column]` references to identify which fact table each measure primarily draws from
- **Confidence scoring:** Flags ambiguous measures (used across multiple fact tables) for human review

---

## How it works

```text
Input: measures_json (raw, no allocations) + mquery_json (fact table definitions)
    ↓
Parse mquery_json: identify fact tables (tables with SUM + GROUP BY in transpiled SQL)
    ↓
For each measure: scan DAX for Table[Column] references
    ↓
Match references against known fact tables
    ↓
Assign confidence: high (one fact), medium (multiple facts), low/none (no match)
    ↓
Return measures_json with proposed_allocation field added
```

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `measures_json` | Yes | Raw measures from Tool 73 or Tool 79 (without allocations) |
| `mquery_json` | Yes | M-Query transpilation from Tool 74 |

---

## Example crew position

```text
Tool 73 (extract measures - raw, no allocations)
Tool 74 (extract M-Query)
    ↓
Tool 87 (allocate measures to fact tables)   ← this tool
    ↓
Tool 86 (generate UC Metric Views - now measures have allocations)
```

---

## Example output

```json
{
  "allocations": [
    {
      "measure_name": "Total Revenue",
      "dax_expression": "SUM(Fact_Sales[Amount])",
      "proposed_allocation": "fact_sales",
      "confidence": "high",
      "matching_tables": ["fact_sales"]
    },
    {
      "measure_name": "Cross Ratio",
      "dax_expression": "DIVIDE(SUM(Fact_Sales[A]), SUM(Fact_HR[B]))",
      "proposed_allocation": "fact_sales",
      "confidence": "medium",
      "matching_tables": ["fact_sales", "fact_hr"]
    },
    {
      "measure_name": "Display Color",
      "dax_expression": "IF([Total Revenue] > 0, \"Green\", \"Red\")",
      "proposed_allocation": null,
      "confidence": "none",
      "matching_tables": []
    }
  ],
  "summary": {
    "total": 150,
    "high_confidence": 120,
    "medium_confidence": 18,
    "low_confidence": 8,
    "unallocated": 4
  }
}
```

---

## Notes

- `confidence: none` measures (like display-only color/format measures) are expected - they will be skipped or flagged by Tool 86
- `confidence: medium` measures (used across multiple fact tables) should be reviewed by the SA - assign them to the primary fact table in the config
- When Tool 73 output already includes `proposed_allocation` (from the PBI model's own table assignments), skip Tool 87

## See also

- [Power BI integration hub](./README.md)
- [Tool 73 - measure conversion pipeline](./tool-73-measure-conversion.md)
- [Tool 74 - M-Query conversion pipeline](./tool-74-mquery-conversion.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

Back to the [Power BI integration hub](./README.md).
