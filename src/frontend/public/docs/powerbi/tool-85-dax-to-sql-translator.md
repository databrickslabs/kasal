# Tool 85 - DAX to SQL Translator

**What it is:** A standalone, deterministic DAX-to-Spark-SQL translator using 14+ pattern-based rules. No LLM, no external API calls - just deterministic regex translation.

---

## Why It Exists

The DAX-to-SQL translation engine inside Tool 86 (UC Metric View Generator) is also exposed as a standalone tool. This lets you translate individual measures without running the full UCMV pipeline - useful for testing, debugging, or building custom workflows.

## What Problem It Solves

- **Iterative testing:** Translate one or a few measures and inspect the SQL before committing to a full run
- **Custom integration:** Use the translation output in your own pipeline instead of Tool 86's YAML output
- **Debugging:** Understand why a specific measure is marked "untranslatable"

---

## How It Works

```
Input: JSON array of {measure_name, dax_expression}
    ↓
Pattern matching against 14+ regex rules (in priority order)
    ↓
Output: JSON array with {measure_name, sql_expr, is_translatable, confidence, skip_reason}
```

No API calls. No LLM. Pure deterministic transformation.

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dax_measures_json` | Yes | JSON array of `{measure_name, dax_expression, original_name}` |
| `table_key` | No | Fact table key for contextual column resolution |
| `config_json` | No | Pipeline config overrides (`filter_sets`, `measure_resolutions`) |

---

## Supported DAX Patterns

| Pattern | Example DAX | SQL Output |
|---------|-------------|------------|
| Simple SUM | `SUM(T[col])` | `SUM(source.col)` |
| CALCULATE+SUM | `CALCULATE(SUM(T[col]))` | `SUM(source.col)` |
| SUMX no filter | `SUMX(T, T[col])` | `SUM(source.col)` |
| SUMX+FILTER | `SUMX(FILTER(T, cond), T[col])` | `SUM(source.col) FILTER WHERE cond` |
| COUNTX+FILTER | `COUNTX(FILTER(T, c), T[col])` | `COUNT(source.col) FILTER WHERE c` |
| AVERAGEX+FILTER | `AVERAGEX(FILTER(T, c), T[col])` | `AVG(source.col) FILTER WHERE c` |
| DIVIDE | `DIVIDE(a, b)` | `a / NULLIF(b, 0)` |
| DISTINCTCOUNTNOBLANK | `DISTINCTCOUNTNOBLANK(T[col])` | `COUNT(DISTINCT source.col)` |
| SAMEPERIODLASTYEAR | `CALCULATE([M], SAMEPERIODLASTYEAR(...))` | SQL + `window: trailing 12 month` |
| VAR/RETURN+DIVIDE | `var x = ... return DIVIDE(x, y)` | Resolved vars + NULLIF |
| Quick reject | FORMAT, Color, ISBLANK, SELECTEDVALUE+SWITCH | Skipped with reason |

---

## Example Input

```json
[
  {
    "measure_name": "Total Revenue",
    "dax_expression": "SUM(Sales[Amount])",
    "original_name": "Total Revenue"
  },
  {
    "measure_name": "Revenue %",
    "dax_expression": "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Target]))",
    "original_name": "Revenue %"
  },
  {
    "measure_name": "Color Flag",
    "dax_expression": "IF([Total Revenue] > 1000, \"Green\", \"Red\")",
    "original_name": "Color Flag"
  }
]
```

## Example Output

```json
{
  "results": [
    {
      "measure_name": "total_revenue",
      "sql_expr": "SUM(source.Amount)",
      "is_translatable": true,
      "confidence": "high",
      "skip_reason": null
    },
    {
      "measure_name": "revenue_pct",
      "sql_expr": "SUM(source.Amount) / NULLIF(SUM(source.Target), 0)",
      "is_translatable": true,
      "confidence": "high",
      "skip_reason": null
    },
    {
      "measure_name": "color_flag",
      "sql_expr": null,
      "is_translatable": false,
      "confidence": null,
      "skip_reason": "PBI display artifact (IF/color pattern)"
    }
  ],
  "summary": {
    "total": 3,
    "translated": 2,
    "untranslatable": 1,
    "rate": "66.7%"
  }
}
```

---

## Notes

- This is the same engine that runs inside Tool 86 - if Tool 86 marks something untranslatable, Tool 85 will too
- For measures Tool 85 cannot handle, Tool 86 offers an LLM fallback (`use_llm_fallback: true`)
- Use this for a quick "what's my translation rate?" check before committing to a full UCMV generation run
