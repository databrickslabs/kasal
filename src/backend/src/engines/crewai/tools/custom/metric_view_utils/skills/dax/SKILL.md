---
name: dax-translation
description: >
  DAX to SQL/UC-metric-view translation patterns. Use when converting
  Power BI DAX measures to SQL aggregations or metric view YAML measure
  expressions. Covers CALCULATE, FILTER, DIVIDE, SWITCH, ALLSELECTED,
  time intelligence, SELECTEDVALUE, and common untranslatable patterns.
---

# DAX to SQL Translation for Metric Views

## Translation Decision Framework

For each DAX measure, classify it into one of these categories:

1. **Translatable (direct)**: `SUM`, `COUNT`, `AVERAGE`, `MIN`, `MAX` on columns -> atomic measure.
2. **Translatable (composed)**: `DIVIDE`, ratio of measures -> composed measure using `MEASURE()`.
3. **Translatable (filtered)**: `CALCULATE(SUM(...), FILTER(...))` -> measure with `FILTER (WHERE ...)` clause.
4. **Architecture change needed**: `SWITCH`/`SELECTEDVALUE` slicer patterns -> replace with dimensions.
5. **Dashboard-layer**: `ISFILTERED`, `CONCATENATEX`, `FORMAT`, `NAMEOF` -> skip, document as display-only.
6. **Not translatable**: `ALLSELECTED`, `USERELATIONSHIP`, certain time-intelligence -> flag for review.
7. **Out of scope**: `EXTERNALMEASURE`, DirectQuery references -> flag.

For the complete pattern catalog, read [PATTERNS.md](PATTERNS.md).
For patterns that cannot be translated, read [UNSUPPORTED.md](UNSUPPORTED.md).
For edge cases and gotchas, read [EDGE_CASES.md](EDGE_CASES.md).
