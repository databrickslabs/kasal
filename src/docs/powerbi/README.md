# Power BI Integration - Documentation Hub

Kasal provides a complete toolkit for migrating Power BI semantic models to Databricks Unity Catalog and for running live analytics against Power BI data. This section covers everything from first-time authentication setup to the full UCMV migration pipeline.

---

## What Can You Do?

| Use Case | Tools | Guide |
|----------|-------|-------|
| **Migrate measures** (DAX → SQL / UC Metrics) | 73, 74, 75, 86-90 | [UCMV Migration Guide](./ucmv-migration-guide.md) |
| **Answer business questions** from PBI with natural language | 72, 79, 80, 81 | [Tool 72](./tool-72-comprehensive-analysis.md) |
| **Execute a known DAX query** | 82 | [Tool 82](./tool-82-dax-executor.md) |
| **Migrate Fabric hierarchies** | 76 | [Tool 76](./tool-76-hierarchies.md) |
| **Migrate Fabric field parameters / calc groups** | 77 | [Tool 77](./tool-77-field-parameters.md) |
| **Understand report-to-measure dependencies** | 78 | [Tool 78](./tool-78-report-references.md) |

---

## Quick Navigation

### Setup
- [Authentication & Service Principal Setup](./01-authentication-setup.md) ← **Start here**
- [Simple Migration Story](./02-simple-migration-story.md)

### Analytics / Q&A Tools
- [Tool 72 - Comprehensive Analysis](./tool-72-comprehensive-analysis.md)
- [Tool 79 - Semantic Model Fetcher](./tool-79-semantic-model-fetcher.md)
- [Tool 80 - DAX Generator](./tool-80-dax-generator.md)
- [Tool 81 - Metadata Reducer](./tool-81-metadata-reducer.md)
- [Tool 82 - DAX Executor](./tool-82-dax-executor.md)

### Migration Tools (Extraction)
- [Tool 73 - Measure Conversion Pipeline](./tool-73-measure-conversion.md)
- [Tool 74 - M-Query Conversion Pipeline](./tool-74-mquery-conversion.md)
- [Tool 75 - Relationships Tool](./tool-75-relationships.md)
- [Tool 76 - Hierarchies Tool](./tool-76-hierarchies.md) *(Fabric only)*
- [Tool 77 - Field Parameters & Calculation Groups](./tool-77-field-parameters.md) *(Fabric only)*
- [Tool 78 - Report References Tool](./tool-78-report-references.md) *(Fabric only, disabled by default)*

### UC Metric View Generation
- [Tool 85 - DAX to SQL Translator](./tool-85-dax-to-sql-translator.md)
- [Tool 86 - UC Metric View Generator](./tool-86-uc-metric-view-generator.md)
- [Tool 87 - PBI Measure Allocator](./tool-87-measure-allocator.md)
- [Tool 88 - Metric View Deployer](./tool-88-metric-view-deployer.md)
- [Tool 89 - Config Generator](./tool-89-config-generator.md)
- [Tool 90 - Pipeline Config Generator](./tool-90-pipeline-config-generator.md)
- [End-to-End UCMV Migration Guide](./ucmv-migration-guide.md)

---

## Tool Map

```
ANALYTICS PATH (answer questions from live PBI data)
─────────────────────────────────────────────────────────────────
  Tool 79: Fetch & cache model metadata
      ↓
  Tool 81: Reduce to question-relevant subset  (optional but recommended)
      ↓
  Tool 80: Generate + execute DAX from natural language
                               OR
  Tool 82: Execute a known DAX query directly
                               OR
  Tool 72: All-in-one: question → DAX → execute (single tool)


MIGRATION PATH (move PBI semantic model → Databricks UC Metric Views)
─────────────────────────────────────────────────────────────────
  PHASE 1: Extract
  ┌─────────────────────────────────────────────────────────┐
  │ Tool 74: Extract M-Query (Admin SP required)            │
  │ Tool 73: Extract DAX measures (Non-Admin SP)            │
  │ Tool 75: Extract relationships (Non-Admin SP, optional) │
  └─────────────────────────────────────────────────────────┘
      ↓
  PHASE 2: Propose Config
  Tool 90 (live PBI API → full config)  OR  Tool 89 (from extracted JSON)
      ↓
  PHASE 3: Human Review (~2-3h first time, 30min repeat)
      ↓
  PHASE 4: Generate
  Tool 87: Allocate measures to fact tables (if needed)
  Tool 86: Generate YAML + SQL (the main pipeline)
      ↓
  PHASE 5: Validate + Deploy
  Tool 88: Dry-run validate → human approval → deploy
```

---

## Authentication at a Glance

| SP Type | Used By | Key Permission |
|---------|---------|----------------|
| Non-Admin SP (workspace member) | Tools 72, 73, 75, 79, 80, 81, 82 | `Dataset.Read.All` |
| Admin SP (tenant-wide) | Tool 74, Tool 90 | `Tenant.Read.All` (Admin Portal required) |
| Fabric SP | Tools 76, 77, 78 | `SemanticModel.ReadWrite.All` |

See [Authentication Setup](./01-authentication-setup.md) for step-by-step instructions.
