# Power BI integration

The toolkit for migrating Power BI semantic models to Databricks Unity Catalog and running live analytics against Power BI data.

Kasal provides a complete toolkit for migrating Power BI semantic models to Databricks Unity Catalog and for running live analytics against Power BI data. This section covers everything from first-time authentication setup to the full UCMV migration pipeline.

## In this section

- [What you can do](#what-you-can-do)
- [Quick navigation](#quick-navigation)
- [Tool map](#tool-map)
- [Authentication at a glance](#authentication-at-a-glance)

## What you can do

The tables below map common goals to the tools and guide that cover them.

| Use case | Tools | Guide |
|----------|-------|-------|
| Answer business questions from PBI with natural language | 72, 79, 80, 81 | [Analytics Q&A case study](./powerbi-analytics-qa-case-study.md) |
| Migrate measures (DAX to SQL / UC Metrics) | 73, 74, 75, 86-90 | [UCMV migration guide](./ucmv-migration-guide.md) |
| Execute a known DAX query | 82 | [Tool 82 - DAX executor](./tool-82-dax-executor.md) |
| Migrate Fabric hierarchies | 76 | [Tool 76 - hierarchies](./tool-76-hierarchies.md) |
| Migrate Fabric field parameters / calc groups | 77 | [Tool 77 - field parameters](./tool-77-field-parameters.md) |
| Understand report-to-measure dependencies | 78 | [Tool 78 - report references](./tool-78-report-references.md) |

## Quick navigation

### Setup

- [Authentication and service principal setup](./01-authentication-setup.md) (start here)
- [Simple migration story](./02-simple-migration-story.md)
- [PBI → UCMV pipeline architecture](./ucmv-pipeline-architecture.md): end-to-end walkthrough — extraction → config → M-query path → LLM-first DAX translation with skill files → deploy, with the code location of each stage

### Case studies and examples

- [Power BI analytics Q&A full case study](./powerbi-analytics-qa-case-study.md): 3-agent crew, context enrichment, business_mappings, field_synonyms, active_filters
- [Example crew: `crew_pbi_analyst_qa.json`](../examples/crew_pbi_analyst_qa.json): import-ready, credentials scrubbed
- [Context enrichment config example](../powerbi-context-enrichment-example.json): copy-paste reference for all 6 enrichment fields

### Analytics and Q&A tools

- [Tool 72 - comprehensive analysis](./tool-72-comprehensive-analysis.md)
- [Tool 79 - semantic model fetcher](./tool-79-semantic-model-fetcher.md)
- [Tool 80 - DAX generator](./tool-80-dax-generator.md)
- [Tool 81 - metadata reducer](./tool-81-metadata-reducer.md)
- [Tool 82 - DAX executor](./tool-82-dax-executor.md)

### Migration tools (extraction)

- [Tool 73 - measure conversion pipeline](./tool-73-measure-conversion.md)
- [Tool 74 - M-Query conversion pipeline](./tool-74-mquery-conversion.md)
- [Tool 75 - relationships tool](./tool-75-relationships.md)
- [Tool 76 - hierarchies tool](./tool-76-hierarchies.md) (Fabric only)
- [Tool 77 - field parameters and calculation groups](./tool-77-field-parameters.md) (Fabric only)
- [Tool 78 - report references tool](./tool-78-report-references.md) (Fabric only, disabled by default)

### UC Metric View generation

- [Tool 85 - DAX to SQL translator](./tool-85-dax-to-sql-translator.md)
- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [Tool 87 - PBI measure allocator](./tool-87-measure-allocator.md)
- [Tool 88 - metric view deployer](./tool-88-metric-view-deployer.md)
- [Tool 89 - config generator](./tool-89-config-generator.md)
- [Tool 90 - pipeline config generator](./tool-90-pipeline-config-generator.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

## Tool map

The two paths below show how the tools chain together for analytics and for migration.

```text
ANALYTICS PATH (answer questions from live PBI data)

  Tool 79: Fetch and cache model metadata
      |
  Tool 81: Reduce to question-relevant subset  (optional but recommended)
      |
  Tool 80: Generate and execute DAX from natural language
                               OR
  Tool 82: Execute a known DAX query directly
                               OR
  Tool 72: All-in-one: question to DAX to execute (single tool)


MIGRATION PATH (move PBI semantic model to Databricks UC Metric Views)

  PHASE 1: Extract
    Tool 74: Extract M-Query (Admin SP required)
    Tool 73: Extract DAX measures (Non-Admin SP)
    Tool 75: Extract relationships (Non-Admin SP, optional)
      |
  PHASE 2: Propose Config
    Tool 90 (live PBI API to full config)  OR  Tool 89 (from extracted JSON)
    NOTE: provide report_id to Tool 90 — it is strongly recommended, not just
          "metadata": without it, measure DAX is fetched in a degraded form
          (bare column names, ~half the measures translatable). Tool 90 now
          auto-discovers the report bound to the dataset if you leave it blank.
      |
  PHASE 3: Human Review (about 2-3h first time, 30min repeat)
      |
  PHASE 4: Generate
    Tool 87: Allocate measures to fact tables (if needed)
    Tool 86: Generate YAML + SQL (the main pipeline)
      |
  PHASE 5: Validate + Deploy
    Tool 88: Dry-run validate, human approval, deploy
```

## Authentication at a glance

Each tool group uses one of three service principal types, summarized below.

| SP type | Used by | Key permission |
|---------|---------|----------------|
| Non-Admin SP (workspace member) | Tools 72, 73, 75, 79, 80, 81, 82 | `Dataset.Read.All` |
| Admin SP (tenant-wide) | Tool 74, Tool 90 | `Tenant.Read.All` (Admin Portal required) |
| Fabric SP | Tools 76, 77, 78 | `SemanticModel.ReadWrite.All` |

See [Authentication and service principal setup](./01-authentication-setup.md) for step-by-step instructions.

## Related

- [Authentication and service principal setup](./01-authentication-setup.md)
- [Simple migration story](./02-simple-migration-story.md)
- [Power BI analytics Q&A case study](./powerbi-analytics-qa-case-study.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)
- [Pipeline config guide](../UCMV_PIPELINE_CONFIG_GUIDE.md)

Back to the [documentation hub](../README.md).
