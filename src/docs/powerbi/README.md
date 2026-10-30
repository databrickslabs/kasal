# Power BI integration

The toolkit for migrating Power BI semantic models to Databricks Unity Catalog metric views and keeping them in sync.

This section covers everything from first-time authentication setup to the full UCMV migration pipeline.

## In this section

- [What you can do](#what-you-can-do)
- [Quick navigation](#quick-navigation)
- [Tool map](#tool-map)
- [Authentication at a glance](#authentication-at-a-glance)

## What you can do

The tables below map common goals to the tools and guide that cover them.

| Use case | Tools | Guide |
|----------|-------|-------|
| Migrate a PBI model to UC Metric Views | 86, 88, 90, 91 | [UCMV migration guide](./ucmv-migration-guide.md) |
| Keep deployed metric views in sync with PBI | 98 | Continuous Drift Monitor crew (BI Specialist workspace) |
| Convert KPI definitions (YAML to DAX / SQL / UC Metrics) | 73 | [Tool 73 - measure conversion](./tool-73-measure-conversion.md) |
| Execute a known DAX query | 82 | [Tool 82 - DAX executor](./tool-82-dax-executor.md) |
| Migrate Fabric hierarchies | 76 | [Tool 76 - hierarchies](./tool-76-hierarchies.md) |
| Migrate Fabric field parameters / calc groups | 77 | [Tool 77 - field parameters](./tool-77-field-parameters.md) |
| Understand report-to-measure dependencies | 78 | [Tool 78 - report references](./tool-78-report-references.md) |

## Quick navigation

### Setup

- [Authentication and service principal setup](./01-authentication-setup.md) (start here)
- [Simple migration story](./02-simple-migration-story.md)
- [PBI → UCMV pipeline architecture](./ucmv-pipeline-architecture.md): end-to-end walkthrough — extraction → config → M-query path → LLM-first DAX translation with skill files → deploy, with the code location of each stage

### Execution

- [Tool 82 - DAX executor](./tool-82-dax-executor.md) (also used by reconciliation)

### Extraction and Fabric tools

- [Tool 73 - measure conversion pipeline](./tool-73-measure-conversion.md)
- [Tool 76 - hierarchies tool](./tool-76-hierarchies.md) (Fabric only)
- [Tool 77 - field parameters and calculation groups](./tool-77-field-parameters.md) (Fabric only)
- [Tool 78 - report references tool](./tool-78-report-references.md) (Fabric only, disabled by default)

### UC Metric View generation

- [Tool 86 - UC Metric View generator](./tool-86-uc-metric-view-generator.md)
- [Tool 88 - metric view deployer](./tool-88-metric-view-deployer.md)
- [Tool 90 - pipeline config generator](./tool-90-pipeline-config-generator.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)

## Tool map

The migration pipeline chains the tools below; the BI Specialist workspace has a
preseeded crew for each step.

```text
  Tool 90: Extract the Power BI model (measures, M-queries, relationships)
      |    provide report_id — without it measure DAX comes back degraded
      |    (Tool 90 auto-discovers the report bound to the dataset if blank)
  Human review of the proposed config
      |
  Tool 86: Generate UC Metric View YAML (+ optional reconciliation vs PBI)
      |
  Tool 91: Validate translated measures against the original DAX
      |
  Tool 88: Deploy (dry-run first)
      |
  Tool 98: Continuous drift monitoring against the live PBI model
```

## Authentication at a glance

Each tool group uses one of three service principal types, summarized below.

| SP type | Used by | Key permission |
|---------|---------|----------------|
| Non-Admin SP (workspace member) | Tools 73, 82, 90, 98 | `Dataset.Read.All` |
| Admin SP (tenant-wide) | Tools 90, 98 | `Tenant.Read.All` (Admin Portal required) |
| Fabric SP | Tools 76, 77, 78 | `SemanticModel.ReadWrite.All` |

See [Authentication and service principal setup](./01-authentication-setup.md) for step-by-step instructions.

## Related

- [Authentication and service principal setup](./01-authentication-setup.md)
- [Simple migration story](./02-simple-migration-story.md)
- [End-to-end UCMV migration guide](./ucmv-migration-guide.md)
- [Pipeline config guide](../UCMV_PIPELINE_CONFIG_GUIDE.md)

Back to the [documentation hub](../README.md).
