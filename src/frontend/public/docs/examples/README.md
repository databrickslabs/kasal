# Kasal example crews and flows

Ready-to-import JSON definitions for the Power BI → Unity Catalog Metric View (UCMV) migration pipeline.

> [!TIP]
> Use the **BI Specialist teamspace** (built into Kasal) for zero-config setup.
> All 9 pipeline crews are pre-seeded with tools enabled — just fill in your credentials.
>
> These JSON files are for users who want to **customise** beyond the defaults or import
> individual crews into other teamspaces.

---

## How to import

1. Go to **Crews** or **Flows** in the Kasal UI
2. Click **Import** and upload the JSON file
3. Fill in the credential placeholders in the tool config
4. Save and run

---

## Available crew templates

### UCMV generation pipeline

| File | Tool | Purpose |
|------|------|---------|
| `crew_ucmv_pipeline_config_generator.json` | 90 | Extract PBI metadata via API, propose pipeline config |
| `crew_uc_metric_view_generator.json` | 86 | Generate UC Metric View YAML + Spark SQL from config |
| `crew_ucmv_quality_validator.json` | 91 | Validate translated measures against original DAX |
| `crew_metric_view_deployer.json` | 88 | Deploy UCMV definitions to Databricks |

### Genie Space and dashboard pipeline

| File | Tool | Purpose |
|------|------|---------|
| `crew_ucmv_genie_config_generator.json` | 93 | Auto-generate Genie Space configuration from deployed UCMVs |
| `crew_genie_space_generator.json` | 92 | Create or update a Databricks Genie Space |
| `crew_pbi_report_references.json` | 78 | Extract visual-to-measure references from Fabric reports |
| `crew_pbi_visual_ucmv_mapper.json` | 94 | Map PBI visuals to UC Metric View metric views |
| `crew_databricks_dashboard_creator.json` | 95 | Create Databricks AI/BI (Lakeview) dashboard |

### Analysis

| File | Tool | Purpose |
|------|------|---------|
| `crew_pbi_analyst_qa.json` | 72 | Natural language queries against Power BI datasets |

### Combined pipelines (single import)

| File | Crews | Purpose |
|------|-------|---------|
| `crew_ucmv_deploy_genie_space_pipeline.json` | 88+93+92 | Deploy UCMVs then create Genie Space |

### Flow definitions

| File | Purpose |
|------|---------|
| `flow_ucmv_full_pipeline.json` | Config Generator → UCMV Generator → Validator |
| `flow_ucmv_plus_validation.json` | UCMV pipeline with inline validation |
| `flow_pbi_to_dashboard_pipeline.json` | References → Visual Mapper → Dashboard Creator |

### Reference data

| File | Purpose |
|------|---------|
| `pbi_report_references_pe002_demo.json` | Sample report references output (PE002 dataset) |
| `genie_space_config_example_iom004.json` | Sample Genie Space config (example IOM004 dataset) |

---

## Required credentials

| Placeholder | Where to find it |
|-------------|-----------------|
| `<YOUR_AZURE_TENANT_ID>` | Azure Portal → Azure Active Directory → Overview |
| `<YOUR_SP_CLIENT_ID>` | Azure Portal → App registrations → your app |
| `<YOUR_SP_CLIENT_SECRET>` | Azure Portal → App registrations → Certificates & secrets |
| `<YOUR_DATABRICKS_PAT>` | Databricks → User settings → Developer → Access tokens |
| `<YOUR_DATABRICKS_WORKSPACE>` | Your Databricks workspace URL |

> [!IMPORTANT]
> Store credentials in Kasal **Settings → API Keys** — never hardcode them in crew configs.

---

## Local utility scripts

For running the UCMV pipeline locally without the Kasal UI, use the
`examples/uc_metric_view_migration/` directory in the repository.

## Related

- [UC Metric View pipeline config guide](../UCMV_PIPELINE_CONFIG_GUIDE.md) — config reference for these crews
- [Power BI tools reference](../powerbi/README.md) — the tools used by the pipeline
- [Genie superstore insights blueprint](../Blueprints/Genie_as_Backend%20_for_Agent_Workflows/README.md) — a worked Genie-backed example

Back to the [documentation hub](../README.md).
