# Kasal Example Crews & Flows

Ready-to-import JSON definitions for the Power BI → Unity Catalog Metric View (UCMV) migration pipeline.
Copy a file into Kasal via **Import** in the Crews or Flows canvas, fill in your credentials, and run.

> **Security**: All credentials are placeholders (`<YOUR_…>`).
> Never commit real tokens, client secrets, or tenant IDs.

---

## UCMV Migration Pipeline — Overview

The full Power BI → UC Metric View migration is a **3-crew flow**:

```
┌─────────────────────────────────┐
│  Crew 1: Pipeline Config        │  Extracts PBI metadata via API,
│  Generator (API-Direct)         │  proposes join maps, filters, etc.
└──────────────┬──────────────────┘
               │ output: config_json
               ▼
┌─────────────────────────────────┐
│  Crew 2: UC Metric View         │  Translates DAX → Spark SQL,
│  Generator (JSON Mode)          │  generates YAML + SQL for each
└──────────────┬──────────────────┘  metric view, deploys to UC.
               │ output: yaml / sql / stats
               ▼
┌─────────────────────────────────┐
│  Crew 3: UCMV Quality           │  Validates every measure's Spark
│  Validator                      │  SQL against original DAX.
└─────────────────────────────────┘  Reports VALID / EQUIVALENT /
                                     REVIEW / INVALID per measure.
```

All three crews are wired together in **`flow_ucmv_plus_validation.json`**.
You can also import and run each crew individually.

---

## Files in This Folder

| File | Type | Purpose |
|------|------|---------|
| `crew_ucmv_pipeline_config_generator.json` | Crew | Crew 1 — extracts PBI metadata, proposes pipeline config |
| `crew_uc_metric_view_generator.json` | Crew | Crew 2 — generates UC Metric View YAML + SQL |
| `crew_ucmv_quality_validator.json` | Crew | Crew 3 — validates translated measures |
| `flow_ucmv_plus_validation.json` | Flow | Full 3-crew flow (Crew 1 → 2 → 3) |

---

## Crew 1 — UCMV Pipeline Config Generator

**File**: `crew_ucmv_pipeline_config_generator.json`

### What it does
Connects to the Power BI REST API and Admin Scanner to extract your semantic model's full metadata, then proposes a `config_json` that drives the UCMV Generator (join key maps, filter sets, SWITCH decompositions, measure resolutions).

### Agent & Task
- **Agent**: Config Generator Agent
- **Task**: Generate Pipeline Config from PBI APIs
- **Tool**: Pipeline Config Generator (Tool 90)

### Required credentials (fill in tool config after import)

| Field | Value |
|-------|-------|
| `workspace_id` | Your Power BI workspace UUID |
| `dataset_id` | Your semantic model (dataset) UUID |
| `report_id` | Any report UUID in that workspace |
| `tenant_id` | Your Azure tenant ID |
| `client_id` | Service principal client ID |
| `client_secret` | Service principal client secret |
| `catalog` | Databricks Unity Catalog name |
| `schema_name` | Target schema for metric views |

### Output
A `config_json` object with:
- `join_key_map` — dimension table join keys
- `fact_join_map` — fact table expressions and filters
- `filter_sets` — named filter value collections
- `switch_decompositions` — SWITCH/IF measure splits
- `measure_resolutions` — cross-table measure fixes

---

## Crew 2 — UC Metric View Generator (JSON Mode)

**File**: `crew_uc_metric_view_generator.json`

### What it does
Takes the `config_json` from Crew 1 (plus PBI metadata inputs: `measures_json`, `mquery_json`, `relationships_json`, `scan_data_json`) and generates UC Metric View YAML definitions and Spark SQL for every fact table. Optionally deploys the views directly to Unity Catalog.

### Agent & Task
- **Agent**: UCMV Migration Agent
- **Task**: Generate UC Metric Views
- **Tool**: UC Metric View Generator (Tool 86)

### Required inputs (execution variables)

| Variable | Description |
|----------|-------------|
| `measures_json` | DAX measure list from Tool 73 (Measure Conversion Pipeline) |
| `mquery_json` | MQuery transpilations from Tool 74 (MQuery Conversion Pipeline) |
| `relationships_json` | PBI relationships from Tool 75 |
| `scan_data_json` | Admin scanner output from Tool 79 |
| `config_json` | Pipeline config from Crew 1 (or hand-edited) |

### Required credentials (fill in tool config after import)

| Field | Value |
|-------|-------|
| `catalog` | `your_catalog` |
| `schema_name` | `your_schema` |
| `tenant_id` | Azure tenant ID |
| `client_id` | Service principal client ID |
| `client_secret` | Service principal client secret |
| `workspace_id` | Power BI workspace UUID |
| `dataset_id` | Power BI dataset UUID |

### Output
```json
{
  "yaml":  { "FactTable1": "version: 1.1\nsource: ...", ... },
  "sql":   { "FactTable1": "CREATE METRIC VIEW ...", ... },
  "stats": { "total_measures": 470, "translated": 450, ... },
  "measures_with_dax": [ ... ]
}
```

The `yaml` dict can be **edited in the Kasal UI** before the Validator runs.
Click **Save** on the result to persist your edits — the Validator picks them up automatically on the next run.

---

## Crew 3 — UCMV Quality Validator

**File**: `crew_ucmv_quality_validator.json`

### What it does
Receives the UCMV Generator output (YAML + measures) and runs the `MetricExpressionValidatorPipeline` for each fact table. Compares each measure's translated Spark SQL expression against the original DAX to detect semantic mismatches.

### Agent & Task
- **Agent**: UCMV Validation Agent
- **Task**: Validate UC Metric Views
- **Tool**: Metric View Validator (Tool 91)

### No credentials required
This crew only needs the UCMV Generator's output — no PBI or Databricks credentials.

### Output — Quality Report

```json
{
  "summary": {
    "tables_validated": 23,
    "total_evaluated": 470,
    "total_valid": 412,
    "total_equivalent": 35,
    "total_review": 18,
    "total_invalid": 5
  },
  "per_table": {
    "FactSalesActuals": {
      "evaluated": 48, "valid": 44, "equivalent": 3, "review": 1, "invalid": 0,
      "details": [ ... ]
    }
  }
}
```

| Status | Meaning |
|--------|---------|
| `VALID` | Spark SQL is semantically identical to the DAX expression |
| `EQUIVALENT` | Different syntax, equivalent result |
| `REVIEW` | Minor differences — human review recommended |
| `INVALID` | Translation error — fix required before deploying |

---

## Flow — UCMV + Validation (full pipeline)

**File**: `flow_ucmv_plus_validation.json`

### What it does
Chains all three crews in sequence using Kasal's Flow canvas:

```
Pipeline Config Generator  →  UC Metric View Generator  →  UCMV Quality Validator
```

Data is passed automatically between crews — no copy-paste of intermediate outputs.

### Import steps
1. Go to **Flows** in the Kasal UI → **Import**
2. Upload `flow_ucmv_plus_validation.json`
3. Open each crew node and fill in the credential placeholders in the tool config
4. Set execution inputs: `measures_json`, `mquery_json`, `relationships_json`, `scan_data_json`
5. Click **Run**

### Editing YAML before validation
After Crew 2 completes you can edit the generated YAML before the Validator runs:

1. Open the execution result for the UCMV Generator crew
2. Use the YAML editor to modify individual metric view definitions
3. Click **Save** — edits are persisted and will be used by the next Validator run

> To pause execution between Crew 2 and Crew 3 for manual review, add a **HITL (Human-in-the-Loop)** node between them in the Flow canvas.

---

## Credentials Reference

All placeholder fields across the three crews:

| Placeholder | Where to find it |
|-------------|-----------------|
| `<YOUR_AZURE_TENANT_ID>` | Azure Portal → Azure Active Directory → Overview |
| `<YOUR_SP_CLIENT_ID>` | Azure Portal → App registrations → your app → Application (client) ID |
| `<YOUR_SP_CLIENT_SECRET>` | Azure Portal → App registrations → your app → Certificates & secrets |
| `<YOUR_ADMIN_SP_CLIENT_ID>` | Service principal with Power BI Admin API access |
| `<YOUR_ADMIN_SP_CLIENT_SECRET>` | Corresponding secret for admin SP |
| `<YOUR_DATABRICKS_PAT>` | Databricks workspace → User settings → Developer → Access tokens |
| `<YOUR_DATABRICKS_WORKSPACE>` | Databricks workspace URL (e.g. `adb-1234567890.12.azuredatabricks.net`) |

> Store credentials in Kasal's **API Keys** store (Settings → API Keys) rather than hardcoding them in tool configs.
