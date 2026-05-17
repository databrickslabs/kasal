# Tool 82 - Power BI DAX Executor

**What it is:** Executes a pre-written DAX `EVALUATE` statement directly against a Power BI semantic model and returns the results. No LLM, no retries.

---

## Why It Exists

Tools 72 and 80 generate DAX from natural language. But sometimes you already *have* the DAX - you wrote it, copied it from Power BI Desktop, or validated it previously. In that case, you don't need LLM generation. Tool 82 is the lightweight execution-only tool.

## What Problem It Solves

- **Known DAX execution:** Run a tested, known-good query programmatically without LLM overhead
- **Integration workflows:** After Tool 80 produces a DAX query, save it and re-run via Tool 82 on future calls (faster, no LLM cost)
- **Debugging:** Test whether a specific DAX query works against a model before building a crew around it

---

## How It Works

```
Receive DAX EVALUATE statement as plain text
    ↓
Authenticate to Power BI Execute Queries API
    ↓
Execute query
    ↓
Return results in chosen format
```

Simple. No generation, no retries, no hallucination detection.

---

## Microsoft API Reference

Uses: `POST /groups/{groupId}/datasets/{datasetId}/executeQueries`
Docs: [Datasets - ExecuteQueries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries)

---

## Authentication

**Non-Admin SP** (workspace member) with `SemanticModel.ReadWrite.All`, or user OAuth token.
See [Authentication Setup](./01-authentication-setup.md).

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workspace_id` | Yes | Power BI Workspace GUID |
| `dataset_id` | Yes | Semantic Model / Dataset GUID |
| `dax_query` | Yes | Full `EVALUATE` DAX statement |
| `auth_method` | Yes | `service_principal`, `service_account`, or `user_oauth` |
| `tenant_id` | SP/SA | Azure AD tenant ID |
| `client_id` | SP | SP application client ID |
| `client_secret` | SP | SP client secret |
| `username` | SA | User principal name |
| `password` | SA | Password |
| `access_token` | OAuth | Pre-obtained Bearer token |
| `output_format` | No | `markdown` (default), `json`, or `table` |

---

## Example Crew

```json
{
  "name": "Run Known DAX Query",
  "tasks": [{
    "name": "Execute revenue by region query",
    "description": "Execute the validated DAX query for revenue by region",
    "tool_ids": [82],
    "tool_config": {
      "82": {
        "workspace_id": "{workspace_id}",
        "dataset_id": "{dataset_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}",
        "auth_method": "service_principal",
        "dax_query": "EVALUATE SUMMARIZECOLUMNS('Geography'[Region], \"Total Revenue\", [Total Revenue]) ORDER BY [Total Revenue] DESC",
        "output_format": "markdown"
      }
    }
  }]
}
```

---

## Example DAX Queries

```dax
-- Simple measure
EVALUATE ROW("Total Revenue", [Total Revenue])

-- Grouped summary
EVALUATE
SUMMARIZECOLUMNS(
    'Date'[Year],
    'Geography'[Country],
    "Revenue", [Total Revenue],
    "Units", [Total Units]
)
ORDER BY 'Date'[Year] DESC, [Revenue] DESC

-- Filtered
EVALUATE
CALCULATETABLE(
    SUMMARIZECOLUMNS('Product'[Category], "Sales", [Total Sales]),
    'Date'[Year] = 2025
)
```

---

## When to Use vs Other Tools

| Scenario | Use |
|----------|-----|
| You have a working DAX query | **Tool 82** (this tool) |
| Natural language → DAX → answer (one shot) | Tool 72 |
| Multi-step: fetch model → reduce → generate | Tools 79 → 81 → 80 |

---

## Notes

- The DAX query must be a complete `EVALUATE` statement - partial expressions like `[Total Revenue]` will fail
- No retry on failure - if the query errors, fix it and re-run
- Results are bounded by Power BI's default row limit for Execute Queries API
