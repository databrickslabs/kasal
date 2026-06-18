# Tool 72 - Power BI Comprehensive Analysis

**What it is:** An all-in-one tool that takes a natural language question, generates a DAX query from it, executes it live against your Power BI semantic model, and returns the answer - with automatic self-correction if the generated DAX fails.

---

## Why It Exists

Business analysts want to ask "What were total sales by region last quarter?" and get an answer without knowing DAX or needing access to Power BI Desktop. Power BI's own Q&A feature is limited to a fixed set of pre-trained question patterns. This tool uses an LLM to generate arbitrary DAX from any question, executes it via the Power BI Execute Queries API, and retries intelligently when the generated query fails.

## What Problem It Solves

- **Non-technical users** can explore Power BI data with plain English
- **SAs demoing Databricks** can answer live customer questions from their PBI model without writing DAX manually
- **Measure validation** - test whether existing DAX logic returns what you expect
- **DAX learning** - see how the LLM translates questions into correct DAX expressions

---

## How It Works

```
User question
    ↓
Extract model context (measures, tables, relationships from TMDL / DAX APIs)
    ↓
LLM generates DAX EVALUATE statement
    ↓
Validate: are all [measure] references real? (hallucination detection)
    ↓
Execute via Power BI Execute Queries API
    ↓
If failed → LLM reads error, fixes DAX → retry (up to 5x)
    ↓
Return results + retry history + visual references (which reports use these measures)
```

---

## Microsoft API Reference

Uses: `POST /groups/{groupId}/datasets/{datasetId}/executeQueries`
Docs: [Datasets - ExecuteQueries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries)

---

## Authentication

Requires **Non-Admin SP** (workspace member) with `SemanticModel.ReadWrite.All` permission, or a user OAuth token.
See [Authentication Setup](./01-authentication-setup.md).

---

## Configuration

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `user_question` | Yes | - | The business question in plain English |
| `workspace_id` | Yes | - | Power BI Workspace GUID |
| `dataset_id` | Yes | - | Semantic Model / Dataset GUID |
| `tenant_id` | SP auth | - | Azure AD tenant ID |
| `client_id` | SP auth | - | SP application client ID |
| `client_secret` | SP auth | - | SP client secret |
| `access_token` | OAuth | - | User OAuth token (alternative to SP) |
| `llm_workspace_url` | No | - | Databricks workspace URL for LLM |
| `llm_token` | No | - | Databricks PAT for LLM |
| `llm_model` | No | `databricks-claude-sonnet-4` | LLM endpoint name |
| `max_dax_retries` | No | `5` | Retry attempts on failure (1-10) |
| `include_visual_references` | No | `true` | Find reports using the queried measures |
| `output_format` | No | `markdown` | `markdown` or `json` |

---

## Example Crew

```json
{
  "name": "Power BI Q&A",
  "agents": [{
    "name": "BI Analyst",
    "role": "Power BI data analyst",
    "goal": "Answer business questions using Power BI data"
  }],
  "tasks": [{
    "name": "Answer business question",
    "description": "Answer this question from our Power BI sales model: {user_question}",
    "tool_ids": [72],
    "tool_config": {
      "72": {
        "workspace_id": "{workspace_id}",
        "dataset_id": "{dataset_id}",
        "tenant_id": "{tenant_id}",
        "client_id": "{client_id}",
        "client_secret": "{client_secret}",
        "llm_workspace_url": "{llm_workspace_url}",
        "llm_token": "{llm_token}",
        "max_dax_retries": 5,
        "output_format": "markdown"
      }
    }
  }]
}
```

---

## Example Output

```markdown
**Question**: What are total sales by region for 2024?

## Generated DAX Query (Attempt 1 - Success)

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Geography'[Region],
    "Total Sales", [Total Sales]
)
ORDER BY [Total Sales] DESC
```

## Results (5 rows)

| Region  | Total Sales |
|---------|-------------|
| North   | 1,234,567   |
| South   | 987,654     |
| East    | 876,543     |
| West    | 765,432     |
| Central | 654,321     |

## Visual References
Reports using these measures:
- **Sales Dashboard** - Revenue Overview page
- **Regional Performance** - All pages
```

---

## When to Use vs Other Tools

| Scenario | Tool |
|----------|------|
| Single natural language question | **Tool 72** (this tool - all-in-one) |
| Complex multi-step analysis, want to cache model | Tools 79 → 81 → 80 (multi-step) |
| You already have a working DAX query | Tool 82 |

---

## Notes

- Without LLM configured, falls back to keyword-based measure matching (less accurate)
- Self-correction handles table name errors, syntax errors, and measure reference issues - but not authentication or timeout errors
- Maximum 20 measures/tables sent to LLM context to avoid token limits on very large models
- All retry attempts and the final query are included in the output for debugging
