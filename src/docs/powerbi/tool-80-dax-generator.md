# Tool 80 - semantic model DAX generator

**What it is:** Takes a user question and model context JSON (from Tool 79), uses an LLM to generate a DAX EVALUATE query, executes it against Power BI, and returns the result, with automatic retry and self-correction.

---

## Why it exists

Tool 72 does everything in one step (model fetch + DAX generation + execution). Tool 80 is the "execution step only", designed for multi-question workflows where the model has already been fetched (Tool 79) and optionally reduced (Tool 81). This avoids fetching the model for every single question.

## What problem it solves

- **Efficiency in multi-question sessions:** Ask 10 questions against the same model without 10 API roundtrips to fetch model metadata
- **Fine-grained control:** Run metadata reduction (Tool 81) before DAX generation to improve accuracy
- **Composability:** In an agent workflow, the model fetch, reduction, and generation steps can each be optimized or replaced independently

---

## How it works

```text
Receive model_context_json (from Tool 79 or Tool 81 output)
  |
  v
Build LLM prompt: user question + relevant model context
  |
  v
LLM generates DAX EVALUATE statement
  |
  v
Validate measure names (hallucination detection)
  |
  v
Execute via Power BI Execute Queries API
  |
  v
If failed: LLM reads error, produces corrected DAX, retry (up to N times)
  |
  v
Return results + retry history
```

---

## Microsoft API reference

Uses: `POST /groups/{groupId}/datasets/{datasetId}/executeQueries`
Docs: [Datasets - ExecuteQueries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries)

---

## Authentication

Same credentials used for the original model fetch. Typically **Non-Admin SP**.
See [Authentication Setup](./01-authentication-setup.md).

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `model_context_json` | Yes (or cache) | JSON from Tool 79 or Tool 81 |
| `user_question` | Yes | Business question in plain English |
| `workspace_id` | Yes | Power BI Workspace GUID |
| `dataset_id` | Yes | Semantic Model / Dataset GUID |
| `tenant_id` | Yes | Azure AD tenant ID |
| `client_id` | Yes | SP client ID |
| `client_secret` | Yes | SP client secret |
| `llm_workspace_url` | Yes | Databricks workspace URL for LLM |
| `llm_token` | Yes | Databricks PAT for LLM |
| `llm_model` | No | Model endpoint (default: `databricks-claude-sonnet-4`) |
| `max_retries` | No | Retry limit (default: `5`) |
| `active_filters` | No | Automatically applied CALCULATE filters |
| `business_terms` | No | Synonym map (e.g. `{"revenue": "Total Revenue"}`) |

---

## Example crew (full multi-step Q&A)

```json
{
  "name": "Multi-Question Power BI Analysis",
  "tasks": [
    {
      "name": "Fetch model",
      "description": "Fetch semantic model for workspace {workspace_id}, dataset {dataset_id}",
      "tool_ids": [79]
    },
    {
      "name": "Reduce context for Q1",
      "description": "Reduce model context for question: What is total revenue by country?",
      "tool_ids": [81],
      "depends_on": ["Fetch model"]
    },
    {
      "name": "Answer Q1",
      "description": "Using reduced model context, answer: What is total revenue by country?",
      "tool_ids": [80],
      "depends_on": ["Reduce context for Q1"]
    }
  ]
}
```

---

## Notes

- If `model_context_json` is not provided, Tool 80 attempts to read from cache using `workspace_id` + `dataset_id`
- `business_terms` lets you map customer language to DAX measure names (e.g. their team says "bookings" but the measure is "Total Orders")
- `active_filters` automatically wraps CALCULATE around the generated DAX - useful for models with default date/region filters
- For a single question without caching concerns, Tool 72 is simpler

## See also

- [Power BI integration hub](./README.md)
- [Authentication and service principal setup](./01-authentication-setup.md)
- [Tool 79 - semantic model fetcher](./tool-79-semantic-model-fetcher.md)
- [Tool 81 - metadata reducer](./tool-81-metadata-reducer.md)
- [Power BI analytics Q&A case study](./powerbi-analytics-qa-case-study.md)

Back to the [Power BI integration hub](./README.md).
