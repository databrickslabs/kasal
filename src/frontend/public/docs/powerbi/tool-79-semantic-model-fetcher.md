# Tool 79 - Semantic Model Fetcher

**What it is:** Fetches and caches the full metadata of a Power BI semantic model - measures, tables, columns, relationships, sample data - using a 3-tier fallback to maximize what it can extract.

---

## Why It Exists

Tools 80 (DAX Generator) and 81 (Metadata Reducer) both need model context to work well. Fetching that context is non-trivial: different PBI API tiers expose different levels of detail. Tool 79 handles this complexity once and caches the result, so subsequent calls in the same session don't repeat the API roundtrips.

## What Problem It Solves

- **Separation of concerns:** Decouple model fetching from DAX generation - you can fetch once, then ask many questions
- **Caching:** Same-day cache means fast response for the second, third question about the same model
- **3-tier fallback:** Fabric TMDL API → Admin Scanner API → DAX-based extraction. If one tier fails, the next is tried automatically

---

## 3-Tier Extraction

| Tier | API | What It Provides | Requires |
|------|-----|-----------------|----------|
| 1 (best) | Fabric TMDL `getDefinition` | Full model: measures, columns, relationships, hierarchies | Fabric workspace |
| 2 | Admin Scanner `scanResult` | Measures, tables, columns, M-Query | Admin SP |
| 3 (fallback) | DAX `INFO.MEASURES()` + `INFO.VIEW.RELATIONSHIPS()` | Measures + relationships only | Non-Admin SP |

---

## Microsoft API Reference

- Tier 1: [Fabric - Get Item Definition](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition)
- Tier 2: [Admin - GetScanResult](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/workspace-info-get-scan-result)
- Tier 3: [Datasets - ExecuteQueries](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries)

---

## Authentication

Highest tier available is used automatically based on SP type provided.
**Non-Admin SP** enables Tier 3 minimum. **Admin SP** enables Tier 2.
See [Authentication Setup](./01-authentication-setup.md).

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workspace_id` | Yes | Power BI Workspace GUID |
| `dataset_id` | Yes | Semantic Model / Dataset GUID |
| `tenant_id` | Yes | Azure AD tenant ID |
| `client_id` | Yes | SP client ID |
| `client_secret` | Yes | SP client secret |
| `output_format` | No | `json` (default) - feeds Tool 80/81 |
| `include_sample_data` | No | Include sample column values (improves DAX accuracy) |

---

## Example Crew (Multi-Step Q&A)

```json
{
  "name": "Power BI Multi-Question Analysis",
  "tasks": [
    {
      "name": "Fetch model metadata",
      "description": "Fetch and cache the full semantic model metadata for workspace {workspace_id}, dataset {dataset_id}",
      "tool_ids": [79],
      "tool_config": {
        "79": {
          "workspace_id": "{workspace_id}",
          "dataset_id": "{dataset_id}",
          "tenant_id": "{tenant_id}",
          "client_id": "{client_id}",
          "client_secret": "{client_secret}"
        }
      }
    },
    {
      "name": "Answer question 1",
      "description": "Using the model context from the previous task, answer: What are total sales by region?",
      "tool_ids": [81, 80],
      "depends_on": ["Fetch model metadata"]
    }
  ]
}
```

---

## Output

Returns a JSON blob with:
```json
{
  "workspace_id": "...",
  "dataset_id": "...",
  "measures": [
    {"name": "Total Revenue", "expression": "SUM(Sales[Amount])", "table": "Sales"},
    ...
  ],
  "tables": [{"name": "Sales", "columns": [...]}],
  "relationships": [...],
  "cached_at": "2026-05-17T14:30:00Z"
}
```

This JSON is passed as `model_context_json` to Tool 80 (DAX Generator) or Tool 81 (Metadata Reducer).

---

## Notes

- Cache is per-day - same workspace+dataset within the same calendar day reuses the cache automatically
- For multi-question workflows, always use Tool 79 → 81 → 80 (not Tool 72 each time) to avoid repeated API calls
- The output JSON can be large for complex models (hundreds of measures) - Tool 81 reduces it to question-relevant subset before sending to the LLM
