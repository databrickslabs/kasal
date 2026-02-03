# Power BI Analysis Tool

The Power BI Analysis Tool converts business questions into DAX queries and executes them against Power BI semantic models. It provides a complete question-to-answer pipeline.

## Overview

This tool implements the following flow:

```
┌─────────────────────────────────────────────────────────────┐
│  User Question: "What are total sales by region?"           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  1. Extract Model Context                                   │
│  - Fetch TMDL definition → measures, tables                 │
│  - Execute INFO.VIEW.RELATIONSHIPS() → relationships        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  2. Generate DAX Query                                      │
│  - Use LLM to convert question + context → DAX              │
│  - Fallback: keyword matching if no LLM configured          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Execute DAX via Power BI Execute Queries API            │
│  - Run query against semantic model                         │
│  - Return structured results                                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Find Visual References                                  │
│  - Identify reports using the semantic model                │
│  - Map measures to report visuals                           │
└─────────────────────────────────────────────────────────────┘
```

## Authentication

This tool requires **ONE** authentication method:

### Option 1: Service Principal (Recommended for Production)
- **Required Permission**: `SemanticModel.ReadWrite.All`
- **Parameters**:
  - `tenant_id`: Azure AD tenant ID
  - `client_id`: Application/Client ID
  - `client_secret`: Client secret

### Option 2: User OAuth
- **Parameters**:
  - `access_token`: User access token from Microsoft OAuth flow

## Configuration Parameters

### Required Parameters
| Parameter | Type | Description |
|-----------|------|-------------|
| `user_question` | string | The business question to answer (e.g., "What are total sales by region?") |
| `workspace_id` | string | Power BI Workspace ID (GUID) |
| `dataset_id` | string | Dataset/Semantic Model ID (GUID) |

### Service Principal Authentication
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tenant_id` | string | If SP auth | Azure AD tenant ID |
| `client_id` | string | If SP auth | Application/Client ID |
| `client_secret` | string | If SP auth | Client secret |

### User OAuth Authentication
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `access_token` | string | If OAuth | User access token |

### LLM Configuration (Optional)
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `llm_workspace_url` | string | null | Databricks workspace URL for LLM |
| `llm_token` | string | null | Databricks access token |
| `llm_model` | string | "databricks-claude-sonnet-4" | Model serving endpoint name |

### Options
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_visual_references` | boolean | true | Search for report visuals using the measures |
| `skip_system_tables` | boolean | true | Skip system tables like LocalDateTable |
| `output_format` | string | "markdown" | Output format: "markdown" or "json" |

## Output Structure

### Markdown Output
```markdown
# Power BI Analysis Results

**Question**: What are total sales by region?
**Workspace**: `workspace-id`
**Dataset**: `dataset-id`

## Model Context

- **Measures**: 15
- **Tables**: 8
- **Relationships**: 12

## Generated DAX Query

```dax
EVALUATE
SUMMARIZECOLUMNS(
    'Geography'[Region],
    "Total Sales", [Total Sales]
)
ORDER BY [Total Sales] DESC
```

## Execution Results

✅ **Success** - 5 rows returned

| Region | Total Sales |
| --- | --- |
| North | 1,234,567 |
| South | 987,654 |
| East | 876,543 |
| West | 765,432 |
| Central | 654,321 |

## Visual References

Reports using the queried measures:

- **Sales Dashboard**: [https://app.powerbi.com/...]
- **Regional Analysis**: [https://app.powerbi.com/...]
```

### JSON Output
```json
{
  "user_question": "What are total sales by region?",
  "workspace_id": "...",
  "dataset_id": "...",
  "model_context": {
    "measures": [...],
    "tables": [...],
    "relationships": [...]
  },
  "generated_dax": "EVALUATE SUMMARIZECOLUMNS(...)",
  "dax_execution": {
    "success": true,
    "data": [...],
    "row_count": 5,
    "columns": ["[Region]", "[Total Sales]"],
    "error": null
  },
  "visual_references": [...],
  "errors": []
}
```

## Use Cases

### 1. Answer Business Questions
```python
tool = PowerBIAnalysisTool(
    user_question="What are total sales by region?",
    workspace_id="your-workspace-id",
    dataset_id="your-dataset-id",
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-secret",
    llm_workspace_url="https://your-workspace.cloud.databricks.com",
    llm_token="your-databricks-token"
)
```

### 2. Simple Query Without LLM
```python
# Without LLM config, the tool uses keyword matching
# to find relevant measures
tool = PowerBIAnalysisTool(
    user_question="Show me revenue",
    workspace_id="your-workspace-id",
    dataset_id="your-dataset-id",
    access_token="user-oauth-token"
)
```

### 3. Dynamic Mode (Databricks Apps Integration)
Configure placeholders in the UI, then pass values at runtime:
```json
{
  "user_question": "{user_question}",
  "workspace_id": "{workspace_id}",
  "dataset_id": "{dataset_id}",
  "access_token": "{access_token}"
}
```

## DAX Generation

### With LLM
When LLM is configured, the tool:
1. Builds a context prompt with available measures, tables, and relationships
2. Sends the user question + context to the LLM
3. Extracts the clean DAX query from the response

### Without LLM (Fallback)
When LLM is not configured:
1. Tokenizes the user question
2. Matches keywords against measure names
3. Generates a simple `SUMMARIZECOLUMNS` query with the best-matching measure

## Error Handling

Errors are captured and returned in the output:
- **Authentication errors**: Token acquisition failures
- **Model extraction errors**: TMDL fetch or relationship query failures
- **DAX generation errors**: LLM call failures (falls back to simple DAX)
- **Execution errors**: Power BI API errors

## Integration with Other Tools

This tool is designed to complement other Power BI tools:
- Use **M-Query Conversion Tool** for data source migration
- Use **Measure Conversion Tool** for measure documentation
- Use **Report References Tool** for detailed visual analysis
- Use **this tool** for ad-hoc business questions

## Best Practices

1. **Configure LLM** for best DAX generation results
2. **Use Service Principal** in production for consistent access
3. **Start with simple questions** to validate model understanding
4. **Check errors array** in output for troubleshooting
5. **Use JSON output** when integrating with downstream systems

## Limitations

- LLM-generated DAX quality depends on model quality and context size
- Visual reference search is limited to report discovery (not detailed visual parsing)
- Complex questions may require manual DAX refinement
- Maximum 20 measures/tables passed to LLM context to avoid token limits
