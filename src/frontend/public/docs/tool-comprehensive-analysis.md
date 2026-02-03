# Power BI Comprehensive Analysis Tool

The Power BI Comprehensive Analysis Tool converts business questions into DAX queries and executes them against Power BI semantic models. It provides a complete question-to-answer pipeline with intelligent self-correction.

## Overview

This tool implements the following flow with **automatic retry and self-correction**:

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
│  2. Generate DAX Query (with LLM)                           │
│  - Use LLM to convert question + context → DAX              │
│  - Validate measure names (hallucination detection)         │
│  - Fallback: keyword matching if no LLM configured          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  3. Execute DAX via Power BI Execute Queries API            │
│  - Run query against semantic model                         │
│  ┌──────────────────────────────────────────┐               │
│  │ If Failed: Self-Correction Loop (Max 5x) │               │
│  │ 1. Capture error                          │               │
│  │ 2. LLM analyzes and fixes query           │               │
│  │ 3. Re-execute corrected query             │               │
│  │ 4. Repeat until success or max retries    │               │
│  └──────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  4. Find Visual References (Optional)                       │
│  - Identify reports using the semantic model                │
│  - Map measures to report visuals                           │
└─────────────────────────────────────────────────────────────┘
```

## Key Features

### 🎯 Intelligent Self-Correction
- **Automatic retry up to 5 times** (configurable)
- LLM analyzes errors and generates corrected DAX
- Handles table/column name errors, syntax errors, relationship issues
- Full retry history logged and included in output

### 🛡️ Measure Hallucination Detection
- Validates that generated DAX uses only available measures
- Logs warnings when LLM invents non-existent measure names
- Prevents incorrect data from being returned

### 📝 Enhanced Logging
- Tracks measure extraction from model
- Shows sample measures for verification
- Logs full LLM prompts and responses
- Detailed retry attempt information

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

### Retry Configuration
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_dax_retries` | int | 5 | Maximum retry attempts if DAX execution fails (1-10) |

### Options
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_visual_references` | boolean | true | Search for report visuals using the measures |
| `skip_system_tables` | boolean | true | Skip system tables like LocalDateTable |
| `output_format` | string | "markdown" | Output format: "markdown" or "json" |

## Output Structure

### Markdown Output (with retries)
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

**Attempts**: 2 (successful on attempt 2)

### Retry History
**Attempt 1**: ❌ Failed
  - Error: Table 'Sales' does not exist or is not visible...
**Attempt 2**: ✅ Success

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

### JSON Output (with retries)
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
  "dax_attempts": [
    {
      "attempt": 1,
      "dax": "EVALUATE\nSUMMARIZECOLUMNS('Sales'[Region]...)",
      "success": false,
      "error": "Table 'Sales' does not exist",
      "row_count": 0
    },
    {
      "attempt": 2,
      "dax": "EVALUATE\nSUMMARIZECOLUMNS('Geography'[Region]...)",
      "success": true,
      "error": null,
      "row_count": 5
    }
  ],
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

### 3. Custom Retry Configuration
```python
# Reduce retries for faster response
tool = PowerBIAnalysisTool(
    user_question="What is my average gross revenue?",
    workspace_id="your-workspace-id",
    dataset_id="your-dataset-id",
    access_token="user-oauth-token",
    max_dax_retries=3  # Only retry 3 times instead of default 5
)
```

### 4. Dynamic Mode (Databricks Apps Integration)
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
4. **Validates that generated DAX uses only available measures**
5. Logs warnings if non-existent measures are detected

**Enhanced Prompt Instructions**:
```
## CRITICAL INSTRUCTIONS
1. **ONLY use measure names from the "Available Measures" list above**
2. **DO NOT invent, modify, or guess measure names**
3. If the question asks for "average", use AVERAGEX or include averaging logic
4. Use EVALUATE with SUMMARIZECOLUMNS or other appropriate DAX functions
5. Return ONLY the DAX query without explanations or markdown formatting

## Examples of CORRECT measure usage:
- If measure is listed as "Total Sales", use [Total Sales]
- If measure is listed as "Revenue Amount", use [Revenue Amount]

## Examples of INCORRECT usage (DO NOT DO THIS):
- DO NOT use [Total_Sales] if only [Total Sales] exists
- DO NOT add suffixes like [measure_doc] or [measure_calc]
- DO NOT modify measure names in any way
```

### Without LLM (Fallback)
When LLM is not configured:
1. Tokenizes the user question
2. Matches keywords against measure names
3. Generates a simple `SUMMARIZECOLUMNS` query with the best-matching measure

## Self-Correction Mechanism

### How It Works

1. **First Attempt**: LLM generates DAX query based on user question
2. **Execute & Check**: Query is executed against Power BI
3. **If Failed**: Enter self-correction loop
   - **Capture Error**: Get exact error message from Power BI
   - **Analyze**: LLM analyzes the previous failed attempt
   - **Learn**: LLM identifies the specific issue
   - **Correct**: Generate a DIFFERENT query that addresses the error
   - **Retry**: Execute the corrected query
4. **Repeat**: Continue until success or max retries reached

### What Gets Corrected

✅ **Table/Column Name Errors**
- Wrong table name → Use correct table from available tables
- Wrong column name → Use correct column from table definition
- Case sensitivity issues → Match exact names from model

✅ **Syntax Errors**
- Missing parentheses
- Incorrect DAX function usage
- Invalid operators

✅ **Relationship Errors**
- Using non-existent relationships
- Incorrect join logic

✅ **Type Mismatches**
- Using text column where number expected
- Incorrect aggregation functions

✅ **Measure Reference Errors**
- Non-existent measure → Use available measures
- Incorrect measure syntax

### Retry Configuration

**Default**: `max_dax_retries: 5` (recommended for complex queries)

**Simple queries**: `max_dax_retries: 2-3` (faster response)
**Production**: `max_dax_retries: 3` (balance between success and latency)

### Performance Impact

- **First attempt success**: No impact (same as before)
- **Retry needed**: +2-5 seconds per retry (LLM call + DAX execution)
- **5 retries**: Max ~25-30 seconds additional latency

## Measure Hallucination Detection

### Problem
LLMs may "hallucinate" measure names that don't exist in the Power BI model, leading to:
- Incorrect data being returned
- Non-existent values appearing in results
- Default/error values (like 1.0) being returned

### Solution
The tool now:
1. **Extracts all available measures** from the Power BI model
2. **Validates generated DAX** by checking all `[measure]` references
3. **Logs warnings** when non-existent measures are detected
4. **Prevents incorrect data** through validation

### Logging Example
```
[DAX Generation] Extracted 15 measures from model
[DAX Generation] Sample measures: ['Total Sales', 'Revenue', 'Profit', ...]
[DAX Generation] LLM may have used non-existent measures: ['country_gross_revenue_doc']
[DAX Generation] Available measures are: ['Total Revenue', 'Sales Amount', ...]
```

## Error Handling

Errors are captured and returned in the output:
- **Authentication errors**: Token acquisition failures
- **Model extraction errors**: TMDL fetch or relationship query failures
- **DAX generation errors**: LLM call failures (falls back to simple DAX)
- **Execution errors**: Power BI API errors (triggers retry mechanism)
- **Visual reference errors**: Report discovery failures

## Integration with Other Tools

This tool is designed to complement other Power BI tools:
- Use **M-Query Conversion Tool** for data source migration
- Use **Measure Conversion Tool** for measure documentation
- Use **Report References Tool** for detailed visual analysis
- Use **this tool** for ad-hoc business questions and data exploration

## Best Practices

1. **Configure LLM** for best DAX generation results and self-correction
2. **Use Service Principal** in production for consistent access
3. **Start with simple questions** to validate model understanding
4. **Check retry history** in output to understand correction process
5. **Monitor logs** for measure hallucination warnings
6. **Use JSON output** when integrating with downstream systems
7. **Adjust retry count** based on use case (2-3 for UI, 5 for background)

## Limitations

- LLM-generated DAX quality depends on model quality and context size
- Visual reference search is limited to report discovery (not detailed visual parsing)
- Complex questions may require manual DAX refinement after self-correction
- Maximum 20 measures/tables passed to LLM context to avoid token limits
- Self-correction may not fix authentication, permission, or timeout errors

## Debugging

### Check Logs
```bash
tail -f src/backend/logs/api.log | grep "DAX Generation"
```

### Look for:
- **Measure extraction**: `[DAX Generation] Extracted N measures from model`
- **Hallucination warnings**: `[DAX Generation] LLM may have used non-existent measures`
- **Retry attempts**: `[DAX Self-Correction] Attempt N Prompt:`
- **Success/Failure**: `✅ DAX execution successful on attempt N` or `❌ DAX execution failed`

### Common Issues

**No measures found**:
```
[DAX Generation] No measures found in model context!
```
→ Check that the semantic model has measures and SP has access

**Measure hallucination**:
```
[DAX Generation] LLM may have used non-existent measures: ['fake_measure']
```
→ LLM invented a measure name - check retry attempts for correction

**All retries failed**:
```
❌ DAX execution failed after 5 attempts
```
→ Review error messages in retry history, may need manual DAX refinement

## Related Documentation

- **Implementation**: `src/backend/src/engines/crewai/tools/custom/powerbi_analysis_tool.py`
- **Retry Mechanism Guide**: [powerbi-retry-mechanism.md](./powerbi-retry-mechanism.md)
- **Hallucination Fix Guide**: [powerbi-measure-hallucination-fix.md](./powerbi-measure-hallucination-fix.md)
- **PowerBI Tools Overview**: [powerbi-tools-guide.md](./powerbi-tools-guide.md)
