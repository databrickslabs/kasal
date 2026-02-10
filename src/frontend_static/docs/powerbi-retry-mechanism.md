# PowerBI Analysis Tool - Self-Correcting Retry Mechanism

## Overview

The PowerBI Analysis Tool now includes an intelligent retry mechanism with LLM self-correction. If a generated DAX query fails, the system will automatically attempt to fix it up to 5 times (configurable).

## How It Works

### 1. First Attempt
- LLM generates DAX query based on user question and model context
- Query is executed against Power BI
- If successful → return results ✅
- If failed → proceed to retry with error feedback

### 2. Self-Correction Loop (Attempts 2-5)
For each retry:
1. **Capture Error**: Get the exact error message from Power BI
2. **Analyze**: LLM analyzes the previous failed attempt(s)
3. **Learn**: LLM identifies the specific issue (wrong table, syntax error, etc.)
4. **Correct**: Generate a DIFFERENT query that addresses the error
5. **Retry**: Execute the corrected query
6. **Repeat**: Continue until success or max retries reached

### 3. Final Result
- **Success**: Return the working query and results
- **Failure**: Return all attempts and final error message

## Configuration

### Default Settings
```python
max_dax_retries: 5  # Maximum retry attempts (1-10)
```

### Custom Configuration
```json
{
  "user_question": "What are total sales by region?",
  "workspace_id": "...",
  "dataset_id": "...",
  "max_dax_retries": 3,  // Custom: only retry 3 times
  // ... other config
}
```

## Example Scenario

### Scenario: Wrong Table Name

**User Question**: "Show me sales by customer"

**Attempt 1** ❌
```dax
EVALUATE
SUMMARIZECOLUMNS(
    Customers[Name],
    "Sales", [Total Sales]
)
```
**Error**: `Table 'Customers' not found`

**Attempt 2** ✅ (Self-Corrected)
```dax
EVALUATE
SUMMARIZECOLUMNS(
    customer_md[CustomerName],
    "Sales", [Total Sales]
)
```
**Result**: Success! 250 rows returned

### Scenario: Syntax Error

**Attempt 1** ❌
```dax
EVALUATE
SUMMARIZECOLUMNS
    customer_md[Country]
    "Revenue", [Total Revenue]
```
**Error**: `Syntax error: Expected '(' after SUMMARIZECOLUMNS`

**Attempt 2** ✅ (Self-Corrected)
```dax
EVALUATE
SUMMARIZECOLUMNS(
    customer_md[Country],
    "Revenue", [Total Revenue]
)
```
**Result**: Success!

## What Gets Corrected

The self-correction mechanism handles:

### ✅ Table/Column Name Errors
- Wrong table name → Use correct table from available tables
- Wrong column name → Use correct column from table definition
- Case sensitivity issues → Match exact names from model

### ✅ Syntax Errors
- Missing parentheses
- Incorrect DAX function usage
- Invalid operators

### ✅ Relationship Errors
- Using non-existent relationships
- Incorrect join logic

### ✅ Type Mismatches
- Using text column where number expected
- Incorrect aggregation functions

### ✅ Measure Reference Errors
- Non-existent measure → Use available measures
- Incorrect measure syntax

## Logging

Each retry attempt is logged with full details:

```
[DAX Generation] LLM Prompt:
You are a DAX query expert. Generate a DAX query...

[DAX Generation] DAX generated (attempt 1): EVALUATE SUMMARIZECOLUMNS...

❌ DAX execution failed on attempt 1: Table 'Customers' not found

[DAX Self-Correction] Attempt 2 Prompt:
You are a DAX query expert. Your previous attempt(s) failed.

### Attempt 1
**DAX Query:**
EVALUATE SUMMARIZECOLUMNS(Customers[Name], "Sales", [Total Sales])
**Result:** ❌ FAILED
**Error:** Table 'Customers' not found

[DAX Self-Correction] LLM Response:
EVALUATE SUMMARIZECOLUMNS(customer_md[CustomerName], "Sales", [Total Sales])

✅ DAX execution successful on attempt 2: rows=250
```

## Output Format

### Markdown Output (with retries)
```markdown
# Power BI Analysis Results

**Question**: Show me sales by customer

## Generated DAX Query

**Attempts**: 2 (successful on attempt 2)

### Retry History
**Attempt 1**: ❌ Failed
  - Error: Table 'Customers' not found
**Attempt 2**: ✅ Success

```dax
EVALUATE
SUMMARIZECOLUMNS(
    customer_md[CustomerName],
    "Sales", [Total Sales]
)
```

## Execution Results

✅ **Success** - 250 rows returned
...
```

### JSON Output (with retries)
```json
{
  "user_question": "Show me sales by customer",
  "generated_dax": "EVALUATE\nSUMMARIZECOLUMNS(...)",
  "dax_attempts": [
    {
      "attempt": 1,
      "dax": "EVALUATE\nSUMMARIZECOLUMNS(Customers[Name]...)",
      "success": false,
      "error": "Table 'Customers' not found",
      "row_count": 0
    },
    {
      "attempt": 2,
      "dax": "EVALUATE\nSUMMARIZECOLUMNS(customer_md[CustomerName]...)",
      "success": true,
      "error": null,
      "row_count": 250
    }
  ],
  "dax_execution": {
    "success": true,
    "data": [...],
    "row_count": 250
  }
}
```

## Benefits

### 🎯 Higher Success Rate
- Automatically fixes common errors
- Reduces manual intervention needed
- Learns from immediate feedback

### 🚀 Faster Development
- No need to manually debug failed queries
- System self-corrects based on Power BI errors
- Reduces back-and-forth iterations

### 📊 Better User Experience
- Users get working results more often
- Clear visibility into retry process
- Transparent error handling

### 🔍 Improved Debugging
- Full history of attempts logged
- See exactly what was tried and why it failed
- Understand LLM reasoning process

## Edge Cases

### When Retries Won't Help
- **Authentication failures**: No amount of retry will fix auth issues
- **Permission errors**: User lacks access to dataset
- **Model corruption**: Power BI model has fundamental issues
- **Timeout errors**: Query is too complex/slow

### Maximum Retries Reached
If all 5 attempts fail:
- System returns the error from the last attempt
- All attempts are logged for debugging
- User receives clear error message
- Can manually review attempts and provide feedback

## Best Practices

1. **Set Appropriate Retry Limit**
   - Simple queries: `max_dax_retries: 2-3`
   - Complex queries: `max_dax_retries: 5`
   - Production systems: `max_dax_retries: 3` (balance between success and latency)

2. **Monitor Retry Patterns**
   - If queries consistently need 3+ retries → improve model context extraction
   - If same error repeats → LLM needs better guidance
   - If authentication errors → fix auth first

3. **Use Logging**
   - Always check `crew.log` to understand retry behavior
   - Look for patterns in failures
   - Identify measures/tables that cause issues

4. **Optimize for Speed**
   - Lower retry count for user-facing queries (faster response)
   - Higher retry count for background/batch processing (higher success)

## Performance Impact

- **First attempt success**: No impact (same as before)
- **Retry needed**: +2-5 seconds per retry (LLM call + DAX execution)
- **5 retries**: Max ~25-30 seconds additional latency

**Recommendation**: Keep `max_dax_retries: 3-5` for best balance between success rate and performance.

## Future Enhancements

Potential improvements:
1. **Pattern Learning**: Cache successful corrections for similar errors
2. **Temperature Adjustment**: Increase creativity on retries
3. **Multi-Model Fallback**: Try different LLM models on failure
4. **Partial Success**: Return best partial result if no full success
5. **User Feedback Loop**: Let users rate corrections to improve prompts

## Related Files

- **Implementation**: `src/backend/src/engines/crewai/tools/custom/powerbi_analysis_tool.py`
- **Logging**: `src/backend/logs/crew.log`
- **Documentation**: `src/docs/tool-comprehensive-analysis.md`
