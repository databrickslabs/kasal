# Knowledge Search Tool Documentation

## Overview

The knowledge search functionality in Kasal has been redesigned from a CrewAI knowledge source approach to a tool-based approach. This provides better control, easier debugging, and engine-agnostic implementation.

## Architecture

### Components

1. **DatabricksKnowledgeService** (`src/services/databricks_knowledge_service.py`)
   - Engine-agnostic service layer
   - Method: `search_knowledge()` - searches Databricks Vector Index
   - Handles authentication, filtering, and result formatting
   - Can be used by any AI engine or API endpoint

2. **DatabricksKnowledgeSearchTool** (`src/engines/crewai/tools/custom/databricks_knowledge_search_tool.py`)
   - Lightweight CrewAI tool wrapper
   - Inherits from `BaseTool`
   - Calls the service layer for actual search
   - Handles async/sync conversion for CrewAI

3. **Tool Factory** (`src/engines/crewai/tools/tool_factory.py`)
   - Registers the tool for agent use
   - Configures with group_id, execution_id, and user_token

## Usage

### For Agents

Agents can now search knowledge by using the tool directly in their tasks:

```python
# In agent configuration
{
    "name": "researcher",
    "role": "Research Specialist",
    "tools": ["DatabricksKnowledgeSearchTool"],
    "goal": "Find relevant information from uploaded documents"
}
```

### In Tasks

The agent can call the tool like this:

```
# Agent's internal usage
result = self.tools.DatabricksKnowledgeSearchTool.run(
    query="What is kasal?",
    limit=5,
    file_paths=["test.txt"]  # Optional filter
)
```

### Tool Input Schema

- **query** (required): The search query string
- **limit** (optional): Maximum number of results (default: 5)
- **file_paths** (optional): List of file paths to filter results

### Tool Output

The tool returns formatted search results:

```
Found 3 relevant results:

--- Result 1 (Score: 0.892) ---
Source: test.txt
Content: Kasal is an AI agent workflow orchestration platform...
---

--- Result 2 (Score: 0.834) ---
Source: test.txt
Content: The platform enables teams to build and deploy AI agents...
---
```

## Migration from Knowledge Sources

### Before (Knowledge Source Approach)

```python
# Agent configuration
{
    "name": "researcher",
    "knowledge_sources": [
        {
            "type": "databricks_volume",
            "source": "test.txt",
            "metadata": {...}
        }
    ]
}

# CrewAI would automatically search knowledge (unpredictably)
```

### After (Tool-Based Approach)

```python
# Agent configuration
{
    "name": "researcher",
    "tools": ["DatabricksKnowledgeSearchTool"]
}

# Agent explicitly searches when needed
# Full control over when and how search happens
```

## Benefits

1. **Explicit Control**: Agents decide when to search, not automatic
2. **Better Debugging**: Clear logs show when searches occur
3. **Engine Agnostic**: Service layer can be used by any engine
4. **Simpler Implementation**: No complex CrewAI knowledge source integration
5. **Testable**: Easy to test the tool independently

## Configuration

The tool is automatically configured with:
- **group_id**: From crew configuration for tenant isolation
- **execution_id**: From crew configuration for execution scoping
- **user_token**: For OBO authentication when available

## Error Handling

The tool handles errors gracefully:
- Returns "No relevant information found" when no results
- Logs detailed errors for debugging
- Times out after 30 seconds to prevent hanging

## Future Enhancements

1. Add caching for repeated queries
2. Support for more advanced filtering options
3. Integration with other vector databases
4. Support for hybrid search (vector + keyword)
5. Query rewriting for better search results

## Testing

To test the knowledge search tool:

1. Upload a document through the UI
2. Create an agent with the tool:
   ```json
   {
     "tools": ["DatabricksKnowledgeSearchTool"]
   }
   ```
3. Create a task that asks the agent to find information
4. Run the execution and verify results

## Troubleshooting

### No Results Found
- Check if documents are properly indexed
- Verify group_id and execution_id match
- Check Databricks Vector Index configuration

### Authentication Errors
- Verify Databricks tokens are configured
- Check user token for OBO authentication
- Verify Vector Index permissions

### Tool Not Available
- Ensure tool is imported in tool_factory.py
- Check for import errors in logs
- Verify tool registration in factory