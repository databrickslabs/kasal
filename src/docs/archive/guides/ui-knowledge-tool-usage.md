# UI Knowledge Search Tool Usage Guide

## Overview

The knowledge search functionality has been updated to use a **tool-based approach** instead of the previous knowledge_sources system. This provides better control and visibility into when agents search for information.

## How It Works

### 1. Upload Knowledge Files
- Click the **Upload Knowledge** button in the chat interface
- Select files to upload or browse Databricks Volume
- Files are automatically indexed for searching

### 2. Select Agents
When you upload files, you'll see a list of available agents:

- **Click on an agent** to grant them knowledge search capability
- Selected agents will have the `DatabricksKnowledgeSearchTool` added to their tools
- Agents with the tool will show a 📚 icon next to their name
- Hover over agents to see their knowledge search status

### 3. Visual Indicators

| Indicator | Meaning |
|-----------|---------|
| 📚 icon | Agent has knowledge search capability |
| Blue chip (filled) | Agent selected for current upload |
| Gray chip (outlined) | Agent not selected |
| Tooltip on hover | Shows if agent has tool or needs it |

### 4. How Agents Use the Tool

Instead of automatic knowledge retrieval, agents now **explicitly** use the tool:

```
Agent thinking: "I need information about X"
Agent action: Use DatabricksKnowledgeSearchTool
Query: "What is X?"
Result: [Knowledge from uploaded files]
```

## Key Differences from Old System

### Before (Knowledge Sources)
- Files were attached as `knowledge_sources` to agents
- CrewAI would unpredictably search knowledge
- No control over when searches happened
- Hard to debug why knowledge wasn't being used

### Now (Tool-Based)
- `DatabricksKnowledgeSearchTool` is added to agent's tools list
- Agent explicitly decides when to search
- Full visibility into search queries and results
- Clear in logs when knowledge is accessed

## Benefits for Users

1. **Transparency**: You can see in the execution logs when agents search for knowledge
2. **Control**: Agents only search when it makes sense for the task
3. **Debugging**: Clear indication of which agents have access to knowledge
4. **Flexibility**: Agents can be given or removed knowledge access at any time

## Configuration in UI

### Agent Configuration
When configuring agents, you NO LONGER need to:
- Add knowledge_sources to agents
- Configure knowledge paths

Instead, simply ensure:
- The agent has `DatabricksKnowledgeSearchTool` in their tools list
- This happens automatically when you select the agent during file upload

### Task Configuration
Tasks don't need any special configuration. If an agent has the tool, they can use it when appropriate.

## Example Workflow

1. **Upload a document** about your company
2. **Select the "Researcher" agent** to give them access
3. The agent now has `DatabricksKnowledgeSearchTool` in their tools
4. **Create a task**: "Write a summary about our company"
5. The agent will:
   - Recognize they need information
   - Use the tool to search: "company information"
   - Get results from your uploaded document
   - Generate the summary

## Troubleshooting

### Agent not finding information?
- Check if the agent has the 📚 icon (has the tool)
- Verify files were uploaded successfully
- Check execution logs for tool usage

### Want to remove knowledge access?
- Click on a selected agent to deselect them
- The tool will be removed from their tools list
- They will no longer search knowledge

### Tool not showing up?
- Ensure backend has been updated with the new tool
- Check that `DatabricksKnowledgeSearchTool` is registered in tool factory
- Verify the tool is available in the tools list

## Technical Details

- Tool name: `DatabricksKnowledgeSearchTool`
- Automatically configured with:
  - `group_id`: For tenant isolation
  - `execution_id`: For execution scoping
  - `user_token`: For authentication
- Searches are filtered by these parameters automatically