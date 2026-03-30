# MCP Integration for CrewAI

This module provides integration between Model Context Protocol (MCP) servers and CrewAI tasks and agents. It allows CrewAI agents and tasks to use MCP servers as tools without modifying the existing MCP or CrewAI infrastructure.

## Overview

The MCP integration consists of the following components:

1. **MCPToolAdapter**: Retrieves MCP server configurations from the database.
2. **MCPTool**: A LangChain-compatible tool that wraps an MCP server.
3. **MCPToolFactory**: Creates MCP tools from MCP server configurations.
4. **MCPCrewAIIntegration**: Provides integration between MCP servers and CrewAI.

The integration automatically patches the CrewAI task and agent helpers to handle MCP tools, making them available to CrewAI tasks and agents without requiring any changes to the existing code.

## Usage

### Automatic Integration

The MCP integration is automatically initialized when the module is imported. This patches the CrewAI task and agent helpers to handle MCP tools.

```python
# Import the MCP integration
from src.engines.crewai.mcp import MCPCrewAIIntegration

# The integration is automatically initialized
```

### Manual Integration

You can also manually initialize the MCP integration:

```python
from src.engines.crewai.mcp import initialize_mcp_integration

# Initialize the MCP integration
initialize_mcp_integration()
```

### Using MCP Tools in CrewAI

To use MCP tools in CrewAI, you need to:

1. Configure MCP servers in the database (using the MCP UI)
2. Enable the servers you want to use
3. Include "mcp" in the list of tool IDs for your agents or tasks

Example agent configuration:

```json
{
    "role": "Assistant",
    "goal": "Help the user with their request",
    "backstory": "You are Kasal, an intelligent assistant.",
    "tools": ["mcp"]  // This will include all enabled MCP servers
}
```

The `mcp` tool ID is a special identifier that will be replaced with all enabled MCP servers at runtime.

### Manual Tool Registration

You can also manually register MCP tools with a tool registry:

```python
from src.engines.crewai.mcp import register_mcp_tools_with_tool_registry
from your_tool_registry import tool_registry

# Register MCP tools with the tool registry
register_mcp_tools_with_tool_registry(tool_registry)
```

### Creating MCP Tools Directly

You can also create MCP tools directly using the `MCPToolFactory`:

```python
from src.engines.crewai.mcp import MCPToolFactory

# Create MCP tools
factory = MCPToolFactory()
mcp_tools = factory.create_mcp_tools()

# Use the tools
for tool in mcp_tools:
    print(f"Tool: {tool.name} - {tool.description}")
```

## Architecture

The MCP integration uses a non-intrusive approach to add MCP support to CrewAI:

1. It patches the `create_task` and `create_agent` functions in the task and agent helpers.
2. When a task or agent is created with the "mcp" tool, the patched functions create MCP tools from enabled MCP servers.
3. These MCP tools are then passed to the original functions, which use them as if they were regular tools.

This approach allows for adding MCP support without modifying the existing code, making it easy to remove if needed.

## Troubleshooting

If you encounter issues with the MCP integration, check the logs for error messages. The MCP integration logs to the `crewai.mcp` logger, which you can configure in your logging setup.

Common issues include:

- No MCP servers enabled in the database
- MCP server URLs unreachable
- Invalid API keys
- Issues with the MCPTool implementation

## Limitations

The current implementation has some limitations:

- It only supports synchronous tool execution
- It doesn't support streaming responses from MCP servers
- It doesn't support model mapping
- It assumes a specific payload format for MCP server requests
- It doesn't handle MCP server configuration changes at runtime (requires restart) 