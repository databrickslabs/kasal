# MCP (Model Context Protocol) Implementation Guide

## Overview

The Kasal platform provides comprehensive MCP server integration with flexible configuration options. This guide documents the complete MCP implementation architecture, configuration patterns, and usage guidelines.

## Architecture

### Three-Tier MCP Configuration System

1. **Global MCP Servers**: Automatically available to all agents and tasks as a baseline
2. **Agent-Level MCP Servers**: Specific to individual agents (additive to global)
3. **Task-Level MCP Servers**: Specific to individual tasks (additive to global and agent)

### Server Resolution Strategy

**Effective MCP Servers = Global ∪ Agent-specific ∪ Task-specific (deduplicated)**

- All three tiers are **combined** (union), not replaced
- Servers are deduplicated by name to avoid conflicts
- Global servers provide a consistent baseline
- Agent/Task servers add specialized capabilities

## Configuration Locations

### 1. Global MCP Configuration

**Location**: `Configuration.tsx` → MCP Server section

**Features**:
- **Server Management**: Add, edit, delete MCP servers
- **Global Enable Toggle**: NEW - Enable MCP servers across all agents/tasks
- **Individual Server Toggle**: Enable/disable specific servers
- **Server Types**: SSE, Streamable
- **Authentication**: API Key, Databricks OBO

**Global Enable Options**:
```typescript
interface MCPGlobalConfig {
  globalEnabled: boolean;           // Master switch for all MCP functionality
  globalServers: string[];         // NEW: Servers enabled globally for all agents/tasks
  individualEnabled: boolean;      // Allow agent/task-specific MCP selection
}
```

### 2. Agent-Level MCP Configuration

**Location**: `AgentForm.tsx` → Tool Configuration section

**UI Elements**:
- MCP Server Selector dropdown (multi-select)
- Visual chip display of selected servers
- Inherited global servers indicator

**Storage**: `agents.tool_configs.MCP_SERVERS = {servers: ['server1', 'server2']}`

### 3. Task-Level MCP Configuration

**Location**: `TaskForm.tsx` → Tool Configuration section

**UI Elements**:
- MCP Server Selector dropdown (multi-select)
- Visual chip display of selected servers
- Inherited global/agent servers indicator

**Storage**: `tasks.tool_configs.MCP_SERVERS = {servers: ['server1', 'server2']}`

## Data Flow Architecture

### Frontend Components

```
Configuration.tsx
├── Global MCP Settings
├── Global Server Enable Toggle (NEW)
└── Individual MCP Server Management

AgentForm.tsx
├── MCPServerSelector Component
├── Inherits Global Settings
└── Stores in agent.tool_configs

TaskForm.tsx
├── MCPServerSelector Component
├── Inherits Global + Agent Settings
└── Stores in task.tool_configs
```

### Backend Processing Flow (Centralized)

```
1. MCPIntegration Module (Central Controller)
   ├── resolve_effective_mcp_servers()
   │   ├── Fetches Global MCP Servers
   │   ├── Combines with Explicit Servers
   │   └── Deduplicates by Server Name
   ├── create_mcp_tools_for_agent()
   │   └── Creates tools with global + agent servers
   └── create_mcp_tools_for_task()
       └── Creates tools with global + task servers

2. CrewPreparation
   ├── collect_agent_mcp_requirements()
   │   └── Gathers MCP servers from assigned tasks
   └── _create_agents()
       └── Passes requirements to agent configuration

3. Agent/Task Helpers
   ├── Uses MCPIntegration.create_mcp_tools_for_agent()
   └── Uses MCPIntegration.create_mcp_tools_for_task()

4. MCP Service Layer
   ├── get_global_servers() - Fetches globally enabled servers
   ├── get_servers_by_names() - Fetches specific servers
   └── Handles authentication and server connections
```

## Implementation Details

### Database Schema

#### MCP Server Storage
```sql
-- Global MCP servers
CREATE TABLE mcp_servers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    server_type VARCHAR(50) NOT NULL,
    server_url TEXT,
    enabled BOOLEAN DEFAULT true,
    global_enabled BOOLEAN DEFAULT false,  -- NEW: Global enablement flag
    api_key TEXT ENCRYPTED,
    auth_type VARCHAR(50),
    -- ... other fields
);

-- Agent MCP configuration
agents.tool_configs = {
    "MCP_SERVERS": {
        "servers": ["news", "gmail"]
    }
}

-- Task MCP configuration  
tasks.tool_configs = {
    "MCP_SERVERS": {
        "servers": ["perplexity", "serper"]
    }
}
```

#### MCP Settings
```sql
CREATE TABLE mcp_settings (
    id INTEGER PRIMARY KEY,
    global_enabled BOOLEAN DEFAULT true,
    individual_enabled BOOLEAN DEFAULT true,  -- NEW: Allow individual selection
    -- ... other settings
);
```

### Frontend Components

#### MCPServerSelector Component
```typescript
interface MCPServerSelectorProps {
  selectedOptions: MCPServerConfig[] | MCPServerConfig | null;
  onChange: (selected: MCPServerConfig[] | MCPServerConfig | null) => void;
  multiple?: boolean;
  disabled?: boolean;
  showGlobalIndicator?: boolean;  // NEW: Show inherited global servers
}
```

#### Configuration MCP Section Enhancement
```typescript
interface MCPConfigurationState {
  servers: MCPServerConfig[];
  globalEnabled: boolean;
  globalServers: string[];        // NEW: Globally enabled servers
  individualEnabled: boolean;     // NEW: Allow individual selection
}
```

### Backend Services

#### MCP Service Methods
```python
class MCPService:
    async def get_global_servers(self) -> List[MCPServerResponse]:
        """Get servers marked as globally enabled"""
        
    async def get_enabled_servers(self) -> List[MCPServerResponse]:
        """Get all enabled servers (for individual selection)"""
        
    async def update_global_settings(self, global_servers: List[str]) -> bool:
        """Update which servers are globally enabled"""
```

#### MCPIntegration Module (Centralized)
```python
class MCPIntegration:
    @staticmethod
    async def resolve_effective_mcp_servers(
        explicit_servers: List[str],
        mcp_service,
        include_global: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Resolve effective MCP servers combining global and explicit selections.
        - Fetches global servers when include_global=True
        - Adds explicit servers (deduplicated)
        - Returns combined list of server configurations
        """
        
    @staticmethod
    async def create_mcp_tools_for_agent(
        agent_config: Dict[str, Any],
        agent_key: str,
        mcp_service
    ) -> List[Any]:
        """
        Create MCP tools for agent with global + explicit servers.
        """
        
    @staticmethod
    async def create_mcp_tools_for_task(
        task_config: Dict[str, Any],
        task_key: str,
        mcp_service
    ) -> List[Any]:
        """
        Create MCP tools for task with global + explicit servers.
        """
```

## Usage Patterns

### Pattern 1: Global Baseline + Additive Configuration
```
Global: ["news", "gmail"] 
Agent A: [] → Gets ["news", "gmail"] (global servers)
Agent B: ["perplexity"] → Gets ["news", "gmail", "perplexity"] (global + agent)
Task 1 (Agent B): [] → Gets ["news", "gmail", "perplexity"] (inherits from agent)
Task 2 (Agent B): ["serper"] → Gets ["news", "gmail", "perplexity", "serper"] (all combined)
```

### Pattern 2: No Global, Explicit Configuration Only
```
Global: [] (none enabled globally)
Agent A: ["news"] → Gets ["news"] only
Agent B: ["gmail", "perplexity"] → Gets ["gmail", "perplexity"] only
Task 1 (Agent A): [] → Gets ["news"] (from agent)
Task 2 (Agent A): ["serper"] → Gets ["news", "serper"] (agent + task)
```

### Pattern 3: Mixed Global and Specific
```
Global: ["news"] (baseline for all)
Agent Research: ["perplexity", "serper"] → Gets ["news", "perplexity", "serper"]
Agent Email: ["gmail"] → Gets ["news", "gmail"]
Task Analysis (Research): ["arxiv"] → Gets ["news", "perplexity", "serper", "arxiv"]
```

## Migration Path

### Phase 1: Database Schema ✅ COMPLETED
```sql
ALTER TABLE mcp_servers ADD COLUMN global_enabled BOOLEAN DEFAULT false;
ALTER TABLE mcp_settings ADD COLUMN individual_enabled BOOLEAN DEFAULT true;
```

### Phase 2: Frontend Components ✅ COMPLETED
- MCPServerSelector with multi-select capability
- Configuration.tsx with global server management
- Agent/TaskForm with MCP server selection UI

### Phase 3: Backend Implementation ✅ COMPLETED
- Centralized MCPIntegration module
- Automatic global + explicit server merging
- Deduplication and tool creation pipeline

### Phase 4: Testing & Validation ✅ COMPLETED
- End-to-end MCP server flow working
- Global servers automatically included
- Tools properly created and assigned

## Configuration Examples

### Example 1: News + Email for All Agents
```javascript
// Configuration.tsx
{
  globalEnabled: true,
  globalServers: ["news", "gmail"],
  individualEnabled: true
}

// Agent Form - no additional selection needed
// Result: All agents get news + gmail tools automatically
```

### Example 2: Task-Specific Research Tools
```javascript
// Global: ["news"] 
// Agent: [] 
// Research Task: ["perplexity", "serper"]
// Result: Research task gets news + perplexity + serper
```

### Example 3: Agent Specialization
```javascript
// Global: []
// Email Agent: ["gmail", "calendar"] 
// Research Agent: ["news", "perplexity", "serper"]
// Social Agent: ["twitter", "linkedin"]
```

## Error Handling

### Common Issues & Solutions

1. **MCP Server Connection Failures**
   - Fallback to cached tool definitions
   - User notification of unavailable tools
   - Graceful degradation without MCP tools

2. **Authentication Failures**
   - Clear error messages for API key issues
   - Databricks OBO token refresh handling
   - Fallback authentication methods

3. **Tool Conflicts**
   - Prefix tools with server name (`news_search_news`)
   - Avoid duplicate tool registration
   - Priority-based tool selection

## Performance Considerations

### Connection Pooling
- Reuse MCP connections across agents
- Connection timeout and retry logic
- Rate limiting per MCP server

### Tool Caching
- Cache MCP tool definitions
- Refresh on server configuration changes
- Background tool availability checks

## Security Guidelines

### API Key Management
- Encrypt MCP server API keys in database
- Use environment variables for fallback authentication
- Databricks OBO for user-context authentication

### Access Control
- Group-level MCP server access (future)
- Audit logging for MCP tool usage
- Rate limiting and abuse prevention

## Testing Strategy

### Unit Tests
- MCPService methods
- Agent/Task helper functions
- Configuration validation

### Integration Tests
- End-to-end MCP server integration
- Tool creation and execution
- Authentication flows

### UI Tests
- MCPServerSelector component
- Configuration form validation
- Visual feedback and error states

## Monitoring & Observability

### Logging
```python
logger.info(f"Agent {agent_key} effective MCP servers: {effective_servers}")
logger.info(f"Created {len(mcp_tools)} MCP tools for agent {agent_key}")
logger.error(f"MCP server {server_name} connection failed: {error}")
```

### Metrics
- MCP server availability
- Tool usage statistics
- Authentication success rates
- Performance metrics (connection time, tool execution time)

## Future Enhancements

### Planned Features
1. **Conditional MCP Enablement**: Enable servers based on task type/context
2. **MCP Server Health Monitoring**: Real-time status dashboard
3. **Tool Usage Analytics**: Track which MCP tools are most used
4. **Dynamic Server Discovery**: Auto-detect available MCP servers
5. **Group-Level MCP Policies**: Organization-wide MCP governance

### API Extensions
- REST API for MCP server management
- Webhook notifications for server status changes
- GraphQL integration for complex MCP queries

---

## Quick Reference

### Key Files
- **Frontend**: `MCPServerSelector.tsx`, `Configuration.tsx`, `AgentForm.tsx`, `TaskForm.tsx`
- **Backend Integration**: `mcp_integration.py` (centralized MCP logic)
- **Backend Services**: `mcp_service.py`, `agent_helpers.py`, `task_helpers.py`, `crew_preparation.py`
- **Schemas**: `mcp.py`, `agent.py`, `task.py`

### Database Tables
- `mcp_servers`: Server definitions and configuration
- `mcp_settings`: Global MCP settings
- `agents.tool_configs`: Agent-level MCP configuration
- `tasks.tool_configs`: Task-level MCP configuration

### Environment Variables
- `DATABRICKS_CLIENT_ID`: OAuth client ID
- `DATABRICKS_CLIENT_SECRET`: OAuth client secret  
- `DATABRICKS_TOKEN`: Fallback API token
- `MCP_DEFAULT_TIMEOUT`: Default connection timeout
- `MCP_MAX_RETRIES`: Maximum retry attempts

This implementation provides a robust, flexible, and scalable MCP integration that supports both global policies and granular control at the agent and task level.