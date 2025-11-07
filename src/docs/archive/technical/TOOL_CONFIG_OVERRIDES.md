# Tool Configuration Overrides Architecture

## Problem Statement
When multiple users run the same tool (like GenieTool), they need different configurations (e.g., different Genie space IDs). Storing this in the global tool configuration would cause conflicts between users.

## Solution: Tool Configuration Overrides

### Architecture Overview
We implement a hierarchical configuration system where tool configurations can be overridden at multiple levels:

1. **Base Tool Configuration** (stored in `tools` table)
   - Default settings for all tools
   - Shared across all users

2. **Agent-Level Overrides** (stored in `agents.tool_configs`)
   - Tool configurations specific to an agent
   - Format: `{"GenieTool": {"spaceId": "agent_specific_space"}}`

3. **Task-Level Overrides** (stored in `tasks.tool_configs`)
   - Tool configurations specific to a task
   - Format: `{"GenieTool": {"spaceId": "task_specific_space"}}`

### Configuration Priority (highest to lowest)
1. Task-level tool_configs
2. Agent-level tool_configs (if task is assigned to agent)
3. Base tool configuration from tools table
4. Tool defaults

### Database Changes

#### New Columns Added:
- `agents.tool_configs` - JSON column for agent-specific tool configurations
- `tasks.tool_configs` - JSON column for task-specific tool configurations

#### Migration:
```sql
ALTER TABLE agents ADD COLUMN tool_configs JSON DEFAULT '{}';
ALTER TABLE tasks ADD COLUMN tool_configs JSON DEFAULT '{}';
```

### Implementation Flow

#### 1. Frontend (UI)
When a user selects a Genie space for a task or agent:
```javascript
// Save with task
const task = {
  name: "My Task",
  tools: ["GenieTool"],
  tool_configs: {
    "GenieTool": {
      "spaceId": "selected_space_id"
    }
  }
};

// Save with agent
const agent = {
  name: "My Agent",
  tools: ["GenieTool"],
  tool_configs: {
    "GenieTool": {
      "spaceId": "selected_space_id"
    }
  }
};
```

#### 2. Backend (ToolFactory)
The ToolFactory merges configurations when creating tools:

```python
def create_tool(self, tool_identifier, agent_config=None, task_config=None):
    # Get base tool configuration
    tool_info = self.get_tool_info(tool_identifier)
    base_config = tool_info.config or {}
    
    # Merge configurations (task > agent > base)
    final_config = {
        **base_config,
        **(agent_config.get(tool_info.title, {}) if agent_config else {}),
        **(task_config.get(tool_info.title, {}) if task_config else {})
    }
    
    # Create tool with merged config
    return ToolClass(tool_config=final_config)
```

#### 3. Execution Flow
```
User selects space in UI
    ↓
Frontend saves tool_configs with task/agent
    ↓
Backend retrieves task/agent with tool_configs
    ↓
ToolFactory merges configs (task > agent > base)
    ↓
Tool created with user-specific configuration
    ↓
Tool runs with correct space ID for user
```

### Example Usage

#### Scenario 1: Different users, same task
- User A saves task with `tool_configs: {"GenieTool": {"spaceId": "space_a"}}`
- User B saves task with `tool_configs: {"GenieTool": {"spaceId": "space_b"}}`
- Each user's execution uses their specific space

#### Scenario 2: Same user, different contexts
- Marketing Agent: `tool_configs: {"GenieTool": {"spaceId": "marketing_space"}}`
- Sales Agent: `tool_configs: {"GenieTool": {"spaceId": "sales_space"}}`
- Each agent queries its relevant Genie space

### Benefits
1. **User Isolation**: Each user can have their own tool configurations
2. **Flexibility**: Different configurations per agent/task
3. **No Conflicts**: Users don't overwrite each other's settings
4. **Backward Compatible**: Existing tools without overrides continue to work
5. **Scalable**: Easy to add more tool-specific configurations

### Frontend Components

#### GenieSpaceSelector Component
- Displays available Genie spaces
- Saves selection to task/agent tool_configs
- Shows current selection when editing

#### Task/Agent Forms
- Include tool_configs field in save payload
- Display tool configuration UI when relevant tools selected
- Persist configurations across edits

### API Changes

#### Agent Schema
```python
class AgentBase(BaseModel):
    tools: List[str]
    tool_configs: Dict[str, Dict[str, Any]]  # New field
```

#### Task Schema
```python
class TaskBase(BaseModel):
    tools: List[str]
    tool_configs: Dict[str, Dict[str, Any]]  # New field
```

### Security Considerations
- Tool configurations are scoped to user's group_id
- Sensitive data in tool_configs should be encrypted
- Validation ensures only valid tool names in tool_configs

## Implementation Status
- [x] Database migration created
- [x] Model changes (Agent, Task)
- [x] Schema updates
- [ ] ToolFactory updates to use overrides
- [ ] Frontend components to manage tool_configs
- [ ] Testing and validation

## Next Steps
1. Update ToolFactory to read and merge tool_configs
2. Update agent/task helpers to pass tool_configs to ToolFactory
3. Update frontend to save tool_configs when Genie space selected
4. Add validation to ensure tool_configs match actual tools
5. Test with multiple users and configurations