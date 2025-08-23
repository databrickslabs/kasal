# Agent & Task Lifecycle

## Overview

This document explains the lifecycle of agents and tasks in Kasal, from configuration to execution. A critical distinction exists between Kasal entities (persistent database records) and CrewAI runtime objects (temporary execution instances). Understanding this distinction is essential for working with the system.

## Conceptual Model

```
┌──────────────────────────────────────────────────────────────┐
│                     Configuration Phase                      │
│  ┌──────────────┐                      ┌──────────────────┐  │
│  │   Frontend   │ ──── JSON Config ──→ │    Backend API   │  │
│  │   (React)    │                      │    (FastAPI)     │  │
│  └──────────────┘                      └──────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                      Storage Phase                           │
│  ┌──────────────┐                      ┌──────────────────┐  │
│  │ Kasal Agent  │ ──── Database ────→  │  Kasal Task     │  │
│  │   (Model)    │      Records         │    (Model)      │  │
│  └──────────────┘                      └──────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                     Runtime Phase                            │
│  ┌──────────────┐                      ┌──────────────────┐  │
│  │ CrewAI Agent │ ──── Execution ────→ │  CrewAI Task    │  │
│  │   (Object)   │                      │    (Object)     │  │
│  └──────────────┘                      └──────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## Kasal Entities vs CrewAI Objects

### Kasal Agents (Database Entities)

**Location**: `src/backend/src/models/agent.py`

```python
class Agent(Base):
    __tablename__ = "agents"
    
    id = Column(UUID(as_uuid=True), primary_key=True)
    name = Column(String, nullable=False)
    role = Column(String, nullable=False)  # Maps to CrewAI agent.role
    goal = Column(String)
    backstory = Column(Text)
    tools = Column(JSON)  # List of tool IDs
    llm = Column(String)  # Model name
    max_iter = Column(Integer, default=25)
    memory = Column(Boolean, default=False)
    # ... other persistent fields
```

**Purpose**:
- Persistent storage of agent configurations
- Template for creating runtime agents
- Maintains relationships and history
- Enables configuration reuse

### CrewAI Agents (Runtime Objects)

**Location**: `src/backend/src/engines/crewai/helpers/agent_helpers.py`

```python
from crewai import Agent

async def create_agent(agent_key, agent_config, resolved_tools, llm):
    """Create a CrewAI Agent instance for execution."""
    agent = Agent(
        role=agent_config["role"],      # This becomes the identifier
        goal=agent_config["goal"],
        backstory=agent_config["backstory"],
        tools=resolved_tools,            # Actual tool instances
        llm=llm,                        # Configured LLM object
        verbose=True,
        allow_delegation=agent_config.get("allow_delegation", False),
        max_iter=agent_config.get("max_iter", 25),
        memory=agent_config.get("memory", False)
    )
    return agent
```

**Purpose**:
- Temporary execution instances
- Contains actual tool and LLM objects
- Executes tasks during crew run
- Exists only during execution

### Key Differences

| Aspect | Kasal Entity | CrewAI Object |
|--------|--------------|---------------|
| **Persistence** | Stored in database | Memory only |
| **Lifetime** | Permanent | Execution duration |
| **References** | UUIDs and foreign keys | Object references |
| **Tools** | Tool IDs/names | Tool instances |
| **LLM** | Model name string | LLM object |
| **Purpose** | Configuration storage | Task execution |

## Agent Lifecycle

### 1. Configuration Phase

Frontend sends agent configuration:

```javascript
// Frontend configuration
{
  "agents_yaml": {
    "researcher": {
      "role": "News Gatherer",  // Primary identifier
      "goal": "Find latest news",
      "backstory": "Expert researcher",
      "tools": [],  // No tools
      "llm": "gpt-4"
    },
    "analyst": {
      "role": "Data Analyst",
      "goal": "Analyze data",
      "backstory": "Expert analyst",
      "tools": ["SerperDevTool"],  // Has tools
      "llm": null  // Uses global model
    }
  }
}
```

### 2. Preparation Phase

**Location**: `src/backend/src/engines/crewai/crew_preparation.py`

```python
async def _prepare_agents(self):
    agents_config = self.config.get("agents_yaml", {})
    
    for agent_key, agent_config in agents_config.items():
        # Resolve tools from names to instances
        tools = await self._resolve_tools(agent_config.get("tools", []))
        
        # Configure LLM (agent-specific > global > default)
        llm = await self._configure_agent_llm(agent_config)
        
        # Create CrewAI agent
        agent = await create_agent(
            agent_key=agent_key,
            agent_config=agent_config,
            resolved_tools=tools,
            llm=llm
        )
        
        # Store for task reference
        self.agents[agent_key] = agent
```

### 3. Execution Phase

During crew execution, agents perform their tasks:

```python
# Agent WITH tools
agent.execute() → 
    agent.use_tool() → 
        AgentAction(agent=self, tool=...) → 
            step_callback(AgentAction)  # Has agent reference

# Agent WITHOUT tools
agent.execute() → 
    llm.complete(prompt) → 
        "Response string" → 
            step_callback("Response string")  # No agent reference!
```

### 4. Identification Challenge

The core challenge: Agents without tools lose their identity in callbacks.

**Solution**: Multi-strategy identification (see [EVENT_TRACING.md](EVENT_TRACING.md))

## Task Lifecycle

### 1. Configuration Phase

Frontend sends task configuration:

```javascript
{
  "tasks_yaml": {
    "research_task": {
      "description": "Research the topic",
      "agent": "researcher",  // References agent by key
      "expected_output": "Research report",
      "tools": []  // Task-specific tools (optional)
    },
    "analysis_task": {
      "description": "Analyze the research",
      "agent": "analyst",
      "expected_output": "Analysis report"
    }
  }
}
```

### 2. Task Creation

**Location**: `src/backend/src/engines/crewai/helpers/task_helpers.py`

```python
async def create_task(task_key, task_config, agent, tools):
    """Create a CrewAI Task instance."""
    task = Task(
        description=task_config["description"],
        agent=agent,  # CrewAI Agent object, not just name!
        expected_output=task_config["expected_output"],
        tools=tools if tools else None,
        async_execution=task_config.get("async_execution", False)
    )
    return task
```

### 3. Task-Agent Relationship

**Critical**: Every task ALWAYS has an agent reference.

```python
async def _prepare_tasks(self):
    tasks_config = self.config.get("tasks_yaml", {})
    
    for task_key, task_config in tasks_config.items():
        # Find the agent for this task
        agent_key = task_config["agent"]
        agent = self.agents[agent_key]  # Already created agent
        
        # Create task with agent reference
        task = await create_task(
            task_key=task_key,
            task_config=task_config,
            agent=agent,  # Full agent object
            tools=tools
        )
        
        self.tasks.append(task)
```

### 4. Task Execution

Tasks are executed by their assigned agents:

```python
# In task callback
def task_callback(task_output):
    # Task ALWAYS has agent reference
    agent = task_output.task.agent
    agent_name = agent.role  # Reliable identification
    
    trace_data = {
        "event_type": "task_completed",
        "extra_data": {
            "agent_role": agent_name,  # Always available
            "task_description": task_output.task.description
        }
    }
```

## Tool Resolution

### Tool Configuration Flow

```
Tool Names (Config) → Tool Resolution → Tool Instances (Runtime)
```

### Resolution Process

```python
async def _resolve_tools(self, tool_names):
    """Resolve tool names to actual tool instances."""
    resolved_tools = []
    
    for tool_name in tool_names:
        if tool_name == "SerperDevTool":
            from crewai_tools import SerperDevTool
            tool = SerperDevTool()
        elif tool_name == "FileReadTool":
            from crewai_tools import FileReadTool
            tool = FileReadTool()
        else:
            # Custom tool handling
            tool = await self._create_custom_tool(tool_name)
        
        resolved_tools.append(tool)
    
    return resolved_tools
```

## LLM Configuration

### Configuration Priority

```
Agent-specific LLM > Global Model > Default Model
```

### Implementation

```python
async def _configure_agent_llm(self, agent_config):
    """Configure LLM for an agent."""
    
    # 1. Check agent-specific LLM
    if agent_config.get("llm"):
        model_name = agent_config["llm"]
        return await LLMManager.configure_crewai_llm(model_name)
    
    # 2. Check global model
    elif self.config.get("model"):
        model_name = self.config["model"]
        return await LLMManager.configure_crewai_llm(model_name)
    
    # 3. Use default
    else:
        return await LLMManager.configure_crewai_llm("gpt-4")
```

## Memory Configuration

### Memory Systems

1. **Short-term Memory**: Per-execution, stored in ChromaDB
2. **Long-term Memory**: Across executions, stored in database or vector store
3. **Entity Memory**: Extracts and stores entities mentioned

### Crew ID Generation

Deterministic ID based on crew configuration:

```python
def _generate_crew_id(self):
    """Generate deterministic crew ID for memory isolation."""
    components = [
        # Agent roles
        *[agent.role for agent in self.agents.values()],
        # Task descriptions
        *[task.description for task in self.tasks],
        # Model
        self.config.get("model", "default"),
        # Group for isolation
        self.group_context.primary_group_id
    ]
    
    # Hash for consistent ID
    crew_string = "|".join(components)
    return hashlib.sha256(crew_string.encode()).hexdigest()[:16]
```

## Agent Identification Strategies

### Why It's Challenging

- CrewAI doesn't consistently pass agent context
- Agents without tools return plain strings
- Multiple agents can be active simultaneously

### Solution Strategies

1. **Direct from Object**: Check if output has agent attribute
2. **Task Context**: Use task.agent (always available)
3. **Cached Context**: Maintain current/last known agent
4. **Crew State**: Check crew's internal state
5. **Single Agent**: If only one agent, use it

### Implementation Example

```python
def extract_agent_name(output, context):
    # Strategy 1: Direct attribute
    if hasattr(output, 'agent'):
        return output.agent.role
    
    # Strategy 2: AgentAction object
    if isinstance(output, AgentAction):
        return output.agent.role
    
    # Strategy 3: Task context
    if context.get('current_task_agent'):
        return context['current_task_agent']
    
    # Strategy 4: Cached context
    if context.get('last_known_agent'):
        return context['last_known_agent']
    
    # Strategy 5: Single agent crew
    if len(context['agents']) == 1:
        return list(context['agents'].values())[0].role
    
    # Fallback
    return "Unknown Agent"
```

## Best Practices

### 1. Unique Agent Roles

Always use unique role names to avoid identification issues:

```python
# Good
"Swiss Market Researcher"
"US Market Analyst"

# Bad (ambiguous)
"Researcher"
"Analyst"
```

### 2. Consistent Naming

Use consistent naming between configuration and runtime:

```python
# Configuration
agent_key = "researcher"
agent_config = {"role": "News Gatherer", ...}

# Runtime reference
self.agents[agent_key] = agent  # Use same key
```

### 3. Tool Assignment

Assign tools at the appropriate level:

```python
# Agent-level tools (used for all agent's tasks)
agent_config["tools"] = ["SerperDevTool"]

# Task-level tools (specific to one task)
task_config["tools"] = ["FileReadTool"]
```

### 4. Memory Management

Configure memory appropriately for your use case:

```python
# Enable for agents that need context
agent_config["memory"] = True

# Configure crew-level memory backend
config["memory_backend"] = {
    "provider": "databricks",
    "config": {...}
}
```

## Troubleshooting

### Common Issues

1. **Agent Not Found**
   - Verify agent key in task configuration
   - Check agent was created before task
   - Ensure configuration is valid

2. **Tool Resolution Fails**
   - Verify tool name is correct
   - Check tool is available in environment
   - Ensure API keys are configured

3. **LLM Configuration Error**
   - Verify model name is valid
   - Check API keys are set
   - Ensure model is available

### Debug Helpers

```python
# Log agent creation
logger.debug(f"Created agent: {agent.role} with tools: {[t.__class__.__name__ for t in tools]}")

# Log task assignment
logger.debug(f"Task '{task.description}' assigned to agent '{agent.role}'")

# Log execution flow
logger.debug(f"Agent '{agent.role}' executing with {'tools' if agent.tools else 'no tools'}")
```

## Related Documentation

- [EVENT_TRACING.md](EVENT_TRACING.md) - Event capture and tracing
- [EXECUTION_FLOW.md](EXECUTION_FLOW.md) - Complete execution flow
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and solutions