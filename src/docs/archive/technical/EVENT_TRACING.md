# Event System & Tracing Architecture

## Overview

Kasal implements a sophisticated event capture and tracing system that provides complete visibility into AI agent executions while maintaining strict isolation between concurrent executions. This document explains the architecture, implementation details, and best practices for working with the event system.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                   CrewAI Execution                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │    Agents    │  │    Tasks     │  │     LLMs     │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
└─────────┼──────────────────┼──────────────────┼─────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────┐
│                    Callback System                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │Step Callback │  │Task Callback │  │ LLM Router   │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
└─────────┼──────────────────┼──────────────────┼─────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────┐
│                     Trace Queue                         │
│                  (Async Processing)                     │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│                  Database Storage                       │
│                 (ExecutionTrace)                        │
└─────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Callback System

The callback system is the primary mechanism for capturing events during CrewAI execution.

#### Step Callback
**Location**: `src/backend/src/engines/crewai/callbacks/execution_callback.py`

Captures every agent step during execution:

```python
def step_callback(step_output):
    """
    Called for EVERY agent step.
    Handles multiple output types based on agent behavior.
    """
    
    # Type 1: AgentAction (agent uses tools)
    if isinstance(step_output, AgentAction):
        agent = step_output.agent  # Has agent reference!
        tool = step_output.tool
        tool_input = step_output.tool_input
    
    # Type 2: Plain string (agent without tools)
    elif isinstance(step_output, str):
        # Challenge: No agent reference available
        agent = extract_agent_from_context()
    
    # Type 3: ToolResult, AgentFinish, etc.
    # ... handle other types
```

#### Task Callback
Captures task completion events:

```python
def task_callback(task_output):
    """
    Called when a task completes.
    ALWAYS has agent reference through task.agent
    """
    agent = task_output.task.agent
    agent_name = agent.role  # Reliable agent identification
```

### 2. LLM Event Router

**Location**: `src/backend/src/engines/crewai/callbacks/llm_event_router.py`

Captures LLM events from the global CrewAI event bus while maintaining execution isolation:

```python
class LLMEventRouter:
    """Routes LLM events to appropriate executions based on agent context."""
    
    @classmethod
    def register_execution(cls, execution_id, crew):
        """Register an execution with its agents."""
        agent_roles = {agent.role for agent in crew.agents}
        cls._active_executions[execution_id] = {
            'agents': agent_roles,
            'trace_queue': get_trace_queue()
        }
    
    @classmethod
    def handle_llm_event(cls, source, event):
        """Route LLM event to correct execution."""
        for exec_id, exec_data in cls._active_executions.items():
            if event.agent_role in exec_data['agents']:
                # Create trace and enqueue to correct execution
                trace_data = create_trace(event, exec_id)
                exec_data['trace_queue'].put_nowait(trace_data)
                break
```

### 3. Trace Queue System

**Location**: `src/backend/src/services/trace_queue.py`

Asynchronous queue for processing traces without blocking execution:

```python
# Global trace queue
trace_queue = asyncio.Queue()

async def process_trace_queue():
    """Background worker that processes traces"""
    while True:
        trace_data = await trace_queue.get()
        
        # Store in database
        await store_trace(trace_data)
        
        trace_queue.task_done()
```

### 4. Agent Identification System

**Location**: `src/backend/src/utils/agent_utils.py`

Multi-strategy approach for identifying agents:

```python
def extract_agent_name(output, context):
    """
    Priority chain for agent identification:
    1. Direct agent attribute (if available)
    2. Task-to-agent mapping
    3. Cached context from previous events
    4. Single-agent crew detection
    5. Fallback to "Unknown Agent"
    """
```

## Event Types

### Captured Events

| Event Type | Source | Agent Info | Description |
|------------|--------|------------|-------------|
| `agent_step` | Step Callback | Sometimes | Agent execution steps |
| `task_completed` | Task Callback | Always | Task completion |
| `llm_call` | LLM Router | Always | LLM interactions |
| `tool_execution` | Step Callback | Yes | Tool usage |
| `crew_started` | Execution | N/A | Crew execution start |
| `crew_completed` | Execution | N/A | Crew execution end |

### Trace Data Structure

```python
trace_data = {
    "job_id": str,              # Execution ID
    "event_source": str,        # Agent name or system component
    "event_context": str,       # Task description or context
    "event_type": str,          # Event type (see table above)
    "timestamp": str,           # ISO format timestamp
    "output_content": str,      # Event output/result
    "extra_data": {
        "agent_role": str,      # Backup agent identification
        "task_description": str,
        "model": str,           # For LLM events
        "tool_name": str,       # For tool events
        "type": str             # Callback type
    }
}
```

## Execution Isolation

### Challenge
Multiple users running crews simultaneously must not see each other's events.

### Solution
Execution-scoped callbacks and agent-based filtering:

1. **Execution-Scoped Callbacks**: Each execution gets its own callback instances
2. **Agent-Based Filtering**: LLM events filtered by agent ownership
3. **Separate Queues**: Each execution can have its own queue if needed
4. **Group Context**: Traces include group_id for tenant isolation

```python
# Each execution has isolated callbacks
execution_1_callbacks = create_execution_callbacks(job_id="exec1")
execution_2_callbacks = create_execution_callbacks(job_id="exec2")

# Callbacks only capture their execution's events
crew1.step_callback = execution_1_callbacks.step_callback
crew2.step_callback = execution_2_callbacks.step_callback
```

## Agent Identification Strategies

### The Challenge
Agents without tools return plain strings in step callbacks, losing agent context.

### Solution: Multi-Strategy Approach

1. **Direct Agent Reference**: Check if output has agent attribute
2. **Object Type Detection**: Handle AgentAction, AgentFinish, ToolResult
3. **Task-to-Agent Mapping**: Use task callbacks to maintain context
4. **Execution Context**: Cache current/last known agent
5. **Single-Agent Detection**: If crew has one agent, use it
6. **Fallback**: "Unknown Agent" as last resort

### Implementation

```python
def create_execution_callbacks(job_id, crew):
    # Build comprehensive lookup structures
    agent_lookup = {}
    for agent in crew.agents:
        agent_lookup[id(agent)] = agent.role
        agent_lookup[agent.role] = agent
    
    # Build task-to-agent mapping
    task_to_agent = {}
    for i, task in enumerate(crew.tasks):
        if hasattr(task, 'agent'):
            task_to_agent[i] = task.agent.role
    
    # Use in callbacks for context
    context = {
        'agent_lookup': agent_lookup,
        'task_to_agent': task_to_agent,
        'current_agent': None,
        'last_known_agent': None
    }
```

## Performance Considerations

### Queue Management

```python
# Monitor queue sizes
if trace_queue.qsize() > 1000:
    logger.warning("Trace queue backlog detected")

# Batch processing for efficiency
async def process_traces_batch():
    batch = []
    while len(batch) < 100:
        try:
            trace = await asyncio.wait_for(
                trace_queue.get(), 
                timeout=1.0
            )
            batch.append(trace)
        except asyncio.TimeoutError:
            break
    
    if batch:
        await store_traces_batch(batch)
```

### Memory Management

- Active executions tracked in memory
- Automatic cleanup on execution completion
- Agent name collision handling for concurrent executions

## Best Practices

### 1. Always Register/Unregister Executions

```python
try:
    # Register execution
    LLMEventRouter.register_execution(execution_id, crew)
    
    # Run crew
    result = await crew.kickoff()
    
finally:
    # Always unregister
    LLMEventRouter.unregister_execution(execution_id)
```

### 2. Handle Agent Name Collisions

Use unique agent roles across different crew configurations to avoid LLM event misrouting.

### 3. Monitor Queue Health

Implement monitoring for queue sizes and processing delays:

```python
# Add metrics
queue_size_metric.set(trace_queue.qsize())
processing_delay_metric.observe(delay)
```

### 4. Graceful Degradation

Handle failures without breaking execution:

```python
try:
    enqueue_trace(trace_data)
except Exception as e:
    logger.error(f"Failed to enqueue trace: {e}")
    # Continue execution without tracing
```

## Troubleshooting

### Common Issues

1. **"Unknown Agent" in traces**
   - Verify agent has unique role
   - Check if agent has tools configured
   - Ensure task callbacks are working

2. **Missing LLM events**
   - Verify LLMEventRouter is initialized
   - Check if execution is registered
   - Ensure CrewAI event bus is accessible

3. **Mixed events between executions**
   - Check for agent name collisions
   - Verify execution isolation
   - Review filtering logic

### Debug Logging

Enable detailed logging for troubleshooting:

```python
# In execution_callback.py
logger.debug(f"Step callback received: {type(step_output)}")
logger.debug(f"Agent identified: {agent_name}")
logger.debug(f"Context state: {context}")
```

## Future Enhancements

1. **Retroactive Agent Updates**: Update "Unknown Agent" traces when agent is identified later
2. **Event Deduplication**: Remove duplicate events from multiple sources
3. **Custom Event Types**: Support for application-specific events
4. **Event Streaming**: Real-time event streaming to frontend via WebSockets
5. **Event Replay**: Ability to replay execution from traces

## Related Documentation

- [AGENT_TASK_LIFECYCLE.md](AGENT_TASK_LIFECYCLE.md) - Agent and task lifecycle details
- [EXECUTION_FLOW.md](EXECUTION_FLOW.md) - Complete execution flow
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and solutions