# Execution Callback & Trace System Documentation

## Overview

The execution callback and trace system provides real-time visibility into AI agent task execution, tracking which agent performs which task and capturing all interactions for display in the execution trace timeline.

## Purpose & Goals

### Why This System Exists
1. **Multi-Agent Visibility**: Track multiple agents working on different tasks in parallel or sequence
2. **Debugging Support**: Understand execution flow when things go wrong
3. **User Transparency**: Show users exactly what their AI agents are doing
4. **Performance Monitoring**: Track execution times and identify bottlenecks
5. **Audit Trail**: Maintain a complete record of all agent actions

## System Architecture

### Core Components

#### 1. Execution Callbacks (`execution_callback.py`)
The heart of the tracing system that captures events during CrewAI execution.

#### 2. Execution Context
Maintains state throughout the execution lifecycle:
- `current_agent`: The agent currently executing
- `current_task`: The task being worked on
- `agent_lookup`: Maps agent IDs to their roles
- `task_to_agent`: Maps tasks to their assigned agents
- `last_known_agent`: Fallback for context preservation
- `task_index`: Tracks progress through task list

#### 3. Trace Queue
Asynchronous queue for processing events without blocking execution.

#### 4. Database Storage
Persistent storage in `execution_trace` table for historical analysis.

## Execution Flow

### 1. Initialization Phase
When a crew starts execution:
1. **Context Setup**: Creates execution context with empty tracking structures
2. **Agent Registration**: Maps all agents by their roles for quick lookup
3. **Task Mapping**: Associates each task with its assigned agent
4. **Callback Creation**: Returns step and task callbacks bound to this execution

### 2. Task Execution Phase

#### Step Callback Flow
For each agent action during task execution:

1. **Event Capture**: Receives step output from CrewAI
2. **Event Type Detection**: Identifies if it's:
   - `AgentAction`: Agent is about to use a tool
   - `AgentFinish`: Agent completed their reasoning
   - `ToolResult`: Result from tool execution
   - `LLM Response`: Direct text output

3. **Agent Attribution Logic** (Critical for correct trace display):
   
   **Priority Order for Determining Current Agent:**
   
   a. **Task Switch Detection** (Highest Priority)
      - Checks if we've moved to a new task
      - Updates context to new task's agent
      - Prevents attribution to previous agent
   
   b. **Task-to-Agent Mapping**
      - Uses pre-built mapping from initialization
      - Most reliable for multi-task crews
      - Ensures correct agent even without explicit context
   
   c. **Direct Agent Reference**
      - Rare but checked when step output includes agent
      - Updates context for future steps
   
   d. **Context Preservation**
      - Uses `current_agent` if already set
      - Falls back to `last_known_agent` if needed
      - Maintains continuity during tool sequences

4. **Data Recording**:
   - Timestamps the event
   - Extracts meaningful content (truncates if needed)
   - Enqueues to trace queue with agent attribution
   - Logs to execution log for debugging

#### Task Callback Flow
When a task completes:

1. **Task Completion Detection**: Identifies completed task
2. **Agent Attribution**: Determines which agent completed it
3. **Context Update for Next Task**:
   - Identifies next task in sequence
   - Pre-loads next agent into context
   - Prevents attribution errors at task boundaries
4. **Recording**: Stores task result with correct agent

### 3. Context Switching Logic

#### The Attribution Problem
When multiple agents work on sequential tasks, the system must correctly attribute actions to the right agent. Without proper context switching:
- Task 2 actions get attributed to Task 1's agent
- The execution trace shows all tasks under one agent
- Users see incorrect task-agent relationships

#### The Solution
The callback system implements several strategies:

1. **Proactive Task Preparation**
   - After task completion, immediately prepare for next task
   - Update `current_agent` to next task's agent
   - Set `current_task` to next task object

2. **Task Switch Detection**
   - Monitor `task_index` to detect transitions
   - Compare expected task with current context
   - Force context update when mismatch detected

3. **Multi-Level Fallback**
   - Primary: Use task-to-agent mapping
   - Secondary: Use current context
   - Tertiary: Use last known agent
   - Final: Mark as "Unknown Agent" for debugging

## Display in Execution Trace Timeline

### How Events Become Timeline Entries

1. **Event Collection**: Callbacks capture and enqueue events
2. **Processing**: Background worker processes queue
3. **Storage**: Events saved to `execution_trace` table with:
   - `event_source`: Agent role (for grouping)
   - `event_type`: Type of action (llm_call, tool_use, etc.)
   - `event_context`: Task description or tool name
   - `timestamp`: When it occurred

4. **Frontend Display**: Timeline groups events by:
   - Agent (top level)
   - Tasks within each agent
   - Individual actions within tasks

### Common Display Issues & Solutions

**Problem**: Tasks appear under wrong agent
- **Cause**: Context not updating at task boundaries
- **Solution**: Task callback updates context for next task

**Problem**: "Unknown Agent" in traces
- **Cause**: Context lost during execution
- **Solution**: Multiple fallback strategies for agent detection

**Problem**: Tool calls not associated with tasks
- **Cause**: Tool results don't carry agent context
- **Solution**: Preserve context across tool call sequences

## Debugging Guide

### Key Log Indicators

1. **Successful Task Switch**:
   ```
   "Detected task switch - new agent: [agent_name]"
   "Preparing for next task with agent: [agent_name]"
   ```

2. **Context Issues**:
   ```
   "Could not determine agent for [step_type]"
   "Multiple agents found, using first: [agent_name]"
   ```

3. **Task Mapping**:
   ```
   "Task [index] mapped to agent: [agent_role]"
   "Registered agent: [agent_role]"
   ```

### Troubleshooting Steps

1. **Check Initialization**: Verify all agents and tasks are registered
2. **Monitor Task Transitions**: Look for task completion events
3. **Verify Context Updates**: Ensure agent context changes with tasks
4. **Review Fallback Usage**: Excessive fallback indicates mapping issues

## Best Practices

### For Developers

1. **Always Pass Crew Reference**: Ensures callbacks have full context
2. **Maintain Agent Roles**: Use consistent, unique role names
3. **Handle Edge Cases**: Account for single-agent and multi-agent crews
4. **Log Context Changes**: Add debug logs for context transitions

### For System Configuration

1. **Enable Debug Logging**: Set appropriate log levels for troubleshooting
2. **Monitor Queue Size**: Ensure trace queue isn't backing up
3. **Database Indexing**: Index execution_trace by job_id and timestamp
4. **Retention Policy**: Clean old traces based on requirements

## Future Improvements

1. **Enhanced Context Tracking**: Add more granular state tracking
2. **Performance Metrics**: Calculate and display execution statistics
3. **Real-time Updates**: WebSocket support for live trace updates
4. **Advanced Filtering**: Better tools for trace analysis
5. **Context Visualization**: Graphical display of context switches