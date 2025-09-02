# Execution Flow: From Frontend to Results

## Overview

This document provides a detailed walkthrough of the complete execution flow in Kasal, from when a user clicks "Run" in the frontend to when results are displayed. Understanding this flow is essential for debugging, extending functionality, and optimizing performance.

## High-Level Flow

```
User Action → API Request → Queue Execution → Prepare Crew → Run Crew → 
Capture Events → Process Traces → Store Results → Poll Status → Display Results
```

## Detailed Flow Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                         Frontend (React)                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ User Clicks  │→ │ Build Config │→ │ POST /api/execute    │  │
│  │    "Run"     │  │     JSON     │  │                      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└────────────────────────────────┬───────────────────────────────┘
                                 │ HTTP Request
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                      Backend API (FastAPI)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Router     │→ │Create Record │→ │ Queue Background     │  │
│  │   Handler    │  │ (Execution)  │  │      Task           │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└────────────────────────────────┬───────────────────────────────┘
                                 │ Background Task
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                    Execution Service                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │Update Status │→ │Prepare Crew  │→ │   Run Crew with      │  │
│  │  (RUNNING)   │  │              │  │    Callbacks        │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└────────────────────────────────┬───────────────────────────────┘
                                 │ CrewAI Execution
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                        CrewAI Engine                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │Execute Agents│→ │Fire Callbacks│→ │  Emit Events        │  │
│  │  and Tasks   │  │              │  │                      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└────────────────────────────────┬───────────────────────────────┘
                                 │ Events & Callbacks
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                     Event Processing                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Callbacks  │→ │ Queue Traces │→ │  Process & Store     │  │
│  │   Capture    │  │              │  │   in Database       │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└────────────────────────────────┬───────────────────────────────┘
                                 │ Status Updates
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                    Frontend Polling                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Poll Status  │→ │Fetch Traces  │→ │  Display Timeline    │  │
│  │              │  │              │  │    and Results      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## Step-by-Step Walkthrough

### Step 1: Frontend Initiation

**Location**: `src/frontend/src/components/Chat/WorkflowChatRefactored.tsx`

User clicks "Run" button, triggering execution:

```typescript
const handleExecute = async () => {
    // Build configuration from UI state
    const config = {
        model: selectedModel,
        agents_yaml: agentConfigs,
        tasks_yaml: taskConfigs,
        inputs: userInputs,
        memory_backend: memoryConfig
    };
    
    // Send execution request
    const response = await executeAgents({
        job_type: selectedJobType,
        data_input: inputData,
        config: config
    });
    
    // Store execution ID for polling
    setExecutionId(response.execution_id);
};
```

### Step 2: API Request

**Location**: `src/frontend/src/api/agent.ts`

```typescript
export const executeAgents = async (payload) => {
    return await axios.post('/api/crewai/execute', payload, {
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json'
        }
    });
};
```

### Step 3: Backend API Handler

**Location**: `src/backend/src/api/agent_router.py`

```python
@router.post("/execute")
async def execute_agents(
    request: ExecutionRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_session),
    group_context: GroupContext = Depends(get_group_context)
):
    # Create execution record with PENDING status
    execution = Execution(
        id=str(uuid.uuid4()),
        status=ExecutionStatus.PENDING,
        config=request.config,
        group_id=group_context.primary_group_id,
        created_at=datetime.utcnow()
    )
    
    # Save to database
    db.add(execution)
    await db.commit()
    
    # Queue background execution
    background_tasks.add_task(
        execution_service.execute_agents,
        execution_id=execution.id,
        config=request.config,
        group_context=group_context
    )
    
    # Return immediately with execution ID
    return {"execution_id": execution.id, "status": "PENDING"}
```

### Step 4: Execution Service

**Location**: `src/backend/src/services/execution_service.py`

```python
async def execute_agents(
    execution_id: str, 
    config: dict, 
    group_context: GroupContext
):
    try:
        # Update status to RUNNING
        await ExecutionStatusService.update_status(
            execution_id, 
            ExecutionStatus.RUNNING,
            message="Preparing crew..."
        )
        
        # Prepare the crew with agents and tasks
        crew_prep = CrewPreparation(config, group_context)
        crew = await crew_prep.prepare_crew()
        
        # Run the crew with callbacks
        await run_crew(
            execution_id=execution_id,
            crew=crew,
            running_jobs=running_jobs,
            group_context=group_context,
            config=config
        )
        
    except Exception as e:
        # Handle failure
        logger.error(f"Execution {execution_id} failed: {str(e)}")
        await ExecutionStatusService.update_status(
            execution_id,
            ExecutionStatus.FAILED,
            message=str(e)
        )
        raise
```

### Step 5: Crew Preparation

**Location**: `src/backend/src/engines/crewai/crew_preparation.py`

```python
class CrewPreparation:
    async def prepare_crew(self) -> Crew:
        # 1. Create agents from configuration
        await self._prepare_agents()
        
        # 2. Create tasks and assign to agents
        await self._prepare_tasks()
        
        # 3. Setup memory backend
        await self._prepare_memory()
        
        # 4. Create crew (callbacks set later)
        crew = Crew(
            agents=list(self.agents.values()),
            tasks=self.tasks,
            memory=self.memory,
            verbose=self.verbose,
            step_callback=None,  # Set in execution_runner
            task_callback=None   # Set in execution_runner
        )
        
        return crew
```

### Step 6: Execution Runner

**Location**: `src/backend/src/engines/crewai/execution_runner.py`

```python
async def run_crew(
    execution_id: str,
    crew: Crew,
    running_jobs: dict,
    group_context: GroupContext,
    config: dict
):
    # Track running execution
    running_jobs[execution_id] = {
        "crew": crew,
        "status": "running",
        "started_at": datetime.utcnow()
    }
    
    try:
        # Create execution-scoped callbacks
        step_callback, task_callback = create_execution_callbacks(
            job_id=execution_id,
            config=config,
            group_context=group_context,
            crew=crew  # Pass crew for agent lookup
        )
        
        # Set callbacks on crew
        crew.step_callback = step_callback
        crew.task_callback = task_callback
        
        # Register with LLM event router
        LLMEventRouter.register_execution(execution_id, crew)
        
        # Extract user inputs
        user_inputs = config.get("inputs", {})
        
        # Run crew in thread to avoid blocking
        result = await asyncio.to_thread(
            crew.kickoff,
            inputs=user_inputs
        )
        
        # Update status to COMPLETED
        await ExecutionStatusService.update_status(
            execution_id,
            ExecutionStatus.COMPLETED,
            result=result,
            message="Execution completed successfully"
        )
        
    except asyncio.CancelledError:
        # Handle cancellation
        await ExecutionStatusService.update_status(
            execution_id,
            ExecutionStatus.CANCELLED,
            message="Execution cancelled by user"
        )
        raise
        
    finally:
        # Cleanup
        LLMEventRouter.unregister_execution(execution_id)
        running_jobs.pop(execution_id, None)
```

### Step 7: Event Capture

**Location**: `src/backend/src/engines/crewai/callbacks/execution_callback.py`

```python
def create_execution_callbacks(job_id, config, group_context, crew):
    # Build agent lookup for identification
    agent_lookup = {}
    for agent in crew.agents:
        agent_lookup[id(agent)] = agent.role
        agent_lookup[agent.role] = agent
    
    # Create context for tracking
    context = {
        'job_id': job_id,
        'agent_lookup': agent_lookup,
        'current_agent': None,
        'trace_queue': get_trace_queue()
    }
    
    def step_callback(step_output):
        """Capture each agent step."""
        # Extract agent name
        agent_name = extract_agent_name(step_output, context)
        
        # Create trace
        trace_data = {
            "job_id": job_id,
            "event_source": agent_name,
            "event_type": "agent_execution",
            "timestamp": datetime.utcnow().isoformat(),
            "output_content": format_output(step_output)
        }
        
        # Queue for processing
        context['trace_queue'].put_nowait(trace_data)
    
    def task_callback(task_output):
        """Capture task completion."""
        # Extract agent from task (always available)
        agent_name = task_output.task.agent.role
        
        # Update context
        context['current_agent'] = agent_name
        
        # Create trace
        trace_data = {
            "job_id": job_id,
            "event_source": "task",
            "event_type": "task_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "extra_data": {
                "agent_role": agent_name,
                "task_description": task_output.task.description,
                "output": str(task_output.output)
            }
        }
        
        # Queue for processing
        context['trace_queue'].put_nowait(trace_data)
    
    return step_callback, task_callback
```

### Step 8: Trace Processing

**Location**: `src/backend/src/services/trace_queue.py`

```python
async def process_trace_queue():
    """Background worker processing traces."""
    while True:
        try:
            # Get trace from queue
            trace_data = await trace_queue.get()
            
            # Create database record
            trace = ExecutionTrace(
                job_id=trace_data["job_id"],
                event_source=trace_data["event_source"],
                event_type=trace_data["event_type"],
                event_context=trace_data.get("event_context"),
                output_content=trace_data.get("output_content"),
                extra_data=trace_data.get("extra_data"),
                timestamp=datetime.fromisoformat(trace_data["timestamp"])
            )
            
            # Store in database
            async with get_session() as session:
                session.add(trace)
                await session.commit()
            
            # Mark as processed
            trace_queue.task_done()
            
        except Exception as e:
            logger.error(f"Error processing trace: {e}")
```

### Step 9: Frontend Polling

**Location**: `src/frontend/src/components/Jobs/JobStatus.tsx`

```typescript
useEffect(() => {
    if (!executionId) return;
    
    // Poll for status updates
    const interval = setInterval(async () => {
        try {
            // Get execution status
            const status = await getExecutionStatus(executionId);
            setExecutionStatus(status);
            
            // If completed or failed, fetch full results
            if (status === 'COMPLETED' || status === 'FAILED') {
                // Fetch execution traces
                const traces = await getExecutionTraces(executionId);
                setTraces(traces);
                
                // Stop polling
                clearInterval(interval);
            }
        } catch (error) {
            console.error('Error polling status:', error);
        }
    }, 1000); // Poll every second
    
    return () => clearInterval(interval);
}, [executionId]);
```

### Step 10: Timeline Display

**Location**: `src/frontend/src/components/Jobs/ShowTraceTimeline.tsx`

```typescript
const ShowTraceTimeline = ({ traces }) => {
    // Group traces by agent and task
    const groupedTraces = useMemo(() => {
        return groupTracesByAgentAndTask(traces);
    }, [traces]);
    
    return (
        <Timeline>
            {groupedTraces.map(agentGroup => (
                <TimelineSection key={agentGroup.agent}>
                    <AgentHeader>
                        <AgentIcon />
                        <AgentName>{agentGroup.agent}</AgentName>
                        <Duration>{agentGroup.duration}s</Duration>
                    </AgentHeader>
                    
                    {agentGroup.tasks.map(task => (
                        <TaskSection key={task.id}>
                            <TaskHeader>{task.description}</TaskHeader>
                            
                            {task.events.map(event => (
                                <EventItem key={event.id}>
                                    <EventType>{event.type}</EventType>
                                    <EventContent>{event.content}</EventContent>
                                    <EventTime>{event.timestamp}</EventTime>
                                </EventItem>
                            ))}
                        </TaskSection>
                    ))}
                </TimelineSection>
            ))}
        </Timeline>
    );
};
```

## Key Flow Components

### Execution Status Lifecycle

```
PENDING → RUNNING → COMPLETED/FAILED/CANCELLED
```

### Status Transitions

| From | To | Trigger |
|------|-----|---------|
| PENDING | RUNNING | Execution starts |
| RUNNING | COMPLETED | Successful completion |
| RUNNING | FAILED | Error occurs |
| RUNNING | CANCELLED | User cancellation |
| ANY | FAILED | Unhandled exception |

### Data Flow

1. **Configuration**: Frontend → API → Execution Service
2. **Status Updates**: Database → API → Frontend (polling)
3. **Traces**: Callbacks → Queue → Database → API → Frontend
4. **Results**: Crew → Database → API → Frontend

## Group Context & Isolation

### Group Context Extraction

```python
@router.dependency
async def get_group_context(
    request: Request,
    token: str = Depends(oauth2_scheme)
) -> GroupContext:
    # Extract from JWT token
    payload = jwt.decode(token, SECRET_KEY)
    
    return GroupContext(
        primary_group_id=payload.get("group_id"),
        group_email=payload.get("email"),
        groups=payload.get("groups", [])
    )
```

### Isolation Enforcement

- Execution records include `group_id`
- Traces include `group_id`
- Queries filter by group context
- Memory backends isolated by group

## Performance Optimizations

### Asynchronous Execution

```python
# Run crew in thread pool to avoid blocking
result = await asyncio.to_thread(
    crew.kickoff,
    inputs=user_inputs
)
```

### Batch Trace Processing

```python
async def process_traces_batch():
    batch = []
    deadline = time.time() + 1.0  # 1 second window
    
    while time.time() < deadline and len(batch) < 100:
        try:
            trace = await asyncio.wait_for(
                trace_queue.get(),
                timeout=0.1
            )
            batch.append(trace)
        except asyncio.TimeoutError:
            break
    
    if batch:
        # Bulk insert
        async with get_session() as session:
            session.add_all(batch)
            await session.commit()
```

### Efficient Polling

```typescript
// Exponential backoff for polling
const pollWithBackoff = async (executionId, maxRetries = 30) => {
    let delay = 1000; // Start with 1 second
    
    for (let i = 0; i < maxRetries; i++) {
        const status = await getExecutionStatus(executionId);
        
        if (status === 'COMPLETED' || status === 'FAILED') {
            return status;
        }
        
        await new Promise(resolve => setTimeout(resolve, delay));
        delay = Math.min(delay * 1.5, 10000); // Max 10 seconds
    }
};
```

## Error Handling

### Execution Failures

```python
try:
    result = await crew.kickoff()
except Exception as e:
    # Log detailed error
    logger.exception(f"Crew execution failed: {e}")
    
    # Update status with error details
    await ExecutionStatusService.update_status(
        execution_id,
        ExecutionStatus.FAILED,
        message=str(e),
        error_details=traceback.format_exc()
    )
    
    # Notify frontend via trace
    error_trace = {
        "job_id": execution_id,
        "event_type": "error",
        "event_source": "system",
        "output_content": str(e)
    }
    trace_queue.put_nowait(error_trace)
```

### Timeout Handling

```python
async def run_with_timeout(crew, timeout_seconds=3600):
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(crew.kickoff),
            timeout=timeout_seconds
        )
        return result
    except asyncio.TimeoutError:
        raise ExecutionTimeout(f"Execution exceeded {timeout_seconds}s")
```

## Monitoring & Observability

### Key Metrics

1. **Execution Metrics**
   - Total executions
   - Success/failure rate
   - Average duration
   - Queue depth

2. **Performance Metrics**
   - API response time
   - Trace processing lag
   - Database query time
   - Memory usage

3. **Business Metrics**
   - Executions per user/group
   - Agent utilization
   - Tool usage frequency

### Logging Strategy

```python
# Structured logging
logger.info("execution_started", extra={
    "execution_id": execution_id,
    "group_id": group_context.primary_group_id,
    "config": config,
    "timestamp": datetime.utcnow().isoformat()
})
```

## Troubleshooting Guide

### Common Issues

1. **Execution Stuck in PENDING**
   - Check if background workers are running
   - Verify database connectivity
   - Check for errors in logs

2. **Missing Traces**
   - Verify trace queue is processing
   - Check database write permissions
   - Ensure callbacks are registered

3. **Slow Performance**
   - Monitor queue sizes
   - Check database indexes
   - Review crew configuration

### Debug Mode

Enable debug logging for detailed flow tracking:

```python
# In .env
LOG_LEVEL=DEBUG
TRACE_CALLBACKS=true
MONITOR_QUEUES=true
```

## Related Documentation

- [EVENT_TRACING.md](EVENT_TRACING.md) - Event capture details
- [AGENT_TASK_LIFECYCLE.md](AGENT_TASK_LIFECYCLE.md) - Agent/task details
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues
- [CREWAI_ENGINE.md](CREWAI_ENGINE.md) - Engine architecture