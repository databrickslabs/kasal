sequenceDiagram
    participant UI as Frontend UI
    participant JobService as JobExecutionService
    participant RunStore as CrewExecutionStore
    participant ApiClient as API Client
    participant Router as Flow API Router
    participant ExecService as ExecutionService
    participant CrewExecService as CrewAIExecutionService
    participant FlowRepo as FlowRepository
    participant FlowService as CrewAIFlowService
    participant FlowRunner as FlowRunnerService (relocated)
    participant Backend as BackendFlow
    participant Engine as CrewAIEngineService
    participant Llm as LLMManager
    participant Keys as ApiKeysService
    participant DB as Database
    participant CrewAI as CrewAI Flow

    Note over UI,CrewAI: Flow Execution Process - Updated Architecture

    UI->>RunStore: handleRunClick(type: flow)
    RunStore->>JobService: executeJob(..., type: flow)
    Note right of JobService: Prepare flow execution config with nodes, edges, flow_id

    JobService->>ApiClient: POST /executions
    ApiClient->>Router: execute_flow(request)
    Router->>ExecService: execute_flow(flow_id, nodes, edges, job_id)
    
    ExecService->>CrewExecService: run_flow_execution(flow_id, nodes, edges, job_id, config)
    
    alt Has flow_id but no nodes
        CrewExecService->>FlowRepo: find_by_id(flow_id)
        FlowRepo->>DB: Query flow data
        DB-->>FlowRepo: Return flow data
        FlowRepo-->>CrewExecService: Return flow entity (nodes, edges, config)
        CrewExecService->>CrewExecService: Update execution_config with loaded data
    end
    
    CrewExecService->>FlowService: run_flow(flow_id, job_id, config)
    
    FlowService->>FlowRunner: run_flow(flow_id, job_id, config)
    
    FlowRunner->>FlowRunner: Create execution record via repository
    FlowRunner->>DB: Create flow execution record
    FlowRunner-->>FlowService: Return job_id and execution_id (async)
    FlowService-->>CrewExecService: Return execution details
    CrewExecService-->>ExecService: Return execution response
    ExecService-->>Router: Return execution response
    Router-->>ApiClient: Return execution response
    ApiClient-->>JobService: Return response with job_id
    JobService-->>RunStore: Return job execution response
    RunStore-->>UI: Update with job_id and status
    Note right of UI: UI starts polling for status

    Note over FlowRunner,CrewAI: Asynchronous Flow Execution

    FlowRunner->>FlowRunner: Create flow execution task
    
    FlowRunner->>FlowRunner: _run_flow_execution(execution_id, flow_id, job_id, config)
    
    FlowRunner->>Keys: Initialize API keys
    Keys-->>FlowRunner: Return API keys
    
    FlowRunner->>FlowRunner: Create event listeners (Agent Trace, Task Completion, Detailed Output)
    
    FlowRunner->>Backend: Create BackendFlow(job_id, flow_id)
    FlowRunner->>Backend: Set repositories (flow, task, agent, tool)
    
    alt No nodes in config
        Backend->>Backend: load_flow() via flow repository
        Backend->>FlowRepo: find_by_id(flow_id)
        FlowRepo->>DB: Query flow data
        DB-->>FlowRepo: Return flow data
        FlowRepo-->>Backend: Return flow entity
        Backend->>Backend: Update config with loaded data
    end
    
    FlowRunner->>Backend: setup output directory
    FlowRunner->>DB: Update execution status to RUNNING via repository
    
    FlowRunner->>Backend: kickoff()
    
    Backend->>Backend: load_flow() if not already loaded

    Backend->>Backend: flow() - create CrewAI Flow
    
    Backend->>DB: Query task, agent and tool data via repositories
    DB-->>Backend: Return task, agent, and tool data
    
    Backend->>Llm: _get_llm()
    Llm->>Keys: get_provider_api_key()
    Keys-->>Llm: Return API keys
    Llm-->>Backend: Return configured LLM

    Backend->>CrewAI: kickoff_async()
    
    CrewAI->>CrewAI: Execute flow with agents and tasks
    Note right of CrewAI: CrewAI handles the LLM calls, tools, and task execution
    
    CrewAI-->>Backend: Return execution result
    
    Backend->>Backend: Convert result to dictionary
    Backend-->>FlowRunner: Return execution result
    
    FlowRunner->>DB: Update execution status to COMPLETED/FAILED via repository
    
    UI->>ApiClient: Poll for execution status
    ApiClient->>Router: get_flow_execution(execution_id)
    Router->>FlowService: get_flow_execution(execution_id)
    FlowService->>FlowRunner: get_flow_execution(execution_id)
    FlowRunner->>DB: Query execution record via repository
    DB-->>FlowRunner: Return execution data
    FlowRunner-->>FlowService: Return execution details
    FlowService-->>Router: Return execution details
    Router-->>ApiClient: Return execution response
    ApiClient-->>UI: Update UI with execution status and result

    Note over UI,CrewAI: Separation of Concerns in New Architecture
    Note right of ExecService: ExecutionService delegates to CrewAIExecutionService
    Note right of CrewExecService: CrewAIExecutionService accesses data via repositories
    Note right of FlowRepo: Repository layer handles all database access
    Note right of FlowService: CrewAIFlowService acts as adapter to FlowRunnerService
    Note right of FlowRunner: FlowRunnerService (now in engines/crewai/flow) handles execution details