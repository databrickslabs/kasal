sequenceDiagram
    participant UI as Frontend UI
    participant CrewStore as CrewExecutionStore
    participant JobService as JobExecutionService
    participant ApiClient as API Client
    participant ApiRouter as API Router
    participant ExecService as ExecutionService
    participant CrewExecService as CrewAIExecutionService
    participant StatusService as ExecutionStatusService
    participant Engine as CrewAIEngineService
    participant ToolFactory as ToolFactory
    participant ToolService as ToolService
    participant ApiKeys as ApiKeysService
    participant CrewPrep as CrewPreparation
    participant Database as Database
    participant CrewAI as CrewAI Library
    participant LLM as LLM Manager

    Note over UI,LLM: Crew Execution Process (Not Flow)

    UI->>CrewStore: handleRunClick('crew')
    CrewStore->>CrewStore: executeCrew(nodes, edges)
    CrewStore->>JobService: executeJob(nodes, edges, planningEnabled, selectedModel, 'crew', additionalInputs)
    JobService->>ApiClient: POST /executions
    ApiClient->>ApiRouter: execute_flow(request)
    ApiRouter->>ExecService: create_execution(config, background_tasks)
    
    ExecService->>Database: create execution record
    Database-->>ExecService: return execution details
    
    ExecService->>StatusService: update_status(execution_id, 'pending', 'Preparing execution')
    
    ExecService->>ExecService: run_crew_execution(execution_id, config, 'crew')
    ExecService->>CrewExecService: run_crew_execution(execution_id, config)
    
    CrewExecService->>CrewExecService: Create asyncio task for prepare_and_run_crew
    CrewExecService-->>ExecService: Return immediate response with execution_id
    ExecService-->>ApiRouter: Return execution response
    ApiRouter-->>ApiClient: Return execution response
    ApiClient-->>JobService: Return response with job_id
    JobService-->>CrewStore: Return execution response
    CrewStore-->>UI: Update with job_id and status
    
    Note over UI,Database: Asynchronous Execution Starts
    
    CrewExecService->>StatusService: update_status(execution_id, 'preparing', 'Preparing crew execution')
    
    CrewExecService->>Engine: _prepare_engine(config)
    Engine->>Engine: initialize(model)
    Engine-->>CrewExecService: Return initialized engine
    
    CrewExecService->>StatusService: update_status(execution_id, 'running', 'Running crew execution')
    
    CrewExecService->>Engine: run_execution(execution_id, config)
    
    Engine->>Engine: normalize_config(config)
    Engine->>Engine: _setup_output_directory(execution_id)
    
    Engine->>StatusService: update_status('preparing', 'Preparing CrewAI execution')
    
    Engine->>ToolService: Create tool service
    Engine->>ApiKeys: Get API keys
    Engine->>ToolFactory: Create tool factory with API keys
    
    Engine->>CrewPrep: Create CrewPreparation(config, tool_service, tool_factory)
    CrewPrep->>CrewPrep: prepare()
    CrewPrep-->>Engine: Return prepared crew
    
    Engine->>Engine: Set up event listeners for agent trace, task completion, detailed output
    
    Engine->>StatusService: update_status('running', 'CrewAI execution is running')
    
    Engine->>LLM: Get LLM instance with API keys
    LLM-->>Engine: Return configured LLM
    
    Engine->>CrewAI: Run crew (using crew.kickoff())
    
    loop For each task in crew
        CrewAI->>LLM: Execute task with agent
        LLM-->>CrewAI: Return task results
        CrewAI->>Engine: Emit events (agent trace, task completion)
        Engine->>StatusService: Update status with progress
        StatusService->>Database: Update execution record
    end
    
    CrewAI-->>Engine: Return execution results
    
    Engine->>StatusService: update_status(execution_id, 'completed', 'Execution completed')
    StatusService->>Database: Update execution status
    
    Note over UI,Database: UI receives completed status via polling