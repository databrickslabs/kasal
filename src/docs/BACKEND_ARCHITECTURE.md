# Kasal Backend Architecture Visualization

This document provides a comprehensive visualization of the Kasal backend architecture using Mermaid diagrams.

## Overall Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        Web[Web Browser]
        API[API Clients]
    end

    subgraph "API Layer - FastAPI Routers"
        Auth[Auth Router]
        Agents[Agents Router]
        Tasks[Tasks Router]
        Crews[Crews Router]
        Exec[Executions Router]
        Flow[Flows Router]
        MCP[MCP Router]
        Genie[Genie Router]
        DB[Databricks Router]
        Memory[Memory Backend Router]
        Tools[Tools Router]
        Templates[Templates Router]
        Models[Models Router]
        Docs[Documentation Router]
    end

    subgraph "Service Layer - Business Logic"
        AuthSvc[Auth Service]
        AgentSvc[Agent Service]
        TaskSvc[Task Service]
        CrewSvc[Crew Service]
        ExecSvc[Execution Service]
        FlowSvc[Flow Service]
        MCPSvc[MCP Service]
        GenieSvc[Genie Service]
        DBSvc[Databricks Service]
        MemSvc[Memory Service]
        ToolSvc[Tool Service]
        TemplateSvc[Template Service]
        ModelSvc[Model Config Service]
        DocSvc[Documentation Service]
        DispatcherSvc[Dispatcher Service]
    end

    subgraph "Repository Layer - Data Access"
        UOW[Unit of Work]
        AgentRepo[Agent Repository]
        TaskRepo[Task Repository]
        CrewRepo[Crew Repository]
        ExecRepo[Execution Repository]
        FlowRepo[Flow Repository]
        MCPRepo[MCP Repository]
        GenieRepo[Genie Repository]
        DBRepo[Databricks Repository]
        MemRepo[Memory Backend Repository]
        ToolRepo[Tool Repository]
        TemplateRepo[Template Repository]
        ModelRepo[Model Config Repository]
        DocRepo[Documentation Repository]
    end

    subgraph "Database Layer"
        SQLite[(SQLite Dev)]
        PostgreSQL[(PostgreSQL Prod)]
        VectorDB[(Vector Storage)]
    end

    subgraph "AI Engine Layer - CrewAI"
        CrewAIEngine[CrewAI Engine Service]
        CrewPrep[Crew Preparation]
        FlowPrep[Flow Preparation]
        ConfigAdapter[Config Adapter]
        ExecRunner[Execution Runner]
        ToolFactory[Tool Factory]
        MCPIntegration[MCP Integration]
        Callbacks[Execution Callbacks]
        MemoryBackend[Memory Backend Factory]
        
        subgraph "Helpers"
            AgentHelpers[Agent Helpers]
            TaskHelpers[Task Helpers]
            ToolHelpers[Tool Helpers]
            ConversionHelpers[Conversion Helpers]
            ModelConvHandler[Model Conversion Handler]
        end
    end

    Web --> Auth
    API --> Auth
    
    Auth --> AuthSvc
    Agents --> AgentSvc
    Tasks --> TaskSvc
    Crews --> CrewSvc
    Exec --> DispatcherSvc
    Flow --> FlowSvc
    MCP --> MCPSvc
    Genie --> GenieSvc
    DB --> DBSvc
    Memory --> MemSvc
    Tools --> ToolSvc
    Templates --> TemplateSvc
    Models --> ModelSvc
    Docs --> DocSvc

    AuthSvc --> UOW
    AgentSvc --> UOW
    TaskSvc --> UOW
    CrewSvc --> UOW
    ExecSvc --> UOW
    FlowSvc --> UOW
    MCPSvc --> UOW
    GenieSvc --> UOW
    DBSvc --> UOW
    MemSvc --> UOW
    ToolSvc --> UOW
    TemplateSvc --> UOW
    ModelSvc --> UOW
    DocSvc --> UOW

    DispatcherSvc --> ExecSvc
    DispatcherSvc --> CrewAIEngine

    UOW --> AgentRepo
    UOW --> TaskRepo
    UOW --> CrewRepo
    UOW --> ExecRepo
    UOW --> FlowRepo
    UOW --> MCPRepo
    UOW --> GenieRepo
    UOW --> DBRepo
    UOW --> MemRepo
    UOW --> ToolRepo
    UOW --> TemplateRepo
    UOW --> ModelRepo
    UOW --> DocRepo

    AgentRepo --> SQLite
    AgentRepo --> PostgreSQL
    TaskRepo --> SQLite
    TaskRepo --> PostgreSQL
    CrewRepo --> SQLite
    CrewRepo --> PostgreSQL
    ExecRepo --> SQLite
    ExecRepo --> PostgreSQL
    FlowRepo --> SQLite
    FlowRepo --> PostgreSQL
    MCPRepo --> SQLite
    MCPRepo --> PostgreSQL
    GenieRepo --> SQLite
    GenieRepo --> PostgreSQL
    DBRepo --> SQLite
    DBRepo --> PostgreSQL
    MemRepo --> SQLite
    MemRepo --> PostgreSQL
    ToolRepo --> SQLite
    ToolRepo --> PostgreSQL
    TemplateRepo --> SQLite
    TemplateRepo --> PostgreSQL
    ModelRepo --> SQLite
    ModelRepo --> PostgreSQL
    DocRepo --> VectorDB

    CrewAIEngine --> CrewPrep
    CrewAIEngine --> FlowPrep
    CrewAIEngine --> ConfigAdapter
    CrewAIEngine --> ExecRunner
    CrewPrep --> AgentHelpers
    CrewPrep --> TaskHelpers
    CrewPrep --> ToolFactory
    FlowPrep --> ToolFactory
    AgentHelpers --> ModelConvHandler
    TaskHelpers --> ConversionHelpers
    ToolFactory --> ToolHelpers
    ToolFactory --> MCPIntegration
    ExecRunner --> Callbacks
    CrewPrep --> MemoryBackend
```

## Clean Architecture Layers

```mermaid
graph LR
    subgraph "External"
        Client[Client Applications]
        ExtServices[External Services<br/>LLMs, Databricks, etc]
    end

    subgraph "Presentation"
        API[API Layer<br/>FastAPI Routers]
        Schemas[Pydantic Schemas<br/>Request/Response DTOs]
    end

    subgraph "Application"
        Services[Service Layer<br/>Business Logic]
        UOW[Unit of Work<br/>Transaction Management]
    end

    subgraph "Domain"
        Models[Domain Models<br/>SQLAlchemy Entities]
        Interfaces[Repository Interfaces<br/>Abstract Base Classes]
    end

    subgraph "Infrastructure"
        Repositories[Repository Implementations<br/>Data Access]
        DB[Database<br/>SQLite/PostgreSQL]
        Integrations[External Integrations<br/>CrewAI, MCP, Vector DB]
    end

    Client --> API
    API --> Schemas
    Schemas --> Services
    Services --> UOW
    UOW --> Repositories
    Services --> Interfaces
    Interfaces <-.-> Repositories
    Repositories --> Models
    Models --> DB
    Services --> Integrations
    Integrations --> ExtServices
```

## CrewAI Engine Integration

```mermaid
graph TB
    subgraph "Execution Flow"
        Request[Execution Request]
        Dispatcher[Dispatcher Service]
        Engine[CrewAI Engine Service]
        ExecSvc[Execution Service]
        Status[Execution Status Service]
    end

    subgraph "CrewAI Components"
        ConfigAdapter[Config Adapter<br/>Normalizes Configuration]
        CrewPrep[Crew Preparation<br/>Builds Agents & Tasks]
        FlowPrep[Flow Preparation<br/>Builds Flow & Routes]
        ExecRunner[Execution Runner<br/>Runs Crew/Flow]
        TraceManager[Trace Manager<br/>Execution Tracing]
    end

    subgraph "Helper Modules"
        AgentHelper[Agent Helpers<br/>Agent Creation & Config]
        TaskHelper[Task Helpers<br/>Task Creation & Deps]
        ToolHelper[Tool Helpers<br/>Tool Resolution]
        ConvHelper[Conversion Helpers<br/>Type Conversions]
        ModelConvHandler[Model Conversion Handler<br/>LLM Config]
        TaskCallbacks[Task Callbacks<br/>Task Event Handling]
    end

    subgraph "Tool System"
        ToolFactory[Tool Factory]
        NativeTools[Native Tools<br/>Built-in CrewAI]
        CustomTools[Custom Tools<br/>Databricks, Genie, Perplexity]
        MCPTools[MCP Tools<br/>Model Context Protocol]
        MCPHandler[MCP Handler<br/>MCP Integration]
    end

    subgraph "Memory System"
        MemFactory[Memory Backend Factory]
        ShortTerm[Short-term Memory]
        LongTerm[Long-term Memory]
        Entity[Entity Memory]
        VectorStorage[Vector Storage<br/>Databricks/ChromaDB]
    end

    subgraph "Callbacks & Monitoring"
        CallbackManager[Callback Manager]
        ExecCallback[Execution Callback<br/>Trace & Logs]
        StreamCallback[Streaming Callback<br/>Real-time Updates]
        StorageCallback[Storage Callback<br/>Result Persistence]
        LoggingCallback[Logging Callbacks<br/>Event Logging]
        ValidationCallback[Validation Callbacks<br/>Output Validation]
    end

    Request --> Dispatcher
    Dispatcher --> ExecSvc
    Dispatcher --> Engine
    Engine --> Status
    ExecSvc --> Status

    Engine --> ConfigAdapter
    ConfigAdapter --> CrewPrep
    ConfigAdapter --> FlowPrep
    
    CrewPrep --> AgentHelper
    CrewPrep --> TaskHelper
    AgentHelper --> ModelConvHandler
    TaskHelper --> ConvHelper
    TaskHelper --> TaskCallbacks
    
    CrewPrep --> ExecRunner
    FlowPrep --> ExecRunner
    ExecRunner --> TraceManager

    CrewPrep --> ToolFactory
    ToolFactory --> ToolHelper
    ToolFactory --> NativeTools
    ToolFactory --> CustomTools
    ToolFactory --> MCPHandler
    MCPHandler --> MCPTools

    CrewPrep --> MemFactory
    MemFactory --> ShortTerm
    MemFactory --> LongTerm
    MemFactory --> Entity
    ShortTerm --> VectorStorage
    LongTerm --> VectorStorage
    Entity --> VectorStorage

    ExecRunner --> CallbackManager
    CallbackManager --> ExecCallback
    CallbackManager --> StreamCallback
    CallbackManager --> StorageCallback
    CallbackManager --> LoggingCallback
    CallbackManager --> ValidationCallback
```

## Database Schema Relationships

```mermaid
erDiagram
    User ||--o{ UserRole : has
    Role ||--o{ UserRole : assigned_to
    Role ||--o{ Privilege : has
    User ||--o{ Group : belongs_to
    
    Group ||--o{ Agent : owns
    Group ||--o{ Task : owns
    Group ||--o{ Crew : owns
    Group ||--o{ Flow : owns
    Group ||--o{ Execution : owns
    
    Crew ||--o{ Agent : contains
    Crew ||--o{ Task : contains
    Task }o--|| Agent : assigned_to
    
    Flow ||--o{ FlowRoute : has
    FlowRoute ||--|| Crew : references
    
    Execution ||--o{ ExecutionHistory : has
    Execution ||--o{ ExecutionLog : generates
    Execution ||--o{ ExecutionTrace : produces
    Execution }o--|| Crew : runs
    Execution }o--|| Flow : runs
    
    Agent ||--o{ Tool : uses
    Task ||--o{ Tool : uses
    
    Tool }o--|| ToolConfig : configured_by
    Agent }o--|| ModelConfig : uses
    
    MCPServer ||--o{ MCPTool : provides
    MCPSettings ||--|| Group : configured_for
    
    MemoryBackend ||--|| Group : configured_for
    DatabricksConfig ||--|| Group : configured_for
    
    Template ||--o{ Agent : generates
    Template ||--o{ Task : generates
    Template ||--o{ Crew : generates
```

## Service Layer Dependencies

```mermaid
graph TD
    subgraph "Core Services"
        AuthService[Auth Service]
        UserService[User Service]
        GroupService[Group Service]
    end

    subgraph "Configuration Services"
        ModelConfigService[Model Config Service]
        EngineConfigService[Engine Config Service]
        MemoryConfigService[Memory Config Service]
        DatabricksService[Databricks Service]
    end

    subgraph "Entity Services"
        AgentService[Agent Service]
        TaskService[Task Service]
        CrewService[Crew Service]
        FlowService[Flow Service]
        ToolService[Tool Service]
    end

    subgraph "Execution Services"
        DispatcherService[Dispatcher Service]
        ExecutionService[Execution Service]
        ExecutionStatusService[Execution Status Service]
        ExecutionHistoryService[History Service]
        ExecutionLogsService[Logs Service]
        ExecutionTraceService[Trace Service]
    end

    subgraph "Integration Services"
        MCPService[MCP Service]
        GenieService[Genie Service]
        DocumentationService[Documentation Service]
    end

    subgraph "Generation Services"
        AgentGenerationService[Agent Generation]
        TaskGenerationService[Task Generation]
        CrewGenerationService[Crew Generation]
        TemplateGenerationService[Template Generation]
    end

    AuthService --> UserService
    UserService --> GroupService

    AgentService --> ModelConfigService
    AgentService --> ToolService
    TaskService --> AgentService
    CrewService --> AgentService
    CrewService --> TaskService
    FlowService --> CrewService

    ExecutionService --> DispatcherService
    DispatcherService --> EngineConfigService
    ExecutionService --> ExecutionStatusService
    ExecutionService --> ExecutionHistoryService
    ExecutionService --> ExecutionLogsService
    ExecutionService --> ExecutionTraceService

    AgentGenerationService --> AgentService
    TaskGenerationService --> TaskService
    CrewGenerationService --> CrewService
    CrewGenerationService --> AgentGenerationService
    CrewGenerationService --> TaskGenerationService
    TemplateGenerationService --> AgentService
    TemplateGenerationService --> TaskService
    TemplateGenerationService --> CrewService

    MCPService --> ToolService
    GenieService --> ToolService
    DocumentationService --> DatabricksService
```

## Request Flow Sequence

```mermaid
sequenceDiagram
    participant Client
    participant Router as API Router
    participant Auth as Auth Middleware
    participant Service
    participant UOW as Unit of Work
    participant Repo as Repository
    participant DB as Database
    participant Engine as CrewAI Engine

    Client->>Router: HTTP Request
    Router->>Auth: Validate JWT
    Auth->>Router: User Context
    Router->>Service: Call Service Method
    Service->>UOW: Begin Transaction
    UOW->>Repo: Create Repository
    Service->>Repo: Execute Operation
    Repo->>DB: SQL Query
    DB->>Repo: Result
    Repo->>Service: Domain Model
    
    alt If AI Execution
        Service->>Engine: Run Execution
        Engine->>Engine: Prepare Crew/Flow
        Engine->>Engine: Execute with Callbacks
        Engine->>Service: Execution Result
    end
    
    Service->>UOW: Commit Transaction
    UOW->>DB: Commit
    Service->>Router: Response Model
    Router->>Client: HTTP Response
```

## Tool System Architecture

```mermaid
graph TB
    subgraph "Tool Registry"
        ToolFactory[Tool Factory<br/>Central Tool Creation]
        ToolRegistry[Tool Registry<br/>Available Tools]
    end

    subgraph "Native Tools"
        SerperTool[Serper Dev Tool<br/>Web Search]
        FileReadTool[File Read Tool]
        DirectoryReadTool[Directory Read Tool]
        WebsiteSearchTool[Website Search Tool]
    end

    subgraph "Custom Tools"
        DatabricksSQL[Databricks SQL Tool<br/>Query Execution]
        DatabricksJobs[Databricks Jobs Tool<br/>Job Management]
        GenieTool[Genie Tool<br/>AI Assistant]
        PerplexityTool[Perplexity Tool<br/>AI Search]
    end

    subgraph "MCP Tools"
        MCPHandler[MCP Handler<br/>Protocol Bridge]
        MCPServer[MCP Server<br/>Tool Provider]
        MCPTransport[MCP Transport<br/>Communication]
    end

    subgraph "Tool Configuration"
        ToolConfig[Tool Config<br/>Per-Tool Settings]
        AgentToolConfig[Agent Tool Config<br/>Tool Overrides]
        TaskToolConfig[Task Tool Config<br/>Tool Overrides]
    end

    ToolFactory --> ToolRegistry
    ToolRegistry --> NativeTools
    ToolRegistry --> CustomTools
    ToolRegistry --> MCPHandler

    MCPHandler --> MCPServer
    MCPServer --> MCPTransport

    ToolConfig --> ToolFactory
    AgentToolConfig --> ToolFactory
    TaskToolConfig --> ToolFactory
```

## Memory Backend Architecture

```mermaid
graph TB
    subgraph "Memory Configuration"
        MemoryConfig[Memory Backend Config<br/>Group-specific Settings]
        DefaultConfig[Default Config<br/>ChromaDB + SQLite]
        DatabricksConfig[Databricks Config<br/>Vector Search]
    end

    subgraph "Memory Factory"
        MemoryFactory[Memory Backend Factory]
        ConfigValidation[Config Validation<br/>Check Requirements]
        BackendSelection[Backend Selection<br/>Choose Implementation]
    end

    subgraph "Memory Types"
        ShortTermMemory[Short-term Memory<br/>Recent Context]
        LongTermMemory[Long-term Memory<br/>Historical Data]
        EntityMemory[Entity Memory<br/>Entity Relationships]
        DocumentMemory[Document Memory<br/>Embedded Docs]
    end

    subgraph "Storage Backends"
        ChromaDB[ChromaDB<br/>Local Vector DB]
        SQLiteStorage[SQLite Storage<br/>Local Persistence]
        DatabricksVector[Databricks Vector<br/>Cloud Vector DB]
    end

    subgraph "Crew Integration"
        CrewMemory[Crew Memory<br/>Deterministic ID]
        GroupIsolation[Group Isolation<br/>Tenant Separation]
        MemoryPersistence[Memory Persistence<br/>Cross-run State]
    end

    MemoryConfig --> MemoryFactory
    DefaultConfig --> MemoryFactory
    DatabricksConfig --> MemoryFactory

    MemoryFactory --> ConfigValidation
    ConfigValidation --> BackendSelection
    
    BackendSelection --> ChromaDB
    BackendSelection --> SQLiteStorage
    BackendSelection --> DatabricksVector

    ShortTermMemory --> ChromaDB
    ShortTermMemory --> DatabricksVector
    LongTermMemory --> ChromaDB
    LongTermMemory --> DatabricksVector
    EntityMemory --> ChromaDB
    EntityMemory --> DatabricksVector
    DocumentMemory --> DatabricksVector

    CrewMemory --> GroupIsolation
    GroupIsolation --> MemoryPersistence
    MemoryPersistence --> ChromaDB
    MemoryPersistence --> DatabricksVector
```

## Authentication & Authorization Flow

```mermaid
graph TB
    subgraph "Authentication"
        JWT[JWT Token<br/>Bearer Auth]
        OBO[OBO Token<br/>User On-Behalf-Of]
        OAuth[OAuth2<br/>Client Credentials]
        PAT[PAT Token<br/>Personal Access Token]
    end

    subgraph "Authorization"
        UserContext[User Context<br/>User + Group]
        RoleCheck[Role Check<br/>User Roles]
        PrivilegeCheck[Privilege Check<br/>Role Privileges]
        GroupCheck[Group Check<br/>Resource Ownership]
    end

    subgraph "Databricks Auth"
        DBAuth[Databricks Auth<br/>Priority Chain]
        OBOAuth[1. OBO Authentication<br/>X-Forwarded-Access-Token]
        PATAuth[2. PAT from DB<br/>Encrypted Storage]
        EnvAuth[3. Environment PAT<br/>DATABRICKS_TOKEN]
        SDKAuth[4. SDK Default<br/>Fallback Chain]
    end

    subgraph "Resource Access"
        APIEndpoint[API Endpoint<br/>Protected Resource]
        VectorSearch[Vector Search<br/>Direct Access Index]
        SQLWarehouse[SQL Warehouse<br/>Query Execution]
        JobsAPI[Jobs API<br/>Job Management]
    end

    JWT --> UserContext
    OBO --> UserContext
    OAuth --> UserContext
    PAT --> UserContext

    UserContext --> RoleCheck
    RoleCheck --> PrivilegeCheck
    PrivilegeCheck --> GroupCheck
    GroupCheck --> APIEndpoint

    APIEndpoint --> DBAuth
    DBAuth --> OBOAuth
    OBOAuth --> PATAuth
    PATAuth --> EnvAuth
    EnvAuth --> SDKAuth

    OBOAuth --> VectorSearch
    PATAuth --> VectorSearch
    OBOAuth --> SQLWarehouse
    PATAuth --> SQLWarehouse
    OBOAuth --> JobsAPI
    PATAuth --> JobsAPI
```

## Key Architecture Principles

1. **Clean Architecture**: Strict separation of concerns with clear boundaries between layers
2. **Repository Pattern**: All database access through repositories for abstraction
3. **Unit of Work**: Transaction management ensuring atomic operations
4. **Dependency Injection**: FastAPI's DI system for loose coupling
5. **Async-First**: All I/O operations are async for performance
6. **Group Isolation**: Complete tenant separation for multi-tenancy
7. **Deterministic IDs**: Consistent crew IDs for memory persistence
8. **Schema Layer**: Centralized schema definitions for consistency
9. **Authentication Chain**: Flexible auth with fallback mechanisms
10. **Event-Driven**: Callbacks and events for real-time updates

## Technology Stack

- **Framework**: FastAPI (async Python web framework)
- **ORM**: SQLAlchemy 2.0 (async support)
- **Database**: SQLite (dev) / PostgreSQL (prod)
- **AI Engine**: CrewAI (agent orchestration)
- **Vector DB**: Databricks Vector Search / ChromaDB
- **Authentication**: JWT with Databricks OAuth
- **Validation**: Pydantic schemas
- **Testing**: Pytest with async support
- **Migration**: Alembic for database migrations