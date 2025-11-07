# Why Kasal - Available on Databricks Marketplace

Transform your Databricks environment into an AI orchestration powerhouse. Build intelligent agent workflows that unlock the full potential of your Databricks lakehouse - now available directly in the Databricks Marketplace for one-click installation.

---

## What problems does it solve?
- **Hard-to-operationalize AI**: Multi-agent apps are complex to build, observe, and scale. Kasal turns that into a repeatable, visual workflow.
- **Glue-code overload**: Stop stitching LLMs, vector search, tools, logs, and schedulers by hand. Kasal gives you a cohesive system out of the box.
- **Enterprise friction**: Works with your Databricks environment (OAuth, Secrets, Vector Search, MLflow, Volumes) and respects org boundaries with group-aware multi-tenancy.

## Who is it for?
- **Data/AI Engineers**: Build reliable multi-agent workflows on top of Databricks.
- **Analysts/Builders**: Design flows visually and reuse templates without deep infra knowledge.
- **Platform Teams**: Standardize how AI workflows are built, governed, and observed.

## What you get (at a glance)
- **Visual Workflow Designer**: Drag-and-drop collaboration between agents and tools.
- **Production-Grade Backend**: FastAPI + async SQLAlchemy, background schedulers, and robust logging.
- **Databricks-Native Integrations**: OAuth, Secrets, Vector Search, Volumes, MLflow, and SQL endpoints.
- **Deep Observability**: Real-time logs, execution traces, run history, and health checks.
- **Extensible AI Engine**: CrewAI integration today, engine abstraction for future engines.
- **Governance & Security**: Group-aware multi-tenant model, API keys, permissions, and auditability.

## Components at a glance
A quick tour of the building blocks—what each part does and why it matters.

### Frontend (React SPA)
- **What it does**: Visual designer for agents, tasks, and flows; live monitoring UI.
- **Why it matters**: Non-technical users can build and operate AI workflows without touching code.

### API (FastAPI)
- **What it does**: Validates requests, exposes REST endpoints, and routes calls to services.
- **Why it matters**: Clear, versioned contracts between UI/automation and backend logic.

### Services (Business Logic)
- **What it does**: Implements orchestration, validation, scheduling, and domain logic.
- **Why it matters**: Keeps HTTP thin and domain logic testable and reusable.

### Repositories (Data Access)
- **What it does**: Encapsulates SQL and external I/O (Databricks APIs, Vector Search, MLflow).
- **Why it matters**: Swappable persistence and integrations without leaking into services.

### Engines (CrewAI Orchestration)
- **What it does**: Prepares crews, runs executions, handles callbacks/guardrails, manages memory.
- **Why it matters**: Pluggable execution engine today (CrewAI) and extensible for future engines.

### Data & Storage
- **What it does**: Async SQLAlchemy sessions, models/schemas, vector indexes, volumes.
- **Why it matters**: Reliable persistence with optional vector search and document storage.

### Scheduler & Background Jobs
- **What it does**: Recurring runs, long tasks, and background queues (e.g., embedding batching).
- **Why it matters**: Production-ready operations beyond single request/response.

### Observability
- **What it does**: Structured logs, execution logs, traces, history, health checks.
- **Why it matters**: Debug fast, audit runs, and understand system behavior end-to-end.

### Security & Governance
- **What it does**: Group-aware multi-tenancy, JWT/Databricks headers, centralized permissions.
- **Why it matters**: Safely share across teams while isolating data and enforcing roles.

### Databricks Integrations
- **What it does**: OAuth, Secrets, SQL Warehouses, Unity Catalog, Volumes, Vector Search, MLflow.
- **Why it matters**: Build where your data and models already live with first-class support.

---

## What Makes Kasal Different

# Core capabilities

- **Build**
  - Visual designer for agents, tasks, flows.
  - AI-assisted generation: agents, crews, tasks, and templates.
  - Reusable tool registry (native + custom tools, MCP support).
  - Documentation embeddings to improve agent planning and generation.

- **Orchestrate**
  - CrewAI-based execution with guardrails and callbacks.
  - Memory backends and entity extraction with model-aware fallbacks.
  - Scheduler for recurring jobs and long-running workflows.
  - Parallelization and background processing where it matters.

- **Integrate (Databricks-first)**
  - Vector Search setup/verification and indexing endpoints.
  - MLflow model integration.
  - Databricks SQL, Volumes, Secrets, and knowledge ingestion.
  - Dispatcher and connectors for external systems and APIs.

- **Operate**
  - Centralized structured logging (file-backed), optional SQL query logging.
  - Execution logs, traces, and run history via dedicated APIs.
  - Database management endpoints (backup/restore where enabled).
  - Health checks and environment validation on startup.

- **Govern**
  - Group-based multi-tenant isolation with role awareness.
  - JWT and Databricks Apps headers for user context.
  - Permissions centralized to keep auth decisions consistent.


## Real Use Cases in Production

### Financial Analysis with Databricks
**Setup**: 3 agents leveraging your lakehouse
- Data Agent queries Unity Catalog tables with SQL
- Analysis Agent uses Databricks ML for anomaly detection
- Report Agent generates executive summary from Delta Lake

**Result**: 4-hour manual process reduced to 5 minutes, all within Databricks

### Customer Support with Databricks Knowledge
**Setup**: Databricks-powered response system
- Store documentation in Databricks Volumes
- Agent uses Vector Search for semantic understanding
- Query customer history from Delta tables
- Escalates complex issues with full context

**Result**: 80% of inquiries handled automatically using your Databricks data

### Research and Intelligence on Lakehouse
**Setup**: Databricks-native information synthesis
- Web Search Agent finds latest market data
- Unity Catalog Agent queries your governed data
- ML Agent leverages Databricks models for analysis
- Presentation Agent creates slides from insights

**Result**: Weekly research that took 2 days now takes 30 minutes, integrated with your lakehouse

---

## How It Actually Works

### 1. Design Your Workflow
Open the visual designer. Drag agents onto the canvas. Each agent represents a worker with specific skills.

### 2. Configure Each Agent
Tell agents what to do in plain language:
- "Search the sales database for Q4 revenue"
- "Analyze this data and find the top 3 trends"
- "Write a summary suitable for the board meeting"

### 3. Connect Your Databricks Resources
Point agents to your lakehouse assets:
- Unity Catalog tables and views
- Databricks SQL warehouses
- Delta Lake tables
- Databricks Volumes for documents
- Vector indexes for semantic search

### 4. Run and Monitor
Press start. Watch agents work in real-time. See their thinking process. Review outputs before they're sent.

---

## The Technology Under the Hood - Databricks Native

### Databricks Apps Platform
Kasal runs natively on Databricks Apps:
- **OAuth Authentication**: Secure user authentication with Databricks identity
- **On-Behalf-Of (OBO)**: Execute operations with user permissions
- **Workspace Integration**: Direct access to your Databricks workspace
- **Native Deployment**: One-click installation from Databricks Marketplace

### Databricks Data Access
Direct integration with your lakehouse:
- **Unity Catalog**: Query governed data with SQL
- **Delta Lake**: Read and write Delta tables
- **SQL Warehouses**: Execute queries on compute resources
- **Databricks SQL**: Full SQL support for data operations

### Databricks AI & ML
Leverage Databricks AI capabilities:
- **Model Serving**: Use Databricks-hosted models (DBRX, Llama, MPT)
- **Vector Search**: Semantic search across documents and data
- **Databricks Volumes**: Store and access knowledge documents
- **MLflow Integration**: Track and deploy ML models

### Databricks-Specific Tools
Purpose-built for your lakehouse:
- **Genie Tool**: Natural language queries against your data
- **Databricks SQL Tool**: Direct SQL execution on warehouses
- **Unity Catalog Tool**: Access governed data assets
- **Vector Search Tool**: Semantic knowledge retrieval
- **Databricks Jobs Tool**: Orchestrate and monitor jobs

### Enterprise Features
Leveraging Databricks security:
- **Workspace Isolation**: Multi-tenant with group separation
- **Secret Management**: Databricks secret scopes for credentials
- **Audit Logging**: Full audit trail in Databricks
- **Permission Model**: Honors Databricks ACLs and permissions

---

## Getting Started with Databricks Marketplace

### One-Click Installation
**Install directly from Databricks Marketplace**:
1. Open Databricks Marketplace in your workspace
2. Search for "Kasal"
3. Click "Get" - automatic installation begins
4. Launch Kasal from your Databricks Apps

### Minute 1: Choose a Template
Pick from Databricks-optimized workflows:
- Unity Catalog Data Pipeline
- Lakehouse Analytics Flow
- Delta Lake Processing
- ML Model Orchestration

### Minute 2: Automatic Databricks Connection
No configuration needed:
- Inherits your Databricks permissions
- Accesses your Unity Catalog automatically
- Connects to your SQL warehouses
- Uses your workspace identity

### Minute 3: Customize for Your Data
Point to your lakehouse assets:
- Select Unity Catalog tables
- Choose Delta Lake sources
- Configure SQL queries
- Set processing rules

### Minute 4: Run Your First Workflow
- Press execute
- Agents query your Databricks data
- Monitor in real-time
- Results stay in your lakehouse

### Minute 5: Deploy in Databricks
- Schedule with Databricks Jobs
- Monitor through Databricks UI
- Scale with SQL warehouses
- Integrate with existing pipelines

---


## Integration Capabilities - Databricks First

### Native Databricks Integration
- **Unity Catalog**: Full access to governed data
- **Delta Lake**: Read/write Delta tables directly
- **SQL Warehouses**: Execute queries on your compute
- **Databricks Volumes**: Store documents and files
- **Vector Search**: Semantic search across your data
- **Model Serving**: Use Databricks-hosted AI models

### File Formats in Databricks
Process files stored in Volumes:
- PDF documents
- Excel spreadsheets
- CSV data files
- JSON/XML structures
- Word documents
- Parquet files

### Additional Connectivity
Extend beyond Databricks when needed:
- REST APIs with authentication
- External databases via JDBC
- Webhook receivers
- Custom integrations via MCP

### AI Models on Databricks
Leverage Databricks Model Serving:
- **Llama 4 Maverick**: Latest Meta foundation model
- **Llama 3.3 70B**: Multi-language with 128K context
- **Llama 3.1 405B**: Largest open model, GPT-4 competitive
- **GPT OSS 120B**: OpenAI's reasoning model
- **Claude on Databricks**: Anthropic models via Foundation APIs
- **Custom Models**: Your MLflow models

---

## Licensing

Kasal is available under the **Databricks License** through the Databricks Marketplace.

### What This Means for You
- **Free to install** from Databricks Marketplace
- **Usage-based pricing** through your existing Databricks consumption
- **No separate subscription** - runs on your Databricks compute
- **Enterprise support** through Databricks

### Cost Structure
- **Compute costs**: Standard Databricks SQL Warehouse pricing
- **Storage costs**: Standard Databricks storage rates for Volumes
- **Model costs**: Pay-per-token for AI models used
- **No additional licensing fees** for Kasal itself

---

## Success Metrics

Teams using Kasal report:
- **75% reduction** in manual data processing
- **10x faster** report generation
- **90% accuracy** in routine decisions
- **50% cost savings** vs custom development

---

## Start Building Today on Databricks

1. **Find Kasal in Databricks Marketplace**
2. **One-click installation to your workspace**
3. **Build your first workflow in minutes**
4. **See immediate results with your data**

Transform how your team leverages the Databricks Data Intelligence Platform.

---

*Kasal: Unleashing the full potential of Databricks for everyone*