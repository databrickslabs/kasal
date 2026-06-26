# Crew Export and Deployment

## Overview

Kasal now supports exporting CrewAI crews to various formats and deploying them to Databricks Model Serving endpoints. This feature allows you to take your visually designed agent workflows and run them in production environments.

## Features

### Export Formats

#### 1. Python Project Export
Exports your crew as a complete Python project with the following structure:
- **README.md**: Setup and usage instructions
- **requirements.txt**: Python dependencies
- **.env.example**: Environment variable template
- **.gitignore**: Git ignore patterns
- **src/{crew_name}/config/agents.yaml**: Agent configurations
- **src/{crew_name}/config/tasks.yaml**: Task configurations
- **src/{crew_name}/crew.py**: Crew class implementation
- **src/{crew_name}/main.py**: Execution entry point
- **tests/test_crew.py**: Unit tests (optional)

**Best for:**
- Local development and testing
- Version control integration
- Custom modifications and extensions
- CI/CD pipelines

#### 2. Databricks Notebook Export
Exports your crew as a single `.ipynb` notebook file compatible with Databricks, containing:
- Title and overview
- Setup instructions
- **Package compatibility warning** (important - read before running)
- Installation commands with proper dependency handling
- Agent and task configurations (YAML)
- Crew implementation code with custom tool placeholders
- Execution logic
- Usage examples

**Best for:**
- Quick prototyping in Databricks
- Interactive development and debugging
- Sharing with team members
- Documentation and demonstrations

**Important Note:**
- Installing CrewAI will upgrade core Databricks packages (numpy, pyarrow, protobuf, grpcio)
- This triggers warnings but is expected behavior
- Recommend using Databricks Runtime 14.3 LTS ML or higher for best compatibility
- After installation, Python kernel will restart automatically

#### 3. Databricks App Export

Exports your crew as a **deployable Databricks App** — a CrewAI-adapted copy of Databricks'
official agent-app template. The generated project wraps your crew behind MLflow's `AgentServer`
(`ResponsesAgent` interface), so it gets a built-in chat UI and a queryable `/invocations` endpoint.

Structure (faithful to the Databricks template):
- **app.yaml** — runs the app via `uv run start-app`
- **databricks.yml** — Databricks Asset Bundle (app + MLflow experiment resources)
- **pyproject.toml** — uv project with CrewAI deps and script entrypoints
- **manifest.yaml** — template metadata + resource specs
- **agent_server/agent.py** — builds the crew from `config/*.yaml` and exposes `@invoke`/`@stream`
- **agent_server/start_server.py**, **utils.py**, **evaluate_agent.py**
- **config/agents.yaml**, **config/tasks.yaml** — your crew's agents and tasks
- **scripts/** — `quickstart`, `preflight`, `start_app`, `discover_tools`, …
- **.claude/skills/** — guidance skills (run-locally, add-tools, deploy, …)
- **.github/workflows/deploy.yml**, **README.md**, **AGENTS.md**, **.env.example**

**How the crew runs:** each chat turn maps the user's latest message to the crew's input key
(auto-detected from `{placeholder}` tokens, default `topic`) and calls `crew.kickoff(inputs=...)`.
Edit `MODEL_OVERRIDE`, `INPUT_KEY`, or `MCP_SERVERS` in `agent_server/agent.py` to tune behavior.

**Tools & MCP fidelity:** the export equips the crew with the tools and MCP servers configured in
Kasal:
- **MCP servers** enabled for the workspace are auto-attached to every agent and authenticated with
  the requesting user's OBO token (falling back to the app service principal).
- **Tools** are pre-wired into `TOOL_MAP` keyed by each tool's title, with their non-secret config
  baked in (e.g. the Genie space id, Serper result count); secrets are read from env vars
  (`.env.example`), never embedded. Self-contained tools — **GenieTool** (Databricks Genie via the
  SDK) and **PerplexityTool** — are bundled under `tools/`, plus the standard `crewai_tools`
  (Serper, Scrape, DALL·E).
- Tools that depend on Kasal's runtime (most Power BI / Databricks-internal tools) can't run
  standalone; they're listed in the export metadata (`unsupported_tools`) and left as commented
  `# unsupported` entries in `TOOL_MAP`. Attach those capabilities over **MCP** instead, or add your
  own implementation under `tools/`.

**Task guardrails:** guardrails configured on a task are carried into the app's `TASK_GUARDRAILS`
map (`agent_server/agent.py`). **LLM guardrails** are reproduced with CrewAI's native
`LLMGuardrail` (using the guardrail's model, or the crew's default) and applied to the matching
`Task`. Kasal **built-in code/factory guardrails** can't be bundled standalone, so they're carried
as `{"type": "code"}` and flagged at runtime — re-implement them under `tools/` if you need them.

**Best for:**
- Shipping a crew as a standalone, queryable Databricks App with a chat UI
- One-click deployment from the Kasal UI (see below)

### Deploy to Databricks Apps (one-click)

When the export format is **Databricks App**, the export dialog shows a **Deploy to Databricks Apps**
button. This generates the project, creates the app, uploads the project to the workspace, deploys it,
and starts it — no local CLI needed. The deploy runs in the background; the dialog polls progress
(`CREATING_APP → CREATING_LAKEBASE → CONFIGURING_APP → UPLOADING → DEPLOYING → STARTING → SUCCEEDED`)
and shows the live app URL when finished.

**Deploy-screen options.** Besides the app name, you can pick the model (from the workspace's enabled
models), the UC catalog/schema (used for tools/memory and the OTel telemetry tables), an MLflow
experiment name + **SQL Warehouse ID** (for Unity Catalog tracing — see below), and a **Lakebase
instance** for persistent memory:
- **None** — no database attached.
- **An existing instance** — chosen from the workspace's Lakebase instances; it's attached to the app
  as a `database` resource (the app's identity gets connect+create on it).
- **Create new** — Kasal creates a new Lakebase instance and then attaches it to the app.

The attached instance is surfaced to the app as `LAKEBASE_INSTANCE_NAME` (and `LAKEBASE_DATABASE_NAME`)
so the crew can connect to it. `postgres` is included in the app's OAuth scopes.

**MLflow tracing → Unity Catalog.** Each conversation turn is traced (`mlflow.crewai.autolog()` +
`@mlflow.trace`). Per the
[traces-in-UC docs](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog), a UC
trace location **can only be bound to an experiment at creation time** — it cannot be added to an
existing experiment. So **the deployed app owns and creates its own experiment** at startup
(`mlflow.set_experiment(name, trace_location=UnityCatalog(catalog, schema, table_prefix="agent"))`),
which provisions the UC Delta tables (`<catalog>.<schema>.agent_otel_*`) through the SQL warehouse and
stores traces there — unlimited storage, fine-grained access, queryable from SQL/notebooks/dashboards.
Requirements: a **SQL warehouse** (`MLFLOW_TRACING_SQL_WAREHOUSE_ID`, runs the table DDL — a deploy-screen
field defaulting to the workspace's configured warehouse), the catalog + schema, MLflow ≥ 3.11, the
workspace UC-tracing previews, and **MODIFY + SELECT** grants on the `<prefix>_otel_*` tables. Because the
app creates the experiment under its own service principal, that experiment is **not** attached as an app
*resource* (and the deploy must not pre-create it — a plain experiment can never be made UC-backed). The
app creates it under `/Shared/<name>` and logs the resolved trace location at startup. This is separate
from the app's OTel telemetry export (the `otel_logs`/`otel_metrics`/`otel_spans` tables), which works
regardless.

**Authentication** follows Kasal's standard chain via `get_workspace_client`:
1. **OBO** — the requesting user's forwarded token (`X-Forwarded-Access-Token`), preferred when Kasal is
   opened from the Databricks workspace.
2. **PAT** — the workspace personal access token stored for the group (used when no OBO token is
   present, e.g. running the deploy locally).
3. **Service principal** — Kasal's configured client credentials, as a last resort.

The app is created under whichever identity authenticates. The deploy fails with a clear message only
when none of the above can authenticate.

**Prerequisites:**
- A valid Databricks identity (OBO login, a configured workspace PAT, or service-principal credentials)
  with permission to create Databricks Apps and write to the workspace.
- Editor or Admin role in Kasal.

### Deployment to Databricks Model Serving

Deploy your crew as an MLflow model behind a Databricks Model Serving endpoint for production use.

**Deployment Process:**
1. Wraps crew as MLflow PyFunc model
2. Logs model to MLflow with dependencies
3. Registers in Unity Catalog (optional)
4. Creates/updates Model Serving endpoint
5. Returns endpoint URL for API invocations

**Configuration Options:**
- **Model Name**: Name for the registered model (required)
- **Endpoint Name**: Name for serving endpoint (defaults to model name)
- **Workload Size**: Small, Medium, or Large
- **Scale to Zero**: Enable automatic scaling to zero
- **Unity Catalog**: Register model in Unity Catalog
- **Catalog/Schema**: Unity Catalog location (required if enabled)

## How to Use

### Prerequisites

1. **Save Your Crew**: You must save your crew before exporting or deploying
2. **Permissions**:
   - Export: Editor or Admin role required
   - Deploy: Admin role required

### Export a Crew

1. Design your crew in the visual canvas
2. Save the crew using the Save button
3. Click the **Export** button (download icon) in the toolbar
4. Select export format:
   - **Python Project**: For local development
   - **Databricks Notebook**: For Databricks environment
5. Configure export options:
   - **Include custom tools**: Include tool implementations
   - **Include comments**: Add explanatory comments
   - **Include tests**: Generate test files (Python Project only)
   - **Model override**: Override LLM model for all agents
6. Click **Export & Download**

The exported file will be downloaded to your browser:
- Python Project: `{crew_name}_project.zip`
- Databricks Notebook: `{crew_name}.ipynb`

### Deploy to Databricks Model Serving

1. Design and save your crew
2. Click the **Deploy** button (rocket icon) in the toolbar
3. Configure deployment:
   - **Model Name**: Choose a unique model name (required)
   - **Endpoint Name**: Optionally specify endpoint name
   - **Workload Size**: Select based on expected load
   - **Scale to Zero**: Enable for cost optimization
   - **Unity Catalog**: Enable for centralized model registry
   - **Catalog/Schema**: Specify Unity Catalog location
4. Click **Deploy to Model Serving**
5. Wait for deployment to complete
6. Copy the endpoint URL and usage example

### Invoke Deployed Crew

#### Using HTTP Request
```python
import requests
import os

# Get Databricks token
token = os.getenv("DATABRICKS_TOKEN")

# Endpoint URL from deployment
endpoint_url = "https://your-workspace.cloud.databricks.com/serving-endpoints/your-crew/invocations"

# Prepare request
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

data = {
    "inputs": {
        "topic": "Artificial Intelligence trends in 2025"
    }
}

# Invoke endpoint
response = requests.post(
    endpoint_url,
    headers=headers,
    json=data
)

print("Status Code:", response.status_code)
print("Result:", response.json())
```

#### Using Databricks SDK
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

response = w.serving_endpoints.query(
    name="your-crew",
    inputs={"topic": "Artificial Intelligence trends in 2025"}
)

print("Result:", response)
```

## Export Options Explained

### Include Custom Tools
When enabled, the export includes implementations for custom tools used by your agents. Standard tools (SerperDevTool, etc.) are imported from crewai-tools, but custom tools need implementations.

### Include Comments
Adds explanatory comments throughout the generated code to help understand the structure and functionality.

### Include Tests
(Python Project only) Generates a basic test file with examples of how to test your crew.

### Model Override
Allows you to override the LLM model for all agents in the exported crew. This is useful when:
- Moving from development to production models
- Testing with different model providers
- Standardizing models across all agents

## Deployment Configuration

### Workload Size
- **Small**: Suitable for development and light production loads
- **Medium**: Balanced performance for moderate loads
- **Large**: High performance for heavy production workloads

### Scale to Zero
When enabled, the endpoint automatically scales down to zero replicas when not in use, reducing costs. It will automatically scale up when requests arrive.

### Unity Catalog Integration
Registering your model in Unity Catalog provides:
- Centralized model registry
- Access control and governance
- Model lineage tracking
- Versioning and lifecycle management

## Best Practices

### Before Export
1. Test your crew thoroughly in Kasal
2. Verify all agents and tasks are properly configured
3. Ensure custom tools are working correctly
4. Save your crew with a descriptive name

### Python Project Export
1. Review the generated code
2. Add custom tool implementations if needed
3. Update environment variables in `.env`
4. Run tests before deploying
5. Commit to version control

### Databricks Notebook Export
1. Import notebook into Databricks workspace
2. Configure Databricks secrets for API keys
3. Run cells sequentially to verify functionality
4. Customize inputs for your use case

### Deployment
1. Start with small workload size for testing
2. Enable scale to zero for development endpoints
3. Use Unity Catalog for production models
4. Monitor endpoint performance and costs
5. Update endpoint configuration as needed

## Troubleshooting

### Export Issues

**Error: "Only editors and admins can export crews"**
- Solution: Contact your admin to get Editor or Admin role

**Error: "Crew not found"**
- Solution: Make sure you've saved the crew before exporting

**Export button is disabled**
- Solution: Save your crew first using the Save button

### Deployment Issues

**Error: "Only admins can deploy crews to Model Serving"**
- Solution: Contact your admin to get Admin role

**Error: "Model name is required"**
- Solution: Provide a unique model name in the deployment form

**Error: "Catalog name and schema name are required"**
- Solution: When using Unity Catalog, both catalog and schema names must be provided

**Deployment status shows "NOT_READY"**
- Solution: Wait for the endpoint to initialize. This can take several minutes for the first deployment.

## API Endpoints

### Export Crew
```
POST /api/crews/{crew_id}/export
```

Request body:
```json
{
  "export_format": "python_project" | "databricks_notebook",
  "options": {
    "include_custom_tools": true,
    "include_comments": true,
    "include_tests": true,
    "model_override": "optional-model-name"
  }
}
```

### Download Export
```
GET /api/crews/{crew_id}/export/download?format={format}
```

Returns: File download (zip or ipynb)

### Deploy as Databricks App (one-click)
```
POST /api/crews/{crew_id}/deploy-app
```

Request body:
```json
{
  "config": {
    "app_name": "my-crew",
    "options": { "include_obo_auth": true, "include_custom_tools": true }
  }
}
```

Returns: `{ "deployment_id": "...", "app_name": "my-crew", "status": "PENDING" }`

### Get App Deployment Status
```
GET /api/crews/{crew_id}/deploy-app/status?deployment_id={id}
```

Returns: `{ "status": "RUNNING|SUCCEEDED|FAILED", "step": "...", "message": "...", "app_url": "..." }`

### Deploy Crew (Model Serving)
```
POST /api/crews/{crew_id}/deploy
```

Request body:
```json
{
  "config": {
    "model_name": "my-crew-model",
    "endpoint_name": "my-crew-endpoint",
    "workload_size": "Small" | "Medium" | "Large",
    "scale_to_zero_enabled": true,
    "unity_catalog_model": true,
    "catalog_name": "main",
    "schema_name": "ml_models"
  }
}
```

### Get Deployment Status
```
GET /api/crews/{crew_id}/deployment/status?endpoint_name={name}
```

### Delete Deployment
```
DELETE /api/crews/{crew_id}/deployment/{endpoint_name}
```

## Technical Details

### MLflow Model Structure
Deployed crews are wrapped as MLflow PyFunc models with:
- Custom `CrewAIModelWrapper` class
- Conda environment with CrewAI dependencies
- Crew configuration stored as model artifact
- Input/output signature for Model Serving

### Authentication
The deployment uses Databricks SDK's authentication chain:
1. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
2. Databricks CLI configuration
3. Default profile

### Memory and State
Deployed crews use CrewAI's built-in memory system for maintaining context across agent interactions within a single execution.

## Future Enhancements

Planned features for future releases:
- Export to other platforms (AWS SageMaker, Azure ML)
- Batch inference endpoints
- Custom deployment configurations
- A/B testing support
- Monitoring and observability integration
