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

### Deploy Crew
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
