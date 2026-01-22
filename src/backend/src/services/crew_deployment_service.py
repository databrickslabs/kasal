"""
Service for deploying CrewAI crews to Databricks Model Serving.
"""

from typing import Dict, Any, Optional, List
import logging
import json
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
import mlflow
import mlflow.pyfunc

from src.schemas.crew_export import (
    ModelServingConfig,
    DeploymentStatus,
    DeploymentResponse
)
from src.repositories.crew_repository import CrewRepository
from src.repositories.agent_repository import AgentRepository
from src.repositories.task_repository import TaskRepository
from src.repositories.tool_repository import ToolRepository
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class CrewDeploymentService:
    """Service for deploying crews to Databricks Model Serving"""

    def __init__(self, session: AsyncSession):
        """
        Initialize deployment service with database session.

        Args:
            session: Database session for operations
        """
        self.session = session
        self.crew_repository = CrewRepository(session)
        self.agent_repository = AgentRepository(session)
        self.task_repository = TaskRepository(session)
        self.tool_repository = ToolRepository(session)

    async def deploy_to_model_serving(
        self,
        crew_id: str,
        config: ModelServingConfig,
        group_context: Optional[GroupContext] = None
    ) -> DeploymentResponse:
        """
        Deploy a crew to Databricks Model Serving

        Args:
            crew_id: ID of crew to deploy
            config: Model serving configuration
            group_context: Group context for authorization

        Returns:
            Deployment response with endpoint details
        """
        logger.info(f"Starting deployment of crew {crew_id} to Model Serving")

        # 1. Get crew data
        crew = await self.crew_repository.get(crew_id)
        if not crew:
            raise ValueError(f"Crew {crew_id} not found")

        # Check group authorization
        if group_context and group_context.is_valid():
            if crew.group_id not in group_context.group_ids:
                raise ValueError(f"Crew {crew_id} not found")

        # Get agents and tasks
        agents = []
        for agent_id in crew.agent_ids:
            agent = await self.agent_repository.get(agent_id)
            if agent:
                agent_dict = await self._agent_to_dict(agent)
                agents.append(agent_dict)

        tasks = []
        for task_id in crew.task_ids:
            task = await self.task_repository.get(task_id)
            if task:
                task_dict = await self._task_to_dict(task)
                tasks.append(task_dict)

        # 2. Create crew data structure
        crew_data = {
            'id': str(crew.id),
            'name': crew.name,
            'agents': agents,
            'tasks': tasks,
        }

        # 3. Create MLflow model
        model_uri, model_version = await self._create_mlflow_model(
            crew_data,
            config
        )

        # 4. Deploy to Model Serving
        endpoint_url, deployment_status = await self._deploy_to_endpoint(
            config.model_name,
            model_version,
            config
        )

        # 5. Generate usage example
        usage_example = self._generate_usage_example(
            endpoint_url,
            config.endpoint_name or config.model_name
        )

        return DeploymentResponse(
            crew_id=str(crew_id),
            crew_name=crew.name,
            deployment_target="databricks_model_serving",
            model_name=config.model_name,
            model_version=model_version,
            model_uri=model_uri,
            endpoint_name=config.endpoint_name or config.model_name,
            endpoint_url=endpoint_url,
            endpoint_status=deployment_status,
            deployed_at=datetime.utcnow().isoformat(),
            metadata={
                'agents_count': len(agents),
                'tasks_count': len(tasks),
                'workload_size': config.workload_size,
                'scale_to_zero': config.scale_to_zero_enabled,
            },
            usage_example=usage_example
        )

    async def _create_mlflow_model(
        self,
        crew_data: Dict[str, Any],
        config: ModelServingConfig
    ) -> tuple[str, str]:
        """
        Create and log MLflow model

        Args:
            crew_data: Crew configuration data
            config: Model serving configuration

        Returns:
            Tuple of (model_uri, model_version)
        """
        # Create model wrapper
        model_wrapper = CrewAIModelWrapper(crew_data)

        # Create temp directory for artifacts
        temp_dir = tempfile.mkdtemp()

        try:
            # Save crew configuration
            crew_config_path = Path(temp_dir) / "crew_config.json"
            with open(crew_config_path, 'w') as f:
                json.dump(crew_data, f, indent=2)

            # Define model signature
            from mlflow.models.signature import infer_signature
            import pandas as pd

            # Sample input/output for signature
            sample_input = pd.DataFrame([{
                "inputs": json.dumps({"topic": "Sample topic"})
            }])
            sample_output = pd.DataFrame([{
                "result": "Sample result",
                "execution_time": 0.0
            }])

            signature = infer_signature(sample_input, sample_output)

            # Set MLflow tracking
            mlflow.set_tracking_uri("databricks")

            # Start MLflow run
            with mlflow.start_run(run_name=f"crew_deployment_{crew_data['name']}"):
                # Log parameters
                mlflow.log_param("crew_name", crew_data['name'])
                mlflow.log_param("agents_count", len(crew_data['agents']))
                mlflow.log_param("tasks_count", len(crew_data['tasks']))

                # Log crew configuration as artifact
                mlflow.log_artifact(str(crew_config_path), "config")

                # Define dependencies
                conda_env = {
                    'channels': ['conda-forge'],
                    'dependencies': [
                        'python=3.9',
                        'pip',
                        {
                            'pip': [
                                'crewai>=0.80.0',
                                'crewai-tools>=0.12.0',
                                'pydantic>=2.0.0',
                                'cloudpickle',
                            ]
                        }
                    ],
                    'name': 'crew_env'
                }

                # Log model
                model_info = mlflow.pyfunc.log_model(
                    artifact_path="model",
                    python_model=model_wrapper,
                    conda_env=conda_env,
                    signature=signature,
                    artifacts={"crew_config": str(crew_config_path)}
                )

                # Register model in Unity Catalog or MLflow Model Registry
                if config.unity_catalog_model and config.catalog_name and config.schema_name:
                    model_name = f"{config.catalog_name}.{config.schema_name}.{config.model_name}"
                else:
                    model_name = config.model_name

                model_version_info = mlflow.register_model(
                    model_uri=model_info.model_uri,
                    name=model_name,
                    tags=config.tags or {}
                )

                model_uri = model_info.model_uri
                model_version = model_version_info.version

                logger.info(f"Model registered: {model_name} version {model_version}")

                return model_uri, model_version

        finally:
            # Cleanup temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)

    async def _deploy_to_endpoint(
        self,
        model_name: str,
        model_version: str,
        config: ModelServingConfig
    ) -> tuple[str, DeploymentStatus]:
        """
        Deploy model to Model Serving endpoint

        Args:
            model_name: Name of registered model
            model_version: Version of the model
            config: Model serving configuration

        Returns:
            Tuple of (endpoint_url, status)
        """
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import (
            EndpointCoreConfigInput,
            ServedEntityInput,
            AutoCaptureConfigInput
        )

        w = WorkspaceClient()

        endpoint_name = config.endpoint_name or config.model_name

        # Define served entity
        served_entity = ServedEntityInput(
            entity_name=model_name,
            entity_version=model_version,
            workload_size=config.workload_size,
            scale_to_zero_enabled=config.scale_to_zero_enabled,
            min_provisioned_throughput=config.min_instances,
            max_provisioned_throughput=config.max_instances,
            environment_vars=config.environment_vars or {}
        )

        # Check if endpoint exists
        try:
            existing_endpoint = w.serving_endpoints.get(endpoint_name)
            logger.info(f"Endpoint {endpoint_name} exists, updating...")

            # Update endpoint
            w.serving_endpoints.update_config(
                name=endpoint_name,
                served_entities=[served_entity],
                auto_capture_config=AutoCaptureConfigInput(
                    catalog_name=config.catalog_name,
                    schema_name=config.schema_name,
                    enabled=True
                ) if config.unity_catalog_model else None
            )

            status = DeploymentStatus.UPDATING

        except Exception:
            logger.info(f"Endpoint {endpoint_name} does not exist, creating...")

            # Create new endpoint
            w.serving_endpoints.create(
                name=endpoint_name,
                config=EndpointCoreConfigInput(
                    served_entities=[served_entity],
                    auto_capture_config=AutoCaptureConfigInput(
                        catalog_name=config.catalog_name,
                        schema_name=config.schema_name,
                        enabled=True
                    ) if config.unity_catalog_model else None
                )
            )

            status = DeploymentStatus.PENDING

        # Get endpoint URL
        endpoint = w.serving_endpoints.get(endpoint_name)
        endpoint_url = f"https://{w.config.host}/serving-endpoints/{endpoint_name}/invocations"

        logger.info(f"Endpoint {endpoint_name} deployed: {endpoint_url}")

        return endpoint_url, status

    async def _convert_tool_ids_to_names(self, tool_ids: List[Any]) -> List[str]:
        """
        Convert tool IDs to tool names

        Args:
            tool_ids: List of tool IDs (can be integers or strings)

        Returns:
            List of tool names (strings)
        """
        tool_names = []
        for tool_id in tool_ids:
            # Try to convert to integer if it's a numeric string
            if isinstance(tool_id, str) and tool_id.isdigit():
                tool_id = int(tool_id)

            # If it's an integer (tool ID), look up the tool name
            if isinstance(tool_id, int):
                tool = await self.tool_repository.get(tool_id)
                if tool:
                    tool_names.append(tool.title)
                    logger.info(f"Converted tool ID {tool_id} to name: {tool.title}")
                else:
                    logger.warning(f"Tool with ID {tool_id} not found in database")
                    # Keep the ID as string if tool not found
                    tool_names.append(str(tool_id))
            # If it's a string (tool name), keep it
            elif isinstance(tool_id, str):
                tool_names.append(tool_id)
                logger.info(f"Tool already has name: {tool_id}")
            else:
                logger.warning(f"Unknown tool type: {type(tool_id)} - {tool_id}")
                tool_names.append(str(tool_id))

        return tool_names

    async def _agent_to_dict(self, agent) -> Dict[str, Any]:
        """Convert agent model to dictionary"""
        # Convert tool IDs to tool names
        tool_names = await self._convert_tool_ids_to_names(agent.tools or [])

        return {
            'id': str(agent.id),
            'name': agent.name,
            'role': agent.role,
            'goal': agent.goal,
            'backstory': agent.backstory,
            'llm': agent.llm,
            'tools': tool_names,
            'max_iter': agent.max_iter,
            'verbose': agent.verbose,
            'allow_delegation': agent.allow_delegation,
        }

    async def _task_to_dict(self, task) -> Dict[str, Any]:
        """Convert task model to dictionary"""
        # Convert tool IDs to tool names
        tool_names = await self._convert_tool_ids_to_names(task.tools or [])

        return {
            'id': str(task.id),
            'name': task.name,
            'description': task.description,
            'expected_output': task.expected_output,
            'agent_id': task.agent_id,
            'tools': tool_names,
            'async_execution': task.async_execution,
            'context': task.context or [],
        }

    def _generate_usage_example(self, endpoint_url: str, endpoint_name: str) -> str:
        """Generate usage example for the endpoint"""
        return f'''# Invoke the deployed crew endpoint

import requests
import os

# Get Databricks token
token = os.getenv("DATABRICKS_TOKEN")

# Define endpoint URL
endpoint_url = "{endpoint_url}"

# Prepare request
headers = {{
    "Authorization": f"Bearer {{token}}",
    "Content-Type": "application/json"
}}

data = {{
    "inputs": {{
        "topic": "Artificial Intelligence trends in 2025"
    }}
}}

# Invoke endpoint
response = requests.post(
    endpoint_url,
    headers=headers,
    json=data
)

# Print result
print("Status Code:", response.status_code)
print("Result:", response.json())

# Alternative: Using Databricks SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

response = w.serving_endpoints.query(
    name="{endpoint_name}",
    inputs={{"topic": "Artificial Intelligence trends in 2025"}}
)

print("Result:", response)
'''


class CrewAIModelWrapper(mlflow.pyfunc.PythonModel):
    """MLflow PyFunc wrapper for CrewAI crews"""

    def __init__(self, crew_config: Dict[str, Any]):
        """
        Initialize wrapper with crew configuration

        Args:
            crew_config: Crew configuration dictionary
        """
        self.crew_config = crew_config

    def load_context(self, context):
        """
        Load model context

        Args:
            context: MLflow context
        """
        import json

        # Load crew configuration from artifacts
        crew_config_path = context.artifacts.get("crew_config")
        if crew_config_path:
            with open(crew_config_path, 'r') as f:
                self.crew_config = json.load(f)

    def predict(self, context, model_input):
        """
        Predict method for MLflow model

        Args:
            context: MLflow context
            model_input: Input DataFrame

        Returns:
            DataFrame with results
        """
        import pandas as pd
        import json
        import time
        from crewai import Agent, Crew, Task, Process

        results = []

        for _, row in model_input.iterrows():
            start_time = time.time()

            try:
                # Parse inputs
                inputs_str = row.get('inputs', '{}')
                if isinstance(inputs_str, str):
                    inputs = json.loads(inputs_str)
                else:
                    inputs = inputs_str

                # Create crew from configuration
                crew = self._create_crew_from_config()

                # Execute crew
                result = crew.kickoff(inputs=inputs)

                # Calculate execution time
                execution_time = time.time() - start_time

                results.append({
                    'result': str(result),
                    'execution_time': execution_time,
                    'status': 'success'
                })

            except Exception as e:
                execution_time = time.time() - start_time
                results.append({
                    'result': None,
                    'execution_time': execution_time,
                    'status': 'failed',
                    'error': str(e)
                })

        return pd.DataFrame(results)

    def _create_crew_from_config(self) -> 'Crew':
        """
        Create CrewAI crew from configuration

        Returns:
            Configured Crew instance
        """
        from crewai import Agent, Crew, Task, Process

        # Create agents
        agents = []
        for agent_config in self.crew_config.get('agents', []):
            agent = Agent(
                role=agent_config['role'],
                goal=agent_config['goal'],
                backstory=agent_config['backstory'],
                llm=agent_config.get('llm', 'databricks-llama-4-maverick'),
                max_iter=agent_config.get('max_iter', 25),
                verbose=agent_config.get('verbose', False),
                allow_delegation=agent_config.get('allow_delegation', False),
            )
            agents.append(agent)

        # Create tasks
        tasks = []
        for task_config in self.crew_config.get('tasks', []):
            # Find agent by ID
            agent_id = task_config.get('agent_id')
            agent = None
            for idx, a_config in enumerate(self.crew_config.get('agents', [])):
                if a_config.get('id') == agent_id:
                    agent = agents[idx]
                    break

            task = Task(
                description=task_config['description'],
                expected_output=task_config['expected_output'],
                agent=agent,
                async_execution=task_config.get('async_execution', False),
            )
            tasks.append(task)

        # Create crew
        crew = Crew(
            agents=agents,
            tasks=tasks,
            process=Process.sequential,
            verbose=True
        )

        return crew
