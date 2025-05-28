from fastapi import APIRouter

from src.api.agents_router import router as agents_router
from src.api.crews_router import router as crews_router
from src.api.databricks_router import router as databricks_router
from src.api.db_management_router import router as db_management_router
from src.api.flows_router import router as flows_router
from src.api.healthcheck_router import router as healthcheck_router
from src.api.logs_router import router as logs_router
from src.api.models_router import router as models_router
from src.api.databricks_secrets_router import router as databricks_secrets_router
from src.api.api_keys_router import router as api_keys_router
from src.api.tasks_router import router as tasks_router
from src.api.templates_router import router as templates_router
from src.api.schemas_router import router as schemas_router
from src.api.tools_router import router as tools_router
from src.api.upload_router import router as upload_router
from src.api.uc_tools_router import router as uc_tools_router
from src.api.task_tracking_router import router as task_tracking_router
from src.api.scheduler_router import router as scheduler_router
from src.api.memory_router import router as memory_router
from src.api.uc_functions_router import router as uc_functions_router
from src.api.agent_generation_router import router as agent_generation_router
from src.api.connections_router import router as connections_router
from src.api.crew_generation_router import router as crew_generation_router
from src.api.task_generation_router import router as task_generation_router
from src.api.template_generation_router import router as template_generation_router
from src.api.execution_logs_router import runs_router
from src.api.executions_router import router as executions_router
from src.api.execution_history_router import router as execution_history_router
from src.api.execution_trace_router import router as execution_trace_router
from src.api.flow_execution_router import router as flow_execution_router
from src.api.mcp_router import router as mcp_router
from src.api.dispatcher_router import router as dispatcher_router
from src.api.engine_config_router import router as engine_config_router
# User management routers
from src.api.auth_router import router as auth_router
from src.api.users_router import router as users_router
from src.api.roles_router import router as roles_router
from src.api.identity_providers_router import router as identity_providers_router

# Create the main API router
api_router = APIRouter()

# Include all the sub-routers
api_router.include_router(agents_router)
api_router.include_router(crews_router)
api_router.include_router(databricks_router)
api_router.include_router(db_management_router)
api_router.include_router(flows_router)
api_router.include_router(healthcheck_router)
api_router.include_router(logs_router)
api_router.include_router(models_router)
api_router.include_router(databricks_secrets_router)
api_router.include_router(api_keys_router)
api_router.include_router(tasks_router)
api_router.include_router(templates_router)
api_router.include_router(schemas_router)
api_router.include_router(tools_router)
api_router.include_router(upload_router)
api_router.include_router(uc_tools_router)
api_router.include_router(task_tracking_router)
api_router.include_router(scheduler_router)
api_router.include_router(memory_router)
api_router.include_router(uc_functions_router)
api_router.include_router(agent_generation_router)
api_router.include_router(connections_router)
api_router.include_router(crew_generation_router)
api_router.include_router(task_generation_router)
api_router.include_router(template_generation_router)
api_router.include_router(executions_router)
api_router.include_router(execution_history_router)
api_router.include_router(execution_trace_router)
api_router.include_router(flow_execution_router)
api_router.include_router(runs_router)
api_router.include_router(mcp_router)
api_router.include_router(dispatcher_router)
api_router.include_router(engine_config_router)
# Include user management routers
api_router.include_router(auth_router)
api_router.include_router(users_router)
api_router.include_router(roles_router)
api_router.include_router(identity_providers_router)

__all__ = [
    "api_router",
    "agents_router",
    "crews_router",
    "databricks_router",
    "db_management_router",
    "flows_router",
    "healthcheck_router",
    "logs_router",
    "models_router",
    "databricks_secrets_router",
    "api_keys_router",
    "tasks_router",
    "templates_router",
    "schemas_router",
    "tools_router",
    "upload_router",
    "uc_tools_router",
    "task_tracking_router",
    "scheduler_router",
    "memory_router",
    "uc_functions_router",
    "agent_generation_router",
    "connections_router",
    "crew_generation_router",
    "task_generation_router",
    "template_generation_router",
    "executions_router",
    "execution_history_router",
    "execution_trace_router",
    "flow_execution_router",
    "mcp_router",
    "dispatcher_router",
    "engine_config_router",
    # Add user management routers to __all__
    "auth_router",
    "users_router",
    "runs_router",
    "roles_router",
    "identity_providers_router"
]
