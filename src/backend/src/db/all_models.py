"""
Collection of all database models for easy import.
"""

# Import base for direct access
from src.db.base import Base

# Import all models to register them with SQLAlchemy
from src.models.agent import Agent
from src.models.api_key import ApiKey

# Billing models
from src.models.billing import BillingAlert, BillingPeriod, LLMUsageBilling
from src.models.chat_history import ChatHistory

# Conversion models
from src.models.conversion import (
    ConversionHistory,
    ConversionJob,
    SavedConverterConfiguration,
)
from src.models.crew import Crew, Plan

# Database configuration models
from src.models.database_config import LakebaseConfig
from src.models.databricks_config import DatabricksConfig

# Documentation models
from src.models.documentation_embedding import (
    DocumentationEmbedding,
    KnowledgeEmbedding,
)
from src.models.engine_config import EngineConfig
from src.models.execution_history import ErrorTrace, ExecutionHistory, TaskStatus
from src.models.execution_logs import ExecutionLog
from src.models.execution_trace import ExecutionTrace
from src.models.flow import Flow
from src.models.flow_execution import FlowExecution, FlowNodeExecution

# Multi-group models (formerly multi-tenant)
from src.models.group import Group, GroupUser
from src.models.group_tool import GroupTool

# HITL (Human-in-the-Loop) models
from src.models.hitl_approval import HITLApproval, HITLWebhook
from src.models.initialization_status import InitializationStatus
from src.models.log import LLMLog
from src.models.mcp_server import MCPServer
from src.models.mcp_settings import MCPSettings

# Memory backend models
from src.models.memory_backend import MemoryBackend
from src.models.model_config import ModelConfig

# PowerBI models
from src.models.powerbi_context_config import (
    PowerBIBusinessMapping,
    PowerBIFieldSynonym,
)
from src.models.powerbi_semantic_model_cache import PowerBISemanticModelCache

# Prompt optimization models
from src.models.prompt_optimization_run import PromptOptimizationRun
from src.models.schedule import Schedule
from src.models.schema import Schema
from src.models.task import Task
from src.models.template import PromptTemplate
from src.models.tool import Tool

# User models (simplified auth)
from src.models.user import User

# Workflow reuse — executed crews kept as reusable recipes, plus the ledger
# that measures whether reusing them actually helps
from src.models.workflow_recipe import WorkflowRecipe
from src.models.workflow_recipe_trial import WorkflowRecipeTrial

# Add additional models here as your application grows
# from src.models.order import Order

# This ensures all models are registered with SQLAlchemy metadata
__all__ = [
    "Base",
    "Agent",
    "Task",
    "ExecutionHistory",
    "TaskStatus",
    "ErrorTrace",
    "Tool",
    "GroupTool",
    "LLMLog",
    "ModelConfig",
    "DatabricksConfig",
    "InitializationStatus",
    "PromptTemplate",
    "ExecutionTrace",
    "Crew",
    "Plan",
    "Flow",
    "FlowExecution",
    "FlowNodeExecution",
    "Schedule",
    "ApiKey",
    "Schema",
    "ExecutionLog",
    "EngineConfig",
    "MCPServer",
    "MCPSettings",
    # Multi-group models
    "Group",
    "GroupUser",
    "ChatHistory",
    # User models (simplified auth)
    "User",
    # Billing models
    "LLMUsageBilling",
    "BillingPeriod",
    "BillingAlert",
    # Documentation models
    "DocumentationEmbedding",
    # Database configuration models
    "LakebaseConfig",
    # Conversion models
    "ConversionHistory",
    "ConversionJob",
    "SavedConverterConfiguration",
    # HITL models
    "HITLApproval",
    "HITLWebhook",
    # PowerBI models
    "PowerBIBusinessMapping",
    "PowerBIFieldSynonym",
    "PowerBISemanticModelCache",
    # Memory backend models
    "MemoryBackend",
    # Prompt optimization models
    "PromptOptimizationRun",
    # Workflow reuse models
    "WorkflowRecipe",
    "WorkflowRecipeTrial",
]
