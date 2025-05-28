"""
Pydantic schemas for execution-related operations.

This module defines schemas used for validating and structuring data
in execution-related API requests and responses.
"""

from typing import Dict, Any, Optional, List, Union
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum
from datetime import datetime
import json
import time

from src.models.execution_status import ExecutionStatus


class ExecutionNameGenerationRequest(BaseModel):
    """Request schema for generating an execution name."""
    agents_yaml: Dict[str, Dict[str, Any]] = Field(..., description="Agent configuration in YAML format")
    tasks_yaml: Dict[str, Dict[str, Any]] = Field(..., description="Task configuration in YAML format") 
    model: Optional[str] = Field(None, description="LLM model to use for name generation")


class ExecutionNameGenerationResponse(BaseModel):
    """Response schema for execution name generation."""
    name: str = Field(..., description="Generated execution name")


class CrewConfig(BaseModel):
    """Configuration for a crew execution."""
    agents_yaml: Dict[str, Dict[str, Any]] = Field(..., description="Agent configuration in YAML format")
    tasks_yaml: Dict[str, Dict[str, Any]] = Field(..., description="Task configuration in YAML format")
    inputs: Dict[str, Any] = Field(..., description="Input values for the execution")
    planning: bool = Field(False, description="Whether to enable planning")
    reasoning: bool = Field(False, description="Whether to enable reasoning")
    model: Optional[str] = Field(None, description="LLM model to use")
    llm_provider: Optional[str] = Field(None, description="LLM provider to use (openai, anthropic, etc)")
    execution_type: Optional[str] = Field("crew", description="Type of execution (crew or flow)")
    schema_detection_enabled: Optional[bool] = Field(True, description="Whether schema detection is enabled")

    @property
    def tasks(self) -> Dict:
        """Ensure tasks are properly structured dictionaries"""
        if not isinstance(self.tasks_yaml, dict):
            raise ValueError("Tasks configuration must be a dictionary")
        
        tasks = {}
        for key, value in self.tasks_yaml.items():
            if isinstance(value, str):
                try:
                    tasks[key] = json.loads(value)
                except json.JSONDecodeError:
                    raise ValueError(f"Task configuration for {key} is not a valid JSON string")
            else:
                tasks[key] = value
        return tasks

    @property
    def agents(self) -> Dict:
        """Ensure agents are properly structured dictionaries"""
        if not isinstance(self.agents_yaml, dict):
            raise ValueError("Agents configuration must be a dictionary")
        
        agents = {}
        for key, value in self.agents_yaml.items():
            if isinstance(value, str):
                try:
                    agents[key] = json.loads(value)
                except json.JSONDecodeError:
                    raise ValueError(f"Agent configuration for {key} is not a valid JSON string")
            else:
                agents[key] = value
        return agents

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ExecutionBase(BaseModel):
    """Base model with common execution fields"""
    execution_id: str = Field(..., description="Unique identifier for the execution")
    status: str = Field(..., description="Current status of the execution")
    created_at: datetime = Field(..., description="When the execution was created")
    result: Optional[Dict[str, Any]] = Field(None, description="Result data from execution")
    error: Optional[str] = Field(None, description="Error message if execution failed")
    run_name: Optional[str] = Field(None, description="Descriptive name for the execution")


class ExecutionResponse(BaseModel):
    """Complete execution response model with all fields"""
    execution_id: str = Field(..., description="Unique identifier for the execution")
    status: str = Field(..., description="Current status of the execution")
    created_at: datetime = Field(..., description="When the execution was created")
    result: Optional[Dict[str, Any]] = Field(None, description="Result data from execution")
    error: Optional[str] = Field(None, description="Error message if execution failed")
    run_name: Optional[str] = Field(None, description="Descriptive name for the execution")
    # Additional fields
    id: Optional[int] = Field(None, description="Database ID of the execution")
    flow_id: Optional[int] = Field(None, description="ID of the flow used (if execution_type is flow)")
    crew_id: Optional[int] = Field(None, description="ID of the crew used (if execution_type is crew)")
    execution_key: Optional[str] = Field(None, description="Optional external key for the execution")
    started_at: Optional[datetime] = Field(None, description="When the execution started")
    completed_at: Optional[datetime] = Field(None, description="When the execution completed")
    updated_at: Optional[datetime] = Field(None, description="When the execution was last updated")
    execution_inputs: Optional[Dict[str, Any]] = Field(None, description="Input data for the execution")
    execution_outputs: Optional[Dict[str, Any]] = Field(None, description="Output data from the execution")
    execution_config: Optional[Dict[str, Any]] = Field(None, description="Configuration used for the execution")

    model_config = ConfigDict(from_attributes=True)


class ExecutionCreateResponse(BaseModel):
    """Simple response for execution creation."""
    execution_id: str = Field(..., description="Unique identifier for the created execution")
    status: str = Field(..., description="Initial status of the execution")
    run_name: Optional[str] = Field(None, description="Descriptive name for the execution") 


class FlowConfig(BaseModel):
    """Configuration for a flow execution."""
    id: Optional[str] = Field(None, description="Flow configuration ID")
    name: str = Field(..., description="Name of the flow")
    listeners: List[Dict[str, Any]] = Field(default_factory=list, description="List of flow listeners")
    actions: List[Dict[str, Any]] = Field(default_factory=list, description="List of flow actions")
    startingPoints: List[Dict[str, Any]] = Field(default_factory=list, description="List of flow starting points")
    type: Optional[str] = Field(None, description="Type of flow")
    crewName: Optional[str] = Field(None, description="Name of the associated crew")
    crewRef: Optional[str] = Field(None, description="Reference to the associated crew")
    model: Optional[str] = Field(None, description="LLM model to use")
    llm_provider: Optional[str] = Field(None, description="LLM provider to use (openai, anthropic, etc)")
    execution_type: str = Field("flow", description="Type of execution (must be 'flow')")
    tools: Optional[List[Dict[str, Any]]] = Field(default_factory=list, description="List of tools to make available")
    max_rpm: Optional[int] = Field(10, description="Maximum requests per minute")
    output_dir: Optional[str] = Field(None, description="Directory for flow execution outputs")
    planning: Optional[bool] = Field(False, description="Whether to enable planning")
    planning_llm: Optional[str] = Field(None, description="LLM to use for planning if different from main model")
    reasoning: Optional[bool] = Field(False, description="Whether to enable reasoning")
    reasoning_llm: Optional[str] = Field(None, description="LLM to use for reasoning if different from main model")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def normalize(self) -> Dict[str, Any]:
        """
        Convert the flow configuration to normalized dictionary format.
        
        Returns:
            Dict[str, Any]: Normalized flow configuration
        """
        return {
            "id": self.id or f"flow-{int(time.time() * 1000)}",
            "name": self.name,
            "listeners": self.listeners,
            "actions": self.actions,
            "startingPoints": self.startingPoints,
            "type": self.type,
            "crewName": self.crewName,
            "crewRef": self.crewRef,
            "model": self.model,
            "llm_provider": self.llm_provider,
            "tools": self.tools,
            "max_rpm": self.max_rpm,
            "output_dir": self.output_dir,
            "planning": self.planning,
            "planning_llm": self.planning_llm,
            "reasoning": self.reasoning,
            "reasoning_llm": self.reasoning_llm
        }