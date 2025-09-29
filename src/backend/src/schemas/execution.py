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
    """Pydantic schema for CrewAI crew execution configuration.
    
    This schema defines the complete configuration structure for executing
    a CrewAI crew, including agents, tasks, inputs, and execution settings.
    It handles validation and normalization of YAML-based configurations.
    
    Attributes:
        agents_yaml: Dictionary of agent configurations in YAML format.
            Each agent defines role, goal, backstory, and tools.
        tasks_yaml: Dictionary of task configurations in YAML format.
            Each task defines description, expected output, and agent assignment.
        inputs: Input values to be passed to the crew execution.
        planning: Enable planning mode for strategic task orchestration.
        reasoning: Enable reasoning mode for enhanced decision-making.
        model: LLM model identifier (e.g., "gpt-4", "claude-3").
        llm_provider: Provider name (openai, anthropic, etc.).
        execution_type: Type of execution ("crew" or "flow").
        schema_detection_enabled: Enable automatic schema detection for outputs.
    
    Properties:
        tasks: Returns normalized task configurations as dictionaries.
        agents: Returns normalized agent configurations as dictionaries.
    
    Example:
        >>> config = CrewConfig(
        ...     agents_yaml={"researcher": {"role": "Research Agent"}},
        ...     tasks_yaml={"research": {"description": "Research topic"}},
        ...     inputs={"topic": "AI"},
        ...     model="gpt-4"
        ... )
    
    Note:
        The schema automatically converts JSON strings to dictionaries
        in the tasks and agents properties for flexibility.
    """
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

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


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
    group_email: Optional[str] = Field(None, description="Email of the user who submitted the execution")
    group_id: Optional[str] = Field(None, description="Group ID the execution belongs to")
    inputs: Optional[Dict[str, Any]] = Field(None, description="Complete inputs including agents_yaml and tasks_yaml")
    agents_yaml: Optional[str] = Field(None, description="Agents configuration as JSON string")
    tasks_yaml: Optional[str] = Field(None, description="Tasks configuration as JSON string")

    # MLflow integration fields
    mlflow_trace_id: Optional[str] = Field(None, description="MLflow trace ID for evaluation linking")
    mlflow_experiment_name: Optional[str] = Field(None, description="MLflow experiment name for reference")
    mlflow_evaluation_run_id: Optional[str] = Field(None, description="MLflow evaluation run ID")

    model_config = ConfigDict(from_attributes=True)


class ExecutionCreateResponse(BaseModel):
    """Simple response for execution creation."""
    execution_id: str = Field(..., description="Unique identifier for the created execution")
    status: str = Field(..., description="Initial status of the execution")
    run_name: Optional[str] = Field(None, description="Descriptive name for the execution") 


class FlowConfig(BaseModel):
    """Pydantic schema for CrewAI flow orchestration configuration.
    
    This schema defines the configuration for complex multi-crew flows,
    enabling orchestration of multiple crews with event-driven execution,
    conditional logic, and sophisticated control flow patterns.
    
    A flow represents a higher-level abstraction over crews, allowing:
    - Event-driven execution with listeners
    - Conditional branching and decision points
    - Multi-crew coordination
    - State management across crews
    
    Attributes:
        id: Optional unique identifier for the flow configuration.
        name: Human-readable name for the flow.
        listeners: Event listeners that trigger flow actions.
        actions: Sequence of actions to execute in the flow.
        startingPoints: Entry points for flow execution.
        type: Classification of flow type (e.g., "sequential", "parallel").
        crewName: Name of the primary crew associated with the flow.
        crewRef: Reference identifier for crew lookup.
        model: LLM model to use for flow orchestration.
        llm_provider: Provider for the LLM (openai, anthropic, etc.).
        execution_type: Must be "flow" for flow executions.
        tools: List of tools available to all crews in the flow.
        max_rpm: Rate limiting for API calls (requests per minute).
        output_dir: Directory for storing flow outputs and artifacts.
        planning: Enable strategic planning across the flow.
        planning_llm: Specific LLM for planning operations.
        reasoning: Enable reasoning for decision-making.
        reasoning_llm: Specific LLM for reasoning operations.
    
    Methods:
        normalize: Convert to normalized dictionary format for execution.
    
    Example:
        >>> flow = FlowConfig(
        ...     name="Research and Analysis Flow",
        ...     listeners=[{"type": "start", "action": "begin_research"}],
        ...     actions=[{"type": "crew", "name": "research_crew"}],
        ...     startingPoints=[{"id": "start"}]
        ... )
    """
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


class StopType(str, Enum):
    """Enumeration of execution stop operation types.
    
    This enum defines the different modes for stopping an in-progress execution,
    allowing for both graceful termination with cleanup and forced immediate
    termination for unresponsive executions.
    
    Values:
        GRACEFUL: Attempt clean shutdown, allowing tasks to complete current work
            and save partial results. This is the recommended approach.
        FORCE: Immediate termination without cleanup. Use only when graceful
            stop fails or execution is completely unresponsive.
    
    Example:
        >>> request = StopExecutionRequest(
        ...     stop_type=StopType.GRACEFUL,
        ...     reason="User requested cancellation"
        ... )
    """
    GRACEFUL = "graceful"
    FORCE = "force"


class StopExecutionRequest(BaseModel):
    """Request schema for stopping an in-progress execution.
    
    This schema defines the parameters for requesting the termination of a
    running execution, supporting both graceful and forced stop operations
    with options for preserving partial results.
    
    The stop execution feature enables users to:
    - Cancel long-running operations that are no longer needed
    - Terminate stuck or unresponsive executions
    - Save partial progress before termination
    - Provide audit trail through stop reasons
    
    Attributes:
        stop_type: Method of stopping (GRACEFUL or FORCE). Defaults to GRACEFUL
            for clean shutdown with resource cleanup.
        reason: Optional human-readable explanation for the stop request.
            Useful for audit trails and debugging.
        preserve_partial_results: Whether to save any completed work before
            stopping. Defaults to True to prevent data loss.
    
    Example:
        >>> # Graceful stop with partial results
        >>> request = StopExecutionRequest(
        ...     stop_type=StopType.GRACEFUL,
        ...     reason="Requirements changed - no longer needed",
        ...     preserve_partial_results=True
        ... )
        
        >>> # Force stop for unresponsive execution
        >>> emergency_stop = StopExecutionRequest(
        ...     stop_type=StopType.FORCE,
        ...     reason="Execution unresponsive for 30 minutes",
        ...     preserve_partial_results=False
        ... )
    
    Note:
        Graceful stops may take time to complete as they allow current
        tasks to finish. Force stops are immediate but may leave resources
        in an inconsistent state.
    """
    stop_type: StopType = Field(StopType.GRACEFUL, description="Type of stop operation (graceful or force)")
    reason: Optional[str] = Field(None, description="Reason for stopping the execution")
    preserve_partial_results: bool = Field(True, description="Whether to preserve partial results")


class StopExecutionResponse(BaseModel):
    """Response schema for stop execution request.
    
    This schema defines the response returned when an execution stop request
    is processed, providing confirmation of the stop operation and any
    preserved partial results.
    
    The response includes:
    - Confirmation that the stop request was received and processed
    - Current execution status after stop request
    - Any partial results that were preserved
    - Human-readable status message
    
    Attributes:
        execution_id: Unique identifier of the execution that was stopped.
            Used for tracking and correlation with the original execution.
        status: Current status after stop request (e.g., "stopping", "stopped",
            "failed_to_stop"). Indicates success of stop operation.
        message: Human-readable message describing the stop operation result.
            May include details about cleanup or errors encountered.
        partial_results: Dictionary containing any results completed before
            the stop. None if preserve_partial_results was False or no
            results were available.
    
    Example:
        >>> response = StopExecutionResponse(
        ...     execution_id="exec_123",
        ...     status="stopped",
        ...     message="Execution stopped gracefully. 3 of 5 tasks completed.",
        ...     partial_results={
        ...         "completed_tasks": ["research", "analysis", "summary"],
        ...         "data": {"research_findings": [...]}
        ...     }
        ... )
    
    Note:
        The partial_results field structure depends on the execution type
        and what work was completed before stopping.
    """
    execution_id: str = Field(..., description="ID of the execution being stopped")
    status: str = Field(..., description="Current status of the execution")
    message: str = Field(..., description="Status message")
    partial_results: Optional[Dict[str, Any]] = Field(None, description="Partial results if available")


class ExecutionStatusResponse(BaseModel):
    """Response schema for execution status query.
    
    This schema provides comprehensive status information about an execution,
    including details about any ongoing or completed stop operations. It's
    used for monitoring execution progress and understanding the current
    state of potentially stopped executions.
    
    The response enables clients to:
    - Monitor execution lifecycle and current state
    - Track stop operation progress
    - Access stop operation metadata for debugging
    - Monitor task-level progress within the execution
    
    Attributes:
        execution_id: Unique identifier of the execution being queried.
        status: Current execution status (pending, running, completed, failed,
            stopping, stopped). Reflects the overall execution state.
        is_stopping: Flag indicating if a stop operation is currently in progress.
            True during graceful shutdown, False otherwise.
        stopped_at: Timestamp when the execution was stopped. None if execution
            completed normally or is still running.
        stop_reason: Human-readable reason provided when stop was requested.
            Useful for audit trails and debugging stopped executions.
        progress: Dictionary containing detailed progress information such as
            completed tasks, current task, completion percentage, etc.
    
    Example:
        >>> # Running execution
        >>> response = ExecutionStatusResponse(
        ...     execution_id="exec_123",
        ...     status="running",
        ...     is_stopping=False,
        ...     progress={
        ...         "total_tasks": 5,
        ...         "completed_tasks": 2,
        ...         "current_task": "data_analysis",
        ...         "percentage": 40
        ...     }
        ... )
        
        >>> # Stopped execution
        >>> stopped_response = ExecutionStatusResponse(
        ...     execution_id="exec_456",
        ...     status="stopped",
        ...     is_stopping=False,
        ...     stopped_at=datetime.utcnow(),
        ...     stop_reason="User requested cancellation",
        ...     progress={"completed_tasks": 3, "total_tasks": 7}
        ... )
    
    Note:
        The progress field structure may vary based on execution type
        (crew vs flow) and implementation details.
    """
    execution_id: str = Field(..., description="ID of the execution")
    status: str = Field(..., description="Current status of the execution")
    is_stopping: bool = Field(False, description="Whether execution is currently being stopped")
    stopped_at: Optional[datetime] = Field(None, description="When the execution was stopped")
    stop_reason: Optional[str] = Field(None, description="Reason for stopping")
    progress: Optional[Dict[str, Any]] = Field(None, description="Current progress information")