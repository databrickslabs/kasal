from datetime import datetime
from typing import List, Optional, Dict, Any, Union, Literal
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict


# Node data models
class Position(BaseModel):
    """Position of a node in the flow diagram."""
    x: float
    y: float


class Style(BaseModel):
    """Visual styling for a node."""
    # CRITICAL: Allow extra fields AND exclude None values from JSON output
    model_config = ConfigDict(
        extra='allow',
        exclude_none=True  # Don't include fields with None values in JSON
    )

    background: Optional[str] = None
    border: Optional[str] = None
    borderRadius: Optional[str] = None
    padding: Optional[str] = None
    boxShadow: Optional[str] = None
    color: Optional[str] = None
    stroke: Optional[str] = None
    strokeWidth: Optional[Union[int, float]] = None


class NodeData(BaseModel):
    """Data associated with a node in the flow diagram."""
    # CRITICAL: Allow extra fields AND exclude None values from JSON output
    model_config = ConfigDict(
        extra='allow',
        exclude_none=True  # Don't include fields with None values in JSON
    )

    label: str
    crewName: Optional[str] = None
    type: Optional[str] = None
    decorator: Optional[str] = None
    listenTo: Optional[List[str]] = None
    routerCondition: Optional[str] = None
    stateType: Optional[str] = None
    stateDefinition: Optional[str] = None
    listener: Optional[Dict[str, Any]] = None
    # Additional common fields (but allow any extra fields via extra='allow')
    allTasks: Optional[List[Dict[str, Any]]] = None
    selectedTasks: Optional[List[Dict[str, Any]]] = None
    order: Optional[int] = None
    crewId: Optional[Union[str, UUID]] = None
    flowConfig: Optional[Dict[str, Any]] = None


class Node(BaseModel):
    """A node in the flow diagram."""
    # CRITICAL: Allow extra fields AND exclude None values from JSON output
    model_config = ConfigDict(
        extra='allow',
        exclude_none=True  # Don't include fields with None values in JSON
    )

    id: str
    type: str
    position: Position
    data: NodeData
    width: Optional[float] = None
    height: Optional[float] = None
    selected: Optional[bool] = None
    positionAbsolute: Optional[Position] = None
    dragging: Optional[bool] = None
    style: Optional[Style] = None
    className: Optional[str] = None
    measured: Optional[Dict[str, Any]] = None


# State management models
class StateWrite(BaseModel):
    """Configuration for writing to flow state."""
    variable: str
    value: Optional[Any] = None
    expression: Optional[str] = None  # Python expression to compute value


class StateOperations(BaseModel):
    """Configuration for state operations during flow transitions."""
    reads: Optional[List[str]] = None  # State variables to read
    writes: Optional[List[StateWrite]] = None  # State variables to write
    condition: Optional[str] = None  # State-based condition for routing


class StateConfig(BaseModel):
    """Configuration for flow state management."""
    enabled: bool = False
    type: Literal['unstructured', 'structured'] = 'unstructured'
    model: Optional[str] = None  # Pydantic model definition for structured state
    initialValues: Optional[Dict[str, Any]] = None


class PersistenceConfig(BaseModel):
    """Configuration for flow persistence."""
    enabled: bool = False
    level: Literal['class', 'method', 'none'] = 'none'
    backend: Literal['sqlite', 'custom'] = 'sqlite'
    path: Optional[str] = None


class Edge(BaseModel):
    """An edge in the flow diagram representing a connection between nodes."""
    # CRITICAL: Allow extra fields AND exclude None values from JSON output
    model_config = ConfigDict(
        extra='allow',
        exclude_none=True  # Don't include fields with None values in JSON
    )

    source: str
    target: str
    id: str
    type: Optional[str] = None
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    style: Optional[Dict[str, Any]] = None
    animated: Optional[bool] = None
    label: Optional[str] = None
    labelStyle: Optional[Dict[str, Any]] = None
    labelBgStyle: Optional[Dict[str, Any]] = None
    className: Optional[str] = None


# Shared properties
class FlowBase(BaseModel):
    """Base Pydantic model for Flows with shared attributes."""
    name: str
    crew_id: Optional[UUID] = None
    nodes: List[Node] = Field(default_factory=list)
    edges: List[Edge] = Field(default_factory=list)
    flow_config: Optional[Dict[str, Any]] = None


# Properties to receive on flow creation
class FlowCreate(FlowBase):
    """Pydantic model for creating a flow."""
    model_config = ConfigDict(from_attributes=True)


# Properties to receive on flow update
class FlowUpdate(BaseModel):
    """Pydantic model for updating a flow."""
    name: str
    nodes: Optional[List[Node]] = None
    edges: Optional[List[Edge]] = None
    flow_config: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


# Properties shared by models stored in DB
class FlowInDBBase(FlowBase):
    """Base Pydantic model for flows in the database, including id and timestamps."""
    id: UUID
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Properties to return to client
class Flow(FlowInDBBase):
    """Pydantic model for returning flows to clients."""
    pass


# Custom response model with string timestamps
class FlowResponse(BaseModel):
    """Pydantic model for Flow response with string timestamps."""
    id: Union[UUID, str]
    name: str
    crew_id: Optional[UUID] = None
    nodes: List[Node]
    edges: List[Edge]
    flow_config: Optional[Dict[str, Any]] = Field(default_factory=dict)
    created_at: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True) 