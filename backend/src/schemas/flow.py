from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict


# Node data models
class Position(BaseModel):
    """Position of a node in the flow diagram."""
    x: float
    y: float


class Style(BaseModel):
    """Visual styling for a node."""
    background: Optional[str] = None
    border: Optional[str] = None
    borderRadius: Optional[str] = None
    padding: Optional[str] = None
    boxShadow: Optional[str] = None


class NodeData(BaseModel):
    """Data associated with a node in the flow diagram."""
    label: str
    crewName: Optional[str] = None
    type: Optional[str] = None
    decorator: Optional[str] = None
    listenTo: Optional[List[str]] = None
    routerCondition: Optional[str] = None
    stateType: Optional[str] = None
    stateDefinition: Optional[str] = None
    listener: Optional[Dict[str, Any]] = None


class Node(BaseModel):
    """A node in the flow diagram."""
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


class Edge(BaseModel):
    """An edge in the flow diagram representing a connection between nodes."""
    source: str
    target: str
    id: str
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None


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