"""
Schemas for dispatcher service.

This module defines the request and response schemas for the dispatcher service
that determines user intent from natural language input.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Literal, Dict, Any, List
from enum import Enum


class IntentType(str, Enum):
    """Enumeration of possible intent types."""

    GENERATE_AGENT = "generate_agent"
    GENERATE_TASK = "generate_task"
    GENERATE_CREW = "generate_crew"
    EXECUTE_CREW = "execute_crew"
    CONFIGURE_CREW = "configure_crew"
    CATALOG_LIST = "catalog_list"
    CATALOG_LOAD = "catalog_load"
    CATALOG_SAVE = "catalog_save"
    CATALOG_SCHEDULE = "catalog_schedule"
    CATALOG_HELP = "catalog_help"
    FLOW_LIST = "flow_list"
    FLOW_LOAD = "flow_load"
    FLOW_SAVE = "flow_save"
    EXECUTE_FLOW = "execute_flow"
    CATALOG_DELETE = "catalog_delete"
    FLOW_DELETE = "flow_delete"
    UNKNOWN = "unknown"


class DispatcherRequest(BaseModel):
    """Request schema for dispatcher service."""

    message: str = Field(
        ...,
        min_length=1,
        max_length=10000,
        description="Natural language message from user",
    )
    model: Optional[str] = Field(
        None, description="LLM model to use for intent detection"
    )
    tools: Optional[List[str]] = Field(
        default_factory=list, description="Available tools for generation"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "message": "Create an agent that can analyze financial data",
                "model": "gpt-4",
                "tools": ["web_search", "calculator"],
            }
        }
    )


class DispatcherResponse(BaseModel):
    """Response schema for dispatcher service."""

    intent: IntentType = Field(..., description="Detected intent type")
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Confidence score of intent detection"
    )
    extracted_info: Dict[str, Any] = Field(
        default_factory=dict, description="Extracted information relevant to the intent"
    )
    suggested_prompt: Optional[str] = Field(
        None, description="Enhanced prompt for the specific generation service"
    )
    source: Optional[str] = Field(
        None,
        description="Origin of the intent detection result: llm, semantic_fallback, cache, circuit_breaker_fallback",
    )
    suggested_tools: List[str] = Field(
        default_factory=list,
        description="Tool titles suggested by the LLM based on the user's intent",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "intent": "generate_agent",
                "confidence": 0.95,
                "extracted_info": {
                    "agent_type": "financial_analyst",
                    "capabilities": ["data analysis", "report generation"],
                },
                "suggested_prompt": "Create a financial analyst agent that can analyze market data and generate reports",
                "source": "llm",
                "suggested_tools": ["SerperDevTool", "ScrapeWebsiteTool"],
            }
        }
    )
