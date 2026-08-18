from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class EngineConfigBase(BaseModel):
    """Base schema with common engine configuration attributes."""

    engine_name: str = Field(..., description="Name of the engine (e.g., 'kasal')")
    engine_type: str = Field(
        ..., description="Type of engine (e.g., 'workflow', 'ai', 'processing')"
    )
    config_key: str = Field(..., description="Configuration key (e.g., 'flow_enabled')")
    config_value: str = Field(
        ..., description="Configuration value (JSON string or simple value)"
    )
    enabled: Optional[bool] = Field(
        True, description="Whether the configuration is enabled"
    )
    description: Optional[str] = Field(
        None, description="Description of the configuration"
    )


class EngineConfigCreate(EngineConfigBase):
    """Schema for creating a new engine configuration."""

    pass


class EngineConfigUpdate(BaseModel):
    """Schema for updating an existing engine configuration."""

    engine_type: Optional[str] = Field(None, description="Type of engine")
    config_key: Optional[str] = Field(None, description="Configuration key")
    config_value: Optional[str] = Field(None, description="Configuration value")
    enabled: Optional[bool] = Field(
        None, description="Whether the configuration is enabled"
    )
    description: Optional[str] = Field(
        None, description="Description of the configuration"
    )


class EngineConfigResponse(EngineConfigBase):
    """Schema for engine configuration responses."""

    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class EngineConfigToggleUpdate(BaseModel):
    """Schema for toggling engine configuration enabled status."""

    enabled: bool = Field(..., description="New enabled status")


class EngineConfigValueUpdate(BaseModel):
    """Schema for updating engine configuration value."""

    config_value: str = Field(..., description="New configuration value")


class EngineConfigListResponse(BaseModel):
    """Schema for list of engine configurations."""

    configs: List[EngineConfigResponse]
    count: int


class KasalFlowConfigUpdate(BaseModel):
    """Schema for updating CrewAI flow configuration."""

    flow_enabled: bool = Field(..., description="Whether flow feature is enabled")


class OtelAppTelemetryConfigUpdate(BaseModel):
    """Schema for updating OTel App Telemetry configuration."""

    enabled: Optional[bool] = Field(
        None, description="Whether OTel App Telemetry is enabled"
    )
    log_level: Optional[str] = Field(
        None,
        description="OTel log export level (DEBUG, INFO, WARNING, ERROR)",
        pattern="^(DEBUG|INFO|WARNING|ERROR)$",
    )


class HarnessDescription(BaseModel):
    """One harness, and whether it can actually run here."""

    name: str = Field(..., description="Harness identifier ('kasal' or 'crewai')")
    label: Optional[str] = Field(None, description="Display name")
    version: Optional[str] = Field(None, description="The runtime's own version")
    available: bool = Field(..., description="Whether this harness can run here")
    unavailable_reason: Optional[str] = Field(
        None,
        description=(
            "Why it cannot, in terms an operator can act on. The UI shows this "
            "beside a disabled option — a greyed-out engine with no reason is "
            "the thing people open a ticket about."
        ),
    )
    capabilities: List[str] = Field(
        default_factory=list,
        description=(
            "What this engine supports (checkpoint_resume, tool_approval, "
            "flow, …). The UI disables what the selected engine cannot do "
            "instead of offering a button that fails."
        ),
    )


class HarnessResponse(BaseModel):
    """The default harness, alongside every harness's availability."""

    harness: str = Field(
        ...,
        description=(
            "The harness new runs DEFAULT to. A run may name its own; this is "
            "what applies when it does not — scheduled runs and API-triggered "
            "runs have no picker."
        ),
    )
    harnesses: List[HarnessDescription] = Field(default_factory=list)


class HarnessUpdate(BaseModel):
    """Change the harness runs default to."""

    harness: str = Field(..., description="'kasal' or 'crewai'")
