from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class UIConfigBase(BaseModel):
    """Shared fields for the per-workspace Predefined UI configuration."""
    # Enabled by default: output formatting is owned by the UI-document emission
    # (apply_ui_emission), so every workspace renders through the design-system
    # UI renderer unless an admin explicitly disables it.
    enabled: bool = True
    catalog_type: str = "minimal"  # "minimal" | "basic" | "custom"
    catalog_json: Optional[str] = None  # used only when catalog_type == "custom"
    style_json: Optional[str] = None  # renderer style overrides (accent/density/theme)


class UIConfigUpdate(UIConfigBase):
    """Schema for updating the Predefined UI configuration (PUT body)."""
    pass


class UIConfigResponse(UIConfigBase):
    """Full Predefined UI configuration as returned to clients."""
    id: Optional[int] = None
    group_id: Optional[str] = None
    created_by_email: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)
