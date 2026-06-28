from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class UIConfigBase(BaseModel):
    """Shared fields for the per-workspace Predefined UI configuration."""
    # Enabled by default: output formatting is owned by the shared A2UI composer
    # (composed post-execution by a2ui_runner), so every workspace renders through
    # the design-system A2UI renderer unless an admin explicitly disables it.
    enabled: bool = True
    # "full" = the full bundled catalog (presentations/dashboards/charts/quizzes/…).
    # This matches the unconfigured-workspace behavior, so enabling A2UI and saving
    # does NOT silently downgrade to a structure-only catalog. "minimal" is an
    # explicit opt-in that restricts the composer to document/conversation surfaces.
    catalog_type: str = "full"  # "full" | "minimal" | "custom"
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
