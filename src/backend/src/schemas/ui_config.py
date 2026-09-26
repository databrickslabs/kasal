from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, field_validator


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
    catalog_type: str = "full"  # "full" | "select" | "minimal" | "custom"
    catalog_json: Optional[str] = None  # used only when catalog_type == "custom"
    # JSON array of component names switched off; only for catalog_type "select".
    disabled_components: Optional[str] = None
    style_json: Optional[str] = None  # renderer style overrides (accent/density/theme)
    # Overrides of the system A2UI defaults, e.g. {"a2ui_compose_retries": 4}.
    # A key left out uses the system default (Configuration → Output design).
    settings_json: Optional[str] = None

    @field_validator("settings_json")
    @classmethod
    def _valid_overrides(cls, value: Optional[str]) -> Optional[str]:
        from src.services.a2ui.settings import validate_overrides

        return validate_overrides(value)


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
    #: The system defaults a workspace override replaces (read-only), so the
    #: Output design form can show "use system default (N)".
    system_defaults: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)
