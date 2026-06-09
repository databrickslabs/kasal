from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text

from src.db.base import Base


class UIConfig(Base):
    """
    Per-workspace "Predefined UI" configuration.

    Controls whether crews in a workspace produce structured, design-system UI
    (rendered consistently in the chat preview) instead of arbitrary HTML. When
    disabled, crews behave normally. The structured-UI format conforms to the
    A2UI protocol (see THIRD_PARTY_NOTICES); naming here is intentionally
    UI-centric rather than protocol-specific.
    """

    __tablename__ = "ui_config"

    id = Column(Integer, primary_key=True)

    # Master switch — ON by default. Output formatting is handled by the
    # UI-document emission, so workspaces render through the design-system UI
    # renderer unless an admin explicitly turns this off.
    enabled = Column(Boolean, default=True, nullable=False)

    # Which component catalog agents may use: "minimal" | "basic" | "custom".
    catalog_type = Column(String(50), default="minimal", nullable=False)
    # Custom catalog JSON (only used when catalog_type == "custom").
    catalog_json = Column(Text, nullable=True)
    # Renderer style overrides (accent color, density, theme) as JSON.
    style_json = Column(Text, nullable=True)

    # Multi-tenant fields
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    created_by_email = Column(String(255), index=True, nullable=True)  # Audit

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
