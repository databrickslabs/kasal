"""Unit tests for the Predefined UI configuration schemas."""
from src.schemas.ui_config import UIConfigBase, UIConfigUpdate, UIConfigResponse
from src.models.ui_config import UIConfig


def test_base_defaults_are_enabled_minimal():
    # UI is enabled by default — the UI-document emission owns output formatting.
    b = UIConfigBase()
    assert b.enabled is True
    assert b.catalog_type == "minimal"
    assert b.catalog_json is None
    assert b.style_json is None


def test_update_inherits_base_fields():
    u = UIConfigUpdate(enabled=True, catalog_type="custom", catalog_json="{}", style_json="{}")
    assert u.enabled is True
    assert u.catalog_type == "custom"


def test_response_reads_from_model_attributes():
    cfg = UIConfig(id=2, group_id="g", enabled=True, catalog_type="basic", created_by_email="a@b.com")
    r = UIConfigResponse.model_validate(cfg)
    assert r.id == 2
    assert r.group_id == "g"
    assert r.enabled is True
    assert r.created_by_email == "a@b.com"
