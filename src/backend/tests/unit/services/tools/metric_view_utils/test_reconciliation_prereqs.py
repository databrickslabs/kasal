"""Unit tests for the reconciliation prerequisite check."""

from src.services.tools.metric_view_utils.reconciliation_prereqs import (
    missing_reconciliation_inputs,
)


def _getter(cfg: dict):
    return lambda key: cfg.get(key)


def test_nothing_provided_lists_all_four():
    missing = missing_reconciliation_inputs(_getter({}))
    joined = " ".join(missing)
    assert "warehouse_id" in joined
    assert "workspace_id" in joined
    assert "dataset_id" in joined
    assert "credentials" in joined.lower()
    assert len(missing) == 4


def test_warehouse_only_still_flags_the_pbi_side():
    """The customer's case: warehouse filled, Power BI side blank."""
    missing = missing_reconciliation_inputs(_getter({"warehouse_id": "wh"}))
    joined = " ".join(missing)
    assert "warehouse_id" not in joined  # the one thing provided
    assert "workspace_id" in joined
    assert "dataset_id" in joined
    assert "credentials" in joined.lower()


def test_service_principal_satisfies_auth():
    cfg = {
        "warehouse_id": "wh",
        "workspace_id": "ws",
        "dataset_id": "ds",
        "tenant_id": "t",
        "client_id": "c",
        "client_secret": "s",
    }
    assert missing_reconciliation_inputs(_getter(cfg)) == []


def test_service_account_satisfies_auth():
    cfg = {
        "warehouse_id": "wh",
        "workspace_id": "ws",
        "dataset_id": "ds",
        "tenant_id": "t",
        "client_id": "c",
        "username": "u@d.com",
        "password": "p",
    }
    assert missing_reconciliation_inputs(_getter(cfg)) == []


def test_access_token_satisfies_auth():
    cfg = {
        "warehouse_id": "wh",
        "workspace_id": "ws",
        "dataset_id": "ds",
        "access_token": "tok",
    }
    assert missing_reconciliation_inputs(_getter(cfg)) == []


def test_partial_service_principal_is_not_enough():
    """tenant+client without a secret is incomplete → auth still missing."""
    cfg = {
        "warehouse_id": "wh",
        "workspace_id": "ws",
        "dataset_id": "ds",
        "tenant_id": "t",
        "client_id": "c",
    }
    missing = missing_reconciliation_inputs(_getter(cfg))
    assert len(missing) == 1
    assert "credentials" in missing[0].lower()
