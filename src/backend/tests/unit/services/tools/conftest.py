"""Shared fixtures for the tool tests."""

import pytest


@pytest.fixture(autouse=True)
def _no_live_parameter_call(monkeypatch):
    """Crew 1 asks the Power BI API for the model's parameter values; unit tests stay offline."""
    monkeypatch.setattr(
        "src.services.powerbi.dataset_parameters.fetch_dataset_parameters",
        lambda *a, **k: {},
    )
