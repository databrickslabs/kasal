"""Helpers for the Metric View Deployer: definition verification, grant preservation and
per-measure execution checks. Each module takes a SQL runner callable, so the steps run
against a fake executor in tests and against the SQL Statement API in production."""
