"""
Databricks: everything the app does against the platform.

One subpackage per surface, because they have nothing in common but the
credentials:

- ``workspace``     — the client, config resolution and connection checks
- ``vector_search`` — Direct Access indexes: setup, verification, CRUD
- ``lakebase``      — the managed Postgres the app can hot-swap onto
- ``secrets``       — secret scopes
- ``genie``         — Genie spaces
- ``agentbricks``   — AgentBricks endpoints
- ``volumes``       — writing task output to a Volume as a file
- ``analytics``     — CI/CD YAML bundles for Genie spaces and Lakeview dashboards

**Every outbound call needs a Kasal User-Agent header** (Partner
Well-Architected Framework) — see src/backend/CLAUDE.md for the three patterns.
Power BI is NOT Databricks: those calls take no such header.
"""
