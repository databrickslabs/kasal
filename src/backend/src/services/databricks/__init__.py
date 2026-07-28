"""
Databricks platform integrations.

Workspace config and auth, Vector Search indexes (setup + verification),
secrets, Genie spaces and AgentBricks. Everything here talks to a Databricks
API; anything that merely RUNS on Databricks does not belong.

Every outbound call needs a Kasal User-Agent header — see src/backend/CLAUDE.md.
"""
