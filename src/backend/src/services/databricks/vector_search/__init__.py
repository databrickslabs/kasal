"""
Databricks Vector Search — Direct Access indexes.

Two rules that have bitten before (details in src/backend/CLAUDE.md):
- go through ``DatabricksVectorIndexRepository``, never the index client directly;
- never hardcode column names or positions — ask ``DatabricksIndexSchemas``.

Auth priority is OBO token → PAT from DB → PAT from env → SDK default. Service
Principal auth was removed: OBO and PAT cover Direct Access indexes.
"""
