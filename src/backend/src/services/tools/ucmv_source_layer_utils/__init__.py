"""Pure builders and checks behind the UCMV Source Layer Manager tool.

Modules (none of them performs I/O; the tool wires real clients in):

- ``names``            identifier quoting, snake_case names, SQL literals
- ``inputs``           parsing of the tool's JSON / CSV inputs
- ``view_specs``       source-layer entries -> ViewSpec -> DDL, RLS review hints
- ``deploy_order``     dependency-safe order for CREATE MATERIALIZED VIEW
- ``pbi_client``       Power BI executeQueries / refresh history (sync HTTP)
- ``snapshot_loader``  PBI-only tables -> typed UC tables, TMDL payload decoding
- ``count_check``      PBI vs UC counts, verdicts, cutoff handling
- ``refresh_plan``     refresh decision and execution order
"""
