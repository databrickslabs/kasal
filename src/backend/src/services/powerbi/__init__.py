"""
Power BI: semantic models, DAX, and measure conversion.

- ``service`` / ``context_config`` / ``semantic_model_cache`` — workspace and
  report metadata, plus the per-workspace context the tools read
- ``dax_rag_retriever`` — few-shot Q→DAX example retrieval
- ``conversions`` / ``kpi_conversion`` — conversion jobs, history and saved
  configs (the BUSINESS side: rows, group scoping, status)

The transformation itself is not here. ``src/converters/`` is a pure library
(KPI model, DAX parse/generate, SQL and UC-Metrics emit) with no DB, no session
and no GroupContext — this package drives it. Keep it that way: a repository
call inside src/converters/ is the bug this note exists to prevent.

Power BI is not Databricks: these calls take no Kasal User-Agent header.
"""
