"""
Power BI: semantic models, DAX and the KPI pipeline.

The heavy lifting lives in the Power BI TOOLS (``services/tools/powerbi_*``);
this package is the app-side support they lean on — workspace/report metadata,
the semantic-model cache, per-workspace context config, few-shot DAX retrieval,
and KPI conversion.

Power BI is not Databricks: these calls take no Kasal User-Agent header.
"""
