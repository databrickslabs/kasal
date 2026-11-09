"""Step modules behind the UC Metric View Generator tool.

The tool class in ``uc_metric_view_generator_tool`` orchestrates these steps.
Each module owns one concern: input resolution and DB fallback, reconciliation
gate, PBI API extraction, pipeline config and model inputs, visual usage, output
shaping, and history persistence.
"""
