"""
Converters Package - Measure Conversion Library

This package provides conversion logic for business measures between formats.

## Data Flow

                    ┌─────────────┐
    SOURCE   ──────►│ KPI Model   │──────► TARGET
                    │  (Internal) │
                    └─────────────┘

    FROM external       Unified           TO external
    formats            representation      formats

## Architecture (Service-Based)

converters/
├── base/              # Framework + core models (BaseConverter, KPI, etc.)
├── common/            # Shared utilities (parsers, translators, processors)
├── services/          # Service domains (organized by technology/platform)
│   ├── powerbi/      # Power BI integration (connector, DAX parser, DAX generation, auth)
│   ├── sql/          # SQL generation (KPI → SQL, multiple dialects)
│   └── uc_metrics/   # Unity Catalog Metrics (KPI → UC Metrics)
└── pipeline.py        # Orchestrates end-to-end conversions

## Supported Services

### Data Sources:
- Power BI (DAX measure extraction, parsing, transpilation)

### Generation Targets:
- DAX (Power BI measures)
- SQL (Databricks/Spark, PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery)
- Unity Catalog Metrics (Databricks)

## Usage

### Direct Usage (API/Service layer):
```python
from converters.services.powerbi import PowerBIConnector, DAXGenerator
from converters.services.sql import SQLGenerator
from converters.services.uc_metrics import UCMetricsGenerator

# Extract from Power BI
connector = PowerBIConnector(...)
with connector:
    kpis = connector.extract_measures()

# Generate to target format
dax_gen = DAXGenerator()
sql_gen = SQLGenerator()
uc_gen = UCMetricsGenerator()
```

### CrewAI Tools:
Use front-end facing tools in engines/crewai/tools/custom/:
- MeasureConversionPipelineTool (universal converter for all formats)
- PowerBIConnectorTool
"""

# Base framework and core models
from .base import (
    BaseConverter,
    ConversionFormat,
    ConverterFactory,
    KPI,
    KPIDefinition,
    DAXMeasure,
    SQLMeasure,
    UCMetric,
)

__all__ = [
    # Framework
    "BaseConverter",
    "ConversionFormat",
    "ConverterFactory",
    # Models
    "KPI",
    "KPIDefinition",
    "DAXMeasure",
    "SQLMeasure",
    "UCMetric",
]
