"""
Metric View utility modules — extracted from the SC Reporting monolith.

Provides parsers, translators, emitters, and a pipeline orchestrator for
converting Power BI measures into Databricks UC Metric View YAML + deploy SQL.
"""

from .data_classes import (
    TranslationResult,
    TableInfo,
    MetricViewSpec,
    MStep,
    ScanTableInfo,
)
from .constants import (
    RE_AGG_COL,
    RE_FROM_CLAUSE,
    RE_LEFT_JOIN,
    RE_GROUP_BY,
    RE_CALC_COL,
    RE_COALESCE_AGG,
    RE_CASE_AGG,
    RE_DAX_DIM_REF,
)
from .utils import to_snake_case, spark_sql_compat, load_mapping, yaml_scalar
from .mquery_parser import MQueryParser
from .scan_data_parser import ScanDataParser
from .pbi_parameter_resolver import PbiParameterResolver
from .m_transform_folder import MTransformFolder
from .sql_post_processor import SqlPostProcessor
from .metadata_generator import MetadataGenerator
from .relationships_loader import RelationshipsLoader
from .join_detector import JoinDetector
from .dax_translator import DaxTranslator
from .yaml_emitter import emit_yaml
from .sql_emitter import emit_deploy_sql
from .pipeline import MetricViewPipeline

__all__ = [
    "TranslationResult",
    "TableInfo",
    "MetricViewSpec",
    "MStep",
    "ScanTableInfo",
    "MQueryParser",
    "ScanDataParser",
    "PbiParameterResolver",
    "MTransformFolder",
    "SqlPostProcessor",
    "MetadataGenerator",
    "RelationshipsLoader",
    "JoinDetector",
    "DaxTranslator",
    "emit_yaml",
    "emit_deploy_sql",
    "MetricViewPipeline",
]
