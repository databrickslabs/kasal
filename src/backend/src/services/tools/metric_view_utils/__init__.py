"""
Metric View utility modules — extracted from the SC Reporting monolith.

Provides parsers, translators, emitters, and a pipeline orchestrator for
converting Power BI measures into Databricks UC Metric View YAML + deploy SQL.
"""

from .artifact_cascade import cross_table_artifact_cascade
from .constants import (
    RE_AGG_COL,
    RE_CALC_COL,
    RE_CASE_AGG,
    RE_COALESCE_AGG,
    RE_DAX_DIM_REF,
    RE_FROM_CLAUSE,
    RE_GROUP_BY,
    RE_LEFT_JOIN,
)
from .data_classes import (
    MetricViewSpec,
    MStep,
    ScanTableInfo,
    TableInfo,
    TranslationResult,
)
from .dax_llm_fallback import translate_batch_with_llm, translate_with_llm
from .dax_translator import DaxTranslator
from .join_detector import JoinDetector
from .m_transform_folder import MTransformFolder
from .metadata_generator import MetadataGenerator
from .mquery_parser import MQueryParser
from .pbi_parameter_resolver import PbiParameterResolver
from .pipeline import MetricViewPipeline
from .relationships_loader import RelationshipsLoader
from .report_emitter import emit_migration_report
from .scan_data_parser import ScanDataParser
from .sql_emitter import emit_deploy_sql
from .sql_post_processor import SqlPostProcessor
from .table_processor import process_table
from .utils import load_mapping, spark_sql_compat, to_snake_case, yaml_scalar
from .yaml_emitter import emit_yaml

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
    "translate_with_llm",
    "translate_batch_with_llm",
    "emit_yaml",
    "emit_deploy_sql",
    "emit_migration_report",
    "MetricViewPipeline",
    "process_table",
    "cross_table_artifact_cascade",
]
