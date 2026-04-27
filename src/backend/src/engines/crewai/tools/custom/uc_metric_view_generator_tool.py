"""UC Metric View Generator Tool for CrewAI — full pipeline."""
import json
import logging
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class UCMetricViewGeneratorSchema(BaseModel):
    """Input schema for UCMetricViewGeneratorTool."""
    measures_json: Optional[str] = Field(
        None, description="JSON string of measure_table_mapping (from Measure Conversion Pipeline)")
    mquery_json: Optional[str] = Field(
        None, description="JSON string of mquery_transpilation (from MQuery Conversion Pipeline)")
    relationships_json: Optional[str] = Field(
        None, description="JSON string of PBI relationships (from Relationships Tool)")
    scan_data_json: Optional[str] = Field(
        None, description="JSON string of PBI scan data (optional, for enrichment)")
    config_json: Optional[str] = Field(
        None, description="JSON pipeline config overrides (join_key_map, fact_join_map, etc.)")
    catalog: Optional[str] = Field(None, description="Target UC catalog name")
    schema_name: Optional[str] = Field(None, description="Target UC schema name")
    inner_dim_joins: bool = Field(False, description="Use INNER JOIN for dimensions")
    unflatten_tables: bool = Field(False, description="Unflatten __-separated table names")


class UCMetricViewGeneratorTool(BaseTool):
    """Generate UC Metric View YAML + deploy SQL from PBI measures and MQuery data."""
    name: str = "UC Metric View Generator"
    description: str = (
        "Full pipeline: takes PBI measures JSON + MQuery transpilation JSON and generates "
        "UC Metric View YAML definitions and deploy SQL per discovered fact table. "
        "Combines MQuery parsing, DAX translation, join detection, and YAML/SQL emission. "
        "Input: measures_json (from tool 73) + mquery_json (from tool 74). "
        "Output: JSON with YAML + SQL per fact table, plus generation stats."
    )
    args_schema: Type[BaseModel] = UCMetricViewGeneratorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = ('measures_json', 'mquery_json', 'relationships_json',
                       'scan_data_json', 'config_json', 'catalog', 'schema_name',
                       'inner_dim_joins', 'unflatten_tables')
        default_config = {}
        for key in config_keys:
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        from src.engines.crewai.tools.custom.metric_view_utils.pipeline import MetricViewPipeline
        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
        from src.engines.crewai.tools.custom.metric_view_utils.relationships_loader import RelationshipsLoader
        from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser

        def _get(key):
            return kwargs.get(key) or self._default_config.get(key)

        measures_raw = _get('measures_json') or '[]'
        mquery_raw = _get('mquery_json') or '[]'
        relationships_raw = _get('relationships_json')
        scan_raw = _get('scan_data_json')
        config_raw = _get('config_json') or '{}'
        catalog = _get('catalog') or 'main'
        schema = _get('schema_name') or 'default'
        inner_joins = _get('inner_dim_joins') or False
        unflatten = _get('unflatten_tables') or False

        try:
            measures = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
            mquery_entries = json.loads(mquery_raw) if isinstance(mquery_raw, str) else mquery_raw
            config = json.loads(config_raw) if isinstance(config_raw, str) else config_raw
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON input: {e}"})

        # Parse MQuery
        parser = MQueryParser()
        mquery_tables = parser.parse_json(mquery_entries)

        # Parse relationships
        relationships_enrichment = {}
        if relationships_raw:
            try:
                rel_data = json.loads(relationships_raw) if isinstance(relationships_raw, str) else relationships_raw
                loader = RelationshipsLoader()
                fact_keys = {k for k, v in mquery_tables.items() if v.is_fact}
                relationships_enrichment = loader.load(rel_data, mquery_tables, fact_keys)
            except Exception as e:
                logger.warning(f"Failed to parse relationships: {e}")

        # Parse scan data
        scan_data = {}
        if scan_raw:
            try:
                scan_obj = json.loads(scan_raw) if isinstance(scan_raw, str) else scan_raw
                scan_parser = ScanDataParser()
                scan_data = scan_parser.parse(scan_obj)
            except Exception as e:
                logger.warning(f"Failed to parse scan data: {e}")

        # Run pipeline
        pipeline = MetricViewPipeline(
            mapping=measures,
            mquery_tables=mquery_tables,
            config=config,
            inner_dim_joins=inner_joins,
            scan_data=scan_data,
            unflatten_tables=unflatten or bool(scan_data),
            relationships_enrichment=relationships_enrichment,
        )
        pipeline.run()

        # Emit YAML + SQL
        yaml_output = pipeline.emit_all_yaml(catalog=catalog, schema=schema)
        sql_output = pipeline.emit_all_sql(catalog=catalog, schema=schema)
        results = pipeline.get_results()

        output = {
            'yaml': yaml_output,
            'sql': sql_output,
            'stats': results['stats'],
            'specs_summary': {
                k: {
                    'view_name': v.get('view_name'),
                    'measures': v.get('measures_count'),
                    'untranslatable': v.get('untranslatable_count'),
                }
                for k, v in results.get('specs', {}).items()
            },
        }
        return json.dumps(output, indent=2)
