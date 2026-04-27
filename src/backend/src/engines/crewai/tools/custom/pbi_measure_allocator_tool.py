"""PBI Measure Allocator Tool for CrewAI — group measures to fact tables."""
import json
import logging
import re
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

logger = logging.getLogger(__name__)


class PbiMeasureAllocatorSchema(BaseModel):
    """Input schema for PbiMeasureAllocatorTool."""
    measures_json: Optional[str] = Field(
        None, description="JSON string of raw measures (from Power BI Connector or Fetcher)")
    mquery_json: Optional[str] = Field(
        None, description="JSON string of mquery_transpilation (from MQuery Conversion Pipeline)")
    config_json: Optional[str] = Field(
        None, description="JSON config overrides (custom allocation rules)")


class PbiMeasureAllocatorTool(BaseTool):
    """Allocate PBI measures to fact tables based on DAX table references."""
    name: str = "PBI Measure Allocator"
    description: str = (
        "Groups Power BI measures into fact tables with confidence scores. "
        "Analyzes DAX expressions to determine which table each measure belongs to "
        "based on column references (Table[Column] patterns). "
        "Input: raw measures JSON + mquery_transpilation JSON. "
        "Output: JSON mapping of measure → fact table allocation with confidence."
    )
    args_schema: Type[BaseModel] = PbiMeasureAllocatorSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        default_config = {}
        for key in ('measures_json', 'mquery_json', 'config_json'):
            val = kwargs.pop(key, None)
            if val is not None:
                default_config[key] = val
        super().__init__(**kwargs)
        self._default_config = default_config

    def _run(self, **kwargs: Any) -> str:
        from src.engines.crewai.tools.custom.metric_view_utils.mquery_parser import MQueryParser
        from src.engines.crewai.tools.custom.metric_view_utils.utils import to_snake_case

        measures_raw = kwargs.get('measures_json') or self._default_config.get('measures_json', '[]')
        mquery_raw = kwargs.get('mquery_json') or self._default_config.get('mquery_json', '[]')

        try:
            measures = json.loads(measures_raw) if isinstance(measures_raw, str) else measures_raw
            mquery_entries = json.loads(mquery_raw) if isinstance(mquery_raw, str) else mquery_raw
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid JSON: {e}"})

        # Parse MQuery to get fact tables
        parser = MQueryParser()
        mquery_tables = parser.parse_json(mquery_entries)
        fact_tables = {k for k, v in mquery_tables.items() if v.is_fact}

        # Allocate measures
        allocations = []
        for m in measures:
            name = m.get('measure_name', m.get('name', ''))
            dax = m.get('dax_expression', '')
            # Find table references in DAX: Table[Column]
            refs = re.findall(r'(\w+)\[(\w+)\]', dax)
            table_refs = {r[0] for r in refs}

            # Find matching fact tables
            matched_facts = table_refs & fact_tables
            if len(matched_facts) == 1:
                allocation = list(matched_facts)[0]
                confidence = 'high'
            elif len(matched_facts) > 1:
                # Multiple facts → pick the one with most references
                ref_counts = {}
                for tbl, col in refs:
                    if tbl in fact_tables:
                        ref_counts[tbl] = ref_counts.get(tbl, 0) + 1
                allocation = max(ref_counts, key=ref_counts.get)
                confidence = 'medium'
            elif table_refs:
                # References non-fact tables only → check if any dim maps to a fact
                allocation = '__unassigned__'
                confidence = 'low'
            else:
                allocation = '__unassigned__'
                confidence = 'none'

            allocations.append({
                'measure_name': name,
                'original_name': m.get('original_name', name),
                'dax_expression': dax,
                'proposed_allocation': allocation,
                'confidence': confidence,
                'table_refs': list(table_refs),
                'direct_fact_refs': list(matched_facts),
            })

        # Summary
        by_table = {}
        for a in allocations:
            tbl = a['proposed_allocation']
            by_table.setdefault(tbl, []).append(a['measure_name'])

        return json.dumps({
            'allocations': allocations,
            'summary': {
                'total': len(allocations),
                'allocated': sum(1 for a in allocations if a['proposed_allocation'] != '__unassigned__'),
                'unassigned': sum(1 for a in allocations if a['proposed_allocation'] == '__unassigned__'),
                'by_table': {k: len(v) for k, v in by_table.items()},
            }
        }, indent=2)
