"""Loader for DAX expression mappings from JSON."""
import json
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class MeasureTableMappingParser:
    """Loader for DAX expression mappings from JSON."""
    
    def __init__(self, json_path: str):
        self.json_path = json_path
        self.mappings = []
        self._mappings_index: Dict[str, Dict] = {}  # lower(name) → mapping for O(1) lookup

    def load(self) -> List[Dict]:
        """Load the measure mapping JSON and build a name index."""
        with open(self.json_path, 'r', encoding='utf-8') as f:
            self.mappings = json.load(f)

        self._mappings_index = {}
        for mapping in self.mappings:
            name = mapping.get('measure_name', '')
            if name:
                self._mappings_index[name.lower()] = mapping

        return self.mappings
    
    def get_measures_for_table(self, table_name: str) -> List[Dict]:
        """Get all measures allocated to a specific fact table."""
        if not self.mappings:
            self.load()
        
        result = []
        for mapping in self.mappings:
            proposed = mapping.get('proposed_allocation', '')
            if proposed == table_name:
                result.append(mapping)
        
        return result
    
    def get_measure_by_name(self, name: str) -> Optional[Dict]:
        """Get a specific measure mapping by name (case-insensitive, O(1) via index)."""
        if not self.mappings:
            self.load()
        return self._mappings_index.get(name.lower())
