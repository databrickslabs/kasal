"""Handles loading and accessing data from metrics view YAML and DAX mapping JSON files."""
import logging
import re
from typing import Dict, List, Optional

from .constants import PBI_COMMENT_MARKER
from .databricks_parser import UCMetricsViewParser
from .measure_table_mapping_parser import MeasureTableMappingParser
from .dax_expression_parser import DAXExpressionParser

logger = logging.getLogger(__name__)


class DataInputHandler:
    """Handles loading and accessing data from metrics view YAML and DAX mapping JSON files."""
    
    def __init__(self, metrics_view_path: str, table_mapping_path: str, table_mappings: Optional[Dict] = None):
        """
        Initialize the DataInputHandler.
        
        Args:
            metrics_view_path: Path to metrics view YAML file
            table_mapping_path: Path to table mapping JSON file
            table_mappings: Optional dictionary for table name mappings
            
        Raises:
            ValueError: If metrics_view_path or table_mapping_path are empty
        """
        if not metrics_view_path or not table_mapping_path:
            raise ValueError("Both metrics_view_path and table_mapping_path must be provided")
            
        self.table_mappings = table_mappings or {}
        self.mv_parser = UCMetricsViewParser(metrics_view_path)
        self.table_mapping_parser = MeasureTableMappingParser(table_mapping_path)
        self.dax_parser = DAXExpressionParser()
        self._yaml_measures_cache = None
        self._dax_measures_cache = None
        
    def get_yaml_measure(self, measure_name: str) -> Optional[Dict]:
        """
        Get a measure from the metrics view YAML by name.
        
        Args:
            measure_name: Name of the measure to retrieve
            
        Returns:
            Dictionary containing measure data or None if not found
            
        Raises:
            ValueError: If measure_name is None or empty
        """
        if not measure_name:
            raise ValueError("measure_name cannot be None or empty")
        return self.mv_parser.get_measure_by_name(measure_name)
    
    def get_dax_measure(self, measure_name: str) -> Optional[Dict]:
        """
        Get a DAX measure from the mapping JSON by name.
        
        Args:
            measure_name: Name of the measure to retrieve
            
        Returns:
            Dictionary containing DAX measure data or None if not found
            
        Raises:
            ValueError: If measure_name is None or empty
        """
        if not measure_name:
            raise ValueError("measure_name cannot be None or empty")
        return self.table_mapping_parser.get_measure_by_name(measure_name)
    
    def get_all_yaml_measures(self) -> List[Dict]:
        """
        Get all measures from the metrics view YAML.
        
        Returns:
            List of measure dictionaries (empty list if none found)
        """
        if self._yaml_measures_cache is None:
            self._yaml_measures_cache = self.mv_parser.extract_measures()
        return self._yaml_measures_cache
    
    def get_all_dax_measures(self) -> List[Dict]:
        """
        Get all DAX measures from the mapping JSON.
        
        Returns:
            List of DAX measure dictionaries (empty list if none found)
        """
        if self._dax_measures_cache is None:
            self._dax_measures_cache = self.table_mapping_parser.load()
        return self._dax_measures_cache
    
    def find_matching_dax_for_yaml_measure(self, yaml_measure: Dict) -> Optional[Dict]:
        """
        Find the matching DAX expression for a YAML measure.
        
        Tries multiple matching strategies:
        1. Exact name match
        2. Case-insensitive name match
        3. PBI comment extraction (e.g., "PBI: Measure_Name")
        
        Args:
            yaml_measure: Dictionary containing YAML measure data
            
        Returns:
            Dictionary containing matching DAX measure data or None if not found
            
        Raises:
            ValueError: If yaml_measure is None or missing 'name' field
        """
        if not yaml_measure:
            raise ValueError("yaml_measure cannot be None")
            
        measure_name = yaml_measure.get('name')
        if not measure_name:
            logger.warning("YAML measure missing 'name' field: %s", yaml_measure)
            return None
        
        # Strategy 1: Exact match
        dax_measure = self.get_dax_measure(measure_name)
        if dax_measure:
            logger.debug("Found exact match for measure '%s'", measure_name)
            return dax_measure
        
        # Strategy 2: Case-insensitive match
        all_dax = self.get_all_dax_measures()
        for dax in all_dax:
            dax_name = dax.get('measure_name', '')
            if dax_name and dax_name.lower() == measure_name.lower():
                logger.debug("Found case-insensitive match for measure '%s'", measure_name)
                return dax
        
        # Strategy 3: Extract from PBI comment using regex
        comment = yaml_measure.get('comment', '')
        if comment and PBI_COMMENT_MARKER in comment:
            try:
                # Extract PBI measure name from comment using regex
                # Pattern: "PBI: <measure_name>" where measure_name is on the first line after PBI:
                pbi_pattern = rf'{re.escape(PBI_COMMENT_MARKER)}\s*([^\n]+)'
                match = re.search(pbi_pattern, comment)
                if match:
                    pbi_name = match.group(1).strip()
                    dax_measure = self.get_dax_measure(pbi_name)
                    if dax_measure:
                        logger.debug("Found PBI comment match for measure '%s' -> '%s'", measure_name, pbi_name)
                        return dax_measure
            except (IndexError, AttributeError, re.error) as e:
                logger.warning("Error extracting PBI measure name from comment: %s", e)
        
        logger.debug("No matching DAX measure found for '%s'", measure_name)
        return None
