"""Core data models for KPI (Key Performance Indicator) conversion"""

from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field, ConfigDict


class KPIFilter(BaseModel):
    """Filter definition for KPI measures"""
    field: str
    operator: str
    value: Any
    logical_operator: Optional[str] = "AND"


class Structure(BaseModel):
    """
    SAP BW Structure for time intelligence and reusable calculations.

    Structures allow defining reusable calculation patterns that can be
    applied to multiple KPIs (e.g., YTD, QTD, prior year comparisons).
    """
    description: str
    formula: Optional[str] = None  # Formula can reference other structures
    filters: List[Union[str, Dict[str, Any]]] = Field(default=[], alias="filter")
    display_sign: Optional[int] = 1
    technical_name: Optional[str] = None
    aggregation_type: Optional[str] = None
    # Structure-specific variables for time intelligence
    variables: Optional[Dict[str, Any]] = None


class KPI(BaseModel):
    """
    Key Performance Indicator (KPI) model.

    Represents a single business measure with its formula, filters,
    aggregation rules, and transformation logic.
    """
    model_config = ConfigDict(populate_by_name=True)

    description: str
    formula: str
    filters: List[Union[str, Dict[str, Any]]] = Field(default=[], alias="filter")
    display_sign: Optional[int] = 1
    technical_name: Optional[str] = None
    source_table: Optional[str] = None
    aggregation_type: Optional[str] = None
    weight_column: Optional[str] = None
    target_column: Optional[str] = None
    percentile: Optional[float] = None
    exceptions: Optional[List[Dict[str, Any]]] = None
    exception_aggregation: Optional[str] = None
    fields_for_exception_aggregation: Optional[List[str]] = None
    fields_for_constant_selection: Optional[List[str]] = None
    # Structure application - list of structure names to apply to this KPI
    apply_structures: Optional[List[str]] = None

    # Currency conversion fields
    currency_column: Optional[str] = None  # Dynamic: column name containing source currency
    fixed_currency: Optional[str] = None  # Fixed: source currency code (e.g., "USD", "EUR")
    target_currency: Optional[str] = None  # Target currency for conversion

    # Unit of measure conversion fields
    uom_column: Optional[str] = None  # Dynamic: column name containing source UOM
    uom_fixed_unit: Optional[str] = None  # Fixed: source unit (e.g., "KG", "LB")
    uom_preset: Optional[str] = None  # Conversion preset type (e.g., "mass", "length", "volume")
    target_uom: Optional[str] = None  # Target unit for conversion


class QueryFilter(BaseModel):
    """Query-level filter definition"""
    name: str
    expression: str


class KPIDefinition(BaseModel):
    """
    Complete KPI definition from YAML input.

    Contains the full specification including metadata, filters,
    structures, and all KPI measures.
    """
    description: str
    technical_name: str
    default_variables: Dict[str, Any] = {}
    query_filters: List[QueryFilter] = []
    # Filters section from YAML (like query_filter with nested filters)
    filters: Optional[Dict[str, Dict[str, str]]] = None
    # Time intelligence and reusable calculation structures
    structures: Optional[Dict[str, Structure]] = None
    kpis: List[KPI]

    def get_expanded_filters(self) -> Dict[str, str]:
        """
        Get all filters as a flat dictionary for variable substitution.

        Returns:
            Dictionary of filter names to filter expressions
        """
        expanded_filters = {}
        if self.filters:
            for filter_group, filters in self.filters.items():
                if isinstance(filters, dict):
                    for filter_name, filter_value in filters.items():
                        expanded_filters[filter_name] = filter_value
                else:
                    expanded_filters[filter_group] = str(filters)
        return expanded_filters


class DAXMeasure(BaseModel):
    """DAX measure output model"""
    name: str
    description: str
    dax_formula: str
    original_kbi: Optional[KPI] = None
    format_string: Optional[str] = None
    display_folder: Optional[str] = None


class SQLMeasure(BaseModel):
    """SQL measure output model"""
    name: str
    description: str
    sql_query: str
    original_kbi: Optional[KPI] = None
    aggregation_level: Optional[List[str]] = None


class UCMetric(BaseModel):
    """Unity Catalog Metric output model"""
    name: str
    description: str
    metric_definition: str
    original_kbi: Optional[KPI] = None
    metric_type: Optional[str] = None
    unit: Optional[str] = None
