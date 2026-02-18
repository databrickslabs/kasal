"""
DAX KBI Context Tracking
Implements context-aware filter tracking for Power BI DAX measures
"""

from typing import List, Optional, Set
from ....base.models import KPI


class DAXBaseKBIContext:
    """
    Defines Base KBI context in relation to calculated KBIs for DAX measures.

    Each base KBI can be used in the context of many higher-level KBIs.
    Even if the formula is the same, filters, aggregations, and constant selection
    definitions may differ based on the parent KBI chain.

    Mirrors the pattern from SQLBaseKBIContext but adapted for DAX specifics.
    """

    def __init__(
        self,
        kbi: KPI,
        parent_kbis: Optional[List[KPI]] = None,
    ):
        """
        Initialize DAX Base KBI Context

        Args:
            kbi: The base KBI for which this context is created
            parent_kbis: Parent KBIs in the dependency chain
        """
        self._kbi = kbi
        self._parent_kbis: List[KPI] = parent_kbis or []

    def __repr__(self):
        parent_names = " → ".join([p.technical_name for p in self._parent_kbis]) if self._parent_kbis else "ROOT"
        return f"DAXContext[{parent_names} → {self.kbi.technical_name}]"

    def __eq__(self, other):
        if isinstance(other, DAXBaseKBIContext):
            return (
                self.kbi.technical_name == other.kbi.technical_name and
                self.parent_kbis_chain == other.parent_kbis_chain
            )
        return False

    def __hash__(self):
        """Hash based on KBI name + parent chain for set membership"""
        hash_str = f"{self.kbi.technical_name}"
        for parent_kbi in self._parent_kbis:
            hash_str += f"_{parent_kbi.technical_name}"
        return hash(hash_str)

    @property
    def id(self) -> str:
        """
        Unique identifier for this context combining base KBI + parent chain

        Examples:
            - Base KBI "revenue" with no parents: "revenue"
            - Base KBI "revenue" with parent "ytd_revenue": "revenue_ytd_revenue"
        """
        context_path = "_".join([k.technical_name for k in self._parent_kbis if k is not self.kbi])
        if context_path:
            return f"{self.kbi.technical_name}_{context_path}"
        else:
            return self.kbi.technical_name

    @property
    def parent_kbis_chain(self) -> str:
        """Returns string representation of parent KBI chain for comparison"""
        return "_".join([k.technical_name for k in self._parent_kbis])

    @property
    def combined_filters(self) -> List[str]:
        """
        Returns combined filters from this KBI and all parent KBIs

        Filters cascade down from parents to children:
        - Parent filter 1
        - Parent filter 2
        - Current KBI filter

        All filters are ANDed together in DAX CALCULATE statement.
        """
        filters = []

        # Collect filters from KBI and all parents
        for context_kbi in [self.kbi, *self._parent_kbis]:
            if context_kbi.filters:
                filters.extend(context_kbi.filters)

        return filters

    @property
    def fields_for_constant_selection(self) -> Set[str]:
        """
        Returns union of constant selection fields from this context chain

        Constant selection (SAP BW GROUP BY) fields from all KBIs in the chain
        are combined. These fields define the granularity level for calculation
        separate from the target columns.
        """
        fields: Set[str] = set()

        for context_kbi in [self.kbi, *self._parent_kbis]:
            if context_kbi.fields_for_constant_selection:
                fields = fields.union(set(context_kbi.fields_for_constant_selection))

        return fields

    @property
    def fields_for_exception_aggregation(self) -> Set[str]:
        """
        Returns union of exception aggregation fields from this context chain

        Exception aggregation fields define the granularity at which the
        base calculation happens before aggregating back to target level.
        """
        fields: Set[str] = set(self.kbi.fields_for_exception_aggregation or [])

        for context_kbi in self._parent_kbis:
            if context_kbi.fields_for_exception_aggregation:
                fields = fields.union(set(context_kbi.fields_for_exception_aggregation))

        return fields

    @property
    def kbi(self) -> KPI:
        """Returns the base KBI for which this context is created"""
        return self._kbi

    @property
    def parent_kbis(self) -> List[KPI]:
        """Returns parent KBIs in the dependency chain"""
        return self._parent_kbis

    @classmethod
    def get_kbi_context(
        cls,
        kbi: KPI,
        parent_kbis: Optional[List[KPI]] = None
    ) -> 'DAXBaseKBIContext':
        """
        Factory method to create a context for a KBI

        Args:
            kbi: Base KBI
            parent_kbis: Parent KBIs in dependency chain

        Returns:
            DAXBaseKBIContext instance
        """
        return DAXBaseKBIContext(kbi=kbi, parent_kbis=parent_kbis)

    @classmethod
    def append_dependency(
        cls,
        kbi: KPI,
        parent_kbis: Optional[List[KPI]]
    ) -> Optional[List[KPI]]:
        """
        Append a KBI to the parent chain if it's valid for context tracking

        Args:
            kbi: KBI to potentially add to parent chain
            parent_kbis: Current parent chain

        Returns:
            Updated parent chain or None
        """
        if cls.is_valid_for_context(kbi=kbi):
            parent_kbis = parent_kbis.copy() if parent_kbis else []
            parent_kbis.append(kbi)
            return parent_kbis
        return parent_kbis

    @classmethod
    def is_valid_for_context(cls, kbi: KPI) -> bool:
        """
        Check if KBI should be tracked in context chain

        A KBI is valid for context if it has:
        - Filters (affects which rows are included)
        - Constant selection fields (affects granularity)
        - Exception aggregation fields (affects calculation level)

        Args:
            kbi: KBI to check

        Returns:
            True if KBI should be part of context chain
        """
        return bool(
            kbi.filters or
            kbi.fields_for_constant_selection or
            kbi.fields_for_exception_aggregation
        )

    def get_dax_filter_expressions(self, table_name: str) -> List[str]:
        """
        Build DAX FILTER function expressions from combined filters

        Args:
            table_name: The table name to use in FILTER functions

        Returns:
            List of FILTER function strings for use in CALCULATE
        """
        if not self.combined_filters:
            return []

        filter_expressions = []
        for filter_condition in self.combined_filters:
            # Each filter becomes a FILTER function
            filter_expr = f"FILTER({table_name}, {filter_condition})"
            filter_expressions.append(filter_expr)

        return filter_expressions

    def get_dax_constant_selection_expressions(self, table_name: str) -> List[str]:
        """
        Build DAX REMOVEFILTERS expressions for constant selection fields

        Args:
            table_name: The table name to use in REMOVEFILTERS

        Returns:
            List of REMOVEFILTERS strings for use in CALCULATE
        """
        if not self.fields_for_constant_selection:
            return []

        removefilters = []
        for field in self.fields_for_constant_selection:
            removefilters.append(f"REMOVEFILTERS({table_name}[{field}])")

        return removefilters

    def get_target_columns_for_calculation(self, base_target_columns: Set[str]) -> Set[str]:
        """
        Determine actual target columns for calculation considering constant selection

        Constant selection fields are calculated separately and then merged,
        so they should be excluded from the base target columns for calculation.

        Args:
            base_target_columns: Original target columns

        Returns:
            Adjusted target columns excluding constant selection fields
        """
        return base_target_columns.difference(self.fields_for_constant_selection)

    def needs_exception_aggregation_expansion(self, target_columns: Set[str]) -> bool:
        """
        Check if exception aggregation requires granularity expansion

        If exception aggregation fields are not already in target columns,
        we need to calculate at a finer granularity and then aggregate back.

        Args:
            target_columns: Current target columns

        Returns:
            True if we need to expand granularity for exception aggregation
        """
        if not self.fields_for_exception_aggregation:
            return False

        # If exception fields are already subset of target, no expansion needed
        return not self.fields_for_exception_aggregation.issubset(target_columns)


class DAXKBIContextCache:
    """
    Cache for DAX KBI contexts to avoid recalculating the same combinations

    Similar to SQLKBIContextCache pattern.
    """

    def __init__(self):
        self._cache: Set[DAXBaseKBIContext] = set()

    def add_context(self, context: DAXBaseKBIContext) -> None:
        """Add a context to the cache"""
        self._cache.add(context)

    def get_all_contexts(self) -> Set[DAXBaseKBIContext]:
        """Get all cached contexts"""
        return self._cache

    def get_contexts_for_kbi(self, kbi_technical_name: str) -> List[DAXBaseKBIContext]:
        """Get all contexts for a specific KBI"""
        return [ctx for ctx in self._cache if ctx.kbi.technical_name == kbi_technical_name]

    def get_unique_filter_combinations(self, table_name: str) -> List[List[str]]:
        """Get unique filter combinations across all contexts as DAX expressions"""
        filter_combinations = set()
        for ctx in self._cache:
            filter_exprs = tuple(ctx.get_dax_filter_expressions(table_name))
            if filter_exprs:
                filter_combinations.add(filter_exprs)
        return [list(combo) for combo in filter_combinations]

    def clear(self) -> None:
        """Clear the cache"""
        self._cache.clear()
