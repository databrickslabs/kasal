"""Currency conversion logic for measure converters

Generates SQL/DAX code for currency conversion based on KPI configuration.
Supports both fixed and dynamic currency sources.
"""

from typing import Optional, Tuple, List
from ...base.models import KPI


class CurrencyConverter:
    """
    Generates currency conversion SQL/DAX code for measures.

    Supports two types of currency conversion:
    1. Fixed currency: Source currency is specified in KPI definition (e.g., "USD")
    2. Dynamic currency: Source currency comes from a column in the data

    Examples:
        Fixed: Convert all values from USD to EUR
        Dynamic: Convert values where each row has its own source currency column
    """

    # Standard currency conversion presets
    SUPPORTED_CURRENCIES = {
        "USD", "EUR", "GBP", "JPY", "CNY", "INR", "AUD", "CAD", "CHF", "SEK", "NOK", "DKK"
    }

    def __init__(self):
        self.exchange_rate_table = "ExchangeRates"  # Default exchange rate table name

    def get_kbi_currency_recursive(self, kbi: KPI, kpi_lookup: Optional[dict] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Get source currency for given KPI by checking all dependencies.

        Recursively searches through KPI formula dependencies to find currency information.

        Args:
            kbi: KPI to check for currency information
            kpi_lookup: Dictionary mapping KPI names to KPI objects (for dependency resolution)

        Returns:
            Tuple[currency_type, currency_value]:
                - currency_type: "fixed", "dynamic", or None
                - currency_value: Currency code (fixed) or column name (dynamic)

        Examples:
            ("fixed", "USD") - All values in USD
            ("dynamic", "source_currency") - Currency per row in column
            (None, None) - No currency conversion needed
        """
        # Check if this KPI has currency information
        if kbi.currency_column:
            return "dynamic", kbi.currency_column

        if kbi.fixed_currency:
            return "fixed", kbi.fixed_currency

        # If no currency info and we have a lookup, check formula dependencies
        if kpi_lookup and kbi.formula:
            # Extract KBI references from formula (pattern: [KBI_NAME])
            import re
            kbi_refs = re.findall(r'\[([^\]]+)\]', kbi.formula)

            for kbi_name in kbi_refs:
                if kbi_name in kpi_lookup:
                    child_kbi = kpi_lookup[kbi_name]
                    currency_type, currency_value = self.get_kbi_currency_recursive(child_kbi, kpi_lookup)
                    if currency_type:
                        return currency_type, currency_value

        return None, None

    def generate_sql_conversion(
        self,
        value_expression: str,
        source_currency: str,
        target_currency: str,
        currency_type: str = "fixed",
        currency_column: Optional[str] = None,
        exchange_rate_table: Optional[str] = None
    ) -> str:
        """
        Generate SQL code for currency conversion.

        Args:
            value_expression: SQL expression for the value to convert
            source_currency: Source currency code (if fixed) or None
            target_currency: Target currency code
            currency_type: "fixed" or "dynamic"
            currency_column: Column name containing currency (if dynamic)
            exchange_rate_table: Name of exchange rate table

        Returns:
            SQL expression for converted value

        Examples:
            Fixed: "value * (SELECT rate FROM ExchangeRates WHERE from_curr='USD' AND to_curr='EUR')"
            Dynamic: "value * er.rate (with JOIN on source_currency column)"
        """
        exchange_table = exchange_rate_table or self.exchange_rate_table

        if currency_type == "fixed":
            # Fixed currency: simple multiplication with exchange rate
            return f"""(
    {value_expression} * (
        SELECT rate
        FROM {exchange_table}
        WHERE from_currency = '{source_currency}'
          AND to_currency = '{target_currency}'
          AND effective_date <= CURRENT_DATE()
        ORDER BY effective_date DESC
        LIMIT 1
    )
)"""

        else:  # dynamic
            # Dynamic currency: requires JOIN with exchange rate table
            # This needs to be handled at the query level, not just expression level
            # Return a placeholder that the generator can expand
            return f"(__CURRENCY_CONVERSION__:{value_expression}:{currency_column}:{target_currency})"

    def generate_dax_conversion(
        self,
        value_expression: str,
        source_currency: str,
        target_currency: str,
        currency_type: str = "fixed",
        currency_column: Optional[str] = None,
        exchange_rate_table: Optional[str] = None
    ) -> str:
        """
        Generate DAX code for currency conversion.

        Args:
            value_expression: DAX expression for the value to convert
            source_currency: Source currency code (if fixed) or None
            target_currency: Target currency code
            currency_type: "fixed" or "dynamic"
            currency_column: Column name containing currency (if dynamic)
            exchange_rate_table: Name of exchange rate table

        Returns:
            DAX expression for converted value

        Examples:
            Fixed: "value * LOOKUPVALUE(ExchangeRates[Rate], ...)"
            Dynamic: "value * RELATED(ExchangeRates[Rate])"
        """
        exchange_table = exchange_rate_table or self.exchange_rate_table

        if currency_type == "fixed":
            # Fixed currency: LOOKUPVALUE for single rate
            return f"""(
    {value_expression} *
    LOOKUPVALUE(
        {exchange_table}[Rate],
        {exchange_table}[FromCurrency], "{source_currency}",
        {exchange_table}[ToCurrency], "{target_currency}"
    )
)"""

        else:  # dynamic
            # Dynamic currency: RELATED for relationship-based lookup
            # Assumes relationship between fact table and exchange rate table
            return f"""(
    {value_expression} *
    LOOKUPVALUE(
        {exchange_table}[Rate],
        {exchange_table}[FromCurrency], [{currency_column}],
        {exchange_table}[ToCurrency], "{target_currency}"
    )
)"""

    def should_convert_currency(self, kbi: KPI) -> bool:
        """
        Check if currency conversion is needed for this KPI.

        Args:
            kbi: KPI to check

        Returns:
            True if currency conversion should be applied
        """
        # Need both a source and a target currency
        has_source = bool(kbi.currency_column or kbi.fixed_currency)
        has_target = bool(kbi.target_currency)

        return has_source and has_target

    def get_required_joins(
        self,
        kbis: List[KPI],
        exchange_rate_table: Optional[str] = None
    ) -> List[str]:
        """
        Get required JOIN clauses for dynamic currency conversion.

        Args:
            kbis: List of KPIs that may need currency conversion
            exchange_rate_table: Name of exchange rate table

        Returns:
            List of SQL JOIN clauses needed for currency conversion
        """
        exchange_table = exchange_rate_table or self.exchange_rate_table
        joins = []

        for kbi in kbis:
            if kbi.currency_column and kbi.target_currency:
                # Dynamic currency needs a JOIN
                join_clause = f"""LEFT JOIN {exchange_table} AS er
    ON er.from_currency = {kbi.source_table}.{kbi.currency_column}
    AND er.to_currency = '{kbi.target_currency}'
    AND er.effective_date = (
        SELECT MAX(effective_date)
        FROM {exchange_table}
        WHERE from_currency = {kbi.source_table}.{kbi.currency_column}
          AND to_currency = '{kbi.target_currency}'
          AND effective_date <= CURRENT_DATE()
    )"""
                joins.append(join_clause)

        return joins
