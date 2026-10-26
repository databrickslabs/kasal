import re

from ...base.models import KPI, KPIDefinition


class FormulaTranslator:
    def __init__(self) -> None:
        # Common SAP BW field patterns to DAX aggregation mapping
        self.aggregation_mappings = {
            "volume": "SUM",
            "amount": "SUM",
            "quantity": "SUM",
            "count": "COUNT",
            "avg": "AVERAGE",
            "max": "MAX",
            "min": "MIN",
            "kvolume": "SUM",  # SAP BW key figure for volume
            "kamount": "SUM",  # SAP BW key figure for amount
        }

        # Pattern to extract table and column information
        self.field_pattern = re.compile(r"bic_([a-zA-Z0-9_]+)")

    def create_measure_name(self, kpi: KPI, definition: KPIDefinition) -> str:
        """Create a clean measure name from KPI description."""
        if kpi.description:
            # Clean up the description for use as measure name
            clean_name = re.sub(r"[^\w\s]", "", kpi.description)
            clean_name = re.sub(r"\s+", " ", clean_name).strip()
            return clean_name

        # Fallback to technical name or formula
        if kpi.technical_name:
            return kpi.technical_name.replace("_", " ").title()

        # Last resort: use formula
        return kpi.formula.replace("bic_", "").replace("_", " ").title()
