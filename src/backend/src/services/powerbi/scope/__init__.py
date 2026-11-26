"""What a Power BI report really needs: the reachability closure and its draft scope table.

Kasal's UCMV generator translates every measure of every fact table; a report
uses a fraction of them, and some it uses are invisible to a plain visual scan
(hidden pages behind buttons and bookmarks, measures reached through calculated
columns, calculation groups, user-defined functions or Power Query merges). This
package answers "which measures, tables and columns does a viewer actually see,
directly or through a dependency chain" from the TMDL and PBIR parts Kasal
already fetches, and drafts a decision for each with a fixed vocabulary
(``keep / snapshot / fold / genie / drop / n/a / review``).

Pure: no I/O, no network, no Kasal imports beyond this package. Ported from a
production migration workflow that rebuilt Power BI reports as materialized
views plus thin UC metric views on top of Kasal's draft output.
"""

from src.services.powerbi.scope.decisions import DECISIONS, MIRRORED
from src.services.powerbi.scope.parts import PartTree
from src.services.powerbi.scope.scope_table import (
    SCOPE_FIELDS,
    analyse,
    build_report_scope,
    scope_csv,
)
from src.services.powerbi.scope.tmdl_model import (
    Model,
    Table,
    load_model,
    relationships_from_rows,
)

__all__ = [
    "DECISIONS",
    "MIRRORED",
    "SCOPE_FIELDS",
    "Model",
    "PartTree",
    "Table",
    "analyse",
    "build_report_scope",
    "load_model",
    "relationships_from_rows",
    "scope_csv",
]
