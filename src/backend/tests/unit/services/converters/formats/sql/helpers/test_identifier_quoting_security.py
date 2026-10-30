"""Generated SQL keeps model-supplied names inside their quotes.

Apps security scan follow-up (2026-10-28): ``_quote_identifier`` wrapped names
in backticks / double quotes without doubling an embedded quote, and the UNION
measure query put the measure alias into ``'{alias}'`` raw — so a measure or
table name could leave its identifier or literal in the SQL the converter emits.
"""

import pytest

from src.services.converters.formats.sql.helpers.sql_structures import (
    SQLStructureExpander,
)
from src.services.converters.formats.sql.models import SQLDialect


@pytest.mark.parametrize(
    "dialect, name, quoted",
    [
        (SQLDialect.DATABRICKS, "sales", "`sales`"),
        (SQLDialect.DATABRICKS, "x` FROM other --", "`x`` FROM other --`"),
        (SQLDialect.STANDARD, "sales", '"sales"'),
        (SQLDialect.STANDARD, 'x" FROM other --', '"x"" FROM other --"'),
    ],
)
def test_identifiers_double_their_quote_char(dialect, name, quoted):
    assert SQLStructureExpander(dialect=dialect)._quote_identifier(name) == quoted


def test_literals_escape_quotes_and_databricks_backslashes():
    databricks = SQLStructureExpander(dialect=SQLDialect.DATABRICKS)
    assert databricks._quote_literal("it's") == "'it''s'"
    assert databricks._quote_literal("a\\") == "'a\\\\'"
    standard = SQLStructureExpander(dialect=SQLDialect.STANDARD)
    assert standard._quote_literal("a\\'") == "'a\\'''"
