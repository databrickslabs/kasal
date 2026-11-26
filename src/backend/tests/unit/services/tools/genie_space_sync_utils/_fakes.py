"""Shared fakes for the Genie Space Sync tests. No network, no warehouse."""

from __future__ import annotations

import json

from src.services.tools.genie_space_sync_utils.genie_api import GenieApiError

SPACE_ID = "0123456789abcdef0123456789abcdef"
ID_A = "a" * 32
ID_B = "b" * 32
ID_C = "c" * 32

VIEW_YAML = """version: 1.1
source: main.sales.orders
dimensions:
  - name: region
    expr: source.region
measures:
  - name: order_fill_rate_pct
    expr: AVG(source.filled)
    format:
      type: percentage
  - name: order_count
    expr: COUNT(1)
"""


def metric_view_describe() -> str:
    """DESCRIBE TABLE EXTENDED ... AS JSON of a metric view."""
    return json.dumps(
        {
            "columns": [
                {"name": "region"},
                {"name": "order_fill_rate_pct", "is_measure": True},
                {"name": "order_count", "is_measure": True},
            ],
            "view_text": VIEW_YAML,
        }
    )


def plain_table_describe(columns: list[str]) -> str:
    return json.dumps({"columns": [{"name": name} for name in columns]})


class FakeWarehouse:
    """A SQL executor that answers DESCRIBE from a table map and COUNT from a rule.

    ``tables`` maps ``catalog.schema.name`` to its DESCRIBE JSON (absent -> the
    describe fails). A statement containing any ``broken`` substring fails.
    """

    def __init__(self, tables: dict[str, str], broken: tuple[str, ...] = ()) -> None:
        self.tables = tables
        self.broken = broken
        self.statements: list[str] = []

    def __call__(self, statement: str) -> dict[str, object]:
        self.statements.append(statement)
        prefix = "DESCRIBE TABLE EXTENDED "
        if statement.startswith(prefix):
            name = statement[len(prefix) : -len(" AS JSON")].replace("`", "")
            text = self.tables.get(name)
            if text is None:
                return {"success": False, "error": "TABLE_OR_VIEW_NOT_FOUND"}
            return {"success": True, "data": {"result": {"data_array": [[text]]}}}
        if any(fragment in statement for fragment in self.broken):
            return {"success": False, "error": "UNRESOLVED_COLUMN"}
        return {"success": True, "data": {"result": {"data_array": [["7"]]}}}


class FakeGenie:
    """A ``GenieHttp`` that answers from a route table and records each call.

    Routes are keyed by ``(method, path)``; a value is returned as is, a callable
    is called with ``(params, body)``, and an exception instance is raised.
    Anything unrouted raises a 599 so a test notices an unexpected call.
    """

    def __init__(self, routes: dict[tuple[str, str], object]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, str, dict[str, object] | None]] = []

    def __call__(
        self,
        method: str,
        path: str,
        body: dict[str, object] | None,
        params: dict[str, str] | None,
    ) -> dict[str, object]:
        self.calls.append((method, path, body))
        if (method, path) not in self.routes:
            raise GenieApiError(599, f"unrouted {method} {path}")
        route = self.routes[(method, path)]
        if isinstance(route, Exception):
            raise route
        if callable(route):
            result: object = route(params, body)
        else:
            result = route
        assert isinstance(result, dict)
        return result

    def methods(self) -> list[str]:
        return [method for method, _path, _body in self.calls]


def serialized_space() -> dict[str, object]:
    """A small but complete serialized space over one metric view and one table."""
    return {
        "version": 2,
        "config": {
            "sample_questions": [{"id": ID_A, "question": ["How many orders?"]}]
        },
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.regions",
                    "column_configs": [{"column_name": "region"}],
                }
            ],
            "metric_views": [
                {
                    "identifier": "main.sales.orders",
                    "column_configs": [{"column_name": "order_count"}],
                }
            ],
        },
        "instructions": {
            "text_instructions": [{"id": ID_A, "content": ["Count orders by region."]}],
            "example_question_sqls": [
                {
                    "id": ID_B,
                    "question": ["Orders per region?"],
                    "sql": [
                        "SELECT region, MEASURE(order_count) FROM main.sales.orders "
                        "GROUP BY ALL"
                    ],
                }
            ],
            "sql_snippets": {
                "filters": [
                    {
                        "id": ID_C,
                        "display_name": "EU only",
                        "sql": ["orders.region = 'EU'"],
                    }
                ]
            },
        },
        "benchmarks": {
            "questions": [
                {
                    "id": ID_B,
                    "question": ["Fill rate?"],
                    "answer": [
                        {
                            "format": "SQL",
                            "content": [
                                "SELECT MEASURE(order_fill_rate_pct) FROM main.sales.orders"
                            ],
                        }
                    ],
                }
            ]
        },
    }


def sample_tables() -> dict[str, str]:
    return {
        "main.sales.orders": metric_view_describe(),
        "main.sales.regions": plain_table_describe(["region", "name"]),
    }
