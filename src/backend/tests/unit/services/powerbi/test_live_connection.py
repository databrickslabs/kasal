"""Live-connection detection: a table whose M-Query uses
AnalysisServices.Database(...) is a live connection to a semantic model, which
the transpiler can't resolve — flag it with which model/table to parse.
"""

from types import SimpleNamespace

from src.services.powerbi.live_connection import (
    derive_live_connections,
    detect_live_connection,
)


class TestDetectLiveConnection:
    def test_detects_and_extracts_server_and_database(self):
        mq = 'let Source = AnalysisServices.Database("powerbi://api.powerbi.com/v1.0/myorg/OTC", "OTC Model", [Query="..."]) in Source'
        assert detect_live_connection(mq) == {
            "server": "powerbi://api.powerbi.com/v1.0/myorg/OTC",
            "database": "OTC Model",
        }

    def test_single_quotes_and_whitespace(self):
        mq = "AnalysisServices.Database( 'srv' , 'db' )"
        assert detect_live_connection(mq) == {"server": "srv", "database": "db"}

    def test_none_for_warehouse_or_empty(self):
        assert detect_live_connection(None) is None
        assert detect_live_connection("") is None
        assert detect_live_connection('Sql.Database("srv","db")') is None
        assert detect_live_connection('let x = Table.FromRows(...) in x') is None


class TestDeriveLiveConnections:
    def test_maps_live_tables_to_their_view_names(self):
        specs = {
            "Fact_OTC": SimpleNamespace(view_name="mv_otc"),
            "Live_Sales": SimpleNamespace(view_name="mv_live_sales"),
        }
        mquery = {
            "Fact_OTC": 'Sql.Database("wh", "db")',  # warehouse — not flagged
            "Live_Sales": 'AnalysisServices.Database("srv", "Sales Model")',
        }
        out = derive_live_connections(specs, mquery)
        assert out == {
            "mv_live_sales": {"server": "srv", "database": "Sales Model", "table": "Live_Sales"}
        }

    def test_empty_when_no_live_connections(self):
        specs = {"Fact_OTC": SimpleNamespace(view_name="mv_otc")}
        assert derive_live_connections(specs, {"Fact_OTC": 'Sql.Database("a","b")'}) == {}
        assert derive_live_connections(specs, None) == {}
        assert derive_live_connections({}, {}) == {}
