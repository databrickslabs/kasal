"""Tests for scan data enrichment — refresh policies + summarization."""
from src.engines.crewai.tools.custom.metric_view_utils.scan_data_parser import ScanDataParser


class TestRefreshPolicies:
    def test_refresh_policy_detected(self):
        parser = ScanDataParser()
        data = {
            "workspaces": [{"datasets": [{
                "name": "test",
                "tables": [{
                    "name": "fact",
                    "refreshPolicy": {"policyType": "basic", "incrementalPeriod": "P30D"},
                    "source": [{"expression": 'Value.NativeQuery(c, "SELECT 1", null, [EnableFolding=true])'}]
                }]
            }]}]
        }
        parser.parse(data)
        rp = parser.get_refresh_policy_tables()
        assert len(rp) == 1
        assert rp[0]['table_name'] == 'fact'

    def test_no_refresh_policy(self):
        parser = ScanDataParser()
        data = {"workspaces": [{"datasets": [{"name": "test", "tables": []}]}]}
        parser.parse(data)
        assert parser.get_refresh_policy_tables() == []


class TestSummarization:
    def test_no_summarize_detected(self):
        parser = ScanDataParser()
        data = {
            "workspaces": [{"datasets": [{
                "name": "test",
                "tables": [{
                    "name": "fact",
                    "columns": [
                        {"name": "id", "summarizeBy": "none"},
                        {"name": "amount", "summarizeBy": "sum"},
                    ],
                    "source": [{"expression": 'Value.NativeQuery(c, "SELECT 1", null, [EnableFolding=true])'}]
                }]
            }]}]
        }
        parser.parse(data)
        cols = parser.get_no_summarize_columns()
        assert len(cols) == 1
        assert cols[0]['column_name'] == 'id'

    def test_no_summarize_empty(self):
        parser = ScanDataParser()
        data = {"workspaces": [{"datasets": [{"name": "test", "tables": []}]}]}
        parser.parse(data)
        assert parser.get_no_summarize_columns() == []
