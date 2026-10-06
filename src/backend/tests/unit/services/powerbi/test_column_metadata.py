"""Regression coverage for column_metadata.py's INFO.VIEW.COLUMNS() query."""

from unittest.mock import MagicMock, patch

from src.services.powerbi import column_metadata as cm


def _resp(status=200, rows=None):
    resp = MagicMock(status_code=status)
    resp.json.return_value = {
        "results": [{"tables": [{"rows": rows or []}]}]
    }
    return resp


class TestExtractColumnSummarizeBy:
    def test_parses_rows_into_nested_table_column_dict(self):
        rows = [
            {"[Table]": "Fact_OTC", "[Column]": "fltp", "[SummarizeBy]": "None"},
            {"[Table]": "Fact_NPS", "[Column]": "nps_value", "[SummarizeBy]": "Sum"},
        ]
        with patch.object(cm, "requests") as rq:
            rq.post.return_value = _resp(rows=rows)
            result = cm.extract_column_summarize_by("tok", "ws", "ds1")
        assert result == {
            "Fact_OTC": {"fltp": "None"},
            "Fact_NPS": {"nps_value": "Sum"},
        }

    def test_tolerates_unbracketed_column_keys(self):
        """The Execute Queries API returns keys either bracketed or not,
        depending on the query/permission path — same tolerance as
        pipeline_config._row_get."""
        rows = [{"Table": "Fact_OTC", "Column": "amount", "SummarizeBy": "Sum"}]
        with patch.object(cm, "requests") as rq:
            rq.post.return_value = _resp(rows=rows)
            result = cm.extract_column_summarize_by("tok", "ws", "ds1")
        assert result == {"Fact_OTC": {"amount": "Sum"}}

    def test_missing_summarize_by_defaults_to_none(self):
        rows = [{"[Table]": "Fact_OTC", "[Column]": "amount"}]
        with patch.object(cm, "requests") as rq:
            rq.post.return_value = _resp(rows=rows)
            result = cm.extract_column_summarize_by("tok", "ws", "ds1")
        assert result == {"Fact_OTC": {"amount": "None"}}

    def test_rows_missing_table_or_column_are_skipped(self):
        rows = [
            {"[Table]": "", "[Column]": "amount", "[SummarizeBy]": "Sum"},
            {"[Table]": "Fact_OTC", "[Column]": "", "[SummarizeBy]": "Sum"},
        ]
        with patch.object(cm, "requests") as rq:
            rq.post.return_value = _resp(rows=rows)
            result = cm.extract_column_summarize_by("tok", "ws", "ds1")
        assert result == {}

    def test_non_200_response_returns_empty_dict(self):
        with patch.object(cm, "requests") as rq:
            rq.post.return_value = _resp(status=401)
            result = cm.extract_column_summarize_by("tok", "ws", "ds1")
        assert result == {}

    def test_exception_returns_empty_dict_fail_open(self):
        with patch.object(cm, "requests") as rq:
            rq.post.side_effect = Exception("network unreachable")
            result = cm.extract_column_summarize_by("tok", "ws", "ds1")
        assert result == {}
