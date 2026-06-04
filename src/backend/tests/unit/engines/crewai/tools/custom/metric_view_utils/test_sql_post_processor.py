"""Tests for SQL post-processor."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.sql_post_processor import SqlPostProcessor


@pytest.fixture
def processor():
    return SqlPostProcessor()


class TestStripAliases:
    def test_strips_from_alias(self, processor):
        sql = "SELECT t.col1, t.col2 FROM catalog.schema.table t WHERE t.col1 = 'x'"
        result = processor._strip_aliases(sql)
        assert 't.col1' not in result
        assert 'col1' in result


class TestFixSqlKeywords:
    def test_normalizes_casing(self):
        sql = "select col from table where col = 'x'"
        result = SqlPostProcessor._fix_sql_keywords(sql)
        assert 'SELECT' in result
        assert 'FROM' in result
        assert 'WHERE' in result


class TestRemoveDeadBranches:
    def test_removes_dead_or(self):
        sql = "WHERE (1 = 0 AND 1 = 0) OR col = 'val'"
        result = SqlPostProcessor._remove_dead_branches(sql)
        assert '1 = 0' not in result
        assert "col = 'val'" in result


class TestProcess:
    def test_full_pipeline(self, processor):
        sql = "select t.a, sum(t.b) as total from cat.sch.tbl t where t.x = 'y' GROUP BY t.a"
        result = processor.process(sql)
        assert 'SELECT' in result
        assert 'FROM' in result
