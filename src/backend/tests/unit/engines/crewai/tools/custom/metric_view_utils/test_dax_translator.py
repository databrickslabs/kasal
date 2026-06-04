"""Tests for DAX→SQL translator — core pattern matching."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.dax_translator import DaxTranslator


@pytest.fixture
def translator():
    return DaxTranslator()


class TestQuickReject:
    def test_format_rejected(self, translator):
        result = translator.translate(
            {'measure_name': 'fmt', 'dax_expression': 'FORMAT(Sales, "#,##0")', 'original_name': 'fmt'},
            'fact_test')
        assert result.is_translatable is False
        assert 'FORMAT' in result.skip_reason

    def test_color_rejected(self, translator):
        result = translator.translate(
            {'measure_name': 'KBI_Color', 'dax_expression': 'IF(x>0, "green", "red")', 'original_name': 'KBI_Color'},
            'fact_test')
        assert result.is_translatable is False
        assert 'Color' in result.skip_reason

    def test_isblank_blank_rejected(self, translator):
        result = translator.translate(
            {'measure_name': 'guard', 'dax_expression': 'IF(ISBLANK(x), BLANK(), x)', 'original_name': 'guard'},
            'fact_test')
        assert result.is_translatable is False

    def test_blank_placeholder(self, translator):
        result = translator.translate(
            {'measure_name': 'blank', 'dax_expression': 'BLANK()', 'original_name': 'blank'},
            'fact_test')
        assert result.is_translatable is False
        assert 'BLANK' in result.skip_reason

    def test_selectedvalue_switch(self, translator):
        result = translator.translate(
            {'measure_name': 'sw', 'dax_expression': 'SWITCH(SELECTEDVALUE(Table[Col]), "a", 1, 2)', 'original_name': 'sw'},
            'fact_test')
        assert result.is_translatable is False

    def test_empty_dax(self, translator):
        result = translator.translate(
            {'measure_name': 'empty', 'dax_expression': '', 'original_name': 'empty'},
            'fact_test')
        assert result.is_translatable is False


class TestSimpleSum:
    def test_basic_sum(self, translator):
        result = translator.translate(
            {'measure_name': 'total', 'dax_expression': 'SUM(Sales[Amount])', 'original_name': 'Total'},
            'fact_test')
        assert result.is_translatable is True
        assert result.sql_expr == 'SUM(source.Amount)'

    def test_calculate_sum(self, translator):
        result = translator.translate(
            {'measure_name': 'total', 'dax_expression': 'CALCULATE(SUM(Sales[Amount]))', 'original_name': 'Total'},
            'fact_test')
        assert result.is_translatable is True
        assert 'SUM(source.Amount)' in result.sql_expr


class TestSimpleSumx:
    def test_sumx_without_filter(self, translator):
        result = translator.translate(
            {'measure_name': 'total', 'dax_expression': 'SUMX(Sales, Sales[Amount])', 'original_name': 'Total'},
            'fact_test')
        assert result.is_translatable is True
        assert result.sql_expr == 'SUM(source.Amount)'


class TestDivide:
    def test_simple_divide(self, translator):
        result = translator.translate(
            {'measure_name': 'ratio', 'dax_expression': 'DIVIDE(SUM(T[A]), SUM(T[B]))', 'original_name': 'Ratio'},
            'fact_test')
        assert result.is_translatable is True
        assert 'NULLIF' in result.sql_expr
        assert 'SUM(source.A)' in result.sql_expr


class TestCountxFilter:
    def test_countx_filter(self, translator):
        result = translator.translate(
            {'measure_name': 'cnt', 'dax_expression': 'COUNTX(FILTER(Sales, Sales[Status]="Active"), Sales[ID])', 'original_name': 'Count'},
            'fact_test')
        assert result.is_translatable is True
        assert 'COUNT(source.ID)' in result.sql_expr
        assert 'FILTER' in result.sql_expr


class TestAveragexFilter:
    def test_averagex_filter(self, translator):
        result = translator.translate(
            {'measure_name': 'avg', 'dax_expression': 'AVERAGEX(FILTER(Sales, Sales[Type]="A"), Sales[Value])', 'original_name': 'Avg'},
            'fact_test')
        assert result.is_translatable is True
        assert 'AVG(source.Value)' in result.sql_expr


class TestDistinctCountNoBlank:
    def test_simple(self, translator):
        result = translator.translate(
            {'measure_name': 'dc', 'dax_expression': 'DISTINCTCOUNTNOBLANK(Sales[Customer])', 'original_name': 'DC'},
            'fact_test')
        assert result.is_translatable is True
        assert 'COUNT(DISTINCT source.Customer)' == result.sql_expr


class TestSameperiodlastyear:
    def test_spely_with_sumx_filter(self, translator):
        dax = 'CALCULATE(SUMX(FILTER(Sales, Sales[Type]="A"), Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))'
        result = translator.translate(
            {'measure_name': 'py', 'dax_expression': dax, 'original_name': 'PY'},
            'fact_test')
        assert result.is_translatable is True
        assert result.window_spec is not None
        assert result.window_spec['order'] == 'fiscper'

    def test_spely_simple_deferred(self, translator):
        dax = 'CALCULATE([Total], SAMEPERIODLASTYEAR(Cal[Date]))'
        result = translator.translate(
            {'measure_name': 'py', 'dax_expression': dax, 'original_name': 'PY'},
            'fact_test')
        assert result.is_translatable is False
        assert 'SAMEPERIODLASTYEAR' in result.skip_reason
