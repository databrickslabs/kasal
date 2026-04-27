"""Tests for DaxToSqlTranslatorTool."""
import json
import pytest
from unittest.mock import patch, MagicMock
from src.engines.crewai.tools.custom.dax_to_sql_translator_tool import DaxToSqlTranslatorTool


class TestDaxToSqlTranslatorTool:
    def test_initialization(self):
        tool = DaxToSqlTranslatorTool()
        assert tool.name == "DAX to SQL Translator"

    def test_translate_simple_sum(self):
        tool = DaxToSqlTranslatorTool()
        measures = [
            {'measure_name': 'total', 'dax_expression': 'SUM(Sales[Amount])', 'original_name': 'Total'}
        ]
        result = tool._run(dax_measures_json=json.dumps(measures), table_key='fact_test')
        data = json.loads(result)
        assert data['summary']['translated'] == 1
        assert data['results'][0]['sql_expr'] == 'SUM(source.Amount)'

    def test_translate_rejects_format(self):
        tool = DaxToSqlTranslatorTool()
        measures = [
            {'measure_name': 'fmt', 'dax_expression': 'FORMAT(x, "#,##0")', 'original_name': 'fmt'}
        ]
        result = tool._run(dax_measures_json=json.dumps(measures), table_key='fact_test')
        data = json.loads(result)
        assert data['summary']['untranslatable'] == 1

    def test_invalid_json(self):
        tool = DaxToSqlTranslatorTool()
        result = tool._run(dax_measures_json='not json')
        data = json.loads(result)
        assert 'error' in data

    def test_with_config(self):
        tool = DaxToSqlTranslatorTool(config_json='{}')
        measures = [
            {'measure_name': 'x', 'dax_expression': 'SUM(T[A])', 'original_name': 'X'}
        ]
        result = tool._run(dax_measures_json=json.dumps(measures), table_key='fact')
        data = json.loads(result)
        assert data['summary']['total'] == 1
