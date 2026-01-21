"""
Unit tests for converters/common/transformers/yaml.py

Tests YAML KPI definition parsing.
"""

import pytest
import yaml
from pathlib import Path
from src.converters.common.transformers.yaml import YAMLKPIParser
from src.converters.base.models import KPI, Structure, KPIDefinition


class TestYAMLKPIParser:
    """Tests for YAMLKPIParser class"""

    @pytest.fixture
    def parser(self):
        """Create YAMLKPIParser instance for testing"""
        return YAMLKPIParser()

    @pytest.fixture
    def simple_yaml_data(self):
        """Simple KPI definition YAML data"""
        return {
            'description': 'Test KPI Definition',
            'technical_name': 'test_def',
            'kbi': [
                {
                    'description': 'Total Sales',
                    'technical_name': 'total_sales',
                    'formula': 'SUM(sales.amount)',
                    'source_table': 'sales'
                }
            ]
        }

    @pytest.fixture
    def complex_yaml_data(self):
        """Complex KPI definition with structures and filters"""
        return {
            'description': 'Revenue Metrics',
            'technical_name': 'revenue_metrics',
            'default_variables': {
                'year': 2024,
                'region': 'Global'
            },
            'filters': {
                'query_filter': {
                    'active_only': 'status = "active"'
                }
            },
            'structures': {
                'YTD': {
                    'description': 'Year to Date',
                    'filter': ['fiscyear = $year', 'fiscper3 < $period'],
                    'display_sign': 1
                },
                'PY': {
                    'description': 'Prior Year',
                    'filter': ['fiscyear = $year - 1'],
                    'display_sign': 1
                }
            },
            'kbi': [
                {
                    'description': 'Revenue',
                    'technical_name': 'revenue',
                    'formula': 'SUM(sales.revenue)',
                    'source_table': 'sales',
                    'filter': ['region = $region'],
                    'apply_structures': ['YTD', 'PY']
                }
            ]
        }

    @pytest.fixture
    def temp_yaml_file(self, tmp_path, simple_yaml_data):
        """Create temporary YAML file for testing"""
        yaml_file = tmp_path / "test_kpi.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(simple_yaml_data, f)
        return yaml_file

    @pytest.fixture
    def temp_yaml_directory(self, tmp_path):
        """Create temporary directory with multiple YAML files"""
        # Create first file
        yaml_data_1 = {
            'description': 'Sales Metrics',
            'technical_name': 'sales_metrics',
            'kbi': [
                {
                    'description': 'Total Sales',
                    'technical_name': 'total_sales',
                    'formula': 'SUM(sales.amount)'
                }
            ]
        }
        yaml_file_1 = tmp_path / "sales.yaml"
        with open(yaml_file_1, 'w', encoding='utf-8') as f:
            yaml.dump(yaml_data_1, f)

        # Create second file with .yml extension
        yaml_data_2 = {
            'description': 'Cost Metrics',
            'technical_name': 'cost_metrics',
            'kbi': [
                {
                    'description': 'Total Cost',
                    'technical_name': 'total_cost',
                    'formula': 'SUM(costs.amount)'
                }
            ]
        }
        yaml_file_2 = tmp_path / "costs.yml"
        with open(yaml_file_2, 'w', encoding='utf-8') as f:
            yaml.dump(yaml_data_2, f)

        return tmp_path

    # ========== Initialization Tests ==========

    def test_parser_initialization(self, parser):
        """Test YAMLKPIParser initializes with empty definitions list"""
        assert parser.parsed_definitions == []
        assert isinstance(parser.parsed_definitions, list)

    # ========== parse_file Tests ==========

    def test_parse_file_success(self, parser, temp_yaml_file):
        """Test parsing a valid YAML file"""
        definition = parser.parse_file(temp_yaml_file)

        assert isinstance(definition, KPIDefinition)
        assert definition.description == 'Test KPI Definition'
        assert definition.technical_name == 'test_def'
        assert len(definition.kpis) == 1

    def test_parse_file_kpi_details(self, parser, temp_yaml_file):
        """Test parsed KPI has correct details"""
        definition = parser.parse_file(temp_yaml_file)

        kpi = definition.kpis[0]
        assert kpi.description == 'Total Sales'
        assert kpi.technical_name == 'total_sales'
        assert kpi.formula == 'SUM(sales.amount)'
        assert kpi.source_table == 'sales'

    def test_parse_file_not_found(self, parser):
        """Test parsing non-existent file raises FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            parser.parse_file("/nonexistent/path/file.yaml")

    def test_parse_file_accepts_path_object(self, parser, temp_yaml_file):
        """Test parse_file accepts Path object"""
        path = Path(temp_yaml_file)
        definition = parser.parse_file(path)

        assert isinstance(definition, KPIDefinition)

    def test_parse_file_accepts_string_path(self, parser, temp_yaml_file):
        """Test parse_file accepts string path"""
        definition = parser.parse_file(str(temp_yaml_file))

        assert isinstance(definition, KPIDefinition)

    # ========== parse_directory Tests ==========

    def test_parse_directory_success(self, parser, temp_yaml_directory):
        """Test parsing directory with multiple YAML files"""
        definitions = parser.parse_directory(temp_yaml_directory)

        assert len(definitions) == 2
        assert all(isinstance(d, KPIDefinition) for d in definitions)

    def test_parse_directory_both_extensions(self, parser, temp_yaml_directory):
        """Test parsing directory handles both .yaml and .yml files"""
        definitions = parser.parse_directory(temp_yaml_directory)

        technical_names = {d.technical_name for d in definitions}
        assert 'sales_metrics' in technical_names
        assert 'cost_metrics' in technical_names

    def test_parse_directory_stores_definitions(self, parser, temp_yaml_directory):
        """Test parsed definitions are stored in parser"""
        definitions = parser.parse_directory(temp_yaml_directory)

        assert parser.parsed_definitions == definitions
        assert len(parser.parsed_definitions) == 2

    def test_parse_directory_not_found(self, parser):
        """Test parsing non-existent directory raises NotADirectoryError"""
        with pytest.raises(NotADirectoryError):
            parser.parse_directory("/nonexistent/directory")

    def test_parse_directory_empty(self, parser, tmp_path):
        """Test parsing empty directory returns empty list"""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        definitions = parser.parse_directory(empty_dir)

        assert definitions == []
        assert parser.parsed_definitions == []

    # ========== _parse_yaml_data Tests ==========

    def test_parse_yaml_data_simple(self, parser, simple_yaml_data):
        """Test parsing simple YAML data"""
        definition = parser._parse_yaml_data(simple_yaml_data)

        assert definition.description == 'Test KPI Definition'
        assert definition.technical_name == 'test_def'
        assert len(definition.kpis) == 1

    def test_parse_yaml_data_with_structures(self, parser, complex_yaml_data):
        """Test parsing YAML data with structures"""
        definition = parser._parse_yaml_data(complex_yaml_data)

        assert definition.structures is not None
        assert 'YTD' in definition.structures
        assert 'PY' in definition.structures

        ytd_structure = definition.structures['YTD']
        assert isinstance(ytd_structure, Structure)
        assert ytd_structure.description == 'Year to Date'

    def test_parse_yaml_data_with_query_filters(self, parser, complex_yaml_data):
        """Test parsing YAML data with query filters"""
        definition = parser._parse_yaml_data(complex_yaml_data)

        assert len(definition.query_filters) == 1
        assert definition.query_filters[0].name == 'active_only'
        assert definition.query_filters[0].expression == 'status = "active"'

    def test_parse_yaml_data_with_default_variables(self, parser, complex_yaml_data):
        """Test parsing YAML data with default variables"""
        definition = parser._parse_yaml_data(complex_yaml_data)

        assert definition.default_variables is not None
        assert definition.default_variables['year'] == 2024
        assert definition.default_variables['region'] == 'Global'

    def test_parse_yaml_data_kpi_with_structures(self, parser, complex_yaml_data):
        """Test KPI applies structures from YAML"""
        definition = parser._parse_yaml_data(complex_yaml_data)

        kpi = definition.kpis[0]
        assert kpi.apply_structures is not None
        assert 'YTD' in kpi.apply_structures
        assert 'PY' in kpi.apply_structures

    def test_parse_yaml_data_kpi_with_filters(self, parser, complex_yaml_data):
        """Test KPI has filters from YAML"""
        definition = parser._parse_yaml_data(complex_yaml_data)

        kpi = definition.kpis[0]
        assert kpi.filters is not None
        assert 'region = $region' in kpi.filters

    # ========== get_all_kbis Tests ==========

    def test_get_all_kbis_empty(self, parser):
        """Test get_all_kbis returns empty list when no definitions"""
        all_kbis = parser.get_all_kbis()

        assert all_kbis == []

    def test_get_all_kbis_single_definition(self, parser, temp_yaml_file):
        """Test get_all_kbis with single definition"""
        parser.parse_file(temp_yaml_file)
        parser.parsed_definitions = [parser.parse_file(temp_yaml_file)]

        all_kbis = parser.get_all_kbis()

        assert len(all_kbis) == 1
        definition, kpi = all_kbis[0]
        assert isinstance(definition, KPIDefinition)
        assert isinstance(kpi, KPI)

    def test_get_all_kbis_multiple_definitions(self, parser, temp_yaml_directory):
        """Test get_all_kbis with multiple definitions"""
        parser.parse_directory(temp_yaml_directory)

        all_kbis = parser.get_all_kbis()

        # Should have 2 KBIs (one from each file)
        assert len(all_kbis) == 2

        # Each item should be a tuple of (definition, kpi)
        for definition, kpi in all_kbis:
            assert isinstance(definition, KPIDefinition)
            assert isinstance(kpi, KPI)

    def test_get_all_kbis_preserves_relationships(self, parser, temp_yaml_directory):
        """Test get_all_kbis preserves definition-KBI relationships"""
        parser.parse_directory(temp_yaml_directory)

        all_kbis = parser.get_all_kbis()

        # Check that each KBI comes from its parent definition
        for definition, kpi in all_kbis:
            assert kpi in definition.kpis

    # ========== Integration Tests ==========

    def test_full_workflow_simple(self, parser, temp_yaml_file):
        """Test complete workflow: parse file → get KBIs"""
        # Parse file
        definition = parser.parse_file(temp_yaml_file)

        # Store in parser
        parser.parsed_definitions = [definition]

        # Get all KBIs
        all_kbis = parser.get_all_kbis()

        assert len(all_kbis) == 1
        def_from_kbis, kpi = all_kbis[0]
        assert def_from_kbis.technical_name == 'test_def'
        assert kpi.technical_name == 'total_sales'

    def test_full_workflow_directory(self, parser, temp_yaml_directory):
        """Test complete workflow: parse directory → get all KBIs"""
        # Parse directory
        _ = parser.parse_directory(temp_yaml_directory)

        # Definitions should be stored automatically
        assert len(parser.parsed_definitions) == 2

        # Get all KBIs
        all_kbis = parser.get_all_kbis()

        assert len(all_kbis) == 2

        # Check KBIs are from correct definitions
        kbi_names = {kpi.technical_name for _, kpi in all_kbis}
        assert 'total_sales' in kbi_names
        assert 'total_cost' in kbi_names

    def test_parse_kpi_with_all_fields(self, parser, tmp_path):
        """Test parsing KPI with all optional fields"""
        yaml_data = {
            'description': 'Complex KPI',
            'technical_name': 'complex',
            'kbi': [
                {
                    'description': 'Weighted Average',
                    'technical_name': 'weighted_avg',
                    'formula': 'AVG(amount)',
                    'source_table': 'transactions',
                    'aggregation_type': 'AVG',
                    'weight_column': 'weight',
                    'target_column': 'target',
                    'percentile': 95,
                    'exceptions': [{'name': 'exception1', 'value': '1'}, {'name': 'exception2', 'value': '2'}],
                    'exception_aggregation': 'SUM',
                    'fields_for_exception_aggregation': ['field1', 'field2'],
                    'fields_for_constant_selection': ['const1', 'const2'],
                    'display_sign': -1
                }
            ]
        }

        yaml_file = tmp_path / "complex.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(yaml_data, f)

        definition = parser.parse_file(yaml_file)
        kpi = definition.kpis[0]

        # Verify all fields were parsed correctly
        assert kpi.description == 'Weighted Average'
        assert kpi.technical_name == 'weighted_avg'
        assert kpi.formula == 'AVG(amount)'
        assert kpi.source_table == 'transactions'
        assert kpi.aggregation_type == 'AVG'
        assert kpi.weight_column == 'weight'
        assert kpi.target_column == 'target'
        assert kpi.percentile == 95
        assert kpi.exceptions == [{'name': 'exception1', 'value': '1'}, {'name': 'exception2', 'value': '2'}]
        assert kpi.exception_aggregation == 'SUM'
        assert kpi.fields_for_exception_aggregation == ['field1', 'field2']
        assert kpi.fields_for_constant_selection == ['const1', 'const2']
        assert kpi.display_sign == -1

    def test_parse_structure_with_formula(self, parser, tmp_path):
        """Test parsing structure with formula (calculated measure)"""
        yaml_data = {
            'description': 'Test Definition',
            'technical_name': 'test_def',
            'structures': {
                'CALCULATED': {
                    'description': 'Calculated Structure',
                    'formula': '[base_measure] * 1.1',
                    'display_sign': 1
                }
            },
            'kbi': []
        }

        yaml_file = tmp_path / "calculated.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(yaml_data, f)

        definition = parser.parse_file(yaml_file)

        calc_structure = definition.structures['CALCULATED']
        assert calc_structure.formula == '[base_measure] * 1.1'
        assert calc_structure.description == 'Calculated Structure'

    def test_parse_handles_missing_optional_fields(self, parser, tmp_path):
        """Test parsing handles missing optional fields gracefully"""
        yaml_data = {
            'description': 'Minimal Definition',
            'technical_name': 'minimal',
            'kbi': [
                {
                    'description': 'Minimal KPI',
                    'formula': 'SUM(amount)'
                    # Missing technical_name and many optional fields
                }
            ]
        }

        yaml_file = tmp_path / "minimal.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(yaml_data, f)

        # Should not raise an error
        definition = parser.parse_file(yaml_file)

        assert definition.description == 'Minimal Definition'
        assert len(definition.kpis) == 1

    def test_parse_empty_kbi_list(self, parser, tmp_path):
        """Test parsing definition with empty KBI list"""
        yaml_data = {
            'description': 'Empty Definition',
            'technical_name': 'empty',
            'kbi': []
        }

        yaml_file = tmp_path / "empty.yaml"
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(yaml_data, f)

        definition = parser.parse_file(yaml_file)

        assert definition.kpis == []
