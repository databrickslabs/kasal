"""Tests for deploy_test.py — UCMV deploy smoke test script.

All tests are mocked — no live Databricks workspace needed.
"""
import os
import sys
import textwrap
from unittest.mock import MagicMock, patch

import pytest
import yaml

# Add examples directory to path so we can import deploy_test
_examples_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '..', '..', '..',
                 '..', '..', 'examples', 'uc_metric_view_migration')
)
sys.path.insert(0, _examples_dir)

import deploy_test  # noqa: E402


# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_YAML = textwrap.dedent("""\
    version: '1.1'

    source: |-
      SELECT * FROM cat.sch.my_table

    dimensions:
      - name: region
        expr: source.region
        comment: Region

    measures:
      - name: total_sales
        expr: SUM(source.sales)
        comment: Total sales
      - name: avg_price
        expr: AVG(source.price)
        comment: Average price
""")

EMPTY_YAML = textwrap.dedent("""\
    version: '1.1'
    source: |-
      SELECT 1
""")


@pytest.fixture
def yaml_file(tmp_path):
    """Write sample YAML to a temp file and return its path."""
    p = tmp_path / 'Test_Table_uc_metric_view.yml'
    p.write_text(SAMPLE_YAML)
    return str(p)


@pytest.fixture
def empty_yaml_file(tmp_path):
    """Write YAML with no measures to a temp file."""
    p = tmp_path / 'Empty_Table_uc_metric_view.yml'
    p.write_text(EMPTY_YAML)
    return str(p)


@pytest.fixture
def mock_client():
    """Create a mock Databricks WorkspaceClient."""
    client = MagicMock()
    client.api_client = MagicMock()
    client.statement_execution = MagicMock()
    return client


# ── Test parse_yaml_measures ──────────────────────────────────────────────────

class TestParseYamlMeasures:
    def test_extracts_measure_names(self, yaml_file):
        """Extracts measure names from a well-formed YAML."""
        measures, data = deploy_test.parse_yaml_measures(yaml_file)
        assert measures == ['total_sales', 'avg_price']
        assert data['version'] == '1.1'
        assert len(data['measures']) == 2

    def test_empty_measures(self, empty_yaml_file):
        """Returns empty list when YAML has no measures key."""
        measures, data = deploy_test.parse_yaml_measures(empty_yaml_file)
        assert measures == []
        assert data is not None
        assert data.get('version') == '1.1'

    def test_invalid_yaml(self, tmp_path):
        """Handles malformed YAML by raising an error."""
        bad_file = tmp_path / 'bad_uc_metric_view.yml'
        bad_file.write_text(': : : not valid yaml [[[')
        with pytest.raises(yaml.YAMLError):
            deploy_test.parse_yaml_measures(str(bad_file))


# ── Test deploy_view ──────────────────────────────────────────────────────────

class TestDeployView:
    def test_success(self, mock_client):
        """Successful deploy calls POST and returns 'deployed' status."""
        mock_client.api_client.do.return_value = {'name': 'cat.sch.mv_test'}

        result = deploy_test.deploy_view(
            mock_client, 'cat', 'sch', 'mv_test', SAMPLE_YAML, 'wh123',
        )

        assert result['status'] == 'deployed'
        assert result['view_name'] == 'cat.sch.mv_test'
        mock_client.api_client.do.assert_called_once_with(
            'POST',
            '/api/2.0/unity-catalog/metric-views',
            body={
                'name': 'cat.sch.mv_test',
                'yaml_body': SAMPLE_YAML,
            },
        )

    def test_conflict_triggers_update(self, mock_client):
        """409 ALREADY_EXISTS triggers a PUT update."""
        mock_client.api_client.do.side_effect = [
            Exception('409 ALREADY_EXISTS: view already exists'),
            {'name': 'cat.sch.mv_test'},  # PUT succeeds
        ]

        result = deploy_test.deploy_view(
            mock_client, 'cat', 'sch', 'mv_test', SAMPLE_YAML, 'wh123',
        )

        assert result['status'] == 'updated'
        assert result['view_name'] == 'cat.sch.mv_test'
        assert mock_client.api_client.do.call_count == 2
        # Second call should be PUT
        second_call = mock_client.api_client.do.call_args_list[1]
        assert second_call[0][0] == 'PUT'

    def test_error(self, mock_client):
        """Non-409 errors return error status."""
        mock_client.api_client.do.side_effect = Exception('500 Internal Server Error')

        result = deploy_test.deploy_view(
            mock_client, 'cat', 'sch', 'mv_test', SAMPLE_YAML, 'wh123',
        )

        assert result['status'] == 'error'
        assert '500' in result['error']


# ── Test test_measure ─────────────────────────────────────────────────────────

class TestTestMeasure:
    def test_success(self, mock_client):
        """Successful measure execution returns 'ok' with value."""
        mock_stmt = MagicMock()
        mock_stmt.status.state = 'SUCCEEDED'
        mock_stmt.result.data_array = [['42.5']]
        mock_client.statement_execution.execute_statement.return_value = mock_stmt

        with patch('deploy_test.StatementState', create=True) as mock_state_cls:
            # Make the comparison work: stmt.status.state == StatementState.SUCCEEDED
            mock_state_cls.SUCCEEDED = 'SUCCEEDED'
            # We need to patch it in the module namespace
            deploy_test_module = sys.modules['deploy_test']
            # Import and patch StatementState within test_measure's scope
            with patch.dict('sys.modules', {'databricks.sdk.service.sql': MagicMock(StatementState=MagicMock(SUCCEEDED='SUCCEEDED'))}):
                # Re-import to pick up the patched module
                result = self._call_test_measure(mock_client, mock_stmt)

        assert result['status'] == 'ok'
        assert result['value'] == '42.5'

    def test_error(self, mock_client):
        """Execution failure returns 'error' with message."""
        mock_client.statement_execution.execute_statement.side_effect = \
            Exception('WAREHOUSE_NOT_FOUND: warehouse wh123 not found')

        with patch.dict('sys.modules', {'databricks.sdk.service.sql': MagicMock(StatementState=MagicMock(SUCCEEDED='SUCCEEDED'))}):
            result = deploy_test.test_measure(
                mock_client, 'cat', 'sch', 'mv_test', 'total_sales', 'wh123',
            )

        assert result['status'] == 'error'
        assert 'WAREHOUSE_NOT_FOUND' in result['error']

    def _call_test_measure(self, mock_client, mock_stmt):
        """Helper to call test_measure with properly mocked StatementState."""
        # Create a mock module for databricks.sdk.service.sql
        mock_sql_module = MagicMock()
        mock_sql_module.StatementState.SUCCEEDED = 'SUCCEEDED'

        with patch.dict('sys.modules', {'databricks.sdk.service.sql': mock_sql_module}):
            return deploy_test.test_measure(
                mock_client, 'cat', 'sch', 'mv_test', 'total_sales', 'wh123',
            )
