import unittest
import os
import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch, PropertyMock
from datetime import datetime

# Use relative imports that will work with the project structure
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.engines.crewai.tools.custom.databricks_custom_tool import (
    DatabricksCustomTool, 
    DatabricksCustomToolSchema
)


class TestDatabricksCustomToolSchema(unittest.TestCase):
    """Unit tests for DatabricksCustomToolSchema"""

    def test_valid_schema(self):
        """Test creating a valid schema"""
        schema = DatabricksCustomToolSchema(
            query="SELECT * FROM table",
            catalog="my_catalog",
            db_schema="my_schema",
            warehouse_id="warehouse123",
            row_limit=100
        )
        
        self.assertEqual(schema.query, "SELECT * FROM table LIMIT 100;")
        self.assertEqual(schema.catalog, "my_catalog")
        self.assertEqual(schema.db_schema, "my_schema")
        self.assertEqual(schema.warehouse_id, "warehouse123")
        self.assertEqual(schema.row_limit, 100)

    def test_schema_with_existing_limit(self):
        """Test schema when query already has LIMIT clause"""
        schema = DatabricksCustomToolSchema(
            query="SELECT * FROM table LIMIT 50",
            row_limit=100
        )
        
        # Should not add another LIMIT
        self.assertEqual(schema.query, "SELECT * FROM table LIMIT 50")

    def test_schema_empty_query_validation(self):
        """Test schema validation with empty query"""
        with self.assertRaises(ValueError) as cm:
            DatabricksCustomToolSchema(query="")
        
        self.assertIn("Query cannot be empty", str(cm.exception))

    def test_schema_whitespace_query_validation(self):
        """Test schema validation with whitespace-only query"""
        with self.assertRaises(ValueError) as cm:
            DatabricksCustomToolSchema(query="   ")
        
        self.assertIn("Query cannot be empty", str(cm.exception))

    def test_schema_default_values(self):
        """Test schema with default values"""
        schema = DatabricksCustomToolSchema(query="SELECT 1")
        
        self.assertEqual(schema.query, "SELECT 1 LIMIT 1000;")
        self.assertIsNone(schema.catalog)
        self.assertIsNone(schema.db_schema)
        self.assertIsNone(schema.warehouse_id)
        self.assertEqual(schema.row_limit, 1000)


class TestDatabricksCustomTool(unittest.TestCase):
    """Unit tests for DatabricksCustomTool"""

    def setUp(self):
        """Set up test environment"""
        # Set up environment variables for authentication
        os.environ["DATABRICKS_HOST"] = "https://test.databricks.com"
        os.environ["DATABRICKS_TOKEN"] = "test-token"
        
        self.tool = DatabricksCustomTool(
            default_catalog="test_catalog",
            default_schema="test_schema",
            default_warehouse_id="test_warehouse"
        )

    def tearDown(self):
        """Clean up after tests"""
        # Remove environment variables
        if "DATABRICKS_HOST" in os.environ:
            del os.environ["DATABRICKS_HOST"]
        if "DATABRICKS_TOKEN" in os.environ:
            del os.environ["DATABRICKS_TOKEN"]
        if "DATABRICKS_CONFIG_PROFILE" in os.environ:
            del os.environ["DATABRICKS_CONFIG_PROFILE"]

    def test_tool_initialization(self):
        """Test tool initialization with defaults"""
        self.assertEqual(self.tool.name, "Databricks SQL Query")
        self.assertEqual(self.tool.default_catalog, "test_catalog")
        self.assertEqual(self.tool.default_schema, "test_schema")
        self.assertEqual(self.tool.default_warehouse_id, "test_warehouse")
        self.assertIn("Execute SQL queries", self.tool.description)

    def test_credential_validation_with_profile(self):
        """Test credential validation with Databricks profile"""
        # Clean up direct auth
        del os.environ["DATABRICKS_HOST"]
        del os.environ["DATABRICKS_TOKEN"]
        
        # Set profile
        os.environ["DATABRICKS_CONFIG_PROFILE"] = "test-profile"
        
        # Should not raise exception
        tool = DatabricksCustomTool()
        self.assertIsNotNone(tool)

    def test_credential_validation_missing(self):
        """Test credential validation with missing credentials"""
        # Remove all credentials
        del os.environ["DATABRICKS_HOST"]
        del os.environ["DATABRICKS_TOKEN"]
        
        with self.assertRaises(ValueError) as cm:
            DatabricksCustomTool()
        
        self.assertIn("Databricks authentication credentials are required", str(cm.exception))

    @patch('databricks.sdk.WorkspaceClient')
    def test_workspace_client_property(self, mock_workspace_client_class):
        """Test workspace client property initialization"""
        mock_client_instance = MagicMock()
        mock_workspace_client_class.return_value = mock_client_instance
        
        # Access the property
        client = self.tool.workspace_client
        
        # Should create and cache the client
        self.assertEqual(client, mock_client_instance)
        mock_workspace_client_class.assert_called_once()
        
        # Second access should return cached client
        client2 = self.tool.workspace_client
        self.assertEqual(client2, mock_client_instance)
        # Still only called once
        mock_workspace_client_class.assert_called_once()

    def test_format_results_empty(self):
        """Test formatting empty results"""
        result = self.tool._format_results([])
        self.assertEqual(result, "Query returned no results.")

    def test_format_results_empty_rows(self):
        """Test formatting results with empty rows"""
        results = [{}]
        result = self.tool._format_results(results)
        self.assertEqual(result, "Query returned empty rows with no columns.")

    def test_format_results_single_row(self):
        """Test formatting single row results"""
        results = [
            {"id": 1, "name": "Test", "value": 100}
        ]
        
        formatted = self.tool._format_results(results)
        
        # Check that result contains expected data
        self.assertIn("id", formatted)
        self.assertIn("name", formatted)
        self.assertIn("value", formatted)
        self.assertIn("1", formatted)
        self.assertIn("Test", formatted)
        self.assertIn("100", formatted)
        self.assertIn("(1 row returned)", formatted)

    def test_format_results_multiple_rows(self):
        """Test formatting multiple rows"""
        results = [
            {"id": 1, "name": "Test1", "value": 100},
            {"id": 2, "name": "Test2", "value": 200},
            {"id": 3, "name": "Test3", "value": None}
        ]
        
        formatted = self.tool._format_results(results)
        
        # Check formatting
        self.assertIn("id", formatted)
        self.assertIn("Test1", formatted)
        self.assertIn("Test2", formatted)
        self.assertIn("Test3", formatted)
        self.assertIn("NULL", formatted)  # None should be displayed as NULL
        self.assertIn("(3 rows returned)", formatted)

    def test_format_results_with_long_values(self):
        """Test formatting results with long values"""
        results = [
            {"id": 1, "description": "This is a very long description that should be handled properly"},
            {"id": 2, "description": "Short"}
        ]
        
        formatted = self.tool._format_results(results)
        
        # Should handle variable width columns
        self.assertIn("This is a very long description", formatted)
        self.assertIn("Short", formatted)

    def test_run_without_databricks_sdk(self):
        """Test error when databricks-sdk is not installed"""
        with patch('databricks.sdk.WorkspaceClient', 
                   side_effect=ImportError("No module named 'databricks'")):
            
            # Clear cached client
            self.tool._workspace_client = None
            
            with self.assertRaises(ImportError) as cm:
                _ = self.tool.workspace_client
            
            self.assertIn("databricks-sdk", str(cm.exception))
            self.assertIn("uv add databricks-sdk", str(cm.exception))


    def test_type_checking_import(self):
        """Test that TYPE_CHECKING import guard works"""
        # This is mainly for coverage of the TYPE_CHECKING block
        from src.engines.crewai.tools.custom.databricks_custom_tool import TYPE_CHECKING
        self.assertFalse(TYPE_CHECKING)  # Should be False at runtime

    def test_format_results_no_column_data(self):
        """Test formatting when rows have no column data"""
        results = [{"col1": None, "col2": None}]
        formatted = self.tool._format_results(results)
        self.assertIn("NULL", formatted)
        self.assertIn("(1 row returned)", formatted)

    def test_format_results_empty_columns_case(self):
        """Test edge case where columns are empty"""
        # This tests the specific code path for empty columns
        results = [{}]
        formatted = self.tool._format_results(results)
        self.assertEqual(formatted, "Query returned empty rows with no columns.")

    def test_format_results_no_rows_no_columns(self):
        """Test specific case where result has rows but they're completely empty"""
        # Mock results where first row exists but has no keys (empty dict)
        results = [{}]  # Row exists but is empty
        formatted = self.tool._format_results(results)
        self.assertEqual(formatted, "Query returned empty rows with no columns.")


if __name__ == '__main__':
    unittest.main()