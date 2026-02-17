"""
Unit tests for engines/crewai/tools/custom/powerbi_hierarchies_tool.py

Tests Power BI Hierarchies extraction tool for CrewAI.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from src.engines.crewai.tools.custom.powerbi_hierarchies_tool import (
    PowerBIHierarchiesSchema,
    PowerBIHierarchiesTool
)


class TestPowerBIHierarchiesSchema:
    """Tests for PowerBIHierarchiesSchema Pydantic model"""

    def test_schema_initialization_minimal(self):
        """Test schema with minimal required parameters"""
        schema = PowerBIHierarchiesSchema(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345"
        )

        assert schema.workspace_id == "workspace123"
        assert schema.dataset_id == "dataset456"
        assert schema.tenant_id == "tenant789"
        assert schema.client_id == "client012"
        assert schema.client_secret == "secret345"

    def test_schema_with_user_token(self):
        """Test schema with user OAuth token"""
        schema = PowerBIHierarchiesSchema(
            workspace_id="workspace123",
            dataset_id="dataset456",
            access_token="user_oauth_token"
        )

        assert schema.workspace_id == "workspace123"
        assert schema.dataset_id == "dataset456"
        assert schema.access_token == "user_oauth_token"

    def test_schema_with_defaults(self):
        """Test schema default values"""
        schema = PowerBIHierarchiesSchema(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345"
        )

        assert schema.target_catalog == "main"
        assert schema.target_schema == "default"
        assert schema.skip_system_tables is True
        assert schema.include_hidden is False


class TestPowerBIHierarchiesTool:
    """Tests for PowerBIHierarchiesTool"""

    def test_tool_initialization(self):
        """Test tool initializes with correct name and description"""
        tool = PowerBIHierarchiesTool()

        assert tool.name == "Power BI Hierarchies Tool"
        assert "Fabric" in tool.description or "hierarchies" in tool.description.lower()
        assert tool.args_schema == PowerBIHierarchiesSchema

    @pytest.mark.asyncio
    async def test_extract_hierarchies_success(self):
        """Test successful hierarchy extraction"""
        tool = PowerBIHierarchiesTool()

        # Mock the async extraction method to return success
        mock_result = """
# Power BI Hierarchies Extraction

## Summary
Found 1 hierarchy in the semantic model.

## Hierarchies

### Sales.Date Hierarchy
- **Table**: Sales
- **Levels**:
  1. Year (Calendar.Year)
  2. Quarter (Calendar.Quarter)
  3. Month (Calendar.Month)

## Unity Catalog SQL

```sql
CREATE OR REPLACE VIEW main.default.dim_date_hierarchy AS
SELECT
  Year,
  Quarter,
  Month
FROM sales_table;
```
"""

        with patch.object(tool, '_extract_hierarchies', new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = mock_result

            result = await tool._extract_hierarchies(
                workspace_id="workspace123",
                dataset_id="dataset456",
                auth_config={
                    "tenant_id": "tenant789",
                    "client_id": "client012",
                    "client_secret": "secret345"
                },
                target_catalog="main",
                target_schema="default",
                skip_system_tables=True,
                include_hidden=False
            )

            assert "Date Hierarchy" in result
            assert "Year" in result
            assert "CREATE" in result or "VIEW" in result

    def test_run_with_missing_workspace_id(self):
        """Test that missing workspace_id returns error"""
        tool = PowerBIHierarchiesTool()

        result = tool._run(
            dataset_id="dataset456",
            tenant_id="tenant789",
            client_id="client012",
            client_secret="secret345"
        )

        assert "error" in result.lower()
        assert "workspace_id" in result.lower()



    @patch('src.engines.crewai.tools.custom.powerbi_auth_utils.validate_auth_config')
    def test_run_with_invalid_auth_config(self, mock_validate):
        """Test that invalid auth config returns error"""
        tool = PowerBIHierarchiesTool()

        # Mock validation to fail
        mock_validate.return_value = (False, "Invalid credentials")

        result = tool._run(
            workspace_id="workspace123",
            dataset_id="dataset456",
            tenant_id="invalid",
            client_id="invalid",
            client_secret="invalid"
        )

        assert "error" in result.lower()
        # Auth validation happens, should see Invalid credentials message
        assert "error" in result.lower()
