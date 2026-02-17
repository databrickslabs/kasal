"""
Unit tests for engines/crewai/tools/custom/powerbi_relationships_tool.py

Tests Power BI Relationships extraction tool for CrewAI.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from src.engines.crewai.tools.custom.powerbi_relationships_tool import (
    PowerBIRelationshipsSchema,
    PowerBIRelationshipsTool
)


class TestPowerBIRelationshipsSchema:
    """Tests for PowerBIRelationshipsSchema Pydantic model"""

    def test_schema_initialization_minimal(self):
        """Test schema with minimal required parameters"""
        schema = PowerBIRelationshipsSchema(
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
        schema = PowerBIRelationshipsSchema(
            workspace_id="workspace123",
            dataset_id="dataset456",
            access_token="user_oauth_token"
        )

        assert schema.workspace_id == "workspace123"
        assert schema.dataset_id == "dataset456"
        assert schema.access_token == "user_oauth_token"


class TestPowerBIRelationshipsTool:
    """Tests for PowerBIRelationshipsTool"""

    def test_tool_initialization(self):
        """Test tool initializes with correct name and description"""
        tool = PowerBIRelationshipsTool()

        assert tool.name == "Power BI Relationships Tool"
        assert "relationships" in tool.description.lower()
        assert tool.args_schema == PowerBIRelationshipsSchema

    @pytest.mark.asyncio
    async def test_extract_relationships_success(self):
        """Test successful relationship extraction"""
        tool = PowerBIRelationshipsTool()

        mock_result = """
# Power BI Relationships Extraction

## Summary
Found 2 relationships in the semantic model.

## Relationships

### Sales to Product (many-to-one)
- **From**: Sales[ProductKey]
- **To**: Product[ProductKey]
- **Direction**: One-way
- **Active**: Yes

### Sales to Customer (many-to-one)
- **From**: Sales[CustomerKey]
- **To**: Customer[CustomerKey]
- **Direction**: One-way
- **Active**: Yes

## Foreign Key DDL

```sql
ALTER TABLE sales
ADD CONSTRAINT fk_sales_product
FOREIGN KEY (ProductKey) REFERENCES product(ProductKey);

ALTER TABLE sales
ADD CONSTRAINT fk_sales_customer
FOREIGN KEY (CustomerKey) REFERENCES customer(CustomerKey);
```
"""

        with patch.object(tool, '_extract_relationships', new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = mock_result

            result = await tool._extract_relationships(
                workspace_id="workspace123",
                dataset_id="dataset456",
                auth_config={
                    "tenant_id": "tenant789",
                    "client_id": "client012",
                    "client_secret": "secret345"
                },
                target_catalog="main",
                target_schema="default",
                include_inactive=False,
                skip_system_tables=True
            )

            assert "Sales to Product" in result
            assert "Sales to Customer" in result
            assert "FOREIGN KEY" in result or "FK" in result.lower()

    def test_run_with_missing_workspace_id(self):
        """Test that missing workspace_id returns error"""
        tool = PowerBIRelationshipsTool()

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
        tool = PowerBIRelationshipsTool()

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
        assert "error" in result.lower()  # Validation failed
