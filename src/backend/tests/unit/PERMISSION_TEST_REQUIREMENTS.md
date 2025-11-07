# Permission Test Requirements for Router Tests

This document outlines the permission test requirements for each router after implementing the three-tier authorization model.

## Summary of Permission Requirements

### Admin-Only Endpoints

**models_router.py**
- POST /models - Create model configuration
- PUT /models/{key} - Update model configuration
- DELETE /models/{key} - Delete model configuration
- PATCH /models/{key}/toggle - Toggle model enabled status
- POST /models/enable-all - Enable all models
- POST /models/disable-all - Disable all models

**mcp_router.py**
- POST /mcp/servers - Create MCP server
- PUT /mcp/servers/{id} - Update MCP server
- DELETE /mcp/servers/{id} - Delete MCP server
- PATCH /mcp/servers/{id}/toggle-enabled - Toggle MCP server
- PATCH /mcp/servers/{id}/toggle-global-enabled - Toggle global status
- POST /mcp/test-connection - Test connection
- PUT /mcp/settings - Update global settings

**databricks_router.py**
- POST /databricks/config - Set Databricks configuration

**engine_config_router.py**
- POST /engine-config - Create engine configuration
- PUT /engine-config/engine/{name} - Update engine configuration
- DELETE /engine-config/engine/{name} - Delete engine configuration
- PATCH /engine-config/engine/{name}/toggle - Toggle engine
- PATCH /engine-config/engine/{name}/config/{key}/value - Update config value

**group_router.py** (already has permissions)
- All mutation operations

**database_management_router.py** (already has permissions)
- All mutation operations

### Admin and Editor Endpoints

**tools_router.py**
- POST /tools - Create tool
- PUT /tools/{id} - Update tool
- DELETE /tools/{id} - Delete tool
- PATCH /tools/{id}/toggle-enabled - Toggle tool

**memory_backend_router.py**
- POST /memory-backend/configs - Create memory config
- PUT /memory-backend/configs/{id} - Update memory config
- DELETE /memory-backend/configs/{id} - Delete memory config

**api_keys_router.py**
- POST /api-keys - Create API key
- PUT /api-keys/{name} - Update API key
- DELETE /api-keys/{name} - Delete API key

**executions_router.py**
- POST /executions/{id}/stop - Stop execution (admin/editor only)
- POST /executions/{id}/force-stop - Force stop execution (admin/editor only)
- POST /executions - Create execution (ALL ROLES allowed)

### No Restrictions (All Authenticated Users)

- All GET endpoints (read operations)
- POST /executions - Execute workflows (operators can execute)

## Test Implementation Checklist

For each router test file, add the following:

### 1. Update Fixtures

Add role-specific group context fixtures:
```python
@pytest.fixture
def mock_group_context():
    """Default admin context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="test@example.com",
        email_domain="example.com",
        user_id="user-123",
        user_role="admin"
    )

@pytest.fixture
def mock_group_context_editor():
    """Editor role context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="editor@example.com",
        email_domain="example.com",
        user_id="user-456",
        user_role="editor"
    )

@pytest.fixture
def mock_group_context_operator():
    """Operator role context."""
    return GroupContext(
        group_ids=["group-123"],
        group_email="operator@example.com",
        email_domain="example.com",
        user_id="user-789",
        user_role="operator"
    )
```

### 2. Add Permission Tests

For admin-only endpoints, add tests for editor and operator forbidden:
```python
def test_create_forbidden_editor(self, app, mock_group_context_editor, ...):
    """Test that editors cannot create."""
    # Override context and test 403 response

def test_create_forbidden_operator(self, app, mock_group_context_operator, ...):
    """Test that operators cannot create."""
    # Override context and test 403 response
```

For admin/editor endpoints, add test for operator forbidden:
```python
def test_create_forbidden_operator(self, app, mock_group_context_operator, ...):
    """Test that operators cannot create."""
    # Override context and test 403 response
```

### 3. Verify Error Messages

Ensure the error messages match what's implemented in the routers:
- Admin-only: "Only admins can {action} {resource}"
- Admin/Editor: "Only admins and editors can {action} {resource}"

## Test Files to Update

1. ✅ `/tests/unit/models/test_models_router.py` - Admin-only permissions
2. ⏳ `/tests/unit/router/test_tools_router.py` - Admin/Editor permissions
3. ⏳ `/tests/unit/router/test_mcp_router.py` - Admin-only permissions
4. ⏳ `/tests/unit/router/test_databricks_router.py` - Admin-only permissions
5. ⏳ `/tests/unit/router/test_engine_config_router.py` - Admin-only permissions
6. ⏳ `/tests/unit/router/test_executions_router.py` - Admin/Editor for stop only
7. ⏳ `/tests/unit/router/test_memory_backend_router.py` - Admin/Editor permissions
8. ⏳ `/tests/unit/router/test_api_keys_router.py` - Admin/Editor permissions

## Running Tests

After updating, run the tests to ensure they pass:

```bash
# Run all router tests
pytest tests/unit/router/ -v

# Run specific router test
pytest tests/unit/router/test_models_router.py -v

# Run with coverage
pytest tests/unit/router/ --cov=src.api --cov-report=html
```