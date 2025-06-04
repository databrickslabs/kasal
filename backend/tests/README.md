# Backend Testing Suite

This directory contains the comprehensive testing suite for the Kasal backend application.

## Overview

The testing suite is organized into multiple layers to ensure thorough coverage:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions and workflows
- **Test Utilities**: Shared fixtures, mocks, and test helpers

## Directory Structure

```
tests/
├── README.md                    # This file
├── conftest.py                  # Global pytest configuration and fixtures
├── pytest.ini                  # Pytest settings and configuration
├── requirements-test.txt        # Test-specific dependencies
├── unit/                        # Unit tests
│   ├── test_execution_service.py
│   ├── test_crew_service.py
│   ├── test_flow_service.py
│   ├── test_agent_service.py
│   ├── test_execution_repository.py
│   ├── test_executions_router.py
│   ├── test_agent_generation_router.py
│   ├── test_crew_generation_router.py
│   ├── test_healthcheck_router.py
│   ├── test_item_service.py
│   ├── test_python_pptx_tool.py
│   └── test_tool_factory.py
└── integration/                 # Integration tests
    ├── __init__.py
    └── test_execution_workflow.py
```

## Running Tests

### Quick Start

```bash
# Run all tests
python run_tests.py

# Run only unit tests
python run_tests.py --type unit

# Run only integration tests
python run_tests.py --type integration

# Run with coverage report
python run_tests.py --coverage --html-coverage
```

### Using pytest directly

```bash
# Install test dependencies
pip install -r tests/requirements-test.txt

# Run all tests
pytest

# Run unit tests only
pytest tests/unit

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_execution_service.py

# Run tests with specific markers
pytest -m "not slow"
```

### Test Markers

- `unit`: Unit tests (automatically applied to tests in unit/ directory)
- `integration`: Integration tests (automatically applied to tests in integration/ directory)
- `slow`: Tests that take longer to run
- `asyncio`: Tests that use async/await

## Test Categories

### Unit Tests

Unit tests focus on testing individual components in isolation using mocks and stubs for dependencies.

**Coverage includes:**
- Service layer logic (ExecutionService, CrewService, FlowService, AgentService)
- Repository data access patterns
- API router request/response handling
- Business logic validation
- Error handling scenarios

**Key Files:**
- `test_execution_service.py`: Core execution logic
- `test_crew_service.py`: Crew management functionality
- `test_flow_service.py`: Flow creation and management
- `test_agent_service.py`: Agent configuration and lifecycle
- `test_execution_repository.py`: Database operations
- `test_executions_router.py`: API endpoint behavior

### Integration Tests

Integration tests verify that components work correctly together in realistic scenarios.

**Coverage includes:**
- End-to-end execution workflows
- API request/response cycles
- Database transaction handling
- Error propagation across layers
- Concurrent execution scenarios

**Key Files:**
- `test_execution_workflow.py`: Complete execution flows

## Writing Tests

### Test Structure

Follow the Arrange-Act-Assert pattern:

```python
@pytest.mark.asyncio
async def test_create_execution_success(self, mock_uow, mock_repository):
    # Arrange
    service = ExecutionService(mock_uow)
    execution_data = {"key": "value"}
    
    # Act
    result = await service.create(execution_data)
    
    # Assert
    assert result is not None
    mock_repository.create.assert_called_once()
```

### Async Testing

Use `pytest.mark.asyncio` for async tests:

```python
@pytest.mark.asyncio
async def test_async_function():
    result = await some_async_function()
    assert result == expected_value
```

### Mocking Dependencies

Use `unittest.mock` for mocking:

```python
from unittest.mock import AsyncMock, MagicMock, patch

@patch("src.services.some_service.SomeClass")
async def test_with_mock(mock_class):
    mock_instance = AsyncMock()
    mock_class.return_value = mock_instance
    # Test logic here
```

### Fixtures

Common fixtures are available in `conftest.py`:

```python
def test_something(mock_logger, sample_execution_data, fixed_datetime):
    # Use pre-configured fixtures
    pass
```

## Coverage Goals

- **Overall Coverage**: 80%+ (enforced by pytest configuration)
- **Service Layer**: 90%+
- **Repository Layer**: 85%+
- **API Layer**: 80%+

### Viewing Coverage

```bash
# Generate HTML coverage report
python run_tests.py --html-coverage

# Open coverage report
open tests/coverage_html/index.html
```

## Best Practices

### Test Naming

- Use descriptive test names: `test_create_execution_with_invalid_data`
- Group related tests in classes: `class TestExecutionService`
- Use consistent naming patterns

### Test Organization

- One test file per module/service
- Group tests logically within files
- Use fixtures for common setup
- Keep tests focused and atomic

### Mocking Strategy

- Mock external dependencies (databases, APIs, file system)
- Use real objects for simple data structures
- Mock at the boundary of the system under test
- Verify mock interactions when behavior matters

### Error Testing

- Test both success and failure scenarios
- Verify proper error messages and types
- Test edge cases and boundary conditions
- Include validation error scenarios

### Async Testing

- Use `pytest.mark.asyncio` for async tests
- Mock async dependencies with `AsyncMock`
- Test concurrent scenarios when applicable
- Verify proper cleanup in async contexts

## Continuous Integration

The test suite is designed to run in CI/CD environments:

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r tests/requirements-test.txt

# Run tests with coverage
python run_tests.py --coverage --type all

# Check coverage threshold
pytest --cov=src --cov-fail-under=80
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure the backend directory is in Python path
2. **Async Test Failures**: Check that `pytest-asyncio` is installed
3. **Mock Issues**: Verify mock paths match actual import paths
4. **Coverage Gaps**: Use `--cov-report=html` to identify untested code

### Debug Mode

Run tests with verbose output and stop on first failure:

```bash
pytest -v -x --tb=long
```

### Performance Issues

For slow tests, use markers to skip them during development:

```bash
pytest -m "not slow"
```

## Contributing

When adding new functionality:

1. Write tests before or alongside implementation
2. Ensure new tests follow existing patterns
3. Update this README if adding new test categories
4. Maintain or improve coverage percentage
5. Add appropriate markers for test categorization