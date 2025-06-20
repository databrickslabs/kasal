# End-to-End Tests

This directory contains end-to-end (E2E) tests for Kasal workflows, particularly focusing on the CrewAI input variable functionality.

## Test Structure

### `test_crewai_input_integration.py`
The main end-to-end integration test that makes actual API calls to test the complete CrewAI input workflow:

**What it tests:**
- Lists existing crews from the backend
- Detects crews with input variables (e.g., `{from}`, `{to}`, `{date}`)
- Creates new agents and tasks with input variables
- Builds crews with proper node/edge structure
- Executes crews with input parameters
- Monitors execution status in real-time
- Fetches and displays execution traces as they happen
- Shows final LLM-generated results

**Test scenarios:**
1. Find and execute an existing crew with input variables
2. Create a new crew from scratch and execute it
3. Monitor execution with real-time trace updates
4. Display the final CrewAI execution result

## Running the Tests

### Prerequisites
1. Ensure the backend is running:
   ```bash
   cd src/backend
   ./run.sh
   ```

2. Activate virtual environment:
   ```bash
   source venv/bin/activate
   ```

### Run the E2E Test
```bash
# Run directly (recommended for seeing real-time output)
cd src/backend/tests
python e2e/test_crewai_input_integration.py

# Or run with pytest
cd src/backend
python -m pytest tests/e2e/test_crewai_input_integration.py -v

# Run with output displayed
python -m pytest tests/e2e/test_crewai_input_integration.py -v -s
```

### Run with Coverage
```bash
python -m pytest tests/e2e/ --cov=src --cov-report=html
```

## Test Scenarios

### 1. Flight Search Workflow
Tests a single agent/task crew with input variables:
- Inputs: `{from}`, `{to}`, `{date}`
- Creates flight search agent and task
- Executes with sample inputs (Zurich → Montreal)
- Monitors execution traces
- Verifies completion

### 2. News Aggregation Workflow
Tests a multi-agent crew with sequential tasks:
- Inputs: `{topic}`, `{date}`
- Creates news fetcher and summarizer agents
- Creates fetch and summarize tasks
- Executes with sample inputs (AI Summit news)
- Verifies multi-agent coordination

### 3. Error Handling
Tests various failure scenarios:
- Missing input variables
- Invalid crew configurations
- Execution timeouts
- API errors

## Key Features Tested

1. **Input Variable Detection**
   - Extracts variables from agent goals, backstories
   - Extracts variables from task descriptions
   - Handles multiple occurrences of same variable

2. **Execution Payload Building**
   - Converts crew nodes to agents_yaml/tasks_yaml
   - Includes all required execution parameters
   - Maintains proper structure for backend

3. **Real-time Monitoring**
   - Fetches execution traces during runtime
   - Formats traces with timestamps and event types
   - Color-codes different event types

4. **Result Verification**
   - Checks execution completion status
   - Extracts and displays final results
   - Validates trace collection

## Integration with Shell Script

These tests mirror the functionality of the `kasal-cli.sh` script:
- List crews → Select crew → Extract variables
- Collect inputs → Execute → Monitor traces → Show result

The Python tests provide better assertion capabilities and can be integrated into CI/CD pipelines.

## Debugging

Enable debug output:
```bash
DEBUG=1 python -m pytest tests/e2e/ -v -s
```

Check test database:
```bash
# Tests use in-memory SQLite by default
# To use persistent test DB:
TEST_DATABASE_URL=sqlite+aiosqlite:///test.db python -m pytest tests/e2e/
```

## Future Enhancements

1. Add WebSocket support for real-time trace streaming
2. Test more complex crew configurations (parallel tasks, conditionals)
3. Add performance benchmarks
4. Test with different LLM models
5. Add tests for planning and reasoning modes