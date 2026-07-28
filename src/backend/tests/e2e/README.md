# End-to-End Tests

End-to-end tests for Kasal workflows, focused on the input-variable functionality.

## These are OPT-IN

They talk to a real backend and CREATE crews and RUN executions against it, so
they are skipped unless you ask for them:

```bash
KASAL_E2E=1 python -m pytest tests/e2e -v          # run them
python -m pytest tests/e2e                          # 3 skipped, ~1s
KASAL_E2E_BASE_URL=http://127.0.0.1:8001 KASAL_E2E=1 python -m pytest tests/e2e
```

Without `KASAL_E2E=1` they report as skipped with the reason. They used to report
nothing at all: the driver was a class named `TestCrewAIRealIntegration` with an
`__init__`, which pytest cannot collect, so the whole tier silently collected
ZERO tests while looking green.

## Test Structure

### `test_kasal_input_integration.py`
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
cd src/backend
python tests/e2e/test_kasal_input_integration.py

# Or with pytest (note the opt-in flag)
KASAL_E2E=1 python -m pytest tests/e2e/test_kasal_input_integration.py -v

# With output displayed
KASAL_E2E=1 python -m pytest tests/e2e/test_kasal_input_integration.py -v -s
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