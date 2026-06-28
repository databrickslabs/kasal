# CrewAI Guardrails

This directory contains guardrail implementations for CrewAI tasks.

## Layout

```
guardrails/
├── base_guardrail.py        # BaseGuardrail abstract base
├── guardrail_factory.py     # GuardrailFactory — type → class dispatch
├── guardrail_wrapper.py     # Wraps a guardrail for CrewAI task validation
├── guardrail_model.py
├── core/                    # Reusable, domain-agnostic guardrails
│   ├── minimum_number_guardrail.py
│   ├── llm_injection_guardrail.py
│   └── self_reflection_guardrail.py
└── demo/                    # Domain demo guardrails (data_processing table family)
    ├── company_count_guardrail.py
    ├── data_processing_guardrail.py
    ├── empty_data_processing_guardrail.py
    ├── data_processing_count_guardrail.py
    └── company_name_not_null_guardrail.py
```

`GuardrailFactory.create_guardrail()` dispatches on the `type` field and supports
**8 guardrail types** (3 reusable in `core/`, 5 demo in `demo/`).

## Available Guardrails

### Reusable guardrails (`core/`)

#### Minimum Number Guardrail

Validates that the task output contains at least a minimum number of items.

**Configuration:**
```json
{
  "type": "minimum_number",
  "min_count": 50
}
```

#### LLM Injection Check Guardrail

Opt-in security guardrail (`LLMInjectionGuardrail`). Sends the task output to an LLM
that classifies it as `SAFE` or `INJECTION`; an `INJECTION` verdict fails validation and
CrewAI retries. Fails open on LLM error. See `README_SECURITY_COMPLIANCE.md`.

**Configuration:**
```json
{
  "type": "prompt_injection_check",
  "llm_model": "databricks-claude-sonnet-4-5"
}
```

#### Self-Reflection Guardrail

Opt-in security guardrail (`SelfReflectionGuardrail`). Asks an LLM whether the output
deviated from the original task goal; a `FAIL` verdict triggers a retry. Fails open on
LLM error.

**Configuration:**
```json
{
  "type": "self_reflection",
  "llm_model": "databricks-claude-sonnet-4-5"
}
```

### Demo guardrails (`demo/`)

These are coupled to the `data_processing` demo table (see schema below).

#### Company Count Guardrail

Validates that the task output contains a minimum number of companies.

**Configuration:**
```json
{
  "type": "company_count",
  "min_companies": 50
}
```

#### Data Processing Guardrail

Validates that a specific record in the database has been processed (processed = true).

**Configuration:**
```json
{
  "type": "data_processing",
  "che_number": "CHE12345"
}
```

### Empty Data Processing Guardrail

Validates that the data_processing table is empty.

**Configuration:**
```json
{
  "type": "empty_data_processing"
}
```

### Data Processing Count Guardrail

Validates that the total number of records in the data_processing table matches the expected count.

**Configuration:**
```json
{
  "type": "data_processing_count",
  "expected_count": 100
}
```

### Company Name Not Null Guardrail

Validates that no records in the data_processing table have a null company_name value.

**Configuration:**
```json
{
  "type": "company_name_not_null"
}
```

## How to Use Guardrails

1. In the Task UI, navigate to the Advanced Configuration section
2. Under "Guardrail Settings", select the type of guardrail you want to use
3. Configure the required parameters for the selected guardrail
4. Enable "Retry on Failure" so the task will be retried until the guardrail validation passes

## Database Schema for Data Processing

The data_processing guardrail uses the following database table:

```sql
CREATE TABLE IF NOT EXISTS data_processing (
  id SERIAL PRIMARY KEY,
  che_number VARCHAR(255) UNIQUE NOT NULL,
  processed BOOLEAN NOT NULL DEFAULT FALSE,
  company_name VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Example Queries

Insert a new CHE number to track:
```sql
INSERT INTO data_processing (che_number, processed, company_name)
VALUES ('CHE12345', false, 'Acme Inc.');
```

Update processing status:
```sql
UPDATE data_processing
SET processed = true
WHERE che_number = 'CHE12345';
```

Check processing status:
```sql
SELECT * FROM data_processing
WHERE che_number = 'CHE12345';
```

Check for null company names:
```sql
SELECT * FROM data_processing
WHERE company_name IS NULL;
```

## How to Add a New Guardrail

1. Create a new Python file for your guardrail class. Put domain-agnostic guardrails in
   `core/`; put demo/data-coupled guardrails in `demo/`. Subclass `BaseGuardrail`.
2. Implement a validation method that returns a tuple of `(bool, result_or_error)`
3. Add an import + `__all__` entry for your class in `__init__.py` (under the matching
   `core` or `demo` block)
4. Register a `type` → class branch in `GuardrailFactory.create_guardrail()`
   (`guardrail_factory.py`) — this is the single dispatch point used to instantiate
   guardrails from a task's `guardrail` config

## Full Example

```python
# Task configuration
task_config = {
    "description": "Find and list at least 50 technology companies that went public in the last 10 years.",
    "expected_output": "A comprehensive list of at least 50 tech companies with their IPO dates and key metrics.",
    "guardrail": {
        "type": "company_count",
        "min_companies": 50
    }
}

# Using the guardrail in code
from src.engines.crewai.paths.crew.task_adapter import create_task

task = await create_task(
    task_key="find_tech_companies",
    task_config=task_config,
    agent=research_analyst
) 