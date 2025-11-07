# CrewAI Guardrails

This directory contains guardrail implementations for CrewAI tasks.

## Available Guardrails

### Company Count Guardrail

Validates that the task output contains a minimum number of companies.

**Configuration:**
```json
{
  "type": "company_count",
  "min_companies": 50
}
```

### Data Processing Guardrail

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

1. Create a new Python file in this directory with your guardrail class
2. Implement a validation method that returns a tuple of `(bool, result_or_error)`
3. Add your new guardrail class to the `__init__.py` export list
4. Update the `task_helpers.py` file to handle your new guardrail type

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
from src.engines.crewai.helpers.task_helpers import create_task

task = await create_task(
    task_key="find_tech_companies",
    task_config=task_config,
    agent=research_analyst
) 