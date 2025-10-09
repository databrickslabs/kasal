# Power BI DAX Generation Integration

This document describes the Power BI DAX query generation integration in the Kasal platform. The integration allows users to generate DAX queries from natural language questions using LLMs, which can then be executed in Databricks notebooks against Power BI XMLA endpoints.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Components](#components)
4. [Usage](#usage)
5. [API Reference](#api-reference)
6. [Tool Integration](#tool-integration)
7. [Databricks Execution](#databricks-execution)
8. [Examples](#examples)
9. [Troubleshooting](#troubleshooting)

## Overview

The Power BI DAX generation feature enables:
- **Natural Language to DAX**: Convert user questions into executable DAX queries
- **Metadata-Aware Generation**: Uses Power BI dataset metadata for accurate query generation
- **CrewAI Tool Integration**: Available as a custom tool for AI agents
- **Databricks Execution**: Generated queries can be executed in Databricks notebooks
- **Question Suggestions**: Automatically suggest relevant questions based on dataset structure

### Key Features

- LLM-powered DAX query generation
- Support for Power BI semantic models via XMLA endpoints
- Dataset metadata extraction and formatting
- Confidence scoring for generated queries
- Sample data support for improved accuracy
- Clean query output (removes HTML/XML artifacts)

## Architecture

### High-Level Flow

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   User      │────>│  DAX Generator   │────>│   LLM Provider  │
│  Question   │     │    Service       │     │  (Databricks)   │
└─────────────┘     └──────────────────┘     └─────────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │  Generated DAX   │
                    │     Query        │
                    └──────────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │   Databricks     │
                    │   Notebook Job   │
                    │  (via pyadomd)   │
                    └──────────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │  Power BI XMLA   │
                    │    Endpoint      │
                    └──────────────────┘
```

### Component Architecture

```
src/backend/src/
├── services/
│   └── dax_generator_service.py      # Core DAX generation logic
├── utils/
│   └── powerbi_connector.py          # Power BI utilities
├── schemas/
│   └── powerbi.py                    # Pydantic validation schemas
├── engines/crewai/tools/custom/
│   └── powerbi_tool.py               # CrewAI tool wrapper
└── api/
    └── powerbi_routes.py             # REST API endpoints
```

## Components

### 1. DAXGeneratorService

**Location**: `src/backend/src/services/dax_generator_service.py`

Main service for generating DAX queries from natural language.

**Key Methods:**
- `generate_dax_from_question()`: Generate DAX from a question
- `generate_dax_with_samples()`: Generate DAX with sample data context
- `suggest_questions()`: Suggest relevant questions based on metadata

### 2. PowerBI Connector Utilities

**Location**: `src/backend/src/utils/powerbi_connector.py`

Utilities for Power BI integration and metadata handling.

**Key Classes:**
- `PowerBIMetadataExtractor`: Extracts and formats dataset metadata
- `PowerBIConnectorConfig`: Configuration for Power BI connections
- `clean_dax_query()`: Cleans generated DAX queries

### 3. PowerBI Schemas

**Location**: `src/backend/src/schemas/powerbi.py`

Pydantic schemas for validation and type safety.

**Key Schemas:**
- `DAXGenerationRequest`: Request for DAX generation
- `DAXGenerationResponse`: Response with generated DAX
- `PowerBIConnectionConfig`: Connection configuration
- `QuestionSuggestionRequest`: Request for question suggestions

### 4. PowerBI CrewAI Tool

**Location**: `src/backend/src/engines/crewai/tools/custom/powerbi_tool.py`

Custom CrewAI tool for agent integration.

**Usage in Agent Configuration:**
```python
{
    "tool_id": "powerbi_tool_id",
    "config": {
        "xmla_endpoint": "powerbi://api.powerbi.com/v1.0/myorg/workspace",
        "dataset_name": "SalesDataset",
        "metadata": {...}
    }
}
```

### 5. API Endpoints

**Location**: `src/backend/src/api/powerbi_routes.py`

REST API endpoints for DAX generation.

## Usage

### Via REST API

#### Generate DAX Query

```bash
curl -X POST "http://localhost:8000/api/v1/powerbi/generate-dax" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What is the total NSR per product?",
    "metadata": {
      "tables": [
        {
          "name": "Products",
          "description": "Product information",
          "columns": [
            {
              "name": "ProductID",
              "data_type": "int",
              "description": "Unique product identifier"
            },
            {
              "name": "ProductName",
              "data_type": "string",
              "description": "Product name"
            },
            {
              "name": "NSR",
              "data_type": "decimal",
              "description": "Net Sales Revenue"
            }
          ]
        }
      ]
    },
    "model_name": "databricks-meta-llama-3-1-405b-instruct",
    "temperature": 0.1
  }'
```

#### Response

```json
{
  "dax_query": "EVALUATE SUMMARIZE(Products, Products[ProductName], \"Total NSR\", SUM(Products[NSR]))",
  "explanation": "Generated DAX query from natural language question",
  "confidence": 0.9,
  "raw_response": "..."
}
```

### Via CrewAI Tool

#### 1. Configure Tool in Database

Add the PowerBITool to your tools configuration:

```json
{
  "title": "PowerBITool",
  "description": "Generate DAX queries for Power BI datasets",
  "config": {
    "xmla_endpoint": "powerbi://api.powerbi.com/v1.0/myorg/workspace",
    "dataset_name": "SalesDataset",
    "metadata": {
      "tables": [...]
    },
    "model_name": "databricks-meta-llama-3-1-405b-instruct",
    "temperature": 0.1
  }
}
```

#### 2. Use in Agent Configuration

```json
{
  "role": "Business Analyst",
  "goal": "Analyze sales data",
  "backstory": "Expert in data analysis",
  "tools": [
    {
      "tool_id": "powerbi_tool_id"
    }
  ]
}
```

#### 3. Agent Usage

The agent can now ask questions like:
- "Generate a DAX query to show total sales by region"
- "Create a query for top 10 products by revenue"
- "Show me the YoY growth calculation in DAX"

## API Reference

### POST /api/v1/powerbi/generate-dax

Generate a DAX query from a natural language question.

**Request Body:**
```typescript
{
  question: string;               // Natural language question
  metadata: {                     // Dataset metadata
    tables: Array<{
      name: string;
      description?: string;
      columns: Array<{
        name: string;
        data_type: string;
        description?: string;
      }>;
      relationships?: Array<{
        relatedTable: string;
        fromColumn: string;
        toColumn: string;
        relationshipType: string;
      }>;
    }>;
  };
  sample_data?: {                 // Optional sample data
    [tableName: string]: Array<Record<string, any>>;
  };
  model_name?: string;            // LLM model (default: databricks-meta-llama-3-1-405b-instruct)
  temperature?: number;           // Temperature 0.0-2.0 (default: 0.1)
}
```

**Response:**
```typescript
{
  dax_query: string;             // Generated DAX query
  explanation: string;            // Explanation of the query
  confidence: number;             // Confidence score 0-1
  raw_response?: string;          // Raw LLM response
}
```

### POST /api/v1/powerbi/suggest-questions

Suggest relevant questions based on dataset metadata.

**Request Body:**
```typescript
{
  metadata: {...};               // Dataset metadata
  model_name?: string;           // LLM model
  num_suggestions?: number;      // Number of suggestions (1-20, default: 5)
}
```

**Response:**
```typescript
{
  questions: string[];           // List of suggested questions
}
```

### GET /api/v1/powerbi/health

Health check endpoint.

**Response:**
```typescript
{
  status: string;               // "healthy"
  service: string;              // "powerbi-dax-generator"
}
```

## Tool Integration

### PowerBITool Configuration

The PowerBITool can be configured with the following parameters:

```python
{
    "xmla_endpoint": "powerbi://api.powerbi.com/v1.0/myorg/workspace",
    "dataset_name": "SalesDataset",
    "metadata": {
        "tables": [...]  # Full dataset metadata
    },
    "model_name": "databricks-meta-llama-3-1-405b-instruct",
    "temperature": 0.1
}
```

### Tool Output Format

The tool returns formatted output with:
- Generated DAX query
- Explanation
- Confidence score
- Execution instructions for Databricks

Example output:
```
DAX Query Generated (Confidence: 90%)

Question: What is the total NSR per product?

DAX Query:
```dax
EVALUATE SUMMARIZE(Products, Products[ProductName], "Total NSR", SUM(Products[NSR]))
```

Explanation: This query summarizes product data and calculates total NSR per product.

---
Execution Instructions:

To execute this DAX query in Databricks:
[Instructions for Databricks notebook execution]
```

## Databricks Execution

### Prerequisites

1. **Power BI XMLA Endpoint**: Access to a Power BI workspace via XMLA
2. **Service Principal**: Azure AD app registration with Power BI permissions
3. **Databricks Environment**: Notebook with `pyadomd` installed

### Execution Template

Use the provided Databricks notebook template to execute generated DAX queries:

```python
import pyadomd

# Configuration from service principal
client_id = dbutils.widgets.get("client_id")
tenant_id = dbutils.widgets.get("tenant_id")
client_secret = dbutils.widgets.get("client_secret")
xmla_endpoint = dbutils.widgets.get("xmla_endpoint")
dataset_name = dbutils.widgets.get("dataset_name")
dax_query = dbutils.widgets.get("dax_query")

# Build connection string
connection_string = (
    f"Provider=MSOLAP;"
    f"Data Source={xmla_endpoint};"
    f"Initial Catalog={dataset_name};"
    f"User ID=app:{client_id}@{tenant_id};"
    f"Password={client_secret};"
)

# Execute DAX query
with Pyadomd(connection_string) as conn:
    cursor = conn.cursor()
    cursor.execute(dax_query)

    # Get results
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Convert to Spark DataFrame
    from pyspark.sql import Row

    data = [Row(**dict(zip(columns, row))) for row in rows]
    df = spark.createDataFrame(data)

    # Display results
    display(df)
```

### Integration with Databricks Jobs Tool

You can combine the PowerBITool with the DatabricksJobsTool for automated execution:

1. **Generate DAX** using PowerBITool
2. **Create Notebook Job** using DatabricksJobsTool
3. **Pass DAX Query** as notebook parameter
4. **Execute and Retrieve** results

## Examples

### Example 1: Simple Aggregation

**Question**: "What is the total sales amount?"

**Generated DAX**:
```dax
EVALUATE SUMMARIZE(Sales, "Total Sales", SUM(Sales[Amount]))
```

### Example 2: Grouped Aggregation

**Question**: "Show me sales by product category"

**Generated DAX**:
```dax
EVALUATE SUMMARIZE(
    Sales,
    Products[Category],
    "Total Sales", SUM(Sales[Amount])
)
```

### Example 3: Top N Query

**Question**: "What are the top 10 products by revenue?"

**Generated DAX**:
```dax
EVALUATE TOPN(
    10,
    SUMMARIZE(
        Sales,
        Products[ProductName],
        "Revenue", SUM(Sales[Amount])
    ),
    [Revenue],
    DESC
)
```

### Example 4: Filtered Query

**Question**: "Show me sales for Q1 2024"

**Generated DAX**:
```dax
EVALUATE FILTER(
    SUMMARIZE(
        Sales,
        Sales[Date],
        Sales[Amount]
    ),
    AND(
        Sales[Date] >= DATE(2024, 1, 1),
        Sales[Date] < DATE(2024, 4, 1)
    )
)
```

### Example 5: Multiple Relationships

**Question**: "What is the revenue per customer segment?"

**Generated DAX**:
```dax
EVALUATE SUMMARIZE(
    Sales,
    Customers[Segment],
    "Total Revenue", SUM(Sales[Amount]),
    "Customer Count", DISTINCTCOUNT(Sales[CustomerID])
)
```

## Troubleshooting

### Common Issues

#### 1. Invalid DAX Syntax

**Problem**: Generated DAX query has syntax errors

**Solutions**:
- Ensure metadata is accurate and complete
- Include sample data for better context
- Lower temperature for more deterministic output
- Verify column and table names match exactly

#### 2. Missing Metadata

**Problem**: Tool returns error about missing metadata

**Solutions**:
- Verify metadata is included in tool configuration
- Check that metadata format matches the schema
- Use `PowerBIMetadataExtractor.format_metadata_for_llm()` for correct formatting

#### 3. Low Confidence Scores

**Problem**: Generated queries have low confidence scores

**Solutions**:
- Provide more detailed column descriptions
- Include sample data
- Use more specific questions
- Verify relationships are correctly defined

#### 4. Execution Errors in Databricks

**Problem**: DAX query fails to execute in Databricks

**Solutions**:
- Verify service principal has proper permissions
- Check XMLA endpoint URL format
- Ensure `pyadomd` library is installed
- Validate connection string format

### Debugging

Enable detailed logging:

```python
import logging
logging.getLogger("src.services.dax_generator_service").setLevel(logging.DEBUG)
logging.getLogger("src.utils.powerbi_connector").setLevel(logging.DEBUG)
```

### Best Practices

1. **Metadata Quality**:
   - Provide comprehensive table and column descriptions
   - Define all relationships
   - Include data types for all columns

2. **Question Formulation**:
   - Be specific about what you want to calculate
   - Mention table names when dealing with multiple tables
   - Specify time periods or filters explicitly

3. **Sample Data**:
   - Include representative sample data
   - Cover edge cases in samples
   - Keep sample size reasonable (10-20 rows per table)

4. **Model Selection**:
   - Use larger models for complex queries
   - Use lower temperature for precision
   - Cache metadata to reduce token usage

5. **Error Handling**:
   - Always validate generated DAX before execution
   - Test queries with small datasets first
   - Implement retry logic for transient failures

## Future Enhancements

Potential future improvements:

1. **Direct XMLA Execution**: Execute DAX directly from Kasal (Windows environment required)
2. **Query Optimization**: Automatic DAX query optimization suggestions
3. **Result Caching**: Cache query results for repeated questions
4. **Visual Generation**: Generate Power BI visualizations from queries
5. **Query History**: Track and reuse previously generated queries
6. **Interactive Refinement**: Multi-turn conversations for query refinement
7. **Schema Learning**: Learn from user corrections to improve future generations

## Support

For issues or questions:
- Check the troubleshooting section
- Review API documentation
- Examine example queries
- Check service logs for detailed error messages

## Version History

- **v1.0.0** (2025-01-09): Initial release
  - Basic DAX generation from natural language
  - CrewAI tool integration
  - REST API endpoints
  - Databricks execution support
