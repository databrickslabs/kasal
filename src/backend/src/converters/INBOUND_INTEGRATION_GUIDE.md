# Inbound Connector Integration Guide

## Architecture Overview

We've created a clean, modular inbound connector system:

```
src/converters/
├── inbound/                    # NEW - Extract from sources
│   ├── base.py                # BaseInboundConnector + ConnectorType enum
│   └── powerbi/
│       ├── connector.py       # PowerBIConnector
│       └── dax_parser.py      # DAXExpressionParser
├── pipeline.py                 # NEW - Orchestrates inbound → outbound
├── common/                     # Shared logic (filters, formulas, etc.)
├── outbound/                   # Generate to targets
│   ├── dax/
│   ├── sql/
│   └── uc_metrics/
└── base/                       # Core models (KPI, KPIDefinition)
```

## Flow

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Power BI       │────>│  ConversionPipeline│────>│   DAX Output    │
│  (Inbound)      │     │                  │     │   (Outbound)    │
└─────────────────┘     │   1. Extract     │     └─────────────────┘
                        │   2. Convert     │
┌─────────────────┐     │                  │     ┌─────────────────┐
│  Tableau        │────>│                  │────>│   SQL Output    │
│  (Future)       │     │                  │     │   (Outbound)    │
└─────────────────┘     └──────────────────┘     └─────────────────┘

                                                  ┌─────────────────┐
                                                  │  UC Metrics     │
                                                  │  (Outbound)     │
                                                  └─────────────────┘
```

## API Endpoints (To Implement)

### 1. List Available Connectors

**GET** `/api/converters/inbound/connectors`

Response:
```json
{
  "connectors": [
    {
      "type": "powerbi",
      "name": "Power BI",
      "description": "Extract measures from Power BI datasets",
      "requires_auth": true,
      "auth_methods": ["service_principal", "device_code", "access_token"]
    }
  ]
}
```

### 2. Connect to Source (Power BI)

**POST** `/api/converters/inbound/connect`

Request:
```json
{
  "connector_type": "powerbi",
  "connection_params": {
    "semantic_model_id": "abc123",
    "group_id": "workspace456",
    "access_token": "eyJ...",  // From frontend OAuth
    "info_table_name": "Info Measures"
  }
}
```

Response:
```json
{
  "success": true,
  "connector_id": "conn_123",  // Session ID for this connector
  "metadata": {
    "connector_type": "powerbi",
    "source_id": "abc123",
    "source_name": "Power BI Dataset abc123",
    "connected": true,
    "measure_count": 42
  }
}
```

### 3. Extract Measures

**POST** `/api/converters/inbound/extract`

Request:
```json
{
  "connector_id": "conn_123",
  "extract_params": {
    "include_hidden": false,
    "filter_pattern": ".*Revenue.*"
  }
}
```

Response:
```json
{
  "success": true,
  "measures": [
    {
      "technical_name": "total_revenue",
      "description": "Total Revenue",
      "formula": "revenue_amount",
      "source_table": "FactSales",
      "aggregation_type": "SUM",
      "filters": ["year = 2024"]
    }
  ],
  "count": 42
}
```

### 4. Convert to Target Format (Full Pipeline)

**POST** `/api/converters/pipeline/convert`

Request:
```json
{
  "inbound": {
    "type": "powerbi",
    "params": {
      "semantic_model_id": "abc123",
      "group_id": "workspace456",
      "access_token": "eyJ..."
    },
    "extract_params": {
      "include_hidden": false
    }
  },
  "outbound": {
    "format": "dax",  // or "sql", "uc_metrics", "yaml"
    "params": {
      "dialect": "databricks"  // for SQL
    }
  },
  "definition_name": "powerbi_measures"
}
```

Response:
```json
{
  "success": true,
  "output": [
    {
      "name": "Total Revenue",
      "expression": "SUM(FactSales[revenue_amount])",
      "description": "Total Revenue",
      "table": "FactSales"
    }
  ],
  "measure_count": 42,
  "metadata": {
    "connector_type": "powerbi",
    "source_id": "abc123",
    "connected": true
  }
}
```

## Frontend Integration Steps

### 1. Create Connector Selection UI

```typescript
interface ConnectorOption {
  type: string;
  name: string;
  description: string;
  requiresAuth: boolean;
  authMethods: string[];
}

// Fetch available connectors
const connectors = await fetch('/api/converters/inbound/connectors').then(r => r.json());

// Show selector
<Select>
  {connectors.map(c => (
    <Option value={c.type}>{c.name}</Option>
  ))}
</Select>
```

### 2. Create Authentication Flow

For Power BI with OAuth:

```typescript
// Step 1: User clicks "Connect to Power BI"
const authUrl = await initiateOAuthFlow();
window.location.href = authUrl;

// Step 2: OAuth callback receives access token
const accessToken = getTokenFromCallback();

// Step 3: Connect to Power BI
const connection = await fetch('/api/converters/inbound/connect', {
  method: 'POST',
  body: JSON.stringify({
    connector_type: 'powerbi',
    connection_params: {
      semantic_model_id: selectedDataset,
      group_id: selectedWorkspace,
      access_token: accessToken
    }
  })
});

const { connector_id } = await connection.json();
```

### 3. Create Conversion UI

```typescript
// Step 1: Select source connector
<Select onChange={setInboundConnector}>
  <Option value="powerbi">Power BI</Option>
  <Option value="tableau">Tableau (Coming Soon)</Option>
</Select>

// Step 2: Authenticate & connect
<Button onClick={handleConnect}>Connect to {inboundConnector}</Button>

// Step 3: Select target format
<Select onChange={setOutboundFormat}>
  <Option value="dax">DAX (Power BI)</Option>
  <Option value="sql">SQL (Databricks/Snowflake)</Option>
  <Option value="uc_metrics">UC Metrics (Databricks)</Option>
  <Option value="yaml">YAML</Option>
</Select>

// Step 4: Execute conversion
<Button onClick={handleConvert}>Convert</Button>

// Step 5: Display results
<CodeEditor value={conversionOutput} language={outboundFormat} />
```

### 4. Example Conversion Flow Component

```typescript
const ConversionWorkflow = () => {
  const [step, setStep] = useState(1);
  const [connectorId, setConnectorId] = useState(null);
  const [output, setOutput] = useState(null);

  const handleConnect = async () => {
    const response = await fetch('/api/converters/inbound/connect', {
      method: 'POST',
      body: JSON.stringify({
        connector_type: 'powerbi',
        connection_params: {
          semantic_model_id: powerbiDatasetId,
          group_id: powerbiWorkspaceId,
          access_token: oauthToken
        }
      })
    });
    const { connector_id } = await response.json();
    setConnectorId(connector_id);
    setStep(2);
  };

  const handleConvert = async () => {
    const response = await fetch('/api/converters/pipeline/convert', {
      method: 'POST',
      body: JSON.stringify({
        inbound: {
          type: 'powerbi',
          params: { /* ... */ },
          extract_params: { include_hidden: false }
        },
        outbound: {
          format: 'dax',
          params: {}
        }
      })
    });
    const result = await response.json();
    setOutput(result.output);
    setStep(3);
  };

  return (
    <Stepper activeStep={step}>
      <Step label="Connect">
        <PowerBIAuthForm onConnect={handleConnect} />
      </Step>
      <Step label="Convert">
        <FormatSelector onConvert={handleConvert} />
      </Step>
      <Step label="Results">
        <OutputDisplay output={output} />
      </Step>
    </Stepper>
  );
};
```

## Backend API Implementation Example

```python
# In src/api/kpi_conversion_router.py or new router

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any

from converters.pipeline import ConversionPipeline, OutboundFormat
from converters.inbound.base import ConnectorType

router = APIRouter(prefix="/api/converters/pipeline", tags=["conversion-pipeline"])


class ConversionRequest(BaseModel):
    inbound: Dict[str, Any]
    outbound: Dict[str, Any]
    definition_name: Optional[str] = "converted_measures"


@router.post("/convert")
async def convert_measures(request: ConversionRequest):
    """Execute full conversion pipeline"""
    try:
        pipeline = ConversionPipeline()

        result = pipeline.execute(
            inbound_type=ConnectorType(request.inbound["type"]),
            inbound_params=request.inbound["params"],
            outbound_format=OutboundFormat(request.outbound["format"]),
            outbound_params=request.outbound.get("params", {}),
            extract_params=request.inbound.get("extract_params", {}),
            definition_name=request.definition_name
        )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
```

## Testing the Pipeline

### Unit Test Example

```python
# tests/unit/converters/test_powerbi_connector.py

def test_powerbi_extraction():
    # Mock Power BI API response
    with patch('requests.post') as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {
            "results": [{
                "tables": [{
                    "rows": [
                        {
                            "[Name]": "Total Revenue",
                            "[Expression]": "SUM(Sales[Amount])",
                            "[Table]": "Sales"
                        }
                    ]
                }]
            }]
        }

        connector = PowerBIConnector(
            semantic_model_id="test123",
            group_id="workspace456",
            access_token="fake_token"
        )

        connector.connect()
        kpis = connector.extract_measures()

        assert len(kpis) == 1
        assert kpis[0].technical_name == "total_revenue"
```

### Integration Test Example

```python
# tests/integration/test_conversion_pipeline.py

def test_full_pipeline():
    pipeline = ConversionPipeline()

    # Mock access token
    with patch.object(PowerBIConnector, '_get_access_token', return_value='fake_token'):
        result = pipeline.execute(
            inbound_type=ConnectorType.POWERBI,
            inbound_params={
                "semantic_model_id": "test123",
                "group_id": "workspace456"
            },
            outbound_format=OutboundFormat.DAX,
            definition_name="test_conversion"
        )

        assert result["success"] is True
        assert len(result["output"]) > 0
```

## Next Steps

1. **Implement API Endpoints**: Create FastAPI router for pipeline endpoints
2. **Add Authentication**: Integrate OAuth flow for Power BI
3. **Create Frontend UI**: Build connector selection and conversion workflow
4. **Add Error Handling**: Comprehensive error messages and retry logic
5. **Add Logging**: Track conversions, performance, errors
6. **Add Caching**: Cache connector metadata and extraction results
7. **Add More Connectors**: Tableau, Looker, etc.

## File Structure Summary

```
Created:
✅ src/converters/inbound/base.py               - Base connector class
✅ src/converters/inbound/powerbi/connector.py  - Power BI connector
✅ src/converters/inbound/powerbi/dax_parser.py - DAX expression parser
✅ src/converters/pipeline.py                   - Conversion orchestrator

Next to Create:
📝 src/api/conversion_pipeline_router.py        - API endpoints
📝 tests/unit/converters/inbound/              - Unit tests
📝 tests/integration/test_pipeline.py          - Integration tests
```

## Key Benefits

1. **Modular**: Easy to add new inbound connectors (Tableau, Looker, etc.)
2. **Flexible**: Any inbound → any outbound format
3. **Clean Architecture**: Separation of concerns (inbound vs outbound)
4. **Extensible**: Simple to add new authentication methods
5. **Testable**: Each component can be tested independently
