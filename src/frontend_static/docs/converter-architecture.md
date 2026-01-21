# Converter Architecture - Modular API Design

## Overview

The Kasal Converter system provides a universal measure conversion platform with a modular, API-driven architecture. Each inbound connector and outbound converter is exposed as an independent REST API, enabling flexible composition and easy extensibility.

## Complete Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              FRONTEND / UI                                  │
│                        (React + TypeScript)                                 │
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │
│  │   Dropdown   │    │   Dropdown   │    │    Button    │                │
│  │   "FROM"     │──→ │    "TO"      │──→ │  "Convert"   │                │
│  │  Power BI    │    │     DAX      │    │              │                │
│  └──────────────┘    └──────────────┘    └──────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ HTTP Requests
                                    ▼
╔═════════════════════════════════════════════════════════════════════════════╗
║                           API GATEWAY LAYER                                 ║
║                     (FastAPI Router Architecture)                           ║
╠═════════════════════════════════════════════════════════════════════════════╣
║                                                                             ║
║  ┌────────────────────────────────────────────────────────────────┐       ║
║  │          DISCOVERY API: /api/converters/discovery              │       ║
║  ├────────────────────────────────────────────────────────────────┤       ║
║  │  GET /capabilities  → List all inbound + outbound connectors   │       ║
║  │  GET /inbound       → List available source connectors         │       ║
║  │  GET /outbound      → List available target converters         │       ║
║  │  GET /health        → Health check all connectors              │       ║
║  └────────────────────────────────────────────────────────────────┘       ║
║                                                                             ║
║  ┌─────────────────────┐  ┌─────────────────────┐  ┌──────────────────┐  ║
║  │   INBOUND API       │  │   PIPELINE API      │  │   OUTBOUND API   │  ║
║  │   (Extractors)      │  │   (Orchestrator)    │  │   (Generators)   │  ║
║  └─────────────────────┘  └─────────────────────┘  └──────────────────┘  ║
║           │                        │                        │              ║
║           ▼                        ▼                        ▼              ║
║  ┌─────────────────────────────────────────────────────────────────────┐  ║
║  │ /api/connectors/inbound/*      /api/converters/pipeline/*          │  ║
║  │                                                                     │  ║
║  │ /powerbi/extract               /execute                            │  ║
║  │ /powerbi/validate              /execute/async                      │  ║
║  │ /powerbi/datasets              /paths                              │  ║
║  │                                /validate/path                      │  ║
║  │ /yaml/parse                                                        │  ║
║  │ /yaml/validate                                                     │  ║
║  │ /yaml/schema                                                       │  ║
║  │                                                                     │  ║
║  │ /tableau/extract                                                   │  ║
║  │ /tableau/workbooks                                                 │  ║
║  │                                                                     │  ║
║  │ /excel/parse/file                                                  │  ║
║  │ /excel/template                                                    │  ║
║  └─────────────────────────────────────────────────────────────────────┘  ║
║                                                                             ║
║  ┌─────────────────────────────────────────────────────────────────────┐  ║
║  │ /api/connectors/outbound/*                                          │  ║
║  │                                                                     │  ║
║  │ /dax/generate                                                       │  ║
║  │ /dax/validate                                                       │  ║
║  │ /dax/preview                                                        │  ║
║  │ /dax/export/file                                                    │  ║
║  │                                                                     │  ║
║  │ /sql/generate/{dialect}                                             │  ║
║  │ /sql/validate/{dialect}                                             │  ║
║  │ /sql/dialects                                                       │  ║
║  │                                                                     │  ║
║  │ /uc-metrics/generate                                                │  ║
║  │ /uc-metrics/deploy                                                  │  ║
║  │ /uc-metrics/catalogs                                                │  ║
║  │                                                                     │  ║
║  │ /yaml/generate                                                      │  ║
║  │ /yaml/export/file                                                   │  ║
║  └─────────────────────────────────────────────────────────────────────┘  ║
║                                                                             ║
║  ┌────────────────────────────────────────────────────────────────┐       ║
║  │    MANAGEMENT APIs: /api/converters/*                          │       ║
║  ├────────────────────────────────────────────────────────────────┤       ║
║  │  /jobs          → Async job management                          │       ║
║  │  /history       → Conversion audit trail                        │       ║
║  │  /configs       → Saved configurations                          │       ║
║  └────────────────────────────────────────────────────────────────┘       ║
╚═════════════════════════════════════════════════════════════════════════════╝
                                    │
                                    │ Calls Core Logic
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CORE CONVERTER ENGINE                               │
│                      (Business Logic - Internal)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Power BI ──┐                                                             │
│   YAML ──────┼─→ [Inbound Connectors] ──→ KPIDefinition ──→ [Outbound] ─┬─→ DAX      │
│   Tableau ───┘      (Extract Logic)       (Internal Format)   (Generate) ├─→ SQL      │
│   Excel ─────┘                                                            ├─→ UC Metrics│
│                                                                           └─→ YAML     │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────┐    │
│   │                    KPIDefinition (Unified Model)                 │    │
│   ├─────────────────────────────────────────────────────────────────┤    │
│   │  {                                                               │    │
│   │    name: "Sales Metrics",                                        │    │
│   │    kpis: [                                                       │    │
│   │      {                                                           │    │
│   │        name: "Total Sales",                                      │    │
│   │        formula: "SUM(Sales[Amount])",                            │    │
│   │        aggregation_type: "SUM",                                  │    │
│   │        source_table: "Sales",                                    │    │
│   │        filters: [...],                                           │    │
│   │        time_intelligence: [...]                                  │    │
│   │      }                                                           │    │
│   │    ],                                                            │    │
│   │    structures: [...]                                             │    │
│   │  }                                                               │    │
│   └─────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│   Components:                                                               │
│   • src/converters/inbound/      - Connector implementations               │
│   • src/converters/outbound/     - Generator implementations               │
│   • src/converters/pipeline.py   - Orchestration logic                     │
│   • src/converters/base/         - Core models & interfaces                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Persists
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SERVICE & REPOSITORY LAYER                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ConverterService ──→ Repositories ──→ Database                           │
│   • Business logic      • Data access    • SQLite/PostgreSQL               │
│   • Multi-tenancy       • Queries        • History                         │
│   • Validation          • Filtering      • Jobs                            │
│                                          • Saved Configs                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Architecture Pattern

### Simplified Conversion Flow

```
Power BI ─┐
YAML ─────┼──→ [Inbound] ──→ KPI Definition ──→ [Outbound] ──┬──→ DAX
Tableau ──┘                  (Internal Format)                ├──→ SQL
Excel ────┘                                                    ├──→ UC Metrics
                                                              └──→ YAML
```

**Key Principle**: All sources convert to a unified **KPI Definition** (internal format), which then converts to any target format.

**Complexity Reduction**:
- Without this pattern: N sources × M targets = **N × M converters** (exponential)
- With this pattern: N inbound + M outbound = **N + M converters** (linear)

## Architecture Flow

### 1. Frontend → API Gateway
```typescript
// User selects: Power BI → DAX
const response = await fetch('/api/converters/pipeline/execute', {
  method: 'POST',
  body: JSON.stringify({
    source: {
      type: 'powerbi',
      config: { semantic_model_id: '...', group_id: '...', access_token: '...' }
    },
    target: {
      type: 'dax',
      config: { process_structures: true }
    }
  })
});
```

### 2. API Gateway → Core Engine
```python
# Pipeline Router receives request
@router.post("/pipeline/execute")
async def execute(request: PipelineRequest):
    # Extract from Power BI
    inbound = PowerBIConnector(request.source.config)
    kpi_definition = await inbound.extract()

    # Generate DAX
    outbound = DAXGenerator(request.target.config)
    dax_code = await outbound.generate(kpi_definition)

    return {"code": dax_code}
```

### 3. Alternative: Direct Connector Usage
```typescript
// Step 1: Extract
const kpiDef = await fetch('/api/connectors/inbound/powerbi/extract', {
  method: 'POST',
  body: JSON.stringify({ semantic_model_id: '...', ... })
});

// Step 2: Generate
const dax = await fetch('/api/connectors/outbound/dax/generate', {
  method: 'POST',
  body: JSON.stringify({ kpi_definition: kpiDef.data })
});
```

## Modular Endpoint Structure

```
API Gateway
│
├─── Discovery Layer
│    └─── GET /api/converters/discovery/capabilities
│         → Returns list of all available inbound/outbound connectors
│
├─── Inbound Connectors (Each is a separate module)
│    ├─── /api/connectors/inbound/powerbi/*
│    │    ├─── POST /extract
│    │    ├─── POST /validate
│    │    └─── GET /datasets
│    │
│    ├─── /api/connectors/inbound/yaml/*
│    │    ├─── POST /parse
│    │    └─── POST /validate
│    │
│    ├─── /api/connectors/inbound/tableau/*
│    │    └─── POST /extract
│    │
│    └─── /api/connectors/inbound/excel/*
│         └─── POST /parse/file
│
├─── Outbound Converters (Each is a separate module)
│    ├─── /api/connectors/outbound/dax/*
│    │    ├─── POST /generate
│    │    ├─── POST /validate
│    │    └─── POST /export/file
│    │
│    ├─── /api/connectors/outbound/sql/*
│    │    ├─── POST /generate/{dialect}
│    │    └─── GET /dialects
│    │
│    ├─── /api/connectors/outbound/uc-metrics/*
│    │    ├─── POST /generate
│    │    └─── POST /deploy
│    │
│    └─── /api/connectors/outbound/yaml/*
│         └─── POST /generate
│
├─── Pipeline Orchestration
│    └─── /api/converters/pipeline/*
│         ├─── POST /execute           (Synchronous conversion)
│         ├─── POST /execute/async     (Background job)
│         └─── GET /paths              (List supported paths)
│
└─── Management
     ├─── /api/converters/jobs/*       (Job tracking)
     ├─── /api/converters/history/*    (Audit trail)
     └─── /api/converters/configs/*    (Saved configurations)
```

## Why This Architecture?

### 1. Each Box = Independent Module
- Adding Power BI? Just add `/api/connectors/inbound/powerbi/*` endpoints
- Adding Looker? Just add `/api/connectors/inbound/looker/*` endpoints
- **No changes to existing code**

### 2. Frontend Can Discover Dynamically
```javascript
// Frontend doesn't hardcode connectors
const capabilities = await fetch('/api/converters/discovery/capabilities');

// Dynamically build dropdown from API response
{
  inbound: [
    { type: 'powerbi', name: 'Power BI', endpoints: [...] },
    { type: 'yaml', name: 'YAML', endpoints: [...] }
  ],
  outbound: [
    { type: 'dax', name: 'DAX', endpoints: [...] },
    { type: 'sql', name: 'SQL', endpoints: [...] }
  ]
}
```

### 3. Two Ways to Use

**Option A: High-Level Pipeline** (Easiest)
```http
POST /api/converters/pipeline/execute
{
  "source": { "type": "powerbi", "config": {...} },
  "target": { "type": "dax", "config": {...} }
}
```

**Option B: Low-Level Direct Control** (More flexible)
```http
1. POST /api/connectors/inbound/powerbi/extract  → KPIDefinition
2. POST /api/connectors/outbound/dax/generate   ← KPIDefinition
```

### Architecture Benefits

- ✅ **Modularity**: Each connector is self-contained
- ✅ **Discoverability**: Frontend learns capabilities from API
- ✅ **Flexibility**: Use high-level pipeline or low-level connectors
- ✅ **Scalability**: Linear growth (N + M, not N × M)
- ✅ **Maintainability**: Change one connector without touching others

---

## 📥 Inbound Connectors

Each inbound connector extracts measures from external systems and converts them to the internal **KPIDefinition** format.

### Power BI Connector

**Base Path**: `/api/connectors/inbound/powerbi`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/extract` | Extract measures from Power BI dataset |
| `POST` | `/validate` | Validate Power BI connection & credentials |
| `GET` | `/datasets` | List available datasets in workspace |
| `GET` | `/datasets/{id}/info` | Get dataset metadata |
| `POST` | `/datasets/{id}/test` | Test connection to specific dataset |

**Example Request**:
```json
POST /api/connectors/inbound/powerbi/extract
{
  "semantic_model_id": "abc123",
  "group_id": "workspace456",
  "access_token": "Bearer ...",
  "info_table_name": "Info Measures",
  "include_hidden": false
}
```

**Returns**: `KPIDefinition` (internal format)

---

### YAML Connector

**Base Path**: `/api/connectors/inbound/yaml`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/parse` | Parse YAML file/content |
| `POST` | `/validate` | Validate YAML schema |
| `GET` | `/schema` | Get YAML schema definition |
| `POST` | `/parse/file` | Parse from file upload |

**Example Request**:
```json
POST /api/connectors/inbound/yaml/parse
{
  "content": "kpis:\n  - name: Total Sales\n    formula: SUM(Sales[Amount])"
}
```

**Returns**: `KPIDefinition`

---

### Tableau Connector

**Base Path**: `/api/connectors/inbound/tableau`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/extract` | Extract calculated fields from workbook |
| `POST` | `/validate` | Validate Tableau connection |
| `GET` | `/workbooks` | List available workbooks |
| `GET` | `/workbooks/{id}/info` | Get workbook metadata |

**Status**: Coming Soon

---

### Excel Connector

**Base Path**: `/api/connectors/inbound/excel`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/parse/file` | Parse Excel file with measure definitions |
| `POST` | `/validate` | Validate Excel structure |
| `GET` | `/template` | Download Excel template |

**Status**: Coming Soon

---

## 🔄 Internal Representation

All inbound connectors produce a unified **KPIDefinition** object:

```typescript
interface KPIDefinition {
  name: string;
  description?: string;
  kpis: KPI[];
  structures?: TimeIntelligenceStructure[];
}

interface KPI {
  name: string;
  formula: string;
  description?: string;
  aggregation_type: 'SUM' | 'AVG' | 'COUNT' | 'MIN' | 'MAX';
  source_table?: string;
  filters?: Filter[];
  time_intelligence?: TimeIntelligence[];
  format_string?: string;
  is_hidden?: boolean;
}
```

This internal format is **source-agnostic** and **target-agnostic**, enabling any-to-any conversions.

---

## 📤 Outbound Converters

Each outbound converter transforms the **KPIDefinition** into a target format.

### DAX Converter

**Base Path**: `/api/connectors/outbound/dax`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/generate` | Generate DAX measures |
| `POST` | `/validate` | Validate DAX syntax |
| `POST` | `/preview` | Preview generated DAX |
| `GET` | `/options` | Get DAX generation options |
| `POST` | `/export/file` | Export DAX to .dax file |
| `POST` | `/export/pbix` | Export to Power BI template |

**Example Request**:
```json
POST /api/connectors/outbound/dax/generate
{
  "kpi_definition": { ... },
  "process_structures": true
}
```

**Returns**: Generated DAX code

---

### SQL Converter

**Base Path**: `/api/connectors/outbound/sql`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/generate/{dialect}` | Generate SQL for specific dialect |
| `POST` | `/validate/{dialect}` | Validate SQL syntax |
| `GET` | `/dialects` | List supported SQL dialects |
| `POST` | `/preview/{dialect}` | Preview generated SQL |
| `POST` | `/optimize/{dialect}` | Optimize SQL for performance |
| `POST` | `/export/file` | Export SQL to .sql file |

**Supported Dialects**:
- `databricks` - Databricks SQL
- `postgresql` - PostgreSQL
- `mysql` - MySQL
- `sqlserver` - SQL Server
- `snowflake` - Snowflake
- `bigquery` - Google BigQuery
- `standard` - ANSI SQL

**Example Request**:
```json
POST /api/connectors/outbound/sql/generate/databricks
{
  "kpi_definition": { ... },
  "include_comments": true,
  "process_structures": true
}
```

**Returns**: Generated SQL code

---

### Unity Catalog Metrics Converter

**Base Path**: `/api/connectors/outbound/uc-metrics`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/generate` | Generate Unity Catalog metric definitions |
| `POST` | `/validate` | Validate UC metric schema |
| `POST` | `/deploy` | Deploy metrics to Unity Catalog |
| `GET` | `/catalogs` | List available catalogs |
| `GET` | `/schemas/{catalog}` | List schemas in catalog |
| `POST` | `/preview` | Preview metric definitions |

**Example Request**:
```json
POST /api/connectors/outbound/uc-metrics/generate
{
  "kpi_definition": { ... },
  "catalog": "main",
  "schema": "default",
  "process_structures": true
}
```

**Returns**: Unity Catalog metric DDL

---

### YAML Converter

**Base Path**: `/api/connectors/outbound/yaml`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/generate` | Generate YAML definition |
| `POST` | `/validate` | Validate YAML output |
| `GET` | `/schema` | Get output YAML schema |
| `POST` | `/export/file` | Export to YAML file |

---

## 🔗 Pipeline Orchestration

The pipeline router provides high-level orchestration for complete conversions.

**Base Path**: `/api/converters/pipeline`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/execute` | Execute full conversion (inbound → outbound) |
| `POST` | `/execute/async` | Create async job for conversion |
| `GET` | `/paths` | List all supported conversion paths |
| `POST` | `/validate/path` | Validate if conversion path is supported |

**Example: Full Pipeline Execution**:
```json
POST /api/converters/pipeline/execute
{
  "source": {
    "type": "powerbi",
    "config": {
      "semantic_model_id": "abc123",
      "group_id": "workspace456",
      "access_token": "Bearer ..."
    }
  },
  "target": {
    "type": "dax",
    "config": {
      "process_structures": true
    }
  }
}
```

**Returns**: Conversion result with generated code

---

## 📊 Discovery & Capabilities API

The discovery router enables dynamic discovery of available connectors.

**Base Path**: `/api/converters/discovery`

### Get All Capabilities

```http
GET /api/converters/discovery/capabilities
```

**Response**:
```json
{
  "inbound": [
    {
      "type": "powerbi",
      "name": "Power BI Connector",
      "version": "1.0.0",
      "status": "active",
      "config_schema": {
        "type": "object",
        "properties": {
          "semantic_model_id": {"type": "string", "required": true},
          "group_id": {"type": "string", "required": true},
          "access_token": {"type": "string", "required": true}
        }
      },
      "endpoints": ["/extract", "/validate", "/datasets"]
    },
    {
      "type": "yaml",
      "name": "YAML Parser",
      "version": "1.0.0",
      "status": "active",
      "config_schema": { ... }
    }
  ],
  "outbound": [
    {
      "type": "dax",
      "name": "DAX Generator",
      "version": "1.0.0",
      "status": "active",
      "config_schema": { ... }
    },
    {
      "type": "sql",
      "name": "SQL Generator",
      "version": "1.0.0",
      "status": "active",
      "dialects": ["databricks", "postgresql", "mysql", "sqlserver", "snowflake", "bigquery"],
      "config_schema": { ... }
    }
  ],
  "supported_paths": [
    {"from": "powerbi", "to": "dax"},
    {"from": "powerbi", "to": "sql"},
    {"from": "powerbi", "to": "uc_metrics"},
    {"from": "yaml", "to": "dax"},
    {"from": "yaml", "to": "sql"},
    ...
  ]
}
```

### List Inbound Connectors

```http
GET /api/converters/discovery/inbound
```

### List Outbound Converters

```http
GET /api/converters/discovery/outbound
```

### Health Check

```http
GET /api/converters/discovery/health
```

---

## 🎛️ Management APIs

### Jobs Management

**Base Path**: `/api/converters/jobs`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/` | Create conversion job |
| `GET` | `/{job_id}` | Get job status & results |
| `PATCH` | `/{job_id}/cancel` | Cancel running job |
| `GET` | `/` | List jobs (with filters) |
| `DELETE` | `/{job_id}` | Delete job record |

### History Tracking

**Base Path**: `/api/converters/history`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/` | Create history entry |
| `GET` | `/{history_id}` | Get history details |
| `GET` | `/` | List conversion history |
| `GET` | `/statistics` | Get conversion statistics |

### Saved Configurations

**Base Path**: `/api/converters/configs`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/` | Save configuration |
| `GET` | `/{config_id}` | Get saved configuration |
| `PATCH` | `/{config_id}` | Update configuration |
| `DELETE` | `/{config_id}` | Delete configuration |
| `GET` | `/` | List saved configurations |
| `POST` | `/{config_id}/use` | Track configuration usage |

---

## 🏗️ File Structure

```
src/
├── api/
│   ├── converters/
│   │   ├── __init__.py
│   │   ├── pipeline_router.py      # Orchestration
│   │   ├── jobs_router.py          # Job management
│   │   ├── history_router.py       # History tracking
│   │   ├── configs_router.py       # Saved configs
│   │   └── discovery_router.py     # Capabilities API
│   │
│   └── connectors/
│       ├── inbound/
│       │   ├── __init__.py
│       │   ├── powerbi_router.py   # Power BI API
│       │   ├── yaml_router.py      # YAML API
│       │   ├── tableau_router.py   # Tableau API
│       │   └── excel_router.py     # Excel API
│       │
│       └── outbound/
│           ├── __init__.py
│           ├── dax_router.py       # DAX API
│           ├── sql_router.py       # SQL API
│           ├── uc_metrics_router.py # UC Metrics API
│           └── yaml_router.py      # YAML output API
│
├── converters/
│   ├── base/                       # Core models & interfaces
│   ├── inbound/                    # Inbound connector implementations
│   │   ├── powerbi/
│   │   ├── yaml/
│   │   └── base.py
│   ├── outbound/                   # Outbound converter implementations
│   │   ├── dax/
│   │   ├── sql/
│   │   ├── uc_metrics/
│   │   └── yaml/
│   ├── common/                     # Shared transformers
│   └── pipeline.py                 # Pipeline orchestration logic
│
├── services/
│   └── converter_service.py        # Business logic layer
│
├── repositories/
│   └── conversion_repository.py    # Data access layer
│
└── schemas/
    └── conversion.py                # Pydantic models
```

---

## 🚀 Adding a New Connector

### Example: Adding Looker Inbound Connector

**Step 1**: Create the router

```python
# src/api/connectors/inbound/looker_router.py
from fastapi import APIRouter, Depends
from src.converters.inbound.looker import LookerConnector
from src.schemas.looker import LookerConfig

router = APIRouter(
    prefix="/api/connectors/inbound/looker",
    tags=["looker"]
)

@router.post("/extract")
async def extract(config: LookerConfig) -> KPIDefinition:
    """Extract calculated fields from Looker."""
    connector = LookerConnector(config)
    return await connector.extract()

@router.get("/dashboards")
async def list_dashboards(auth: LookerAuth) -> List[Dashboard]:
    """List available Looker dashboards."""
    client = LookerClient(auth)
    return await client.list_dashboards()

@router.post("/validate")
async def validate(config: LookerConfig) -> ValidationResult:
    """Validate Looker connection."""
    connector = LookerConnector(config)
    return await connector.validate()
```

**Step 2**: Register the router

```python
# src/api/connectors/inbound/__init__.py
from .powerbi_router import router as powerbi_router
from .yaml_router import router as yaml_router
from .looker_router import router as looker_router  # NEW

def register_inbound_routers(app):
    app.include_router(powerbi_router)
    app.include_router(yaml_router)
    app.include_router(looker_router)  # NEW
```

**Step 3**: Implement the connector

```python
# src/converters/inbound/looker/connector.py
from src.converters.base.converter import BaseInboundConnector
from src.converters.base.models import KPIDefinition

class LookerConnector(BaseInboundConnector):
    async def extract(self) -> KPIDefinition:
        # Implementation here
        pass
```

**That's it!** No changes needed to:
- Existing connectors
- Pipeline orchestration
- Database models
- Frontend (discovers new connector via capabilities API)

---

## 🎯 Key Benefits

### 1. **True Modularity**
- Each connector is independent
- Add/remove/update connectors without affecting others
- Easy to maintain and test

### 2. **API-First Design**
- Frontend dynamically discovers capabilities
- Third-party integrations via REST API
- Consistent interface across all connectors

### 3. **Linear Complexity**
- N inbound + M outbound = N + M implementations
- No exponential growth as connectors are added

### 4. **Easy Composition**
```bash
# Option 1: Manual composition
POST /api/connectors/inbound/powerbi/extract → KPIDefinition
POST /api/connectors/outbound/dax/generate  ← KPIDefinition

# Option 2: Pipeline orchestration
POST /api/converters/pipeline/execute
```

### 5. **Independent Testing**
```bash
# Test each connector in isolation
pytest tests/connectors/inbound/test_powerbi.py
pytest tests/connectors/outbound/test_dax.py
```

### 6. **Versioning Support**
```
/api/v1/connectors/inbound/powerbi/...
/api/v2/connectors/inbound/powerbi/...  # Breaking changes
```

### 7. **Multi-Tenant Isolation**
- All operations filtered by `group_id`
- History tracking per tenant
- Configuration isolation

---

## 📈 Usage Examples

### Example 1: Direct Connector Usage

```python
# Extract from Power BI
response = requests.post(
    "http://api/connectors/inbound/powerbi/extract",
    json={
        "semantic_model_id": "abc123",
        "group_id": "workspace456",
        "access_token": "Bearer ..."
    }
)
kpi_definition = response.json()

# Generate DAX
response = requests.post(
    "http://api/connectors/outbound/dax/generate",
    json={
        "kpi_definition": kpi_definition,
        "process_structures": True
    }
)
dax_code = response.json()["code"]
```

### Example 2: Pipeline Orchestration

```python
response = requests.post(
    "http://api/converters/pipeline/execute",
    json={
        "source": {
            "type": "powerbi",
            "config": {
                "semantic_model_id": "abc123",
                "group_id": "workspace456",
                "access_token": "Bearer ..."
            }
        },
        "target": {
            "type": "sql",
            "config": {
                "dialect": "databricks",
                "include_comments": True
            }
        }
    }
)
result = response.json()
```

### Example 3: Async Job

```python
# Create job
response = requests.post(
    "http://api/converters/pipeline/execute/async",
    json={
        "source": {...},
        "target": {...}
    }
)
job_id = response.json()["job_id"]

# Check status
response = requests.get(f"http://api/converters/jobs/{job_id}")
status = response.json()["status"]  # pending, running, completed, failed
```

### Example 4: Frontend Discovery

```javascript
// Discover available connectors
const response = await fetch('/api/converters/discovery/capabilities');
const capabilities = await response.json();

// Render dropdowns based on discovery
const inboundOptions = capabilities.inbound.map(c => ({
  label: c.name,
  value: c.type,
  schema: c.config_schema
}));

const outboundOptions = capabilities.outbound.map(c => ({
  label: c.name,
  value: c.type,
  schema: c.config_schema
}));
```

---

## 🔒 Security Considerations

### Authentication
- All endpoints require authentication (JWT tokens)
- Group-based authorization via `group_id`
- API keys stored encrypted in database

### Data Isolation
- Multi-tenant design with strict `group_id` filtering
- No cross-tenant data leakage
- Repository-level enforcement

### Credential Management
- OAuth tokens never logged
- Encrypted storage for sensitive credentials
- Token refresh handling

---

## 📊 Monitoring & Observability

### Metrics
- Conversion success/failure rates per connector
- Execution time per conversion path
- Popular conversion paths
- Error rates by connector type

### Logging
- All conversions logged to history
- Audit trail with full configuration
- Error messages with context

### Health Checks
```bash
GET /api/converters/discovery/health

{
  "status": "healthy",
  "connectors": {
    "powerbi": "active",
    "yaml": "active",
    "dax": "active",
    "sql": "active"
  }
}
```

---

## 🚦 Current Status

| Connector | Type | Status | Version |
|-----------|------|--------|---------|
| Power BI | Inbound | ✅ Active | 1.0.0 |
| YAML | Inbound | ✅ Active | 1.0.0 |
| Tableau | Inbound | 🚧 Coming Soon | - |
| Excel | Inbound | 🚧 Coming Soon | - |
| DAX | Outbound | ✅ Active | 1.0.0 |
| SQL | Outbound | ✅ Active | 1.0.0 |
| UC Metrics | Outbound | ✅ Active | 1.0.0 |
| YAML | Outbound | ✅ Active | 1.0.0 |

---

## 📚 Additional Resources

- [Frontend Integration Guide](./FRONTEND_INTEGRATION_GUIDE.md)
- [Inbound Integration Guide](./INBOUND_INTEGRATION_GUIDE.md)
- [API Reference](./API_REFERENCE.md)
- [Developer Guide](./DEVELOPER_GUIDE.md)

---

## 🤝 Contributing

When adding a new connector:

1. Create router in appropriate directory (`inbound/` or `outbound/`)
2. Implement connector logic in `src/converters/`
3. Add tests in `tests/connectors/`
4. Update discovery configuration
5. Document in this README

The modular design ensures your connector is completely isolated and won't affect existing functionality.

---

**Last Updated**: 2025-12-01
**Version**: 1.0.0
