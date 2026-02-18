# Frontend Integration Guide - Unified Measure Conversion Pipeline

## Overview

The **Measure Conversion Pipeline** (Tool ID 74) is a unified tool that replaces individual converter tools with a dropdown-based architecture for better UX scalability.

Instead of separate tools like:
- ❌ PowerBIToDAXTool
- ❌ PowerBIToSQLTool
- ❌ YAMLToDAXTool
- ❌ YAMLToSQLTool
- ❌ etc. (N×M tool explosion)

We now have:
- ✅ **One unified tool** with two dropdown selections:
  1. **Inbound Connector** (Source): Power BI, YAML, Tableau (future), Excel (future)
  2. **Outbound Format** (Target): DAX, SQL, UC Metrics, YAML

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│         Measure Conversion Pipeline (Tool 74)           │
│                                                         │
│  ┌──────────────────────┐   ┌────────────────────────┐ │
│  │ Inbound Connector ▼  │   │ Outbound Format ▼      │ │
│  ├──────────────────────┤   ├────────────────────────┤ │
│  │ • Power BI           │   │ • DAX                  │ │
│  │ • YAML               │   │ • SQL (multiple        │ │
│  │ • Tableau (future)   │   │   dialects)            │ │
│  │ • Excel (future)     │   │ • UC Metrics           │ │
│  └──────────────────────┘   │ • YAML                 │ │
│                             └────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
        ┌────────────────────────────┐
        │   Dynamic Configuration    │
        │   (based on selections)    │
        └────────────────────────────┘
```

## UI Implementation

### 1. Tool Configuration Form

```typescript
interface MeasureConversionConfig {
  // ===== INBOUND SELECTION =====
  inbound_connector: 'powerbi' | 'yaml' | 'tableau' | 'excel';

  // ===== POWER BI CONFIG (shown if inbound_connector === 'powerbi') =====
  powerbi_semantic_model_id?: string;
  powerbi_group_id?: string;
  powerbi_tenant_id?: string;
  powerbi_client_id?: string;
  powerbi_client_secret?: string;
  powerbi_info_table_name?: string;
  powerbi_include_hidden?: boolean;
  powerbi_filter_pattern?: string;

  // ===== YAML CONFIG (shown if inbound_connector === 'yaml') =====
  yaml_content?: string;
  yaml_file_path?: string;

  // ===== OUTBOUND SELECTION =====
  outbound_format: 'dax' | 'sql' | 'uc_metrics' | 'yaml';

  // ===== SQL CONFIG (shown if outbound_format === 'sql') =====
  sql_dialect?: 'databricks' | 'postgresql' | 'mysql' | 'sqlserver' | 'snowflake' | 'bigquery' | 'standard';
  sql_include_comments?: boolean;
  sql_process_structures?: boolean;

  // ===== UC METRICS CONFIG (shown if outbound_format === 'uc_metrics') =====
  uc_catalog?: string;
  uc_schema?: string;
  uc_process_structures?: boolean;

  // ===== DAX CONFIG (shown if outbound_format === 'dax') =====
  dax_process_structures?: boolean;

  // ===== GENERAL =====
  definition_name?: string;
  result_as_answer?: boolean;
}
```

### 2. React Component Example

```tsx
import React, { useState } from 'react';
import { FormControl, InputLabel, Select, MenuItem, TextField, Switch } from '@mui/material';

const MeasureConversionPipelineConfig: React.FC = () => {
  const [config, setConfig] = useState<MeasureConversionConfig>({
    inbound_connector: 'powerbi',
    outbound_format: 'dax',
    powerbi_info_table_name: 'Info Measures',
    powerbi_include_hidden: false,
    sql_dialect: 'databricks',
    sql_include_comments: true,
    sql_process_structures: true,
    uc_catalog: 'main',
    uc_schema: 'default',
    uc_process_structures: true,
    dax_process_structures: true,
    result_as_answer: false,
  });

  return (
    <div>
      {/* ===== INBOUND CONNECTOR DROPDOWN ===== */}
      <FormControl fullWidth margin="normal">
        <InputLabel>Inbound Connector (Source)</InputLabel>
        <Select
          value={config.inbound_connector}
          onChange={(e) => setConfig({...config, inbound_connector: e.target.value})}
        >
          <MenuItem value="powerbi">Power BI</MenuItem>
          <MenuItem value="yaml">YAML</MenuItem>
          <MenuItem value="tableau" disabled>Tableau (Coming Soon)</MenuItem>
          <MenuItem value="excel" disabled>Excel (Coming Soon)</MenuItem>
        </Select>
      </FormControl>

      {/* ===== POWER BI CONFIGURATION (conditional) ===== */}
      {config.inbound_connector === 'powerbi' && (
        <>
          <TextField
            fullWidth
            label="Dataset/Semantic Model ID"
            value={config.powerbi_semantic_model_id || ''}
            onChange={(e) => setConfig({...config, powerbi_semantic_model_id: e.target.value})}
            margin="normal"
            required
            helperText="Power BI dataset ID to extract measures from"
          />
          <TextField
            fullWidth
            label="Workspace ID"
            value={config.powerbi_group_id || ''}
            onChange={(e) => setConfig({...config, powerbi_group_id: e.target.value})}
            margin="normal"
            required
            helperText="Power BI workspace ID containing the dataset"
          />
          <TextField
            fullWidth
            label="OAuth Access Token"
            value={config.powerbi_client_secret || ''}
            onChange={(e) => setConfig({...config, powerbi_client_secret: e.target.value})}
            margin="normal"
            required
            type="password"
            helperText="OAuth access token for Power BI authentication"
          />
          <TextField
            fullWidth
            label="Info Table Name"
            value={config.powerbi_info_table_name || 'Info Measures'}
            onChange={(e) => setConfig({...config, powerbi_info_table_name: e.target.value})}
            margin="normal"
            helperText="Name of the Info Measures table (default: 'Info Measures')"
          />
          <FormControl fullWidth margin="normal">
            <label>
              Include Hidden Measures
              <Switch
                checked={config.powerbi_include_hidden || false}
                onChange={(e) => setConfig({...config, powerbi_include_hidden: e.target.checked})}
              />
            </label>
          </FormControl>
        </>
      )}

      {/* ===== YAML CONFIGURATION (conditional) ===== */}
      {config.inbound_connector === 'yaml' && (
        <>
          <TextField
            fullWidth
            label="YAML Content"
            value={config.yaml_content || ''}
            onChange={(e) => setConfig({...config, yaml_content: e.target.value})}
            margin="normal"
            multiline
            rows={10}
            helperText="Paste YAML KPI definition content here"
          />
          <TextField
            fullWidth
            label="YAML File Path (Alternative)"
            value={config.yaml_file_path || ''}
            onChange={(e) => setConfig({...config, yaml_file_path: e.target.value})}
            margin="normal"
            helperText="Or provide path to YAML file"
          />
        </>
      )}

      {/* ===== OUTBOUND FORMAT DROPDOWN ===== */}
      <FormControl fullWidth margin="normal">
        <InputLabel>Outbound Format (Target)</InputLabel>
        <Select
          value={config.outbound_format}
          onChange={(e) => setConfig({...config, outbound_format: e.target.value})}
        >
          <MenuItem value="dax">DAX (Power BI)</MenuItem>
          <MenuItem value="sql">SQL (Multiple Dialects)</MenuItem>
          <MenuItem value="uc_metrics">Unity Catalog Metrics</MenuItem>
          <MenuItem value="yaml">YAML Definition</MenuItem>
        </Select>
      </FormControl>

      {/* ===== SQL CONFIGURATION (conditional) ===== */}
      {config.outbound_format === 'sql' && (
        <>
          <FormControl fullWidth margin="normal">
            <InputLabel>SQL Dialect</InputLabel>
            <Select
              value={config.sql_dialect || 'databricks'}
              onChange={(e) => setConfig({...config, sql_dialect: e.target.value})}
            >
              <MenuItem value="databricks">Databricks</MenuItem>
              <MenuItem value="postgresql">PostgreSQL</MenuItem>
              <MenuItem value="mysql">MySQL</MenuItem>
              <MenuItem value="sqlserver">SQL Server</MenuItem>
              <MenuItem value="snowflake">Snowflake</MenuItem>
              <MenuItem value="bigquery">BigQuery</MenuItem>
              <MenuItem value="standard">Standard SQL</MenuItem>
            </Select>
          </FormControl>
          <FormControl fullWidth margin="normal">
            <label>
              Include Comments
              <Switch
                checked={config.sql_include_comments !== false}
                onChange={(e) => setConfig({...config, sql_include_comments: e.target.checked})}
              />
            </label>
          </FormControl>
        </>
      )}

      {/* ===== UC METRICS CONFIGURATION (conditional) ===== */}
      {config.outbound_format === 'uc_metrics' && (
        <>
          <TextField
            fullWidth
            label="Unity Catalog Catalog"
            value={config.uc_catalog || 'main'}
            onChange={(e) => setConfig({...config, uc_catalog: e.target.value})}
            margin="normal"
            helperText="Unity Catalog catalog name (default: 'main')"
          />
          <TextField
            fullWidth
            label="Unity Catalog Schema"
            value={config.uc_schema || 'default'}
            onChange={(e) => setConfig({...config, uc_schema: e.target.value})}
            margin="normal"
            helperText="Unity Catalog schema name (default: 'default')"
          />
        </>
      )}
    </div>
  );
};
```

### 3. API Integration

```typescript
// Add tool to agent configuration
const agentConfig = {
  name: "Measure Migration Agent",
  tools: [
    {
      id: 74, // Measure Conversion Pipeline
      config: {
        inbound_connector: "powerbi",
        powerbi_semantic_model_id: "abc-123-def",
        powerbi_group_id: "workspace-456",
        powerbi_client_secret: userOAuthToken, // From OAuth flow
        outbound_format: "sql",
        sql_dialect: "databricks",
        sql_include_comments: true,
      }
    }
  ]
};

// Execute agent
const response = await fetch('/api/crews/execute', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(agentConfig)
});
```

## Power BI Authentication Flow

### 1. OAuth Flow Setup

```typescript
// Use Microsoft Authentication Library (MSAL)
import { PublicClientApplication } from "@azure/msal-browser";

const msalConfig = {
  auth: {
    clientId: "YOUR_CLIENT_ID",
    authority: "https://login.microsoftonline.com/common",
    redirectUri: window.location.origin,
  }
};

const msalInstance = new PublicClientApplication(msalConfig);

// Login and get access token
const loginRequest = {
  scopes: ["https://analysis.windows.net/powerbi/api/.default"]
};

const loginResponse = await msalInstance.loginPopup(loginRequest);
const accessToken = loginResponse.accessToken;

// Use token in tool config
const toolConfig = {
  inbound_connector: "powerbi",
  powerbi_client_secret: accessToken,
  // ... other config
};
```

### 2. Token Management

- Store tokens securely in frontend state (React Context, Redux, etc.)
- Refresh tokens before expiration
- Handle token refresh in background
- Clear tokens on logout

## Common Use Cases

### Use Case 1: Power BI → Databricks SQL

```typescript
{
  inbound_connector: "powerbi",
  powerbi_semantic_model_id: "dataset-123",
  powerbi_group_id: "workspace-456",
  powerbi_client_secret: "eyJ...",
  outbound_format: "sql",
  sql_dialect: "databricks",
  sql_include_comments: true,
  sql_process_structures: true
}
```

**Result**: SQL queries optimized for Databricks with comments and time intelligence

### Use Case 2: YAML → Power BI DAX

```typescript
{
  inbound_connector: "yaml",
  yaml_content: `
    description: Sales Metrics
    kpis:
      - name: Total Revenue
        formula: SUM(Sales[Amount])
  `,
  outbound_format: "dax",
  dax_process_structures: true
}
```

**Result**: DAX measures ready for Power BI semantic model

### Use Case 3: Power BI → Unity Catalog Metrics

```typescript
{
  inbound_connector: "powerbi",
  powerbi_semantic_model_id: "dataset-123",
  powerbi_group_id: "workspace-456",
  powerbi_client_secret: "eyJ...",
  outbound_format: "uc_metrics",
  uc_catalog: "sales_analytics",
  uc_schema: "metrics",
  uc_process_structures: true
}
```

**Result**: Unity Catalog metrics definitions with lineage tracking

### Use Case 4: Power BI → YAML (Backup/Documentation)

```typescript
{
  inbound_connector: "powerbi",
  powerbi_semantic_model_id: "dataset-123",
  powerbi_group_id: "workspace-456",
  powerbi_client_secret: "eyJ...",
  outbound_format: "yaml"
}
```

**Result**: Portable YAML definitions for version control and documentation

## UI/UX Recommendations

### 1. Progressive Disclosure
- Show only relevant configuration fields based on dropdown selections
- Hide irrelevant options to reduce cognitive load
- Use clear section headers for inbound vs outbound config

### 2. Validation
- Validate required fields based on selections:
  - Power BI: semantic_model_id, group_id, access_token required
  - YAML: Either yaml_content OR yaml_file_path required
- Show validation errors inline
- Disable submit until all required fields are filled

### 3. Defaults
- Pre-populate common defaults:
  - `powerbi_info_table_name`: "Info Measures"
  - `sql_dialect`: "databricks"
  - `uc_catalog`: "main"
  - `uc_schema`: "default"
  - All `process_structures` flags: true

### 4. Help Text
- Provide contextual help for each field
- Link to documentation for complex fields (OAuth setup, etc.)
- Show examples for text inputs

### 5. Results Display
- Show conversion results in code editor with syntax highlighting
- Support different formats: DAX, SQL, YAML
- Provide download/copy buttons
- Show metadata: measure count, source info, warnings

## Migration from Legacy Tools

### Backwards Compatibility

The following legacy tools are still supported but deprecated:
- YAMLToDAXTool (Tool 71)
- YAMLToSQLTool (Tool 72)
- YAMLToUCMetricsTool (Tool 73)
- PowerBIConnectorTool (Tool 74 - old version)

**Recommendation**: Migrate to unified Measure Conversion Pipeline (Tool 74) for:
- Better UX scalability
- Easier addition of new sources/targets
- Consistent configuration pattern
- Single tool to maintain

### Migration Path

1. **Identify usages** of legacy tools in agent configurations
2. **Map configurations** to unified tool format:
   ```typescript
   // Old: YAMLToDAXTool
   { yaml_content: "...", process_structures: true }

   // New: Measure Conversion Pipeline
   {
     inbound_connector: "yaml",
     yaml_content: "...",
     outbound_format: "dax",
     dax_process_structures: true
   }
   ```
3. **Update UI** to use new tool selection
4. **Test conversions** to ensure same results
5. **Remove legacy tool references**

## Troubleshooting

### Common Issues

**Issue**: "Error: Missing required parameters"
- **Solution**: Check that all required fields for selected inbound connector are filled
- Power BI requires: semantic_model_id, group_id, access_token
- YAML requires: yaml_content OR yaml_file_path

**Issue**: "Error: Invalid outbound_format"
- **Solution**: Ensure outbound_format is one of: dax, sql, uc_metrics, yaml

**Issue**: "Error: Conversion failed - authentication error"
- **Solution**: Verify Power BI access token is valid and not expired
- Implement token refresh mechanism

**Issue**: "Error: YAML conversion failed - parse error"
- **Solution**: Validate YAML content syntax before submission
- Check for proper indentation and structure

## Support and Documentation

- **Backend Implementation**: `src/converters/pipeline.py`
- **Tool Implementation**: `src/engines/crewai/tools/custom/measure_conversion_pipeline_tool.py`
- **Seed Configuration**: `src/seeds/tools.py` (Tool ID 74)
- **Complete Integration Summary**: `src/converters/COMPLETE_INTEGRATION_SUMMARY.md`

## Future Enhancements

### Planned Inbound Connectors
- **Tableau**: Extract measures from Tableau workbooks
- **Excel**: Parse Excel-based KPI definitions
- **Looker**: Extract LookML measures

### Planned Outbound Formats
- **Python**: Generate pandas/polars code
- **R**: Generate dplyr/tidyverse code
- **JSON**: REST API-friendly format

### UI Enhancements
- Preview mode: Preview conversion before full execution
- Batch conversion: Process multiple sources at once
- Conversion history: Save and reuse previous conversions
- Template library: Pre-configured conversion templates
