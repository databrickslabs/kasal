# Kasal Example Crews

This directory contains example crew configurations for Power BI workflow automation using the Kasal platform.

## 📋 Available Examples

### 1. **crew_hierarchyconverter.json**
Converts Power BI hierarchy definitions into SQL syntax for Databricks.

**Use Case**: Migrate Power BI hierarchies to Databricks SQL models
**Tools Used**: Power BI Hierarchies Tool
**Agent**: PBI Hierarchies Converter (Claude Opus 4.6)

---

### 2. **crew_measureconverter.json**
Converts Power BI DAX measures into SQL expressions for Databricks.

**Use Case**: Migrate Power BI measures to Databricks SQL
**Tools Used**: Measure Conversion Pipeline Tool
**Agent**: PBI Measure Converter (Claude Opus 4.6)

---

### 3. **crew_relationshipconverter.json**
Extracts and converts Power BI model relationships into SQL DDL.

**Use Case**: Recreate Power BI relationships in Databricks
**Tools Used**: Power BI Relationships Tool
**Agent**: PBI Relationships Converter (Claude Opus 4.6)

---

### 4. **crew_mqueryconverter.json**
Converts Power BI M Query (Power Query) transformations into SQL.

**Use Case**: Migrate Power Query ETL logic to Databricks SQL
**Tools Used**: Power BI M Query Tool
**Agent**: PBI M Query Converter (Claude Opus 4.6)

---

### 5. **crew_fparamcalcgroup.json**
Processes Power BI Field Parameters and Calculation Groups.

**Use Case**: Convert complex Power BI features to Databricks equivalents
**Tools Used**: Power BI Field Parameters Tool, Calculation Groups Tool
**Agent**: PBI Field Parameters Converter (Claude Opus 4.6)

---

### 6. **crew_referencefinder.json**
Analyzes Power BI DAX measures to find and extract column/table references.

**Use Case**: Dependency analysis and impact assessment
**Tools Used**: Power BI Reference Finder Tool
**Agent**: PBI Reference Analyzer (Claude Opus 4.6)

---

### 7. **crew_pbi_analyst.json**
Comprehensive Power BI analysis tool that answers questions about your Power BI data model.

**Use Case**: Natural language queries against Power BI datasets
**Tools Used**: PowerBIAnalysisTool, Power BI Comprehensive Analysis Tool
**Agent**: PBI Data Analyst (Claude Opus 4.6)

**Features**:
- Generates DAX queries from natural language questions
- Executes queries against Power BI datasets
- Returns structured results with visual references
- Supports business mappings and field synonyms
- Handles active filters and report context

---

## 🔧 Configuration

### Required Credentials

All example files use placeholder values that need to be replaced with your actual credentials:

```json
{
  "tenant_id": "00000000-0000-0000-0000-000000000000",  // Azure AD tenant ID
  "workspace_id": "your-workspace-id",                   // Power BI workspace ID
  "dataset_id": "your-dataset-id",                       // Power BI dataset ID
  "client_id": "your-client-id",                         // Azure AD app client ID
  "client_secret": "your-client-secret",                 // Azure AD app secret
  "llm_token": "your-databricks-token",                  // Databricks PAT token
  "llm_workspace_url": "https://your-workspace.cloud.databricks.com"
}
```

### Authentication Methods

These examples use **Service Principal authentication** for Power BI access:

1. **Azure AD App Registration**: Register an app in Azure AD
2. **Power BI API Permissions**: Grant Power BI API permissions to the app
3. **Workspace Access**: Add the service principal to your Power BI workspace

For OBO (On-Behalf-Of) authentication, the system automatically uses user tokens when available.

---

## 🚀 How to Use

### 1. Import into Kasal UI

1. Navigate to the Kasal web interface
2. Go to **Crews** → **Import**
3. Upload one of the example JSON files
4. Update the `tool_configs` with your actual credentials
5. Save and run the crew

### 2. API Import

```bash
curl -X POST http://localhost:8000/api/crews/import \
  -H "Content-Type: application/json" \
  -d @crew_hierarchyconverter.json
```

### 3. Modify for Your Use Case

Each example can be customized:

- **Agent configuration**: Adjust model, temperature, max_iter
- **Task descriptions**: Customize instructions for your specific needs
- **Tool configurations**: Add/remove tools or modify parameters
- **Workflow structure**: Chain multiple crews together

---

## 📊 Power BI Setup Requirements

### Azure AD App Registration

1. Go to [Azure Portal](https://portal.azure.com) → Azure Active Directory → App registrations
2. Create a new registration
3. Add API permissions:
   - `Dataset.Read.All`
   - `Dataset.ReadWrite.All`
   - `Workspace.Read.All`
4. Create a client secret
5. Note the **Tenant ID**, **Client ID**, and **Client Secret**

### Power BI Workspace Setup

1. Open [Power BI Service](https://app.powerbi.com)
2. Navigate to your workspace settings
3. Add the service principal as a **Member** or **Admin**
4. Note the **Workspace ID** from the URL

### Dataset ID

Find your dataset ID in the Power BI URL:
```
https://app.powerbi.com/groups/{workspace-id}/datasets/{dataset-id}/...
```

---

## 🔒 Security Best Practices

- **Never commit credentials** to version control
- Use **environment variables** or **secure vaults** for secrets
- Rotate **client secrets** regularly
- Use **least-privilege access** - grant only necessary permissions
- Consider using **OBO authentication** in production for user-scoped access

---

## 📚 Additional Resources

- [Kasal Documentation](../src/docs/)
- [Power BI REST API](https://docs.microsoft.com/en-us/rest/api/power-bi/)
- [Azure AD App Registration](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
- [CrewAI Documentation](https://docs.crewai.com)

---

## 🐛 Troubleshooting

### Common Issues

**Error: "workspace_id is required"**
- Ensure `workspace_id` is in the `tool_configs` section
- Verify the tool has `workspace_id` in its `credential_fields`

**Error: "403 Forbidden"**
- Check service principal has workspace access
- Verify Azure AD app has Power BI API permissions
- Ensure client secret hasn't expired

**Error: "Dataset not found"**
- Verify dataset ID is correct
- Check workspace ID matches the dataset's workspace
- Ensure dataset is published and not deleted

---

## 📝 File Structure

```
examples/
├── README.md                                  # This file
├── crew_hierarchyconverter.json               # Sanitized examples (use these)
├── crew_measureconverter.json
├── crew_relationshipconverter.json
├── crew_mqueryconverter.json
├── crew_fparamcalcgroup.json
├── crew_referencefinder.json
├── crew_pbi_analyst.json
├── crew_*_static.json                         # Internal templates (with real creds)
├── crew_*_dynamic.json                        # Dynamic versions (require runtime params)
└── powerbi_full_pipeline.ipynb                # Jupyter notebook example

```

**Note**: Files ending with `_static.json` contain actual credentials and should not be shared publicly. The sanitized versions (without `_static`) are safe to share.

---

## 🤝 Contributing

To add new examples:

1. Create your crew configuration with placeholder credentials
2. Test it thoroughly with real credentials (in a `*_static.json` file)
3. Run the sanitization script to create the public version
4. Update this README with the new example description

---

## 📄 License

These examples are provided as-is for demonstration purposes. Modify and use them according to your organization's policies.
