# Tool 78 - report references tool

**What it is:** Extracts the mapping between report visuals and the measures/tables they use, generating a cross-reference matrix with direct page URLs for impact analysis.

> [!IMPORTANT]
> Disabled by default. Requires Microsoft Fabric PBIR format reports. Enable in seeds if needed.

---

## Why it exists

When an SA modifies a measure during migration, they need to know which report pages will be affected. Power BI Desktop can show this, but only per-report and manually. This tool scans all reports associated with a semantic model and produces a complete dependency map.

## What problem it solves

- **Impact analysis before migration:** "If I change [Total Revenue], which 12 report pages break?"
- **Unused measure detection:** Find measures referenced by zero reports - safe to deprioritize
- **Migration documentation:** Produce a complete inventory of what uses what

---

## Fabric-only requirement

Requires Fabric reports in **PBIR format** (Power BI Enhanced Report Format). Classic `.pbix` files uploaded to Power BI Service are not supported. Reports must be in a Fabric workspace and in the new PBIR format.

---

## Microsoft API reference

Uses: `GET /groups/{groupId}/items/{itemId}/getDefinition` (report definition)
Docs: [Fabric - Get Item Definition](https://learn.microsoft.com/en-us/rest/api/fabric/core/items/get-item-definition)

---

## Authentication

**Non-Admin SP** with `Report.ReadWrite.All` permission.
See [Authentication Setup](./01-authentication-setup.md).

---

## Configuration

| Parameter | Required | Description |
|-----------|----------|-------------|
| `workspace_id` | Yes | Fabric Workspace GUID |
| `dataset_id` | Recommended | Discovers ALL reports using this dataset |
| `report_id` | Alternative | Single specific report GUID |
| `tenant_id` | Yes | Azure AD tenant ID |
| `client_id` | Yes | SP client ID |
| `client_secret` | Yes | SP client secret |
| `output_format` | No | `json`, `markdown`, or `matrix` (default: `json`) |
| `group_by` | No | `page`, `measure`, or `table` (default: `page`) |

---

## Example output (Markdown)

```markdown
## Sales Dashboard - Revenue Overview
- Measures: Total Revenue, YoY Growth, Revenue Target
- Tables: Fact_Sales, Dim_Calendar
- Page URL: https://app.powerbi.com/groups/.../reports/.../ReportSection1

## Sales Dashboard - Regional Breakdown
- Measures: Total Revenue, Units Sold
- Tables: Fact_Sales, Dim_Geography
- Page URL: https://app.powerbi.com/groups/.../reports/.../ReportSection2

## Measure Usage Summary
- **Total Revenue**: Used in 8 pages across 3 reports
- **YoY Growth**: Used in 2 pages across 1 report
- **Unused measures**: [DAX_Internal_Helper, Calc_Debug_Flag]
```

---

## Notes

- Provide `dataset_id` (not `report_id`) to automatically discover and scan all reports that use the semantic model
- The direct page URLs in the output let you immediately open the affected page in Power BI to verify impact
- If a customer is on legacy Power BI Service (not Fabric), this tool returns empty results - Tool 72 can be used to find measure usage instead

## See also

- [Power BI integration hub](./README.md)
- [Authentication and service principal setup](./01-authentication-setup.md)
- [Tool 72 - comprehensive analysis](./tool-72-comprehensive-analysis.md)
- [Tool 76 - hierarchies tool](./tool-76-hierarchies.md)
- [Tool 77 - field parameters and calculation groups](./tool-77-field-parameters.md)

Back to the [Power BI integration hub](./README.md).
