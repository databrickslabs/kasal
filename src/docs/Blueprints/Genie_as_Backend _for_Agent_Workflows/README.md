# Genie Superstore Insights Blueprint

This blueprint demonstrates how to integrate **Databricks Genie with Kasal agent workflows** to retrieve enterprise data and generate business insights.

The workflow retrieves structured sales data from a Genie Space connected to Unity Catalog, processes it with multiple agents, and produces enterprise-ready outputs such as summaries or dashboards.

---

# Architecture Overview

The workflow follows this architecture:

1. Agents query a Genie Space connected to Unity Catalog  
2. Retrieved data is passed back into the workflow  
3. Subsequent agents perform post-processing (analysis, summarization, reporting)  
4. Results can integrate with BI tools such as Power BI  
5. Output becomes enterprise-ready artifacts (executive summaries, dashboards, insights)

---

# Blueprint Components

This blueprint contains the following documentation:

### 1️⃣ Creating a Genie Space

How to create and configure a Genie Space connected to the Superstore dataset.

➡ See:  
`create-genie-space.md`

---

### 2️⃣ Kasal Agent Workflow

How to import and run the Kasal workflow that uses Genie as backend.

➡ See:  
`kasal-agent-workflow.md`

---

# Example Use Case

Example business question:

Which regions had the lowest profit margin last quarter and which product categories drove the losses?

The workflow retrieves the data from Genie and produces:

- structured insights
- executive summary
- potential dashboard inputs for BI tools such as Power BI.

---

# Enterprise Extensions

This blueprint can be extended with:

- automated Power BI dashboard updates  
- anomaly detection agents  
- financial reporting workflows  
- multi-space Genie queries

---

# Files Included

- `workflow.json` – importable Kasal workflow
- `create-genie-space.md` – Genie setup guide
- `kasal-agent-workflow.md` – Kasal workflow instructions