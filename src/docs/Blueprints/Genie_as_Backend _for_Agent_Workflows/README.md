# Genie superstore insights blueprint

Integrate Databricks Genie with Kasal agent workflows to retrieve enterprise data and generate business insights from the Superstore dataset.

This blueprint demonstrates how to integrate **Databricks Genie with Kasal agent workflows** to retrieve enterprise data and generate business insights.

The workflow retrieves structured sales data from a Genie Space connected to Unity Catalog, processes it with multiple agents, and produces enterprise-ready outputs such as summaries or dashboards.

---

## Architecture overview

The workflow follows this architecture:

1. Agents query a Genie Space connected to Unity Catalog  
2. Retrieved data is passed back into the workflow  
3. Subsequent agents perform post-processing (analysis, summarization, reporting)  
4. Results can integrate with BI tools such as Power BI  
5. Output becomes enterprise-ready artifacts (executive summaries, dashboards, insights)

---

## Blueprint components

This blueprint contains the following documentation:

### Creating a Genie Space

How to create and configure a Genie Space connected to the Superstore dataset. See [Creating a Genie Space for the Superstore dataset](./create_genie_space.md).

### Kasal agent workflow

How to import and run the Kasal workflow that uses Genie as backend. See [Running the Kasal agent workflow](./kasal-agent-workflow.md).

---

## Example use case

Example business question:

Which regions had the lowest profit margin last quarter and which product categories drove the losses?

The workflow retrieves the data from Genie and produces:

- structured insights
- executive summary
- potential dashboard inputs for BI tools such as Power BI.

---

## Enterprise extensions

This blueprint can be extended with:

- automated Power BI dashboard updates  
- anomaly detection agents  
- financial reporting workflows  
- multi-space Genie queries

---

## Files included

- `workflow.json` – importable Kasal workflow
- [Creating a Genie Space for the Superstore dataset](./create_genie_space.md) – Genie setup guide
- [Running the Kasal agent workflow](./kasal-agent-workflow.md) – Kasal workflow instructions

## Related

- [Creating a Genie Space for the Superstore dataset](./create_genie_space.md) — set up the Genie backend
- [Running the Kasal agent workflow](./kasal-agent-workflow.md) — build and run the workflow
- [Example crews and flows](../../examples/README.md) — more importable templates

Back to the [documentation hub](../../README.md).