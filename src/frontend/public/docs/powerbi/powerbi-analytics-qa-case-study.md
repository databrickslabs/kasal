# Power BI Analytics Q&A — Case Study & Setup Guide

Natural language questions against a live Power BI semantic model, answered with DAX — no SQL, no exports, no manual measure lookup. This guide walks through the full setup: crew architecture, credential wiring, and — most importantly — **context enrichment** that makes the difference between a tool that technically works and one your analysts actually love.

---

## The 3-Agent Crew Architecture

The example crew (`crew_pbi_analyst_qa.json`) uses three agents in sequence:

```
User question: "What is my total sampling per customer and material?"
          │
          ▼
┌─────────────────────────────────────────────────┐
│  Agent 1: PBI Fetcher                           │
│  Tool 79 — Power BI Semantic Model Fetcher      │
│                                                 │
│  Connects to PBI REST API, downloads the full   │
│  semantic model metadata (measures, tables,     │
│  relationships, M-Query). Caches the result.    │
└────────────────────────┬────────────────────────┘
                         │ model_context_json (full)
                         ▼
┌─────────────────────────────────────────────────┐
│  Agent 2: PBI Metadata Reducer                  │
│  Tool 81 — Metadata Reducer                     │
│                                                 │
│  Slims the full model context down to the       │
│  tables, measures, and columns actually         │
│  relevant to the user's question. Reduces       │
│  token overhead by 60–80% before sending        │
│  to the LLM.                                    │
└────────────────────────┬────────────────────────┘
                         │ model_context_json (reduced)
                         ▼
┌─────────────────────────────────────────────────┐
│  Agent 3: PBI Analyst                           │
│  Tool 80 — DAX Generator                        │
│                                                 │
│  Reads reduced context + question, generates    │
│  DAX EVALUATE statement via LLM, executes it    │
│  against PBI Execute Queries API, auto-retries  │
│  on failure with error feedback.                │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
                  Answer + data table
```

**Why three agents instead of one (Tool 72)?**
Tool 72 does everything in a single call — great for one-off questions. The 3-agent crew shines when:
- You have a large semantic model (400+ measures) — the Reducer cuts context by 80%
- You're running multiple questions in a session — model fetch is cached, so agents 2+3 skip the API call
- You want to customize the reduction step independently (e.g. apply custom `visible_tables` filtering)

---

## Quick Start — Import the Example Crew

1. Download [`crew_pbi_analyst_qa.json`](../examples/crew_pbi_analyst_qa.json) from the examples folder
2. In Kasal UI → **Crews** → **Import** → select the file
3. Open each task node in the canvas and fill in the credential placeholders (see table below)
4. Set `user_question` as an execution input variable
5. Click **Run**

### Minimum Required Credentials (per task)

| Field | Where | Value |
|-------|-------|-------|
| `workspace_id` | All 3 tasks | Power BI workspace GUID |
| `dataset_id` | All 3 tasks | Semantic model / dataset GUID |
| `tenant_id` | All 3 tasks | Azure AD tenant ID |
| `client_id` | All 3 tasks | Service principal client ID |
| `client_secret` | All 3 tasks | Service principal client secret |
| `llm_workspace_url` | Task 3 (DAX tool) | `https://<your-workspace>.azuredatabricks.net` |
| `llm_token` | Task 3 (DAX tool) | Databricks PAT |

> The SP needs **Dataset.Read.All** in your Power BI workspace. Admin permissions are NOT required for Q&A.

---

## Context Enrichment — The Real Power

This is what separates a passable demo from a production-grade analytics assistant. The DAX Generator (Tool 80) accepts six context enrichment fields that are configured once in the tool's task form and then automatically applied to every question.

### The Transformation in Practice

**Without context enrichment:**
```
User asks:  "What is the number of customers with Complete CGR in the Italian BU in week 1?"

Tool needs: "What is [num_customers] in [Initial_Sizing]
             where [Initial_Sizing][description] = 'Complete CGR'
             AND [Initial_Sizing][BU] = 'Italy'
             AND [Initial_Sizing][Week] = 1
             AND [Initial_Sizing][Mandatory_Version] IN ('Landline','Mobile')?"
```
Without context, the LLM would hallucinate measure names, get the table wrong, and miss the implicit Mandatory_Version filter entirely.

**With context enrichment:**
```
User asks:   "What is the number of customers with Complete CGR in the Italian BU in week 1?"

Tool resolves automatically:
  "Complete CGR"  → [Initial_Sizing][description] = 'Complete CGR'   (business_mappings)
  "Italian BU"    → [Initial_Sizing][BU] = 'Italy'                    (business_mappings)
  "week 1"        → [Initial_Sizing][Week] = 1                        (business_mappings)
  "customers"     → num_customers measure                              (field_synonyms)
  Mandatory_Version filter → auto-applied from active_filters          (active_filters)
```

---

### Field-by-Field Reference

#### `business_mappings` — natural language → DAX filter expressions

Maps phrases your users actually say to the DAX filter expressions that evaluate them. The tool applies these automatically via `TREATAS` or `FILTER`.

```json
"business_mappings": {
  "Complete CGR":     "[Initial_Sizing][description] = 'Complete CGR'",
  "Italian BU":       "[Initial_Sizing][BU] = 'Italy'",
  "German BU":        "[Initial_Sizing][BU] = 'Germany'",
  "week 1":           "[Initial_Sizing][Week] = 1",
  "week 2":           "[Initial_Sizing][Week] = 2",
  "high-value":       "[Customer_Segment] = 'Premium'",
  "last quarter":     "[Quarter] = QUARTER(TODAY()) - 1",
  "this year":        "[Year] = YEAR(TODAY())",
  "Landline or Mobile": "[Initial_Sizing][Mandatory_Version] IN ('Landline', 'Mobile')"
}
```

**How to build yours**: Open Power BI Desktop → Data view → note the exact table and column names → map your team's language to those names.

---

#### `field_synonyms` — alternative names for measures and columns

Your users say "revenue" but the measure is named "Total Net Sales". Define the mapping here.

```json
"field_synonyms": {
  "num_customers":    ["number of customers", "customer count", "clients", "subscribers"],
  "BU":               ["business unit", "region", "division", "territory"],
  "description":      ["type", "category", "classification", "desc"],
  "Mandatory_Version":["version", "service type", "product type"],
  "Total Net Sales":  ["revenue", "sales", "income", "turnover"],
  "Churn_Rate":       ["attrition", "customer loss", "churn"]
}
```

**Rule of thumb**: Add synonyms for every measure or column that has an "internal" name that differs from how your business talks about it.

---

#### `active_filters` — implicit context from the current view

Simulates the Power BI slicer/page-level filter state. These filters are **automatically applied** to every DAX query, even if the user doesn't mention them.

```json
"active_filters": {
  "Mandatory_Version": ["Landline", "Mobile"],
  "Fiscal_Year":       2025
}
```

Use case: Your analysts always work within a specific fiscal year or product line. Instead of asking them to repeat "for FY2025" in every question, set it once here.

**Format**: The tool accepts both single values (`"Fiscal_Year": 2025`) and lists (`"Mandatory_Version": ["Landline", "Mobile"]`).

---

#### `context_knowledge` — domain knowledge as plain text

Free-text explanation of business rules, calculation logic, or model-specific gotchas. This goes directly into the LLM prompt.

```
"context_knowledge": "Complete CGR means the customer completed the full CGR
onboarding process. Partial CGR means they started but did not finish.
Revenue is always reported net of VAT. The Week column is an ISO week number,
not a sequential counter. BU values match country names (e.g. 'Italy',
'Germany') not codes."
```

When to add something here: any time the LLM generates a plausible-looking query that returns wrong results because it misunderstood a business term.

---

#### `reference_dax` — working DAX examples

Paste one or more verified, working DAX EVALUATE statements. The LLM uses them as syntax and pattern reference — dramatically reducing hallucination on complex measures.

```
"reference_dax": "
EVALUATE
SUMMARIZECOLUMNS(
  Initial_Sizing[BU],
  Initial_Sizing[Week],
  \"Customer Count\", [num_customers]
)
ORDER BY Initial_Sizing[BU], Initial_Sizing[Week]

EVALUATE
CALCULATETABLE(
  SUMMARIZECOLUMNS(
    Initial_Sizing[description],
    \"Count\", [num_customers]
  ),
  Initial_Sizing[Mandatory_Version] IN {\"Landline\", \"Mobile\"}
)"
```

**Tip**: Start with 2–3 representative queries that cover your main measure types (simple aggregation, filtered, time-intelligence). The LLM generalises from these patterns.

---

#### `visible_tables` — scope the model to relevant tables

For large models (100+ tables), limit which tables the Reducer and Generator consider. Dramatically reduces token usage and hallucination risk.

```json
"visible_tables": ["Initial_Sizing", "Customer_Details", "Revenue_Summary", "Calendar"]
```

If not set, all tables are in scope. Set this to the 5–15 tables your Q&A use case actually touches.

---

#### `conversation_history` — multi-turn context

Pass previous Q&A pairs so the LLM understands follow-up questions ("break that down by region", "same thing but for last month").

```json
"conversation_history": [
  {
    "question": "What is total revenue for Italy?",
    "answer":   "€1.5M",
    "filters_used": {"BU": "Italy", "Quarter": 1}
  },
  {
    "question": "How many customers do we have?",
    "answer":   "50,000 customers"
  }
]
```

In an agent workflow, append each Q&A pair to this list between runs to maintain session context.

---

## Where to Configure These Fields in Kasal

Context enrichment fields are set on the **Task 3 node** (Run PowerBI DAX Generation Tool) in the crew canvas:

```
Crew canvas
  └── Task node: "Run PowerBI DAX Generation Tool..."
        └── Tool config (gear icon / edit panel)
              ├── user_question        ← set as {user_question} variable
              ├── workspace_id         ← your workspace GUID
              ├── dataset_id           ← your dataset GUID
              ├── [auth credentials]
              ├── business_mappings    ← paste JSON object
              ├── field_synonyms       ← paste JSON object
              ├── active_filters       ← paste JSON object
              ├── context_knowledge    ← paste plain text
              ├── reference_dax        ← paste DAX examples
              └── visible_tables       ← paste JSON array
```

All fields accept direct JSON input in the Kasal tool config panel. The `user_question` field should be set to `{user_question}` to map it from the execution input form.

---

## Full Working Configuration Example

See [`../powerbi-context-enrichment-example.json`](../powerbi-context-enrichment-example.json) for a complete, copy-paste-ready configuration showing all fields together.

The CGR customer analysis example in that file covers a real scenario:
- Question: *"What is the number of customers with Complete CGR in the Italian BU in week 1?"*
- Model: telecom semantic model with `Initial_Sizing`, customer tables, and time dimensions
- All six enrichment fields configured end-to-end

---

## Troubleshooting Context Issues

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Wrong measure name in generated DAX | Missing `field_synonyms` entry | Add synonym mapping for that measure |
| Filter not applied / wrong values | Wrong column path in `business_mappings` | Verify exact `[Table][Column]` path in PBI Desktop |
| "EVALUATE" syntax error on first attempt | Model uses non-standard table/column names | Add 1–2 `reference_dax` examples using those exact names |
| Correct DAX but wrong numbers | Implicit filter missing | Add missing filter to `active_filters` |
| Slow / high token usage | Full model context passed to generator | Enable Tool 81 (Reducer) and set `visible_tables` |
| Follow-up question ignores previous answer | `conversation_history` not updated | Append each Q&A pair to history between calls |

---

## Next Steps

- **More complex models**: See [Tool 79 - Semantic Model Fetcher](./tool-79-semantic-model-fetcher.md) for caching large models (400+ measures)
- **Reduce cost & latency**: See [Tool 81 - Metadata Reducer](./tool-81-metadata-reducer.md) for aggressive context reduction strategies
- **Migrate measures to UC**: Once you know your model well from Q&A, run the [UCMV Migration](./ucmv-migration-guide.md) to translate those measures to Unity Catalog Metric Views
