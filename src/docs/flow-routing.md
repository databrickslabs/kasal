# Flow Routing and Output Schemas

Routers let a flow **choose which crew runs next** based on the result of the
previous crew. This guide explains the concept and how to configure it on the
flow canvas.

## What a Router does

In a normal connection, crew **A then B** means "run B after A" (sequential). A
**Router** connection is conditional: after the source crew finishes, the router
evaluates a condition on its result and only continues down the branches whose
condition is true.

```text
                      (risk_level == "high")  -> Escalation crew
Risk crew -> Router
                      (risk_level == "low")   -> Auto-approve crew
```

## The key idea: a crew produces one structured result

A crew does not emit output "per row" or "per item". It produces **one result
for the whole run**. That result is the output of the crew's **final task**.

To route on it, that result needs a **shape** the router can read. That shape is
an **output schema**: a small set of named fields (a Pydantic model under the
hood). For example, a crew that loads data into a table can return:

```json
{
  "table": "customers",
  "rows_inserted": 1532,
  "rows_failed": 0,
  "status": "success",
  "success": true
}
```

You are **not** describing each row: `rows_inserted` is the **aggregate outcome
of the entire workload**. The router then branches on `rows_inserted` or
`status` or `success`.

## Routing variables come from the output schema

When you pick an output schema in the Router configuration, its fields become the
**variables** you can branch on. A condition is simply:

```text
<variable>  <operator>  <value>
```

for example `rows_inserted > 0`, `status == "failed"`, or `risk_level == "high"`.

Only **scalar** fields are routable: `string`, `number`, `integer`, `boolean`.
List/object fields are not shown in the variable picker because the operators
(`=`, `>`, `contains`, and so on) compare single values.

## Configure a Router (step by step)

1. **Connect** the two crews on the canvas, then click the connection.
2. Set **Flow Logic Type** to **Router**.
3. Under **Output schema**, **pick an existing schema** or **Add new schema**.
   - The schema is applied to the **source crew's final task** so that the crew
     produces this structured result on every run.
4. Choose a **variable** (a field from the schema), an **operator**, and a
   **value** to define the condition.
5. **Save**.

> A Router requires a schema. Without one there are no variables to branch on,
> so Save stays disabled until you pick or create one.

## Choosing or creating a schema

- **Pick existing**: Kasal ships with ready-made, routing-friendly schemas
  (see below). Selecting one immediately lists its fields as variables.
- **Add new schema**: define a name and a few fields (name and type). It is
  saved to your schema library and applied to the source crew's final task.

Schemas you create here also appear in the normal task editor under
**Output Pydantic Model**, and vice-versa. It is one shared library.

## Important: the value must actually be produced

Assigning an output schema asks the crew's final task to **format its answer**
to match that schema. The field values are what the **agent reports**, not an
automatic capture of a tool's return value. So for a field like `rows_inserted`
to be accurate:

- the task's agent should have access to the result (e.g. the tool returns the
  count), and
- the task's **Expected Output** should ask it to report that value.

The schema guarantees the **shape**; the task description guarantees the value is
**populated truthfully**.

## Built-in schemas

These are seeded and tuned for routing (every field is a scalar outcome):

| Schema | Use case | Example variables |
| --- | --- | --- |
| `OperationResult` | Any action workload (DB writes, API calls, jobs) | `success`, `status`, `rows_affected`, `error_count` |
| `DataLoadResult` | ETL / data loading | `rows_inserted`, `rows_failed`, `status` |
| `SupportTicketTriage` | Support routing | `priority`, `category`, `requires_human` |
| `SentimentAnalysis` | CX / social | `sentiment`, `score` |
| `IntentClassification` | Conversational routing | `intent`, `confidence`, `fallback` |
| `CustomerFeedback` | CX | `sentiment`, `nps_score`, `action_required` |
| `ApprovalDecision` | Approvals | `decision`, `confidence` |
| `LeadQualification` | Sales | `qualified`, `score`, `tier` |
| `ResumeScreening` | Recruiting | `match_score`, `recommended`, `decision` |
| `Evaluation` | Scoring / review | `score`, `verdict` |
| `RiskAssessment` | Risk / compliance | `risk_level`, `requires_escalation` |
| `ContentModeration` | Trust & safety | `flagged`, `action`, `severity` |
| `FraudCheck` | Security | `is_fraud`, `recommended_action` |
| `ExpenseApproval` | Finance | `policy_compliant`, `approval_status` |
| `InvoiceData` | Finance / AP | `total_amount`, `status` |
| `WebSearchResult` | Online / web search | `results_found`, `has_results`, `relevance_score` |

## Example

A data-pipeline crew uses `DataLoadResult`. The router has two branches:

| Branch (target crew) | Condition |
| --- | --- |
| Notify success | `rows_failed == 0` |
| Alert on failure | `rows_failed > 0` |

On each run the crew reports e.g. `{ "rows_inserted": 1532, "rows_failed": 0, "status": "success" }`,
the router evaluates the conditions, and the matching branch runs.

## Example: routing on a web search

A research crew uses a web search tool (e.g. Serper, Tavily, or DuckDuckGo) to look
something up online. You don't route on each individual hit; you route on the
**aggregate outcome** of the search. Assign the **`WebSearchResult`** schema to the
crew's final task:

```json
{
  "query": "latest openssl CVE 2024",
  "results_found": 8,
  "has_results": true,
  "answer_found": true,
  "top_result_url": "https://example.com/advisory/...",
  "top_result_title": "OpenSSL Security Advisory",
  "relevance_score": 0.86
}
```

Set **Flow Logic Type** to **Router** on the connection, choose `WebSearchResult`, and add
branches:

| Branch (target crew) | Condition |
| --- | --- |
| Summarize the findings | `has_results == true` |
| Broaden / retry the search | `has_results == false` |
| Ask a human to verify | `relevance_score < 0.5` |

So a search that returns nothing routes to a "broaden the search" crew, a confident
result routes to a "summarize" crew, and a weak result routes to a human, all from
one structured outcome object, no per-result handling.

> Remember: `results_found` / `relevance_score` are whatever the **agent reports**.
> Give the final task a web search tool and an **Expected Output** that asks it to
> report the number of results, the top hit, and a relevance score.
