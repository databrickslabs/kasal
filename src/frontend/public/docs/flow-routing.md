# Flow Routing and Output Schemas

Routers let a flow **choose which crews run next** based on the result of the
previous crew. This guide explains the concept and how to configure it on the
flow canvas.

## What a Router does

In a normal connection, crew **A then B** means "run B after A" (sequential). A
**Router** connection is conditional: after the source crew finishes, the router
evaluates every branch's condition and runs **all the branches whose condition is
true**.

```text
                      (category is "politics")  -> Politics crew
Classify crew -> Router
                      (category is "sports")    -> Sports crew
```

If the result contains both politics and sports, **both branches run**, at the
same time. If only one matches, only that one runs. If none match, the router
takes the **otherwise** branch if you configured one (see below).

## What a crew produces

A crew produces **one result for the whole run** — the output of its final task.
To route on it, that result needs a **shape** the router can read: an **output
schema**, a small set of named fields.

Two shapes are useful, and you can route on both.

**A single outcome.** A crew that loads data into a table:

```json
{ "table": "customers", "rows_inserted": 1532, "rows_failed": 0, "status": "success" }
```

Route on `rows_failed` or `status`.

**A list of things.** A crew that classifies a batch of articles:

```json
{
  "classification": [
    { "category": "Politics", "title": "Senate confirms…" },
    { "category": "Sports",   "title": "MLB trade deadline…" }
  ]
}
```

Route on **"Any classification → category"**. See *Routing on a list* below.

## Choosing what to route on

Pick an output schema in the Router configuration and its fields become the
values you can branch on. A condition is:

```text
<value>  <operator>  <what to compare it to>
```

The picker shows values in plain language, whatever their shape:

| What you see | What it means |
| --- | --- |
| `category` | a single value |
| `classification → category` | a value inside a group |
| `Any article → category` | that value across **every** item in a list |
| `Any order → Any line → sku` | across a list inside a list |

You never have to type a path. Under each row the builder echoes the condition
back in plain English, so you can check you picked what you meant.

## Routing on a list

When the value comes from a list — anything labelled **"Any …"** — the operators
mean *"at least one item"*:

| Operator | Meaning |
| --- | --- |
| `is` | at least one item matches |
| `contains` | at least one item contains it |
| `is more than` | at least one item is greater |
| `starts with` | at least one item starts with it |
| **`is never`** | **no item matches** |

> **The one to watch.** `is never` means *none of them*, not *some of them
> differ*. "Any article → category **is never** politics" is true only when there
> is no politics article at all. For list values the builder shows `is never`
> rather than `is not`, precisely so this reads the way it behaves.

## Saying which thing you mean

A condition is built as one or more boxes. Each box names **what you are talking
about** — the result as a whole, or one item of a list — and everything inside
that box is asked of that same thing.

```text
Route when

  ┌ Any article ── where, all on the same one ────────┐
  │   category      is              politics          │
  │   and score     is more than    5                 │
  └───────────────────────────────────────────────────┘
```

That box is true only if **one** article is politics *and* scores over 5.

Two boxes are independent, which is how you say "the batch contains both":

```text
  ┌ Any article ┐        ┌ Any article ┐
  │ category is │  AND   │ category is │
  │  politics   │        │   sports    │
  └─────────────┘        └─────────────┘
```

That is true when there is a politics article and a sports article — different
ones. Both readings are things people mean, which is why the subject is chosen
before the values rather than guessed afterwards.

A schema with no lists has only one possible subject, so the picker is hidden
and the box is just a list of conditions. Subjects appear only for a list whose
items have their own fields; a list of plain values ("Any tag") and a value two
lists deep ("Any order → Any line → sku") stay on the result, where they keep
the any-element meaning described above.

## Capitalisation does not matter

String comparisons ignore case. A model may write `Politics` on one run and
`politics` on the next; a condition you built from a dropdown should not stop
matching because of that. `Politics`, `politics` and `POLITICS` all match.

> If you ever route on something where case is meaningful — an ID or a product
> code — remember that two values differing only in case will compare as equal.

## The "otherwise" branch

Tick **"Run this branch when no other route matches"** on a Router connection to
make it the fallback. It needs no condition — it runs precisely when every other
condition was false.

Without one, a result that matches nothing simply ends the flow. With one, you
get a path you chose: retry, ask a human, or send a "nothing to report" notice.

## What runs after the branches

Connect the branches to a following crew and pick how it waits:

| Logic type | When it runs |
| --- | --- |
| **OR** | as soon as the **first** branch finishes |
| **AND** | after **every branch that ran** finishes |

**AND** is usually what you want after a router. It waits only for the branches
that actually ran — a route that was not taken never blocks it — and the crew
receives **every** branch's output, not just the last to finish. So a "send an
email" crew after a politics branch and a sports branch gets both decks.

Use **OR** when you want to act the moment anything is ready.

## Configure a Router (step by step)

1. **Connect** the two crews on the canvas, then click the connection.
2. Set **Flow Logic Type** to **Router**.
3. Under **Output schema**, pick an existing schema or **Add new schema**.
   - The schema is applied to the source crew's **final task**, so the crew
     produces this structured result on every run.
4. Choose a **value**, an **operator**, and something to compare it to.
5. **Save.**

Repeat for each branch. Tick the "otherwise" box on one connection to make it the
fallback instead — that one needs no schema or condition.

## Creating a schema

**Add new schema** lets you describe what the step produces. Each field has a
name and a type:

- **Text**, **Number**, **Yes/No** — a single value
- **List of items** — expands so you can say what one item contains
- **Group** — related values kept together

```text
articles      List of items
  └─ Each item has:
     title      Text
     category   Text
     score      Number
```

You can nest up to three levels, which covers a report of sections of items or an
order of line items. Deeper than that becomes hard to read and harder for a model
to produce reliably.

Schemas you create here also appear in the task editor under **Output Pydantic
Model**, and vice versa — it is one shared library. Editing a schema never
discards parts of it the editor cannot display.

## The value must actually be produced

An output schema asks the crew's final task to **format its answer** to match
that shape. The values are what the **agent reports** — not an automatic capture
of a tool's return value. So for `rows_inserted` to be accurate:

- the agent should have access to the result (e.g. the tool returns the count),
  and
- the task's **Expected Output** should ask it to report that value.

The schema guarantees the **shape**; the task description guarantees the value is
**populated truthfully**.

## When no branch runs

If nothing matched and there is no "otherwise" branch, the flow stops there. The
run log says exactly why, in one block:

```text
Router route_by_category matched no route (flow stops here).
  route_to_politics : state.get("classification[].category", "") == "politics"  ->  False
  route_to_sports   : state.get("classification[].category", "") == "sports"    ->  False
Unresolved lookups:
  classification[].category -> no such path
Addressable state paths:
  starting_point_0 = "# Top News Stories Across ABC, CBS, and NBC…"
  Classify Category = '{ "classification": [] }'
  classification[] (0 items)
```

It lists every branch, its condition, what that condition evaluated to, anything
it could not find, and the values that **were** available. Usually the answer is
right there — in this example the classify crew returned an empty list, so there
was no category to match.

## Built-in schemas

These are seeded and tuned for routing:

| Schema | Use case | Example values |
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

## Example: a single outcome

A data-pipeline crew uses `DataLoadResult`:

| Branch (target crew) | Condition |
| --- | --- |
| Notify success | `rows_failed` is `0` |
| Alert on failure | `rows_failed` is more than `0` |

Exactly one of these is ever true, so one branch runs.

## Example: a batch of items

A crew classifies the day's news into `{ "classification": [ { "category": … } ] }`:

| Branch (target crew) | Condition |
| --- | --- |
| Politics presentation | `Any classification → category` **is** `politics` |
| Sports presentation | `Any classification → category` **is** `sports` |
| Nothing to report *(otherwise)* | — |

A mixed batch runs **both** presentation crews at once. A politics-only batch
runs one. An empty classification runs the "nothing to report" branch. Connect
all three to a "Send an email" crew with **AND**, and it waits for whichever ran
and receives all of their output.

## Example: routing on a web search

A research crew uses a web search tool. You route on the **aggregate outcome**,
not each hit. Assign `WebSearchResult` to the crew's final task:

```json
{
  "query": "latest openssl CVE 2024",
  "results_found": 8,
  "has_results": true,
  "relevance_score": 0.86
}
```

| Branch (target crew) | Condition |
| --- | --- |
| Summarize the findings | `has_results` is `true` |
| Broaden / retry the search | `has_results` is `false` |
| Ask a human to verify | `relevance_score` is less than `0.5` |

Note that the last two can both be true at once, and both branches will run.

> Remember: `results_found` and `relevance_score` are whatever the **agent
> reports**. Give the final task a web search tool and an **Expected Output**
> that asks it to report the number of results and a relevance score.
