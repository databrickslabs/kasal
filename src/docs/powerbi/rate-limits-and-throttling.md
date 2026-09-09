# Rate Limits & Throttling — Power BI → UC Metric View Tool

**Runbook for `REQUEST_LIMIT_EXCEEDED` during a UCMV generation run.**

## The symptom

A run slows to a crawl or fails, and `execution_logs` / `flow.log` show:

```
litellm.RateLimitError: databricksException - {"error_code":"REQUEST_LIMIT_EXCEEDED",
"message":"REQUEST_LIMIT_EXCEEDED: Exceeded workspace input tokens per minute rate
limit for databricks-claude-opus-4-8. Work with your Databricks account team to
request a higher FMAPI rate limit tier."}
...
[DatabricksRetryLLM] rate_limit in _handle_non_streaming (attempt N/5) ... Retrying in 120.0s
```

Two failure shapes follow from sustained throttling:
- **Silent stall** — the run sits in 120 s retry back-offs for many minutes (looks
  "hung" but isn't).
- **Downstream failure** — the run drags on long enough that the user/OBO token TTL
  lapses, and a later call fails with `Embedding API error 403: Invalid Token`,
  ending the flow.

## Why it happens

The workspace's **Foundation Model API (FMAPI) pay-per-token endpoint** enforces a
shared **input-tokens-per-minute (TPM)** ceiling per model (e.g.
`databricks-claude-opus-4-8`). A large report drives a lot of tokens per minute
across several paths (crew-agent reasoning, DAX→SQL translation, M→SQL recovery,
crew-memory embeddings), and the sum exceeds the TPM ceiling — especially when other
users in the same workspace are also hitting the same model.

**Important framing:** UCMV generation is an **occasional drafting / migration task**,
not a runtime path. So optimize for "succeed a handful of times," not "sustained
throughput."

## What the tool already does (no action needed)

These token-demand reductions are built in — you do **not** need to do anything for them:
- **Batched DAX→SQL translation** — measures are translated in batches (one LLM call
  per ~12 measures) instead of one call per measure, so the ~14 k-token skill corpus
  is amortised across the batch (~10× fewer input tokens). Databricks silently drops
  Anthropic prompt caching, so batching — not caching — is what keeps the token cost down.
- **Fail-open retry with back-off** — a throttled call retries and recovers; one
  stuck call no longer hangs the run indefinitely.

So the levers below are the **operator** options when throttling still occurs.

## What to do when you hit throttling

### 1. Raise the ceiling (quota — ~free) — do this first

Request a **higher FMAPI rate-limit tier** for the workspace (exactly what the error
message advises). It lifts the per-minute cap on the shared endpoint; usage stays
**pay-per-token**, so there's no new cost model — just a higher ceiling. Fastest infra
lever. Go through your Databricks account team.

### 2. Dedicated capacity (pay — the robust option)

Provision a **Provisioned Throughput (PT)** serving endpoint for the model. Capacity is
**reserved** and billed by throughput (DBUs/hour), and is **not** subject to the shared
per-minute limit → throttling disappears entirely.

- **Right for:** bulk onboarding — many reports / many tenants in a window.
- **How to use economically:** spin PT **up for the onboarding window and release it
  afterwards.** Do **not** run it permanently for an occasional drafting job — that's
  over-provisioning.

### 3. Operational stopgaps (no change, good for one-offs)

- **Run off-hours.** The TPM limit is **per-workspace and shared**, so running when
  colleagues aren't also hitting the same model gives real headroom.
- **Retry / run again.** The tool already backs off and recovers; simply re-running
  once the per-minute window resets often gets a one-off draft through. Fragile, but
  fine for a non-real-time tool.

## Choosing the right mode by cadence

| How often you run | Recommended approach |
|---|---|
| A few reports (one-off drafting) | Quota bump (#1) + off-hours / retry (#3). **Don't** pay for permanent PT. |
| Bulk onboarding (many reports/tenants) | **Temporary** Provisioned Throughput (#2) for the onboarding window, then release. |
| Frequent / ongoing | Provisioned Throughput or a dedicated endpoint (#2). |

## Quick checklist

1. Confirm it's throttling: `REQUEST_LIMIT_EXCEEDED` on a specific model in the logs.
2. One-off? → run off-hours and/or re-run (#3).
3. Recurring or time-critical? → request a higher FMAPI tier (#1).
4. Bulk onboarding? → temporary Provisioned Throughput for the window (#2).
5. Still failing after the token expired mid-run (`Embedding API error 403`)? → that's
   a *consequence* of a too-long throttled run; fixing the throttling (above) also fixes it.
