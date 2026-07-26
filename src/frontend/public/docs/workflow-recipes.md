# Workflow recipes

Why Kasal keeps the crews you have already run, and why it refuses to reuse them until a person says they were any good.

- [The problem](#the-problem)
- [What a recipe is](#what-a-recipe-is)
- [The pipeline](#the-pipeline)
- [Why curation is the gate](#why-curation-is-the-gate)
- [How reuse reaches generation](#how-reuse-reaches-generation)
- [Why effectiveness needs a control group](#why-effectiveness-needs-a-control-group)
- [What this is not](#what-this-is-not)
- [Related](#related)

---

## The problem

Every time someone asks Kasal to build a crew, an LLM derives the whole thing from scratch: which agents, what each one is for, how the tasks chain, which tools to bind. That derivation costs a call, takes seconds, and — because it is a fresh sample every time — produces a slightly different crew for the same request.

The waste is not hypothetical. In one workspace the request "Load US and EU" was derived from scratch **29 times**. Twenty-nine LLM calls to re-answer a question the system had already answered twenty-eight times, each answer thrown away the moment the run finished.

Meanwhile, a completed run already **is** a validated plan. It holds the crew graph, the tools it actually exercised, and the fact that it ran to completion. All of that was discarded.

## What a recipe is

One distilled run, scoped to a workspace:

| Field | What it holds |
|-------|---------------|
| `intent_text` / `intent_hash` | What this crew was asked to do — the run name plus each task's description |
| `embedding` | The vector that later matches a new prompt against this intent |
| `agents_yaml` / `tasks_yaml` | The reusable artifact: the crew graph itself |
| `tool_names` | Tools **observed in the trace**, not merely configured |
| `mcp_servers` | MCP servers the crew selected |
| `run_count` / `mined_job_ids` | How often this intent recurred, and every run folded in |
| `curation` | The one human judgement: `good`, `bad`, `hidden`, or unset |

Two details in that table carry more weight than they look:

**Tools come from the trace, not the config.** A tool that was bound to an agent but never called is not part of what made the crew work. Shipping it in a recipe would propagate dead configuration into every crew that learns from it.

**Repeats collapse.** All 29 runs of one intent become one row, refreshed to the newest run, not 29 near-identical rows each competing for the same retrieval slot and each costing an embedding call.

## The pipeline

```
completed crew run
   │
   ├─ mine     (every 5 min, parent process)   → distil into a recipe, dedupe by intent
   ├─ embed    (same sweep, separate step)     → make it retrievable
   │
   │  ... a human marks it good ...
   │
   └─ retrieve (at crew generation)            → similar? curated? above threshold?
        └─ inject                              → few-shot exemplars in the generation prompt
```

**Mining** runs as a periodic parent-side sweep rather than a hook on the status write. The crew path writes its terminal status from *inside* a spawned subprocess, so a hook there would execute in the child interpreter and reach nothing. A parent-side sweep sidesteps that entirely: it is idempotent, cannot fail a run, is decoupled from the status write, and back-fills existing history on its first pass.

**Embedding** is a separate step from mining on purpose. An embedder outage then degrades to "captured but not yet retrievable" instead of losing the recipe — the structure is stored on the sweep, and the vector is filled in whenever the embedder returns.

**Retrieval** matches the user's prompt against recipe intents. Note the asymmetry it matches across: a recipe's `intent_text` is built from the *generated* run name and task descriptions, while the query is the user's own phrasing. They describe the same job in different registers, so scores run lower than a prose-to-prose comparison would, and the similarity floor is calibrated for that.

## Why curation is the gate

Nothing is ever reused until a human marks the recipe `good`. That restriction is the entire safety story, and it is worth being precise about why.

Mining can only observe that a crew **finished**. `COMPLETED` says the process exited cleanly — it says nothing about whether the output was correct. A crew can run flawlessly and load garbage.

If merely-completed runs were reused, the failure mode is not "occasionally a bad suggestion". It is a **feedback loop**: a mined recipe shapes the next generation, that generated crew runs and gets mined in turn, and the library converges on whatever shape happened to survive first — looking healthier each round precisely because it is agreeing with itself. There is a guard against this in the data model (`source_recipe_id` marks runs that came from a recipe so mining skips them), but the guard is a backstop. The real protection is that the only thing that qualifies a recipe for reuse is a person having looked at the result.

That is also why the curation control lives on the **run list**, next to the run's actual output, and not in the crew catalog. The catalog's thumbs-up is a judgement about a crew in general. Curation is a judgement about *this run produced the right answer — build like it again*, and it can only be made honestly with the answer in view.

`bad` and `hidden` differ in meaning — one is a judgement, the other a preference — but both mean "never offer this", and the retrieval query filters them, so no present or future reuse path can forget to honour a human's rejection.

## How reuse reaches generation

A curated, above-threshold match is rendered as a compact few-shot block appended to the crew-generation system prompt: the intent it was built for, the agent roles, the task labels, the tools that were actually used, and the MCP servers. Not the full graph — the block is evidence of what works in this workspace, not a template to copy, and the prompt says exactly that. The current request takes precedence wherever they differ.

The consequence worth stating plainly: **with nothing curated, the generation prompt is byte-for-byte what it was before this feature existed.** The feature switches itself on as a workspace curates, rather than shipping enabled and hoping.

## Why effectiveness needs a control group

Exemplars change a prompt, and a changed prompt produces a different crew. Whether that crew is *better* is not observable from the generation itself, so it has to be measured against outcomes — and that measurement is easy to get wrong in a way that flatters the feature.

The tempting comparison is "generations that had a match" against "generations that had no match". It is worthless. A prompt that matches a past crew **is** repeat work by definition, and repeat work succeeds more often than novel work whether or not exemplars exist. That comparison would credit reuse for the familiarity of the request, and would look just as good if the exemplar text were replaced with lorem ipsum.

The only fair comparison holds eligibility fixed and varies the treatment: among generations that *all* found a curated, above-threshold match, withhold the exemplars from a random share. Those withheld generations are the control arm. Both arms are repeat work; they differ only in whether the model saw the exemplars.

That is what the `workflow_recipe_trials` ledger records — one row per generation, with the arm it was assigned, the candidates retrieved, and the ids of the agents produced, which later link the generation to the run it became. For running an actual measurement, see [How to measure workflow-recipe effectiveness](./workflow-recipe-measurement.md).

## What this is not

- **Not a cache.** A retrieved recipe is never executed unreviewed; it informs generation, and a human stays at the canvas.
- **Not a quality score.** `run_count`, `duration_ms`, `tool_call_count` and `error_span_count` are descriptive. Ranking on them would actively reward a crew that did less work. Recency is the tiebreaker, which is at least honest about knowing nothing.
- **Not cross-workspace.** A recipe carries a crew's full structure and tool bindings, so every read is group-scoped and returns nothing rather than falling back to unscoped results.
- **Not applicable to every run.** Only crews are mined. A light-agent (chat) run is one agent with one task — there is no structure to reuse — and flows are authored by hand rather than derived by an LLM, so neither has a derivation cost to remove.

## Related

- [How to measure workflow-recipe effectiveness](./workflow-recipe-measurement.md) — run a holdout and read the report
- [Architecture guide](./ARCHITECTURE_GUIDE.md) — where the service, repository and router layers sit
- [API endpoints](./api_endpoints.md) — the `/workflow-recipes` routes
- [Memory](./MEMORY.md) — the other place Kasal keeps something across runs, and how it differs

[← Back to documentation index](./README.md)
