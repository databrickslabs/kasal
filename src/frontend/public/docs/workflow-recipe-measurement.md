# How to measure workflow-recipe effectiveness

Run a controlled measurement of whether reusing past crews actually improves the crews Kasal generates, and read the result without fooling yourself.

- [Before you begin](#before-you-begin)
- [1. Build a curated library](#1-build-a-curated-library)
- [2. Turn on the holdout](#2-turn-on-the-holdout)
- [3. Let it run](#3-let-it-run)
- [4. Read the report](#4-read-the-report)
- [How the numbers are computed](#how-the-numbers-are-computed)
- [Configuration reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Related](#related)

---

## Before you begin

- You need the **admin** or **editor** role — the report describes how the workspace's generation behaviour is being shaped.
- The workspace needs mined recipes. Mining runs automatically every 5 minutes and back-fills existing history on its first pass, so if crews have been run at all, recipes exist.
- An embedder must be reachable (Databricks in production, local Ollama in dev). Recipes without vectors are captured but not retrievable.
- Read [Workflow recipes](./workflow-recipes.md) first if the terms *recipe*, *curation* and *arm* are not already familiar. In particular, understand why a control group is required — without it the numbers below cannot support a causal claim.

## 1. Build a curated library

Nothing is injected into generation until a human marks recipes `good`, so a measurement cannot start before curation does.

In **Job History**, completed crew runs that were mined show a bookmark control in their actions. Open it and choose:

| Choice | Effect |
|--------|--------|
| **Good — reuse this** | Offered as an exemplar when generating similar crews |
| **Not good** | Never suggested again |
| **Hide** | Never suggested again (a preference, not a judgement) |
| **Clear mark** | Back to uncurated |

Curate against the run's **result**, not its status. A run that completed is not the same as a run that was right.

You can also curate over the API:

```bash
curl -X PATCH "$KASAL/api/v1/workflow-recipes/{recipe_id}/curation" \
  -H 'Content-Type: application/json' \
  -d '{"curation": "good"}'
```

Aim for enough coverage that a meaningful share of new prompts find a curated match. Check progress with the coverage figures in step 4.

## 2. Turn on the holdout

Set the holdout fraction on the backend and restart it:

```bash
export WORKFLOW_RECIPE_HOLDOUT=0.2   # withhold exemplars from 20% of eligible generations
```

Every generation that finds a curated, above-threshold match now has a 20% chance of being denied it and recorded as a **control**. Both arms are repeat work; they differ only in treatment.

The holdout has a real cost — that 20% gets a deliberately worse-informed generation — which is why it defaults to `0.0`. Turn it on for a measurement window, then turn it off.

## 3. Let it run

Two background steps run on the existing 5-minute sweep and need no action:

- **Mining and embedding** keep the library current.
- **Linking** attaches each trial to the run its generated crew produced, matching the agent ids recorded at generation against the `agents_yaml` keys of executions. The join is exact, not fuzzy.

Most trials never link, and that is expected — generated crews get edited past recognition, merged into a larger canvas, or never run at all. Trials stop being retried for linking after 30 days.

How long to wait depends on how strong an effect you expect. A large effect is visible in a few hundred generations with both arms populated; a small one needs more than a single workspace typically produces. The `linked_runs` column is the honest sample size — not `generations`.

## 4. Read the report

In **Job History**, open any mined run's bookmark menu and choose **Is reuse helping?**. Or:

```bash
curl "$KASAL/api/v1/workflow-recipes/effectiveness?days=30"
```

Read `comparable` first. When it is `false`, one of the two arms has no data and **no causal claim is available** — the coverage numbers are still real, but any difference against the `none_available` arm is confounded by repeat-versus-novel work.

The three arms:

| Arm | Meaning |
|-----|---------|
| `exemplar` | Found a curated match and got the exemplars |
| `control` | Found a curated match and was denied it by the holdout |
| `none_available` | The library had nothing curated above the threshold — the baseline the workspace lives with today |

**`exemplar` vs `control` is the comparison.** `none_available` is context, not a control: those requests differ because they were novel, not because of anything this feature did.

The top-line figures answer a different question — not "does it help?" but "could it ever matter here?":

- **Had a match** (`coverage_rate`) — the ceiling. If the library knows nothing about what people ask, nothing downstream can help.
- **Got exemplars** (`injection_rate`) — how much of that ceiling curation has actually unlocked.

## How the numbers are computed

**Rates are over linked runs, not generations.** A generated crew that nobody ran is not a crew that failed — counting it as one would punish whichever arm produced crews people chose not to run. `linked_runs` is reported next to every rate so a completion rate computed on three runs is visibly that.

**Failed runs are included.** Mining only reads `COMPLETED` runs, but measurement must not: a generation that produced a crew which then failed is exactly the outcome that needs counting. Dropping failures would leave only successes in both arms and flatten the difference being looked for.

**Medians, not means.** Run durations have a long tail; one pathological run would move a mean and tell you nothing.

**Arm assignment is per generation, not per prompt.** A hash-based split would be stable, but a workspace repeats a handful of intents, so it would pin whole intents permanently into one arm — comparing different *work* rather than different treatment.

## Configuration reference

| Variable | Default | Effect |
|----------|---------|--------|
| `WORKFLOW_RECIPE_HOLDOUT` | `0.0` | Fraction of eligible generations denied exemplars, as the control arm. `0.0` disables the control (and with it, any causal reading). |
| `WORKFLOW_RECIPE_EXEMPLARS` | `true` | Kill-switch for injection entirely. Set `false` when diagnosing a generation regression. |
| `WORKFLOW_RECIPE_MIN_SIMILARITY` | `0.75` | Cosine floor for a recipe to be offered. **Model-dependent** — see below. |
| `WORKFLOW_RECIPE_MINE_BATCH` | `100` | Executions examined per mining sweep. |

The similarity default was measured on a dev corpus (51 recipes, local `nomic-embed-text`): genuinely matching prompts scored 0.818 / 0.826 / 0.831, while an unrelated prompt ("build me a snake game in python") topped out at 0.456. `0.75` sits in that gap with margin on the noise side, because the costs are asymmetric — missing a reusable crew just means generating it as before, whereas offering the wrong crew wastes attention and erodes trust in every later suggestion.

**That absolute scale does not transfer across embedders.** Production `databricks-gte-large-en` produces a different distribution than dev Ollama. Re-measure before assuming the default holds, using the `candidates` recorded on each trial (they carry the scores retrieval actually saw).

## Troubleshooting

**`generations` is 0.** Trials are only written by the full crew-generation path, and only when the request carries a group context. Check the backend log for `[WorkflowRecipes] Trial recorded`.

**`injection_rate` is 0 but `coverage_rate` is high.** Retrieval is finding matches that nobody has curated. This is the expected state before step 1 is done — look at `with_blessed_candidates`.

**`coverage_rate` is 0.** Either no recipes have embeddings (check that the embedder is reachable) or the similarity floor is too high for your embedder. Inspect the `candidates` on recent trials for the scores retrieval saw.

**Both arms populated but `linked_runs` is tiny.** The generated crews are not being run as generated. That is a real finding about the workflow, not a bug in the measurement — but it does mean outcome comparison will take a long time to reach significance.

## Related

- [Workflow recipes](./workflow-recipes.md) — what a recipe is and why curation gates reuse
- [API endpoints](./api_endpoints.md) — the `/workflow-recipes` routes
- [MLflow tracing setup](./mlflow-tracing-setup.md) — the trace data the outcome columns are derived from

[← Back to documentation index](./README.md)
