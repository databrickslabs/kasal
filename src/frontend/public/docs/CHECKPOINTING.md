# Checkpointing and resume

A checkpoint answers one question: **what completed, and what did it produce.**
If a run crashes or is killed part-way through, its checkpoint is what lets the
work already done be kept instead of redone.

Crew runs and flow runs both have one, in the same shape, over the same API.

## What a checkpoint contains

A checkpoint is a list of **units**. A unit is:

| Run type | A unit is | Keyed by |
|---|---|---|
| **Agent Builder** (`crew`) | one completed task | position in the task list |
| **Flow Builder** (`flow`) | one completed crew | completion sequence (1-based) |

That is the only difference between the two, and it is a label. Everything else
— storage, lifecycle, API, UI — is shared.

Each unit records the output, who produced it, when it finished, and whether the
output was **truncated**. Outputs are capped at 500,000 characters: resuming with
a shortened context beats redoing the work, but the run says so rather than
quietly handing back less than it had.

## How it is recorded

Checkpoints are **written as work completes**, by a recorder listening on the
run's event bus inside the execution subprocess. Nothing in the crew or flow
builders has to remember to checkpoint.

Four properties the recorder guarantees:

- **Idempotent** — units are keyed, so recording one twice overwrites rather
  than appends. Retries and resumes cannot corrupt a checkpoint.
- **Bounded** — outputs are capped, and the cap is flagged.
- **Fail-open** — a checkpoint failure never fails the run it exists to protect.
- **Event-driven** — no hooks in business logic.

Flow checkpoints used to be *derived* from execution traces rather than written.
That is gone entirely. Traces are **telemetry**: they are retention-pruned on a
schedule, and can be sampled, truncated or reshaped for reasons that have
nothing to do with resume — so a crew whose trace row had aged out simply looked
like it never ran, and the resume built a plausible result on top of the gap.

A flow that ran before checkpoints were written therefore has none, and runs
from the start. That is slower and correct; the alternative was a resume that
quietly skipped work it could no longer see.

## Lifecycle

```
active ──resume──> resumed
  │
  └──expire────> expired
```

- **active** — set when the first unit is written. A **crew** clears it on
  success; a **flow** keeps it (see below).
- **resumed** — this checkpoint has been used as the source of another run.
- **expired** — someone dismissed it.

`resumed` and `expired` are both terminal, and they are distinguished because
"somebody already resumed this" and "somebody threw this away" are different
answers to *why can't I see it any more?*

Expiring a checkpoint does **not** delete the recorded outputs — an operator can
still inspect what the failed run produced.

## Resuming

A resume **creates a new run**, linked to the original by
`resumed_from_execution_id`. The failed run stays failed.

This is deliberate, and it replaced re-running the same record in place:

- **Traces and logs stay readable.** Both are keyed by run id. Reusing the id
  meant a resumed run's rows interleaved with the crashed attempt's, and the
  timeline showed two attempts as one.
- **The audit trail is append-only.** A terminal FAILED record that mutates back
  to RUNNING is not a record of what happened.
- **Cost is attributable per attempt.** A resume that silently adds to the
  original run's token total makes budgets unenforceable.

By default a resume continues from the first incomplete unit. You can also pick
an **earlier** unit to restart from, to redo work whose output you did not like —
everything before that unit is restored, that unit and everything after re-runs.

A resume only restores work whose inputs have not changed. Crew tasks carry a
content-addressed identity; if the task list or the run's inputs changed since
the checkpoint was written, the run starts over rather than resuming against
stale context.

### Re-running a successful flow from the middle

**A flow keeps its checkpoint after it succeeds**, and a completed run can be
resumed from any point. This is the iteration loop:

1. Run the flow. Crews 1-4 complete; you like 1-3 and not 4.
2. Change crew 4 — swap the agents, rewrite the tasks, replace the node.
3. Resume from crew 4. Crews 1-3 are restored from the checkpoint and never
   re-run; crew 4 onward executes fresh.

That is why a flow checkpoint is not discarded on success the way a crew's is: a
crew checkpoint is crash recovery, a flow checkpoint is a re-run point.

**Editing an upstream crew is safe.** Each crew's checkpoint stores a content
hash of what that crew *was* — its tasks, its agents, and the model each agent
runs on. On resume, a crew is only skipped if it still hashes the same. Change
any of these and it re-runs instead of replaying a stale result:

- a task's description or expected output
- an agent's role, goal or backstory
- **an agent's model** — the most common tuning edit, and the one a task hash
  alone would miss
- the crew's name, or the order of its tasks

So you can edit anywhere in the flow, not just downstream of the resume point.
An edited crew re-runs; everything genuinely unchanged is still reused.

**One exception, and it is visible in the log.** Checkpoints written before
identities existed carry no hash. Those crews are still skipped, exactly as they
always were, and the run logs `Skipping crew 'X' UNVERIFIED`. Refusing them
instead would have made every existing checkpoint worthless. Run the flow once
more and its checkpoint gains identities.

### When resume is unavailable

Only a run that is still **in flight** — running, pending, queued — blocks a
resume, because it is still writing units and resuming would race it. Every
finished run is resumable, including a successful one. When resume is blocked,
the API and the UI give the same reason rather than a bare error.

## Using it

In **Run History**, a failed run shows a resume control. It opens the checkpoint:
how much completed, whether anything was truncated, and where to pick up from.

Over the API:

```
GET    /executions/{job_id}/checkpoints              what completed
GET    /executions/{job_id}/checkpoints/{unit_key}   one unit's full output
POST   /executions/{job_id}/resume                   optional {"from_unit": "3"}
DELETE /executions/{job_id}/checkpoints              expire it
```

Resuming and expiring require the **Admin** or **Editor** role.

The older flow-scoped endpoints (`/flows/{flow_id}/checkpoints`) still work and
are scoped to a saved flow rather than to one run. They are deprecated: they are
why crew runs had no checkpoint UI for so long, since checkpoints were filed
under the thing being executed rather than under the execution. They now read
the same written checkpoints as everything else — a run with none does not
appear.

## What is not checkpointed

**Chat** (`execution_type="agent"`) has no checkpointing and will not get any. It
is a single in-process agent turn, sub-second, with nothing to resume.

**Flow method state** — where a flow is in its method graph — is separate. It
lives in its own table with its own lifecycle and is *referenced* from the
checkpoint rather than folded into it. Crews have no equivalent, because a crew's
state is just its completed task outputs. That is a property of crews, not a gap.
