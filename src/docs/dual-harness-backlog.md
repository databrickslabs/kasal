# Dual-harness backlog

Open items from running Kasal and CrewAI side by side. Each one is written so it
can be picked up cold: what was observed, why it matters, and where the work is.
Nothing here is in progress.

---

## 1. Conversational flow — borrow what CrewAI's experimental state has

**Status:** deferred by decision, not blocked.

Kasal's conversational flow lives in `flow_builder/conversation/` and runs on
Kasal's own `Flow` under BOTH harnesses, so it is not a parity gap — CrewAI's
version is never reached, because we never construct a CrewAI `Flow`.

CrewAI 1.15.16 ships `crewai/experimental/conversational.py`
(`Flow(conversational=True)`), whose `ConversationState` is:

```
id, messages, current_user_message, last_user_message,
last_intent, ended, events, agent_threads, session_ready
```

Four of those are already our channels. Three are not, and are the reason to
look again later:

| Theirs | What it buys | Ours today |
| --- | --- | --- |
| `events` | Agent/tool scratch work with `private` / `public` visibility, kept out of the user-facing history | No equivalent; scratch work is not modelled |
| `agent_threads` | Per-agent message history inside one conversation | No equivalent; one thread per conversation |
| `ended` | An explicit end-of-conversation marker | Inferred |

What we have and they do not: per-channel reducers (`replace`, `append`,
`merge`, `add`) as a DECLARED policy, crew-output reuse, fork-from-history, and
the interrupt channel.

**Why it was deferred:** their feature is marked EXPERIMENTAL in its own module
docstring — "APIs in this module and the conversational methods on `Flow` may
change without a major-version bump until the feature graduates". Adopting it
would trade per-channel reducers for an API that can move under us.

**If picked up:** add `events` and `agent_threads` as channels in
`conversation/channels.py` + `state_model.py` rather than adopting their Flow.
The field names already match theirs, so a later migration stays cheap.

---

## 2. The tool-round ceiling does not bind under CrewAI

`MAX_TOOL_ROUNDS = 15` (`core/llm/transport/budget.py`, overridable per agent by
`max_iter`) lives in Kasal's transport tool loop. Under the CrewAI harness that
loop is bypassed — `delegate_tool_calls` hands the loop to CrewAI's executor —
so the ceiling never applies and CrewAI's own `max_iter` (25) governs instead.

Observed: an email agent reached 46 LLM rounds across 4 agent executions.

The same crew therefore gets a different budget depending on the harness. Make
it one number, whichever way: either enforce the Kasal cap around the delegated
loop, or set CrewAI's `max_iter` from the same source Kasal reads.

---

## 3. Checkpoint identity drifts between builds, forcing replays

A HITL resume compares each crew's identity — `md5(crew name | per-task
key/agent/tools)` — against the checkpoint's. A mismatch replays the crew and
everything downstream.

Mitigated, not fixed: a run resuming ITSELF now trusts its resume point
(`CrewSkipPolicy.decide(..., same_execution=True)`), because nothing can have
been edited between a pause and its approval. Cross-run resumes still verify.

The underlying drift is unexplained. `checkpoint_skip._identity_changed` now
logs both hashes and the ingredients on a mismatch; one cross-run resume with
that line will name the field that moves. Candidates seen so far: a tool set
that resolves differently on the rebuild (MCP availability), and the model
string, which reaches the fingerprint on CrewAI but often not on Kasal.

---

## 4. Trace rows carry the PREVIOUS task's name

The same `task_id` appears in `execution_trace` with two different `task_name`
values — the summarize task's rows stamped with the gather task's description —
so the timeline labels every task row with one name and a run looks like it ran
the same task twice.

This is the ambient-context lag the event bus documents in `bus.emit`: identity
from the event's own payload beats ambient context, but events that carry no
task object fall back to ambient values that still describe the previous task.
Under CrewAI more events take that fallback, because they arrive translated from
CrewAI and carry only what the Kasal event class declares.

---

## 5. Misleading CrewAI vocabulary in Kasal's own flow layer

`flow_builder` imports Kasal's Flow as `CrewAIFlow`:

```python
from src.services.flow_builder.runtime import Flow as CrewAIFlow
```

It is Kasal's flow class on both harnesses. The log lines that said "Creating
CrewAI Flow" have been fixed; the alias itself has not. In a dual-harness
codebase this name actively misleads — it is what made "is this run really on
CrewAI?" unanswerable from a log.
