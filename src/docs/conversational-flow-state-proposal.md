# Conversational flow state

A proposal for LangGraph-style state across the nodes of a Kasal flow, so a flow can hold a multi-turn conversation instead of running once and forgetting.

- [The short version](#the-short-version)
- [What Kasal already has](#what-kasal-already-has)
- [How the other systems do it](#how-the-other-systems-do-it)
- [The five gaps](#the-five-gaps)
- [Proposed design: threads over the flow you already have](#proposed-design-threads-over-the-flow-you-already-have)
- [What this looks like in the product](#what-this-looks-like-in-the-product)
- [Sequencing](#sequencing)
- [Decisions to make](#decisions-to-make)
- [What to do with the uncommitted typed-state work](#what-to-do-with-the-uncommitted-typed-state-work)

## The short version

You are closer than it looks. Kasal already has an append-only checkpoint table keyed
by a flow UUID, restore-on-kickoff, a subprocess-safe persistence backend, a
human-in-the-loop pause, and a session model with messages and compaction. That is most
of what LangGraph calls persistence.

Four things are missing, and none of them is a checkpointer:

1. **A thread identity.** Every run mints a fresh `state.id`, so nothing binds turn 2 to turn 1.
2. **Turn-scoped execution.** A second `kickoff` on the same flow instance runs the start methods and *silently skips every listener*.
3. **Reducers.** State updates overwrite. A conversation needs `messages` to append.
4. **A declared channel schema.** `flow_config.state.model` is read by the builder and written by nobody.

The proposal is to add those four, reusing the checkpoint machinery you already built,
rather than adopting a second state system beside it.

## What Kasal already has

This is an inventory, not a plan — everything here exists on `kasal-engine` today.

| Capability | Where it lives | State |
|---|---|---|
| Flow state, dict or typed | `services/flow_builder/runtime/flow.py` (`Flow(Generic[T])`, `initial_state`, `_build_initial_state`) | Typed path exists; nothing produces a schema |
| State restore on kickoff | `Flow._restore_state`, triggered by `inputs["id"]` | Works; only reachable via an explicit resume request |
| Append-only checkpoints | `flow_states` table (`flow_uuid`, `method_name`, `state_json`, `created_at`) | Works; one row per completed method |
| Subprocess-safe persistence | `services/flow_builder/kasal_flow_persistence.py` | Works, including the async-in-sync bridge for Lakebase |
| Unit-level checkpoints and resume | `services/execution/checkpointing/` + `flow_builder/checkpoint_*.py` | Works; shared crew/flow contract, content-hash identity |
| HITL pause | `FlowPausedForApprovalException`, approval records | Works; this is an `interrupt()` in all but name |
| Thread-like session | `chat_sessions` (id, `running_job_id`, `context_summary`, group isolation) | Works, for chat only |
| Turn history | `chat_history` (`session_id`, `message_type`, `content`, `intent`) | Works, for chat only |
| Cross-thread long-term memory | `services/memory/` (typed records, validity windows, vector substrate) | Works, for crews |

Two structural facts shape everything below:

- **A flow run is a spawned subprocess** (`services/flow_builder/process_executor.py`).
  The `Flow` object does not survive the turn, and cannot.
- **Persistence is opt-in per flow**, enabled when any edge carries `checkpoint: true`
  (`modules/flow_builder.py:262`). So the checkpoint producer exists in the UI already —
  unlike the state schema, which does not.

## How the other systems do it

### LangGraph

LangGraph splits the problem into four independent pieces, and that separation is the
part worth copying.

- **A state schema of channels.** Each key is a channel; a node returns a partial update
  rather than a whole state, and the graph merges it in.
- **Reducers decide how an update merges.** Without one, a write overwrites. With
  `Annotated[list, operator.add]` it appends; `add_messages` is the specialized reducer
  for conversation history. This is the mechanism that makes a `messages` channel
  accumulate across turns instead of being replaced by the newest turn.
- **A checkpointer, keyed by `thread_id`.** A checkpoint is saved at every superstep.
  Invoking the graph again with the same `thread_id` resumes from the last checkpoint —
  which is precisely what makes turn N+1 a continuation rather than a new run.
- **A store, keyed across threads.** Long-term memory (user preferences, learned facts)
  deliberately does *not* live in the thread's state.

Human-in-the-loop is expressed on the same substrate: `interrupt()` inside a node pauses
the graph, and `Command(resume=value)` continues it, with the value becoming the return
of the `interrupt()` call.

### CrewAI conversational flows

CrewAI keeps a `Flow` instance alive and calls `handle_turn(message, session_id=...)`
per user line, which internally calls `kickoff(inputs={"id": session_id})`. State is a
`ChatState` carrying `id`, `messages`, `last_user_message`, `last_intent` and
`session_ready`; handlers call `append_assistant_message(reply)`; `@persist` is
recommended on a *single terminal step* so a snapshot is taken once per turn rather than
mid-run.

The important detail for Kasal: this pattern assumes a live in-process object. **Kasal
runs each flow in a fresh subprocess, so the instance is gone before turn 2 arrives.**
The CrewAI shape is a useful contract (`ChatState`, one kickoff per line, intent routing)
but its lifecycle is not available here. The LangGraph shape — rebuild the graph, restore
state by thread id — is the one that fits.

### Comparison

| Concern | LangGraph | CrewAI flows | Kasal today |
|---|---|---|---|
| Thread key | `thread_id` in config | `session_id` passed as `inputs["id"]` | `flow_uuid`, minted fresh per run |
| Turn boundary | One `invoke` per turn | One `kickoff` per user line | No concept of a turn |
| Update semantics | Per-channel reducers | Whole-state mutation by handlers | Overwrite |
| History | `messages` channel + `add_messages` | `state.messages` + helper | `chat_history` table, chat path only |
| Pause / resume | `interrupt` / `Command(resume=)` | Flow-level persistence | `FlowPausedForApprovalException` + approval |
| Long-term memory | Store, cross-thread | Not in flow state | `services/memory/`, crew-scoped |

## The five gaps

Each of these is verified against the code, not inferred.

### Gap 1: no thread identity, and the id is not adopted on the first turn

`Flow._build_initial_state` mints `uuid4()` for `state.id` on every construction, and
`_kickoff_inputs` supplies an `id` only when `resume_from_flow_uuid` is set on the run
config. Nothing connects a run to a conversation. A published flow invoked twice from
chat produces two unrelated `flow_uuid`s and two unrelated checkpoint lineages.

Worse, supplying an id is not enough on its own. `_restore_state` loads the stored state
and returns early when there is none — which is always the case on turn 1 — so the id is
never adopted and the turn saves under its random UUID:

```text
turn 1 state.id: 6c0f9ae8-…          # not the key that was passed in
rows written under: ['6c0f9ae8-…']
turn 2 sees seen = 1                 # wanted 2
```

Every turn writes a fresh lineage and reads nothing back.

### Gap 2: a flow instance cannot run a second turn

`Flow._fire_listeners` skips any listener already in `self._completed`, and `_completed`
is only reset in `__init__`. A second `kickoff_async` on the same instance therefore
re-runs the `@start()` methods and runs **no listeners at all**:

```text
turn 1: ['a', 'b']
turn 2: ['a', 'b', 'a']     # listener 'b' never fired again
```

That is the same failure class as the ChatMode-versus-Flow-Builder divergence you
already hit: the run completes, reports success, and quietly executes half the graph.
Any multi-turn design must reset per-turn execution bookkeeping (`_completed`,
`_scheduled`, `_and_progress`) while *keeping* state.

### Gap 3: updates overwrite, so history cannot accumulate

`Flow._merge_inputs` does `setattr` per key (or `dict.update`), and `_restore_state`
does the same. There is no per-key merge policy. A `messages` channel restored from the
checkpoint and then handed `{"messages": [new_turn]}` ends up holding only the new turn.
Reducers are the missing primitive, and they are cheap — a policy per channel, applied
in one place.

### Gap 4: the checkpoint is latest-only and untenanted

`KasalFlowPersistence.load_state` calls `get_latest_state_json(flow_uuid)`. The table is
append-only, so the history is *there*, but only the newest row is reachable: no replay,
no fork-from-turn-3, no time travel. Separately, `flow_states` carries **no `group_id`**
— every other tenant-facing model in the codebase does. A flow UUID is unguessable, so
this is not an open door, but it is the one table in the checkpoint path with no tenant
filter, and a thread keyed on a chat session makes it user-facing data.

### Gap 5: no channel schema

`state_config.get("model")` in `modules/flow_builder.py` is read and passed nowhere, and
no UI writes it — your exported flow confirmed `flow_config.state: null`. Without a
declared schema there is nothing for a reducer to attach to.

## Proposed design: threads over the flow you already have

The design is one sentence: **a conversation is a thread, a thread is a checkpoint
lineage, and a turn is one flow run that restores that lineage and appends to it.**

### The thread

Use the chat session, but **not as the lineage key itself**. `session_id` is already on
`ExecutionRequest` (it scopes memory recall, and is documented as "stable across messages
in a conversation"), so the correlation id is on the wire. It cannot be the `flow_uuid`
directly for one decisive reason: a single session routes to MANY capabilities — that is
what "Use existing" does — while `flow_states` is keyed on one `flow_uuid`. Sharing a key
across two flows means turn 2 of flow B restores flow A's state. LangGraph hit the same
wall and answers it with `checkpoint_ns` beside `thread_id`.

Derive the lineage key instead:

```python
FLOW_THREAD_NS = uuid.UUID("…")  # fixed namespace constant

def thread_state_uuid(group_id: str, session_id: str, flow_id: str) -> str:
    """The checkpoint lineage for one conversation with one flow.

    Deterministic, so turn N+1 addresses turn N's lineage without storing a
    mapping. Scoped by flow_id because a session talks to many flows, and by
    group_id for the same reason every other tenant key is.
    """
    return str(uuid.uuid5(FLOW_THREAD_NS, f"{group_id}:{session_id}:{flow_id}"))
```

This matches the house pattern — crew memory ids are already a deterministic hash of crew
structure plus `group_id` — and it means the minimal thread needs **no new table**: turn
N+1 kicks off with `inputs = {"id": thread_state_uuid(...), **turn_inputs}` and the
existing restore path does the rest.

A `flow_threads` row keyed BY that derived uuid is still worth adding later for what
derivation cannot express — forking (a fork needs a genuinely new lineage), a concurrency
guard when two turns race on one thread, turn counts and listing, and an explicit "start
over in this session". Because the row is keyed by the derived value, adding it is not a
migration of identity.

> [!IMPORTANT]
> `Flow._restore_state` returns early when nothing is stored yet, so the id is never
> adopted and turn 1 saves under its random UUID — every turn then writes a fresh lineage
> and reads nothing. Verified: `turn 2 sees seen = 1` where 2 was expected. Adopt the id
> unconditionally and restore only when there is something to restore; two lines, and
> nothing threads without it.

### The turn

A turn is a normal flow execution with three additions:

1. `resume_from_flow_uuid` is set to the thread's `state_uuid` (the existing config field
   — no new plumbing to the subprocess).
2. Per-turn bookkeeping is reset before the graph runs, so listeners fire again:
   ```python
   def begin_turn(self) -> None:
       """New turn, same state. Execution bookkeeping resets; channels do not."""
       self._completed.clear()
       self._scheduled.clear()
       self._and_progress.clear()
       self._method_outputs.clear()
   ```
3. The turn's user message is written to the `messages` channel *through its reducer*
   before the start methods run.

### The channels

Extend the declared state schema with a per-property reducer. The schema shape stays the
one the product already uses for declared inputs (`publications.input_schema`), so this
is one schema concept, not two:

```json
{
  "type": "object",
  "properties": {
    "messages":  { "reducer": "append" },
    "topic":     { "reducer": "replace" },
    "findings":  { "reducer": "merge" },
    "turn_count":{ "reducer": "add" }
  }
}
```

Three reducers cover everything observed in the existing flows, and `replace` stays the
default so nothing changes for a flow that declares none:

| Reducer | Merge rule | Use |
|---|---|---|
| `replace` | New value wins | Scalars: `topic`, `has_results` |
| `append` | Concatenate lists, de-duplicating by identity | `messages`, accumulated findings |
| `merge` | Shallow dict merge | Per-turn structured context |
| `add` | Numeric sum | Counters |

The merge belongs in exactly one place — the state object — so every writer gets it:
`_merge_inputs`, `_restore_state`, a node's return value, and a state operation.

### The conversational contract

Adopt CrewAI's field names, since they are already documented and a flow author reading
either doc should not have to translate:

```python
class ConversationState(DictLikeState):
    id: str = ""                      # thread id
    messages: list = []               # reducer: append
    last_user_message: str = ""       # reducer: replace
    last_intent: str = ""             # reducer: replace
    session_ready: bool = False       # one-time bootstrap
```

A flow's own declared channels sit alongside these. Routers then branch on
`state.last_intent` exactly as they branch on `state.has_results` today — no new
condition syntax, no new evaluator.

### Interrupt and resume

You already have the hard half. `FlowPausedForApprovalException` stops the graph and the
approval record holds the pending decision; the checkpoint holds what completed. What is
missing is the *return value* semantics — LangGraph's `interrupt()` returns whatever the
resume supplies, so a node can ask a question and read the answer in the same line. On
Kasal that becomes: the approval's decision payload is written into a declared channel
before the resumed turn runs, and the gate node reads it from state. No new pause
machinery; one new write on the resume path.

This is also what makes an interrupt indistinguishable from a turn: both are "restore the
thread, apply an input, continue". A conversational flow and an approval gate stop being
two features.

### Long-term memory stays out of the thread

Follow LangGraph's split. The thread holds *this* conversation; `services/memory/` holds
what should outlive it. Do not let `messages` become the memory system — you already
found what unbounded shared recall does to answer quality. `chat_sessions.context_summary`
and its compaction window are the right precedent: a thread that grows past a bound gets
folded into a summary channel rather than replayed verbatim.

## What this looks like in the product

- **Chat.** "Use existing" already routes a prompt to a published flow. Today that is
  one-shot. With threads, the second message in the same session continues the same flow
  run instead of starting a new one — the flow becomes a conversational skill rather than
  a function call.
- **Flow Builder.** A state panel listing channels and their reducers, defaulted from
  what the canvas mentions. This is the state-schema editor; the reducer column is what
  makes it worth opening.
- **Traces.** A thread view: turns down the page, each turn's nodes beneath it. The
  checkpoint rows already contain this; nothing renders it yet.
- **Resume and fork.** `flow_states` is append-only, so "fork this conversation from turn
  3" is a query away once the API stops reading only the latest row.

## Implementation status

Steps 1–4 — the spine — are built and tested on the working tree (not committed;
this is experimental). What exists:

| Piece | Where | Lines |
|---|---|---|
| Reducers (`replace`/`append`/`merge`/`add`) | `flow_builder/modules/flow_state_channels.py` | 107 |
| Channel compiler + `DictLikeState.merge` | `flow_builder/modules/flow_state_model.py` | 268 |
| Turn contract (`ConversationState`, `turn_inputs`, `close_turn`) | `flow_builder/modules/flow_conversation.py` | 172 |
| Derived thread key | `flow_builder/flow_thread.py` | 68 |
| `begin_turn`, id adoption, reducer-aware merge, `save_checkpoint` | `flow_builder/runtime/flow.py` | edits |
| Turn wiring (`_thread_id`, `_kickoff_inputs`, `_close_turn`) | `flow_builder/backend_flow.py` | edits |
| `session_id` / `user_message` on the wire | `schemas/execution.py`, `execution/service.py`, chat config builder | edits |

Verified end to end across THREE separate flow instances, which is what a
subprocess-per-turn runtime actually gives you: one lineage, history 2→4→6
messages, declared channels accumulating beside it, and every node running on
every turn.

Not built: the state panel (step 6), the interrupt payload channel (step 7),
`group_id` on `flow_states` and the checkpoint history API (step 8). A flow
becomes conversational by carrying `flow_config.state.conversational: true`,
which nothing in the UI writes yet — so this is reachable from the API and the
database, not from the Flow Builder.

## Sequencing

Each step is independently shippable and independently useful. Sizes are rough and
assume tests.

| Step | Change | Size | Unlocks |
|---|---|---|---|
| 1 | Channel schema + reducers in the state model; `replace` default | ~250 lines | Accumulating state |
| 2 | `begin_turn()` reset in `Flow` | ~30 lines | A second turn runs the whole graph |
| 3 | Derived thread key + unconditional id adoption in `_restore_state` | ~80 lines | Turn N+1 continues turn N |
| 4 | Conversational channels (`messages`, `last_user_message`, `last_intent`) + append on turn entry | ~200 lines | Actual conversation |
| 5 | Chat wiring: session → thread, "Use existing" continues a thread | ~250 lines | The user-visible feature |
| 6 | State panel in Flow Builder (channels + reducers) | ~400 lines | Authoring without JSON |
| 7 | Interrupt payload into a channel; unify with the HITL gate | ~150 lines | Ask-and-answer inside a flow |
| 8 | `group_id` on `flow_states`; checkpoint history API (list/fork) | ~200 lines | Tenancy, time travel |

Steps 1–4 are the spine. Step 5 is where you can see it. Steps 6–8 are follow-through.

> [!IMPORTANT]
> Step 2 is small but load-bearing. Without it, a thread's second turn runs only the
> starting crew and reports success — the same silent-partial-run failure you have already
> spent debugging time on twice.

## Decisions to make

1. **Non-chat threads.** The derived key needs a `session_id`, and an API caller or a
   scheduled run has none. Either accept a caller-supplied correlation id in the same
   slot, or fall back to the execution's own job id (a one-turn thread). Decide which,
   because it determines whether a flow can hold a conversation outside chat.
2. **Where a turn's inputs come from.** In chat it is the user message. Via the API it is
   whatever the caller sends. Worth deciding whether a turn requires a declared
   `last_user_message` or accepts arbitrary channel writes.
3. **Snapshot frequency.** CrewAI's docs recommend `@persist` on a single terminal step
   rather than class-level, so a snapshot lands once per turn with all handler updates
   applied. Kasal currently saves after every method (`Flow._save_state`). Per-turn is
   cheaper and has cleaner semantics; per-method gives finer replay. Pick one.
4. **Thread lifetime and cost.** A thread whose `messages` grows without bound makes every
   turn more expensive. Reuse the `context_summary` compaction pattern rather than
   inventing a second one.
5. **Concurrency.** Two turns on one thread at once will interleave writes to the same
   lineage. `flow_threads.status` plus a guard is probably enough; decide whether the
   second turn queues or is rejected.

## What to do with the uncommitted typed-state work

There is currently uncommitted work that compiles a declared state schema into a real
state class (`services/flow_builder/modules/flow_state_model.py`), wires it into the
builder, and derives the schema from the canvas on the frontend. It is tested and green,
but it was designed for a different goal: catching a misspelled input on a one-shot run.

Three options:

- **Land it as Step 1's foundation.** `build_state_model` becomes the channel compiler:
  the reducer is one more key per property, `DictLikeState` is where the merge policy
  lives, and the derived producer stays as the default until the state panel exists. This
  is the least wasted work — the module is already the right shape and the right size.
- **Land only the backend half.** Keep the compiler; drop the automatic canvas
  derivation, so nothing changes for existing flows until a schema is declared
  deliberately. This is the conservative option, and it removes the one behavioral change
  in the current diff (every flow silently becomes typed).
- **Hold it.** Keep the branch, revisit after the thread design is settled.

The recommendation is the second: the compiler is a prerequisite for reducers either way,
and deferring the derivation keeps the "every flow becomes typed" decision out of a change
you are still evaluating.

## Related

- [Checkpointing and resume](./CHECKPOINTING.md) — the record contract, identity hashing, and what resume restores
- [Flow routing](./flow-routing.md) — how routers and conditions read state today
- [Memory](./MEMORY.md) — the cross-thread store this design deliberately keeps separate
- [Architecture guide](./ARCHITECTURE_GUIDE.md) — where flows sit among the three execution paths

External references used for the comparison:

- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence) — checkpointers, threads, stores
- [LangGraph graph API](https://docs.langchain.com/oss/python/langgraph/graph-api) — state schemas and reducers
- [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts) — `interrupt()` and `Command(resume=…)`
- [CrewAI: mastering flow state](https://docs.crewai.com/v1.14.7/en/guides/flows/mastering-flow-state) — structured state, `@persist`, resume by id
- [CrewAI: conversational flows](https://docs.crewai.com/v1.14.7/en/guides/flows/conversational-flows) — `ChatState`, `handle_turn`, intent routing

Back to the [documentation hub](./README.md).
