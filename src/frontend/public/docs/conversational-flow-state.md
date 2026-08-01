# Conversational flow state

How a flow carries state across nodes and across turns: channels with merge policies, a checkpoint lineage keyed to a conversation, and the turn contract that lets a flow answer a follow-up question.

- [The problem this solves](#the-problem-this-solves)
- [The four ideas](#the-four-ideas)
- [Anatomy of a turn](#anatomy-of-a-turn)
- [Channels and reducers](#channels-and-reducers)
- [The state class](#the-state-class)
- [Threads](#threads)
- [The turn contract](#the-turn-contract)
- [Where each piece lives](#where-each-piece-lives)
- [Staying on the flow across turns](#staying-on-the-flow-across-turns)
- [Make a flow conversational](#make-a-flow-conversational)
- [Limits and known gaps](#limits-and-known-gaps)

> [!NOTE]
> This describes work that is implemented and unit-tested but has not yet been
> exercised against a live instance. For the design rationale and the comparison
> with LangGraph and CrewAI, see the [conversational flow state proposal](./conversational-flow-state-proposal.md).

## The problem this solves

A flow's state used to be a bare dict, and a dict accepts any key. Send an input
named `topci` instead of `topic` and the value lands under the typo, the router
reading `topic` sees nothing, the flow takes the other branch, and the run
reports success. Nothing anywhere says a value was lost.

The same dict cannot hold a conversation. Every write overwrites, so a history
restored from a checkpoint is replaced by the newest turn the moment anything
writes to it — a flow that remembers exactly one message, which is
indistinguishable from a flow that remembers nothing.

Both are fixed by the same thing: giving state a declared shape and a merge
policy per field.

## The four ideas

| Idea | What it is | Why it is needed |
|---|---|---|
| Channel | A declared field of flow state | A closed field set is what makes a misspelled input an error instead of a silent loss |
| Reducer | How a write to a channel merges | Overwriting cannot accumulate a conversation |
| Thread | A checkpoint lineage, stable across turns | Continuity has to survive a process that ends between turns |
| Turn | One flow run that restores a thread and appends to it | The unit a conversation is made of |

The structural fact behind all four: **a flow run is a spawned subprocess**
(`services/flow_builder/process_executor.py`). The `Flow` object does not
survive the turn. So continuity cannot live in memory — it lives in the
checkpoint, and every turn rebuilds the graph and restores.

## Anatomy of a turn

What happens when a second message arrives in a chat session that is talking to
a conversational flow:

1. **The run is dispatched** with `session_id` and `user_message` on the
   execution request (`schemas/execution.py`), copied into the flow's execution
   config by `services/execution/service.py`.
2. **The lineage is derived** — `BackendFlow._thread_id()` calls
   `thread_state_uuid(session_id, flow_id, group_id)`, a `uuid5`. Same
   conversation and same flow, same value, every time.
3. **The kickoff inputs are assembled** — `BackendFlow._kickoff_inputs()`
   returns the caller's inputs, plus this turn's writes from `turn_inputs()`,
   plus `id` set to the lineage.
4. **The state class is built** — the builder compiles
   `flow_config.state.model` into a real class, on `ConversationState` when the
   flow is conversational (`modules/flow_builder.py`).
5. **The graph runs** — `Flow.kickoff_async()` adopts the id, restores the
   thread's last checkpoint, then merges the turn's writes THROUGH each
   channel's reducer. The user's line appends to the restored history.
6. **The turn closes** — `BackendFlow._close_turn()` records the answer if the
   flow did not record one itself, trims the history to the cap, and calls
   `Flow.save_checkpoint("turn_end")` so the edits reach the checkpoint the
   next turn will restore.

Step 5 is the one to internalise. Merging happens in exactly one place, so
there is no second path that could forget to apply a reducer.

## Channels and reducers

A channel is declared in `flow_config.state.model`, which uses the same JSON
Schema shape as a publication's `input_schema` — one schema concept in the
product, not two — with one addition, `reducer`:

```json
{
  "type": "object",
  "properties": {
    "topic":       {},
    "has_results": {},
    "findings":    { "reducer": "append" },
    "attempts":    { "reducer": "add" }
  }
}
```

Four reducers exist (`modules/flow_state_channels.py`):

| Reducer | Merge rule | Typical channel |
|---|---|---|
| `replace` | The newest value wins. The default | `topic`, `has_results` |
| `append` | Concatenates onto a list | `messages`, accumulated findings |
| `merge` | Shallow dict merge, incoming keys winning | Per-turn structured context |
| `add` | Numeric sum | Counters |

Three behaviours are worth knowing because they are not obvious:

- **A declared reducer implies the channel's shape.** `append` without a type
  becomes a list, `add` becomes an integer, `merge` becomes an object
  (`_IMPLIED_TYPE`). Without this an `add` channel would default to `None` and
  the first `state.count + 1` in a node would fail on NoneType.
- **An unknown reducer name falls back to `replace` and logs.** A schema is
  authored data; a typo in it must not make the flow unrunnable.
- **`replace` declared explicitly is recorded**, so a flow can override a
  reducer it inherits from `ConversationState`. Recording only non-defaults
  would let that override parse and do nothing.

## The state class

`build_state_model(schema, name, base)` compiles a schema into a pydantic class
(`modules/flow_state_model.py`). Returns `None` for anything unusable, so a
malformed schema leaves the flow on a dict rather than failing its kickoff.

**It answers to dict access.** Every condition ever written for a flow uses it —
`state.get("has_results", "")` is what the UI generates — and on a plain
pydantic model that raises. `DictLikeState` therefore provides `get`, `[]`,
`in`, `keys`/`items`/`values`, so all three access forms work:

```python
state.get("has_results", "") == True    # what the UI generates
state["topic"] == "swiss news"
state.topic == "swiss news"
```

**`merge` and `update` are different operations, deliberately.** `update` seeds
a value (it is what the builder's `__init__` calls for `initialValues`);
`merge` combines one with what is already there, through the reducer. Seeding a
conversation with `merge` would double it; merging a turn with `update` would
erase it.

**Extra keys are writable, inputs are checked.** The state is `extra="allow"`,
because the flow writes to its own state at runtime — the builder stores
`state["previous_output"]` between methods, a state operation writes whatever
variable its node names — and none of that is ever declared. What is still
rejected is an INPUT naming a channel the state does not have: `merge` and
`_merge_inputs` check with `hasattr`, and an extra that has not been set yet
fails that check.

**`id` is always present.** It is the checkpoint handle, and a schema that
forgot it would make the flow unresumable.

## Threads

A thread is a checkpoint lineage, and its key is derived rather than stored
(`flow_builder/flow_thread.py`):

```python
thread_state_uuid(session_id, flow_id, group_id)
# -> uuid5(FLOW_THREAD_NAMESPACE, f"{group_id}:{session_id}:{flow_id}")
```

**Why not use the chat session id directly.** One session routes to many
capabilities — that is what "Use existing" does — while `flow_states` is keyed
by a single `flow_uuid`. Sharing the session id as that key would put two
different flows in one lineage, and turn 2 of one would restore the other's
state. Folding the flow id in is the same answer LangGraph gives with
`checkpoint_ns` beside `thread_id`.

**Why derived.** The value is the same every time, so turn N+1 addresses turn
N's lineage with no mapping table to keep in sync — the pattern crew memory ids
already use. `FLOW_THREAD_NAMESPACE` must never change: every existing thread's
id is derived through it.

**No session, no thread.** `thread_state_uuid` returns `None` when either half
is missing, and the run proceeds as a one-shot — which is what every flow does
today. An explicit `resume_from_flow_uuid` always wins over the derived value:
the caller named a lineage and must get that one.

Two runtime details make threading work at all:

- **The id is adopted unconditionally.** `_restore_state` returns early when the
  lineage is empty — always true on turn 1 — so `kickoff_async` adopts the id
  BEFORE restoring (`_adopt_state_id`). Without this, turn 1 saved under the
  random `uuid4` it was constructed with and turn 2 restored nothing.
- **Execution bookkeeping resets per turn.** `Flow.begin_turn()` clears
  `_completed`, `_scheduled`, `_and_progress` and `_method_outputs`, and
  `kickoff_async` calls it automatically when the instance has already run.
  `_completed` exists so a listener fires once per RUN; without the reset a
  second kickoff re-ran the `@start()` methods and fired no listeners at all,
  completing successfully having executed part of the graph.

## The turn contract

A conversational flow's state is built on `ConversationState`
(`modules/flow_conversation.py`), whose field names follow CrewAI's so a flow
author reading either project's docs does not have to translate:

| Channel | Reducer | Meaning |
|---|---|---|
| `id` | `replace` | The thread |
| `messages` | `append` | The conversation, as `{role, content}` pairs |
| `last_user_message` | `replace` | What was said this turn |
| `last_intent` | `replace` | This turn's classification, when one was made |
| `session_ready` | `replace` | One-time bootstrap marker |

`last_user_message` is a convenience — the same content is the last entry of
`messages` — so that a router condition does not have to index into a list.

**Recording the answer.** `append_assistant_message(state, text)` is explicit: a
flow's last method output is not always the reply (a router returns a route
name), so a runtime that guessed would fill the history with control-flow noise.
`close_turn()` fills the gap only when the flow recorded nothing itself, which
guarantees the history never has two user lines in a row without ever
duplicating a real answer. A structured result is serialised with
`model_dump_json()` rather than `str()`, so a pydantic result does not enter the
conversation as its repr.

**History is capped** at `MAX_THREAD_MESSAGES` (100) by `trim_messages`, applied
at the end of a turn so the checkpoint the NEXT turn restores is already
bounded. The turn that just ran keeps everything it reasoned over.

## Where each piece lives

| File | Responsibility |
|---|---|
| `services/flow_builder/modules/flow_state_channels.py` | The four reducers, name normalisation |
| `services/flow_builder/modules/flow_state_model.py` | `DictLikeState`, `build_state_model` |
| `services/flow_builder/modules/flow_conversation.py` | `ConversationState`, `turn_inputs`, `close_turn`, `trim_messages` |
| `services/flow_builder/flow_thread.py` | `thread_state_uuid` |
| `services/flow_builder/runtime/flow.py` | `begin_turn`, id adoption, reducer-aware `_merge_inputs`, `save_checkpoint` |
| `services/flow_builder/backend_flow.py` | `_state_config`, `_thread_id`, `_kickoff_inputs`, `_close_turn` |
| `services/flow_builder/modules/flow_builder.py` | Chooses the state base and installs it as `initial_state` |
| `schemas/execution.py`, `services/execution/service.py` | `session_id` and `user_message` on the wire |
| `frontend/src/utils/flowStateSchema.ts` | Derives channel NAMES from the canvas |
| `frontend/src/store/flowState.ts` | Stores the DECLARED half — reducers, conversational |
| `frontend/src/components/Flow/FlowStateDialog.tsx` | The authoring panel |

**The split between derived and declared is the load-bearing idea on the
frontend.** Channel names come from the canvas — router conditions and
`{placeholders}` — and are rederived on every save, update and run, so those
three paths cannot drift. Reducers and the conversational flag cannot be
inferred from anything, so they are stored per tab and passed back into
`buildFlowConfiguration`. A rebuild that dropped them would silently turn an
appending channel back into an overwriting one, and the flow would forget every
turn but its newest.

## Staying on the flow across turns

A thread only continues if the *same* capability is chosen again, and in
ChatMode the capability is chosen per turn by the router. Two things make it
choose the same one:

- **The router can see who answered.** A routed run records its capability on
  the answer message (in the `__chatmode` envelope of
  `chat_history.generation_result`), so the next turn's conversation renders as
  `[answer 2, from swiss_news_flow]`. The catalogue marks which capabilities
  hold a conversation, and the routing prompt carries one rule: a turn that
  continues, refines or questions such an answer picks that capability again,
  even when the message is a fragment.
- **A decline falls back to the held conversation.** If the router still
  declines — a fragment like "and Germany?" matches nothing on its own words —
  `held_conversation` checks whether the last answer came from a capability that
  is conversational *and* still published to this group, and routes there
  instead of answering in the chat. Without it the flow would never learn the
  turn happened.

The fallback extracts no inputs: the turn's text reaches the flow as its user
message, and inventing values from a fragment is the guessing the extraction
rules forbid. It is confined to conversational capabilities — a one-shot crew
keeps the old behaviour, where a question about the answer on screen is answered
in the chat rather than re-running the crew.

## Make a flow conversational

1. Open the flow in the Flow Builder and click **Flow state and conversation**
   in the right sidebar.
2. Turn on **Hold a conversation across turns**. The four conversation channels
   appear in the list.
3. Set a reducer for any channel that should accumulate. Leave the rest on
   **Replace**.
4. Save the flow. The declaration is written to `flow_config.state`.

From then on, each message in a chat session that routes to this flow continues
the same state instead of starting a new run. To inspect a thread:

```sql
SELECT method_name, created_at, state_json
FROM flow_states
WHERE flow_uuid = '<thread id>'
ORDER BY created_at;
```

The table is append-only, so the whole history of the conversation is there —
one row per completed method, plus a `turn_end` row per turn.

## Limits and known gaps

- **Checkpoint reads are scoped to one group, exactly.** A run with a group sees
  only that group's rows; a run without one sees only rows that also carry none.
  A lineage id is not an authorisation to read a lineage. Consequence:
  checkpoints written before the column existed carry NULL and cannot be read
  from a tenanted run, so those threads start fresh rather than resuming.
- **History and fork are readable.** `get_history` returns the timeline;
  `get_state_at` returns one checkpoint by row id. A fork COPIES a checkpoint
  into a new lineage rather than rewinding the old one, so the conversation that
  actually happened stays intact.
- **Compaction folds, it no longer truncates.** Past the verbatim window, older
  messages are summarised into a `summary` channel using the same summarizer
  chat uses (same prompt, same `CHAT_COMPACTION` kill-switch). A flow that does
  not declare a `summary` channel does not fold — there would be nowhere to put
  the result.
- **Concurrent turns are detected, not prevented.** Two turns racing on one
  thread are reported by comparing the checkpoint a turn restored from against
  the newest one at close. The write still proceeds last-writer-wins; a hard
  guard needs the `flow_threads` row this design deferred.
- **A turn's inputs are checked; a node's own writes are not.** That asymmetry
  is deliberate, but it means a typo in a node's write creates an extra rather
  than raising.
- **Threads need a chat session.** An API caller or scheduled run has no
  `session_id`, so it runs as a one-shot.

## Related

- [Conversational flow state proposal](./conversational-flow-state-proposal.md): the design rationale, and how LangGraph and CrewAI solve the same problem
- [Checkpointing and resume](./CHECKPOINTING.md): the checkpoint record, identity hashing, and what a resume restores
- [Flow routing](./flow-routing.md): how routers and conditions read state
- [Memory](./MEMORY.md): the cross-thread store this deliberately keeps separate from a thread

Back to the [documentation hub](./README.md).
