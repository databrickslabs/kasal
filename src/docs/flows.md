# Flows

What a Kasal flow is, how one is authored, how it is compiled and executed, and how state, routing, checkpoints and conversations work — end to end, from the canvas to the database.

- [What a flow is](#what-a-flow-is)
- [The pieces on the canvas](#the-pieces-on-the-canvas)
- [How a flow is stored](#how-a-flow-is-stored)
- [The execution path](#the-execution-path)
- [Compiling a flow into a class](#compiling-a-flow-into-a-class)
- [The flow runtime](#the-flow-runtime)
- [Flow state](#flow-state)
- [Routing](#routing)
- [Crews inside a flow](#crews-inside-a-flow)
- [Checkpoints and resume](#checkpoints-and-resume)
- [Human approval gates](#human-approval-gates)
- [Conversations across turns](#conversations-across-turns)
- [Turn selection (proposed)](#turn-selection-proposed)
- [Results and the chat surface](#results-and-the-chat-surface)
- [Running a flow from chat](#running-a-flow-from-chat)
- [Debugging a flow](#debugging-a-flow)
- [Limits and known gaps](#limits-and-known-gaps)

## What a flow is

A **crew** is a set of agents working through an ordered list of tasks. It runs
start to finish and produces one output. That is enough for "research this
topic and write it up", and not enough for anything with a decision in it.

A **flow** orchestrates crews. It is a graph: crews are the nodes, and the edges
say what waits for what — including edges that fire only when a condition holds.
Between the crews sits a shared **state**, which one crew writes and a later
condition reads. So a flow can do the things a single crew cannot:

- run crew B only when crew A found something,
- run crews B and C in parallel and wait for both,
- pause for a human to approve before continuing,
- resume from the middle after a crash instead of re-running everything,
- hold a multi-turn conversation, carrying what it learned between turns.

Flows are one of Kasal's three execution paths. The other two are **Chat**
(a single agent, in-process, for sub-second answers) and **Agent Builder**
(one crew, in a subprocess). A flow's `execution_type` on the wire is `"flow"`.
For how the three relate, see `src/backend/src/services/execution/CLAUDE.md`.

## The pieces on the canvas

A flow is authored in the Flow Builder — the canvas you get from the Flow mode
in the tab bar. It has four kinds of element.

**Crew nodes.** Each is a reference to a saved crew, drawn with its tasks
listed. `nodeTypes` in `FlowCanvas.tsx` also registers `agentNode`, `taskNode`
and `managerNode` so a crew canvas loaded into the same component still renders,
but a flow is built from `crewNode`s.

**Edges.** An edge from crew A to crew B is configured in the edge dialog
(`EdgeConfigDialog.tsx`) and carries:

| Field | Meaning |
|---|---|
| `listenToTaskIds` | Which of the SOURCE crew's tasks must finish first |
| `targetTaskIds` | Which of the TARGET crew's tasks this edge runs |
| `logicType` | `NONE`, `AND`, `OR`, or `ROUTER` |
| `routerCondition` | For a `ROUTER` edge: the expression that selects this route |
| `routerConfig.stateMappings` | Maps a source task's output field to a state variable |
| `checkpoint` | Save a resumable checkpoint after this step |
| `hitl` | Pause here for human approval (requires `checkpoint`) |

An edge with no incoming edges to its source makes that source a **starting
point**. An edge with `logicType: 'AND'` makes the target wait for every named
predecessor; `OR` fires on the first.

**Router edges.** `logicType: 'ROUTER'` turns the source into a decision point.
Several router edges leaving one node form one router with several **routes**,
each with its own condition.

**Conditions.** Written against flow state, in the shape the condition builder
generates:

```python
state.get("has_results", "") == True
state.region == "DACH" and state.count > 1
```

## How a flow is stored

The `flows` table (`src/models/flow.py`) holds:

| Column | Contents |
|---|---|
| `id`, `name`, `crew_id` | Identity, and the crew the flow was built from |
| `nodes`, `edges` | The canvas, as ReactFlow JSON |
| `flow_config` | The compiled description the backend executes |
| `group_id`, `created_by_email` | Tenant isolation and audit |

`flow_config` is not a copy of the canvas — it is a *derivation* of it, produced
by `buildFlowConfiguration(nodes, edges, name, declaredState)` in
`frontend/src/utils/flowConfigBuilder.ts`:

```json
{
  "listeners":      [ { "crewId": "...", "listenToTaskIds": [...], "tasks": [...], "conditionType": "AND" } ],
  "startingPoints": [ { "crewId": "...", "taskId": "...", "isStartPoint": true } ],
  "routers":        [ { "name": "router_crew_1", "listenTo": "starting_point_0",
                        "routes": {"found": [...], "empty": [...]},
                        "routeConditions": {"found": "state.get(\"has_results\") == True"},
                        "stateMappings": [...] } ],
  "actions":        [ ... ],
  "state":          { "enabled": true, "type": "structured", "model": {...} },
  "persistence":    { "enabled": true, "level": "flow" }
}
```

**The same builder runs on save, on update, and before every run.** That is
deliberate: a saved config is only as current as the last save, and a flow whose
router condition was added afterwards would otherwise reach the backend with no
routers at all — and only its starting crew would run. Rebuilding on every path
is what stops the Flow Builder and chat from executing the same flow
differently.

## The execution path

```
POST /executions  (execution_type: "flow")
  └─ ExecutionService.create_execution           schemas/execution.py → execution config
      └─ KasalExecutionService.run_flow_execution  loads the flow, adds flow_id / group context
          └─ run_flow_in_process                   flow_execution_runner.py
              └─ ProcessFlowExecutor.run_flow_isolated
                  └─ mp.get_context("spawn").Process(...)     ← a NEW OS process
                      └─ BackendFlow.kickoff_async            backend_flow.py
                          └─ FlowBuilder._create_dynamic_flow  builds the class
                              └─ Flow.kickoff_async            runtime/flow.py
```

**Every flow run is a spawned subprocess.** This is the single most important
fact about the architecture, and most of the design follows from it:

- Nothing in memory survives the run. Continuity between runs lives in the
  database, never in an object.
- The subprocess re-imports the world. A module split that passes in-process
  tests can still break the spawned interpreter, which is why
  `services/execution/CLAUDE.md` warns against drive-by refactors of modules the
  subprocess imports.
- Configuration is passed as a serializable dict. A field that is not copied
  into it does not exist inside the run — several bugs have been exactly this.

## Compiling a flow into a class

`FlowBuilder._create_dynamic_flow` (`modules/flow_builder.py`) turns
`flow_config` into a real Python class, built with `type()` so the decorators
are processed at class creation. Methods are generated in three families:

| Config element | Generated method | Decorator |
|---|---|---|
| `startingPoints[i]` | `starting_point_<i>` | `@start()` |
| `listeners[i]` | `listener_<i>` | `@listen(<predecessor>)` |
| `routers[i]` | `router_<name>_<i>` | `@router(<listenTo>)` |
| each route of a router | `route_<router>_<route>_<i>` | `@listen("<route name>")` |

Three processors do the work of turning config into method inputs —
`FlowProcessorManager.process_starting_points`, `.process_listeners`,
`.process_routers` — and `FlowMethodFactory` builds the callables
(`modules/flow_methods.py`).

The method **names are the contract** between frontend and backend: the
frontend emits `listenTo: "starting_point_0"` or `"listener_2"`, and the backend
must generate exactly those names or the graph silently has no edges.

The generated class also gets:

- `__init__`, which applies `state.initialValues` if any,
- `initial_state`, when the flow declares a state schema,
- the `@persist` decorator, when persistence is enabled.

## The flow runtime

`services/flow_builder/runtime/flow.py` is Kasal's own implementation of the
flow DSL — first-party code, not a dependency.

**Decorators.** `@start()` marks an entry point. `@listen(x)` fires when `x`
completes. `@router(x)` fires like a listener but its RETURN VALUE is a route
name, which fires any listener waiting on that name. `and_()` / `or_()` combine
triggers.

**Registration.** `Flow.__init_subclass__` walks the class at definition time
and records `_start_methods`, `_listeners` (name → trigger) and `_routers`.

**Execution.** `kickoff_async(inputs)`:

1. resets per-turn bookkeeping if this instance has already run,
2. adopts `inputs["id"]` as the state id and restores that checkpoint,
3. merges the remaining inputs into state,
4. emits `FlowStartedEvent`,
5. runs every `@start()` method concurrently with `asyncio.gather`,
6. after each method: appends to `_method_outputs`, marks it complete, saves a
   checkpoint, then fires the listeners waiting on it (and, for a router, on its
   returned route name),
7. emits `FlowFinishedEvent` and returns the last output.

`FlowStartedEvent` and `FlowFinishedEvent` open and close the outermost
causality scope on the event bus, which is what makes the crew runs a flow
drives appear as children of the flow in the trace rather than as separate
roots.

**AND vs OR.** `_fire_listeners` tracks partial progress per listener in
`_and_progress`; an `AND` listener runs only once every named predecessor has
signalled.

## Flow state

State is the shared scratchpad every node reads and writes. It comes in two
forms.

**Untyped (the default).** A plain dict with an auto-generated `id`. Accepts any
key. This is what a flow that declares no schema runs on.

**Typed.** When `flow_config.state.model` declares a schema, the builder
compiles it into a real class (`modules/flow_state_model.py`) and installs it as
`initial_state`. A typed state has a closed set of fields, so an input naming a
field it does not have RAISES at kickoff instead of vanishing:

```
Flow state has no field(s) ['topci']. This state accepts: ['has_results', 'id', 'topic'].
```

That error is the point. On an untyped dict the misspelled value lands under the
typo, the condition reading the correct name sees nothing, the flow takes the
other branch, and the run reports success.

**The generated state answers to dict access** — `state.get("x")`, `state["x"]`,
`"x" in state` — as well as attribute access. Every condition ever authored uses
the dict form, so a state that answered only to attributes would break every
existing flow.

**Channels and reducers.** Each declared field is a channel, and may name a
merge policy (`modules/flow_state_channels.py`):

| Reducer | Merge rule |
|---|---|
| `replace` | Newest value wins. The default |
| `append` | Concatenates onto a list |
| `merge` | Shallow dict merge |
| `add` | Numeric sum |

`merge()` applies reducers; `update()` seeds a value without them. Writes the
flow makes to its own state at runtime — `previous_output`, a state operation's
output variable — are allowed even though nothing declares them; only INPUTS are
checked against the schema.

For the full treatment see [Conversational flow state](./conversational-flow-state.md).

## Routing

A router is where a flow makes a decision. When its trigger completes:

1. **The eval context is built.** State goes in as `state`. Then the previous
   crew's output is parsed — a bare JSON object, a fenced block, or a JSON array
   — and merged into `state`, so a crew that emits
   `{"has_results": true, "count": 6}` makes both readable.
2. **Values are coerced.** `"true"` becomes `True`, `"6"` becomes `6`, so a
   condition comparing to `True` or to a number behaves as written.
3. **Each route's condition is evaluated** in order; the first that holds wins.
4. **The route name is returned**, which fires the `@listen("<route>")` method
   holding that route's crews.
5. **If nothing matches**, a route named `default` is taken if one exists;
   otherwise the flow stops there.

Conditions are evaluated by `FlowStateManager.evaluate_condition` over
`safe_eval`, an AST evaluator — not `eval`. A condition that cannot be evaluated
at all (unknown name, bad syntax) raises `ConditionEvaluationError` rather than
returning `False`. The distinction matters: "the condition is false" and "the
condition is broken" used to look identical, and a broken one silently sent
every run down the same branch.

`stateMappings` on a router edge are the declarative alternative to JSON
parsing: they map a named output field of a source task onto a state variable.

## Crews inside a flow

Each generated method that owns crews builds a `Crew` from its task list and
kicks it off. Several things happen around that:

- **Inputs.** `crew_inputs_from_state(flow)` passes the flow's state as the
  crew's kickoff inputs, minus `id`. This is what makes `{topic}` in a task
  description resolve. Without it a crew interpolated nothing and executed its
  template literally.
- **Agents and tools.** Task and agent configs are adapted
  (`task_adapter.py`, `agent_adapter.py`), tools built by the tool factory, and
  duplicate tools between agent and task de-duplicated.
- **Memory.** `configure_flow_crew_memory` and `attach_memory_seams` wire the
  crew's memory scope and label.
- **Model limits.** `get_model_context_limits` resolves context window and max
  output for the agent's model.
- **MCP.** `FlowConfigManager` collects the MCP servers the flow's tasks need.
- **Callbacks.** `create_execution_callbacks` attaches the trace and event
  callbacks so the crew's steps land in the execution trace.

## Checkpoints and resume

**Turning it on.** Persistence is enabled when any edge carries
`checkpoint: true`; the builder then sets `flow_config.persistence.enabled` and
applies `@persist`.

**Where it goes.** `KasalFlowPersistence` writes to Kasal's own database rather
than a stray SQLite file, so checkpoints survive restarts and work on Lakebase.
Because the persistence API is synchronous and runs inside the flow's event
loop, each database call is run on a short-lived dedicated thread and loop.

**The table.** `flow_states` is append-only: one row per completed method,
holding `flow_uuid`, `method_name`, `state_json`, `group_id` and `created_at`.
The newest row for a `flow_uuid` is the current checkpoint, and every read is
scoped to the running group.

**The history is readable.** `get_history` returns a lineage as a timeline and
`get_state_at` returns one checkpoint by row id, which is what a **fork** reads:
forking copies a checkpoint into a NEW lineage rather than rewinding the old
one, so the conversation that actually happened stays intact and answerable.

**Both halves are on the trace.** A restored crew emits
``crew_checkpoint_restored``; a checkpoint WRITE emits ``flow_checkpoint_saved``,
carrying the method, the lineage id, and — when the write failed — the reason.
Failures are traced precisely because they do not fail the run: the answer still
comes back, so without a row the flow silently loses its memory and every later
turn starts from scratch. A flow with no persistence attached emits nothing
rather than claiming a checkpoint it never wrote.

**Two kinds of checkpoint.** The state snapshots above are one; the other is the
**execution checkpoint record** in `services/execution/checkpointing/`, a shared
contract for crews and flows that records what completed and what it produced.
For a flow, a "unit" is a crew.

**Resuming.** A run can resume by state (`resume_from_flow_uuid`) or from an
earlier execution (`resume_from_execution_id`, optionally
`resume_from_crew_sequence`). Crews before the resume point are replaced by stub
methods that return their recorded output.

**Editing a crew invalidates its checkpoint.** `checkpoint_identity.py` hashes
everything about a crew that can change its output — its tasks, its agents'
roles and goals, and the model — computed from the same runtime objects at
record time and at resume time. If a crew has been edited since, its stored
output is refused and the crew re-runs. Without that check, editing a crew was
silently ignored on resume.

Resume reads only the written checkpoint. It used to fall back to reconstructing
from `execution_trace` rows; that was removed, because traces are telemetry —
retention-pruned and reshapeable — so a crew whose trace had aged out looked
like it never ran, and the resume produced a plausible run built on gaps.

For more, see [Checkpointing and resume](./CHECKPOINTING.md).

## Human approval gates

An edge with `hitl` configured generates a gate method that listens to the
previous crew and then:

1. creates an `HITLApproval` record,
2. sets the execution status to `WAITING_FOR_APPROVAL`,
3. sends webhook notifications,
4. raises `FlowPausedForApprovalException`.

That exception derives from `BaseException`, not `Exception`, so ordinary
error handling does not swallow it — it is a controlled pause, not a failure.
The run resumes from the checkpoint once a decision is recorded, which is why a
gate requires `checkpoint: true` on the edge.

## Conversations across turns

By default a flow answers one request and forgets. A flow whose state block
carries `conversational: true` instead answers a **turn**:

- its state is built on `ConversationState`, which adds `messages` (with an
  `append` reducer), `last_user_message`, `last_intent` and `session_ready`,
- its checkpoint lineage is derived from the conversation —
  `uuid5(group:session:flow)` — so every message in a chat session addresses the
  same state,
- each turn restores that state, appends the user's line, runs the whole graph,
  records the answer and saves.

Set it in the Flow Builder: **Flow state and conversation** in the right
sidebar. The mechanism is documented in full in
[Conversational flow state](./conversational-flow-state.md).

## Turn selection (proposed)

Routing and selection are two different decisions that both get called
"routing", and keeping them apart is what makes a conversational flow
affordable.

| | Condition router | Turn selection |
|---|---|---|
| Question | "Given what the flow computed, which branch is valid?" | "Given what the person asked, what must this turn produce?" |
| When | DURING execution, at a node | BEFORE execution, at kickoff |
| Reads | state values — `has_results`, `region` | the turn's text, and what each goal produces |
| Fails as | wrong branch on unexpected data | wrong artefact for the question |
| Exists | today | proposed |

A condition router is part of the flow's LOGIC — it would exist even if flows
never held a conversation. Turn selection is part of its ENTRY, and exists only
because a conversation asks the same graph for different things on different
turns.

**Selection does not belong inside a router.** A flow with no router would get
no selection at all — a linear pipeline has no decision point to hook into — a
router that HAS conditions could never select, and selection is about the whole
graph while a router sees only its own children.

### Goals and material

A **goal** is a crew that produces something a person would ask for: the
terminal crews by default, plus any the author marks. It carries a description
of what it produces, and that description is what selection matches against —
the same contract a published capability has with the chat router. Everything
else is **material**: work that exists to feed a goal.

A turn then becomes a build request:

```text
turn: "now turn that into a mindmap"
  target      = mindmap crew                   (selection)
  required    = mindmap + its ancestors        (the graph)
    gather    ✓ already in state  -> reuse
    features  ✓ already in state  -> reuse
    mindmap   ✗ not yet           -> run
  cost        = one classification + one crew
```

### How a turn would execute

`kickoff_async` runs every `@start()` and lets listeners cascade — push-based.
Target-driven execution is the same machinery with a filter:

1. **Select** the goal(s). No confident selection means no filter, and the flow
   runs exactly as it does today.
2. **Resolve** `required = targets ∪ ancestors(targets)` from the listener map
   the builder already has.
3. **Run** as now, with two rules: a method outside `required` never fires, and
   a method inside it whose output is already in state — with a matching content
   hash — returns that output instead of running.
4. **Routers still route** normally inside the required set.

### Rules where the two meet

These decide whether it can be trusted:

- **Selection chooses targets; routers choose paths.** Neither overrides the
  other.
- **If a router routes away from the selected goal, the goal is unreachable this
  turn and the turn says so.** It must not quietly return whatever the router
  did pick — that is a confident answer to a question nobody asked.
- **The selected goal always runs.** Reuse applies to material; reusing the
  target returns the previous turn's answer.
- **A declined selection degrades to today's behaviour**, not to nothing: a
  model outage runs the whole flow, which is slow and correct.
- **Reuse is content-hashed**, so editing a crew mid-conversation is never
  silently ignored.

Naming, since "router" is already taken: the condition node stays a **router**;
this is a **turn goal**.

### Open decisions

1. Terminal crews as goals by default, or only crews explicitly marked?
2. One goal per turn, or several — "compare them and make a quiz" is two.
3. On an unreachable goal: say so and stop, or run what the router chose and
   explain?
4. Does a goal need its own description, or is the crew's task text enough?
5. How does someone force a full run?

## Results and the chat surface

A flow's final result is unwrapped and then passed through
`wrap_result_with_surface`, the same A2UI composer the chat and Agent Builder
paths use, so a flow's answer renders as a deliverable rather than raw text.
One composer serves chat, Agent Builder, flows and the exported app; a second
one here is how they would drift.

A structured (pydantic) result is serialised with `model_dump_json()` rather
than `str()`, so it reaches the surface as JSON instead of an object repr.

## Running a flow from chat

With **Use existing** selected, a ChatMode prompt can be routed to an
already-published flow instead of generating a new crew. The router matches the
prompt against the publication catalogue; on a match, `buildFlowConfig` assembles
the run:

- `nodes` / `edges` from the saved flow,
- `flow_config` **rebuilt** with `buildFlowConfiguration`, with the flow's saved
  config underneath it for anything the builder does not produce,
- `inputs` resolved from the flow's declared variables — derived from BOTH
  router conditions and `{placeholders}`, since neither alone is complete,
- `session_id` and `user_message`, which is what makes the run a turn.

## Debugging a flow

**The log.** `logs/flow.log` carries the flow logger. It is verbose by design
and shows method creation, router evaluation with the condition and its result,
and each crew kickoff.

**Useful checks:**

```bash
# Which methods were generated, and what listens to what
grep -E "Creating method|Created router|route listener" logs/flow.log | tail -40

# Every router decision with its condition
grep "evaluated to" logs/flow.log | tail -20

# What the run passed into state
grep "to kickoff_async" logs/flow.log | tail -5
```

```sql
-- The state timeline for one flow run or thread
SELECT method_name, created_at, state_json
FROM flow_states WHERE flow_uuid = '<id>' ORDER BY created_at;
```

**Symptoms and their usual causes:**

| Symptom | Where to look |
|---|---|
| Only the first crew ran | Router condition raised or matched nothing, and there is no `default` route |
| A branch was never taken | The value the condition reads never reached state — check the crew's output and `stateMappings` |
| A task ran with `{topic}` unresolved | The input never reached state, or the crew got no inputs |
| Runs in the Flow Builder, not from chat | A config difference between the two paths — both rebuild, so compare the emitted `flow_config` |
| Resume re-ran a crew | Its identity hash changed, i.e. the crew was edited |

## Limits and known gaps

- **Checkpoint reads are scoped to one tenant, exactly.** `flow_states` carries
  a `group_id`, and a read matches it in both directions: a run with a group
  sees only that group's rows, a run without one sees only rows that also carry
  none. A lineage id is not an authorisation to read a lineage. The consequence
  to know about: checkpoints written before the column existed carry NULL and
  are therefore **unreadable from a tenanted run**, so those runs re-run from
  the start rather than resuming.
- **Router conditions see a merged view.** JSON parsed from the previous crew's
  output is merged into the eval context's `state`, which is convenient and
  means a condition can read a value that was never persisted to state.
- **A conversation's history is truncated, not summarised**, at 100 messages.
- **No concurrency guard on a thread.** Two turns at once will interleave writes
  to the same lineage.
- **Several flow modules are over the file-size ceiling** —
  `modules/flow_builder.py` and `modules/flow_methods.py` in particular. Do not
  add to them; extract a sibling module instead, and be careful: they are
  imported by the spawned subprocess.

## Related

- [Conversational flow state](./conversational-flow-state.md): channels, reducers, threads and turns in detail
- [Checkpointing and resume](./CHECKPOINTING.md): the checkpoint record, identity hashing, and what a resume restores
- [Flow routing](./flow-routing.md): router conditions and output schemas from the authoring side
- [Solution architecture](./ARCHITECTURE_GUIDE.md): where flows sit among Kasal's layers
- [Memory](./MEMORY.md): what a crew inside a flow can recall, and from where

Back to the [documentation hub](./README.md).
