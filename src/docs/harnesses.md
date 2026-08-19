# Harnesses: Kasal and CrewAI

Kasal runs agents on its own runtime. It can also run them on **CrewAI**. The
thing that is swapped is called a **harness**, and this page is what you need to
know about choosing one and about where the two differ.

---

## What a harness is, and is not

A harness supplies four things: the **agent**, the **task**, the **crew**, and
the **loop that drives them**. That is all.

Everything around them is Kasal's, on both harnesses:

| Stays Kasal's | Where |
| --- | --- |
| LLM calls, retries, token accounting, context trimming | `core/llm/transport/` |
| Tools, and the policy around them (approval, replay, failure handling) | `services/tools/`, `kernel/` |
| Memory — recall, writes, labelling, backends | `services/memory/` |
| Guardrails | `services/guardrails/` |
| **Flows**, their state, checkpoints and routing | `services/flow_builder/` |
| Traces, the timeline, MLflow | `services/otel_tracing/` |

Two consequences that surprise people:

* A run on CrewAI still emits `kasal.*` traces, still reads and writes Kasal
  memory, and still uses Kasal's LLM transport. Seeing `kasal` in a log is not
  evidence that the CrewAI harness was ignored.
* **Flows are always Kasal's.** A flow's `@start` / `@listen` graph, its state
  channels and its checkpoints behave identically under either harness — only
  the crews inside the nodes change.

---

## Choosing one

**Configuration → Engines.** One setting, workspace-wide, applying to runs
started after the change.

```text
GET  /api/v1/engine-config/harness      # what is configured, and what is available
PUT  /api/v1/engine-config/harness      # change it
```

There is deliberately **no per-run picker in the UI**. One existed briefly, next
to the model in the chat composer; it was removed because choosing an agent
framework is an operator's decision, not something to put in front of someone
writing a chat message. An API caller may still name a harness on the execution
payload (`CrewConfig.harness` / `FlowConfig.harness`), which is how a scheduled
or externally triggered run could pin one.

### A run's harness is decided once

At creation, and recorded on `execution_history.harness`. From then on it
travels with the run: on the row, in `config["_harness"]` for the spawned
interpreter, and in `KASAL_HARNESS` in that interpreter's environment.

The setting is never re-read mid-run. If it were, a switch landing between
agent-build and task-build would produce a run whose agents are one runtime and
whose later tasks are another, and nothing would report it. A **resume** reads
the harness back from the row rather than from the setting, because a checkpoint
can only be replayed into the runtime that produced it.

---

## Telling which one ran

Three places, in increasing order of detail:

1. **Job History** shows a chip beside the run name — `Kasal` or `CrewAI`.
2. **The API** returns it: `harness` on `GET /executions/{id}`,
   `/executions/{id}/status` and the run list.
3. **The log**, first thing a spawned interpreter prints:

   ```text
   [harness] this interpreter runs on crewai (from the run's payload)
   ```

   The parenthesis names the source — the run's payload, the environment, or the
   default — so a stale environment variable is visible rather than inferred.

---

## Where the two differ

Both harnesses implement the same declared capabilities, with two exceptions.
The list is in `services/execution/harnesses/binding.py` (`Capability`) and each
binding declares its own, so the difference is a fact of the code rather than a
note in a document.

| Capability | Kasal | CrewAI | Why |
| --- | --- | --- | --- |
| Checkpoint resume | yes | yes | |
| Tool approval (HITL) | yes | yes | |
| Tool replay | yes | yes | |
| Memory context / output sinks | yes | yes | |
| Run deadline | yes | yes | |
| Hierarchical process | yes | yes | |
| Flows | yes | yes | |
| **Agent plan** (`todo`) | yes | **no** | Written for Kasal's executor — one tool call per round, the plan carried in the conversation that executor owns. CrewAI's agent executor plans its own way; given both, one run called `todo` 28 times without writing a single item |
| **Export** | yes | **no** | An exported Databricks App vendors the Kasal runtime so it needs no third-party framework. Shipping CrewAI into exported apps is a separate project |

### Behavioural differences that are not capabilities

* **The tool-round ceiling.** `MAX_TOOL_ROUNDS` (15, or the agent's `max_iter`)
  lives in Kasal's transport tool loop. Under CrewAI that loop is bypassed —
  `delegate_tool_calls` hands it to CrewAI's executor — so CrewAI's own
  `max_iter` governs instead. The same crew can therefore get a different
  budget depending on the harness.
* **Structured output.** `output_pydantic` / `output_json` pass through to
  CrewAI's own converter rather than Kasal's schema gate. Both produce a typed
  `.pydantic` on the crew output — so router conditions on nested schemas work
  on either — but they differ in what happens when the model returns something
  malformed, and in how many retries that costs.
* **Retries after a failed turn.** Kasal's executor will not replay a turn that
  already ran tools, because their side effects are not idempotent. CrewAI's
  executor has its own retry behaviour.

---

## For developers

The binding is one interface: `HarnessBinding` in
`services/execution/harnesses/binding.py` — `build_agent`, `build_task`,
`build_crew`, `build_llm`, `adapt_tools`, `guardrail`, `process`, `crew_memory`,
`wire_memory`, `event_bridge`, `capabilities`.

Rules worth knowing before changing anything here:

* **Never construct an agent, task or crew directly.** Go through
  `active_harness().build_*`. The kernel is the single place both the crew and
  flow paths construct from, which is what keeps the two harnesses comparable.
* **Do not bridge an event a Kasal subsystem already emits.** LLM calls, tools,
  memory and guardrails reach the bus under both harnesses; bridging CrewAI's
  copies would double every row. `_SOURCED_FROM_KASAL` in
  `harnesses/crewai/events.py` records which those are, and a test cross-checks
  it so a gap reads as an omission rather than a decision.
* **CrewAI's flow events are not flows here.** In CrewAI 1.15 the agent executor
  *is* a `Flow`, so every agent turn emits `FlowStartedEvent`. Those are
  deliberately not bridged: `flow_started` opens the outermost causality scope,
  and bridging them re-rooted every trace after each agent turn.
* **Capabilities are declared, not discovered.** If a harness cannot do
  something, remove it from that binding's set — do not special-case the call
  site. The API and the UI read the same declaration.

Open items are tracked in [the dual-harness backlog](./dual-harness-backlog.md).

---

## Related

- [Architecture guide](./ARCHITECTURE_GUIDE.md)
- [Code structure guide](./CODE_STRUCTURE_GUIDE.md)
- [Checkpointing](./CHECKPOINTING.md)
- [Flows](./flows.md)
