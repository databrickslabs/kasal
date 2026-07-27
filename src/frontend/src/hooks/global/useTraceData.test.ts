/**
 * Tests for processTraces task grouping — specifically the task-less light/chat
 * agent path (Agent.kickoff_async), whose traces carry no crew task id and must
 * NOT be surfaced as an "Unassigned" task in the Execution Trace Timeline.
 */
import { describe, it, expect } from 'vitest';
import { processTraces } from './useTraceData';
import { Trace } from '../../types/execution/trace';

let _id = 0;
const makeTrace = (overrides: Partial<Trace>): Trace => ({
  id: ++_id,
  event_source: 'Assistant',
  event_context: 'chat',
  event_type: 'response_run',
  output: { tool_name: 'Response', content: 'done' },
  created_at: new Date(2024, 0, 1, 0, 0, _id).toISOString(),
  ...overrides,
});

describe('processTraces — light/chat agent (task-less run)', () => {
  it('relabels the synthetic "Unassigned" bucket to the user request and flags it', () => {
    const traces: Trace[] = [
      makeTrace({ event_type: 'tool_usage', event_context: 'Top Swiss news today',
                  output: { tool_name: 'PerplexitySearch' } }),
      makeTrace({ event_type: 'perplexitysearch_run', event_context: 'Top Swiss news today',
                  output: { tool_name: 'PerplexitySearch', content: '...' } }),
      makeTrace({ event_type: 'response_run', event_context: 'Top Swiss news today',
                  output: { tool_name: 'Response', content: 'answer' } }),
    ];

    const result = processTraces(traces);

    expect(result.agents).toHaveLength(1);
    const agent = result.agents[0];
    expect(agent.agent).toBe('Assistant');
    expect(agent.tasks).toHaveLength(1);
    const task = agent.tasks[0];
    // Not framed as the literal "Unassigned" crew task...
    expect(task.taskName).not.toBe('Unassigned');
    // ...but as the user's request, and flagged so the UI drops the task chrome.
    expect(task.taskName).toBe('Top Swiss news today');
    expect(task.unassigned).toBe(true);
  });

  it('falls back to the agent name when no request context is present', () => {
    const traces: Trace[] = [
      makeTrace({ event_source: 'Helper', event_type: 'response_run',
                  // event_context === event_type → treated as "no context"
                  event_context: 'response_run' }),
    ];

    const result = processTraces(traces);
    const task = result.agents[0].tasks[0];
    expect(task.taskName).toBe('Helper');
    expect(task.unassigned).toBe(true);
  });
});

describe('processTraces — additive row durations', () => {
  it('row durations sum to the task span, even across filtered raw traces', () => {
    const t0 = new Date('2024-01-01T00:00:00.000Z').getTime();
    const at = (ms: number) => new Date(t0 + ms).toISOString();

    const traces: Trace[] = [
      makeTrace({ event_source: 'Worker', event_type: 'task_started', task_id: 'T1',
                  event_context: 'Do the work', created_at: at(0) }),
      makeTrace({ event_source: 'Worker', event_type: 'memory_retrieval', task_id: 'T1',
                  trace_metadata: { query_time_ms: 601, results_count: 6 },
                  output: { content: 'mem' }, created_at: at(100) }),
      makeTrace({ event_source: 'Worker', event_type: 'llm_call', task_id: 'T1',
                  trace_metadata: { model: 'm' }, created_at: at(500) }),
      makeTrace({ event_source: 'Worker', event_type: 'llm_response', task_id: 'T1',
                  output: { content: 'answer' }, created_at: at(6000) }),
      // Filtered raw trace (guardrail_started → null): its wall time must be
      // donated to the preceding visible row (the LLM Response), not vanish.
      makeTrace({ event_source: 'Worker', event_type: 'guardrail_started', task_id: 'T1',
                  created_at: at(6300) }),
      makeTrace({ event_source: 'Worker', event_type: 'llm_guardrail', task_id: 'T1',
                  trace_metadata: { success: true }, output: { content: 'ok' },
                  created_at: at(16000) }),
      makeTrace({ event_source: 'Worker', event_type: 'task_completed', task_id: 'T1',
                  event_context: 'Do the work', output: { content: 'done' },
                  created_at: at(17000) }),
    ];

    const result = processTraces(traces);
    const task = result.agents[0].tasks[0];

    // Task header duration = first-event → last-event span.
    expect(task.duration).toBe(17000);

    // Additivity: sum of the visible rows' wall slices equals the task span
    // (sub-50ms suppressed-row slack allowed, well within 200ms here).
    const sum = task.events.reduce((acc, e) => acc + (e.duration ?? 0), 0);
    expect(Math.abs(sum - task.duration)).toBeLessThanOrEqual(200);

    // The LLM Response row carries its TRUE gap to the next visible row
    // (response → guardrail), including the filtered guardrail_started slice.
    const response = task.events.find(e => e.type === 'llm_response');
    expect(response!.duration).toBe(10000);

    // Intrinsic op time is detail (intrinsicMs), not the column value: the
    // memory row's duration is its wall slice, not query_time_ms.
    const memory = task.events.find(e => e.type === 'memory_retrieval');
    expect(memory!.duration).toBe(400);
    expect(memory!.intrinsicMs).toBe(601);

    // Last row closes the span (task end − its own timestamp).
    const last = task.events[task.events.length - 1];
    expect(last.duration).toBe(0);
  });
});

describe('processTraces — crew spine (execution DAG)', () => {
  const t0 = new Date('2024-01-01T00:00:00.000Z').getTime();
  const at = (ms: number) => new Date(t0 + ms).toISOString();

  /**
   * A flow run: flow_started is the root; each crew kickoff is its child (via
   * parent_span_id, which the backend derives from the bus's parent_event_id);
   * each agent's work descends from its own crew.
   */
  const flowRun = (): Trace[] => [
    makeTrace({ event_source: 'flow', event_type: 'flow_started', event_context: 'flow',
                span_id: 'FLOW', created_at: at(0) }),
    makeTrace({ event_source: 'crew', event_type: 'crew_started', event_context: 'crew',
                span_id: 'C1', parent_span_id: 'FLOW',
                trace_metadata: { crew_name: 'company on cnn' }, created_at: at(100) }),
    makeTrace({ event_source: 'AgentOne', event_type: 'task_started', task_id: 'T1',
                event_context: 'First task', span_id: 'T1S', parent_span_id: 'C1',
                created_at: at(200) }),
    makeTrace({ event_source: 'AgentOne', event_type: 'task_completed', task_id: 'T1',
                event_context: 'First task', span_id: 'T1E', parent_span_id: 'C1',
                output: { content: 'one' }, created_at: at(300) }),
    makeTrace({ event_source: 'crew', event_type: 'crew_completed', event_context: 'crew',
                span_id: 'C1E', parent_span_id: 'FLOW', created_at: at(400) }),
    makeTrace({ event_source: 'crew', event_type: 'crew_started', event_context: 'crew',
                span_id: 'C2', parent_span_id: 'FLOW',
                trace_metadata: { crew_name: 'gather company info' }, created_at: at(500) }),
    makeTrace({ event_source: 'AgentTwo', event_type: 'task_started', task_id: 'T2',
                event_context: 'Second task', span_id: 'T2S', parent_span_id: 'C2',
                created_at: at(600) }),
    makeTrace({ event_source: 'AgentTwo', event_type: 'task_completed', task_id: 'T2',
                event_context: 'Second task', span_id: 'T2E', parent_span_id: 'C2',
                output: { content: 'two' }, created_at: at(700) }),
    makeTrace({ event_source: 'crew', event_type: 'crew_completed', event_context: 'crew',
                span_id: 'C2E', parent_span_id: 'FLOW', created_at: at(800) }),
    makeTrace({ event_source: 'flow', event_type: 'flow_completed', event_context: 'flow',
                span_id: 'FLOWE', parent_span_id: 'FLOW', created_at: at(900) }),
  ];

  it('puts only flow events at the top level — crew starts are no longer stacked there', () => {
    const result = processTraces(flowRun());
    expect(result.globalEvents.start.map(t => t.event_type)).toEqual(['flow_started']);
    expect(result.globalEvents.end.map(t => t.event_type)).toEqual(['flow_completed']);
  });

  it('shows ONE flow row per run even when a HITL pause/resume kicks off twice', () => {
    // A HITL gate pauses the flow; checkpoint-resume rebuilds and kicks it off
    // again, so one job legitimately emits flow_started more than once.
    const traces = flowRun();
    traces.push(
      makeTrace({ event_source: 'flow', event_type: 'flow_started', event_context: 'flow',
                  span_id: 'FLOW2', created_at: at(1000) }),
      makeTrace({ event_source: 'flow', event_type: 'flow_completed', event_context: 'flow',
                  span_id: 'FLOW2E', parent_span_id: 'FLOW2', created_at: at(1100) }),
    );

    const result = processTraces(traces);
    expect(result.globalEvents.start).toHaveLength(1);
    expect(result.globalEvents.end).toHaveLength(1);
    // First start, last completion — the full extent of the run.
    expect(result.globalEvents.start[0].span_id).toBe('FLOW');
    expect(result.globalEvents.end[0].span_id).toBe('FLOW2E');
  });

  it('builds one section per crew, each owning its own agent, paired with its completion', () => {
    const result = processTraces(flowRun());

    expect(result.crewSections).toHaveLength(2);
    const [s1, s2] = result.crewSections;

    expect(s1.crewName).toBe('company on cnn');
    expect(s2.crewName).toBe('gather company info');
    expect(s1.end?.span_id).toBe('C1E');
    expect(s2.end?.span_id).toBe('C2E');

    const namesOf = (idxs: number[]) => idxs.map(i => result.agents[i].agent);
    expect(namesOf(s1.agentIdxs)).toEqual(['AgentOne']);
    expect(namesOf(s2.agentIdxs)).toEqual(['AgentTwo']);
  });

  it('attributes an agent by span ancestry, not by timestamp order', () => {
    // AgentThree's traces are ALL timestamped inside crew 1's window (before
    // crew 2 even starts), but they descend from crew 2's span. Ancestry must
    // win: a pure chronological rule would file this agent under crew 1.
    const traces = flowRun();
    traces.push(
      makeTrace({ event_source: 'AgentThree', event_type: 'task_started', task_id: 'T3',
                  event_context: 'Late-parented', span_id: 'T3S', parent_span_id: 'C2',
                  created_at: at(150) }),
      makeTrace({ event_source: 'AgentThree', event_type: 'task_completed', task_id: 'T3',
                  event_context: 'Late-parented', span_id: 'T3E', parent_span_id: 'C2',
                  output: { content: 'three' }, created_at: at(160) }),
    );

    const result = processTraces(traces);
    const owner = result.crewSections.findIndex(s =>
      s.agentIdxs.some(i => result.agents[i].agent === 'AgentThree')
    );
    expect(owner).toBe(1);
  });

  it('flattens to render order: crew banner, its agents, its completion', () => {
    const result = processTraces(flowRun());
    const shape = result.timelineItems.map(i =>
      i.kind === 'agent' ? `agent:${result.agents[i.agentIdx].agent}` : i.kind
    );
    expect(shape).toEqual([
      'crew-start', 'agent:AgentOne', 'crew-end',
      'crew-start', 'agent:AgentTwo', 'crew-end',
    ]);
    // Agents inside a crew are indented under its banner.
    expect(result.timelineItems.filter(i => i.kind === 'agent')
      .every(i => i.kind === 'agent' && i.nested)).toBe(true);
  });

  it('never drops an agent group that resolves to no crew', () => {
    const traces = flowRun();
    // An orphan with no span linkage at all, before any crew started.
    traces.push(makeTrace({ event_source: 'Orphan', event_type: 'response_run',
                            event_context: 'stray', created_at: at(50) }));
    const result = processTraces(traces);
    const placed = result.crewSections.flatMap(s => s.agentIdxs);
    expect(new Set(placed).size).toBe(result.agents.length);
    expect(result.agents.map(a => a.agent)).toContain('Orphan');
  });

  it('keeps a crew boundary out of the agent rows even when stamped with an agent', () => {
    // Real recorded shape: the bridge leaked the ambient event context onto
    // CrewKickoffCompletedEvent, so crew_completed landed with
    // event_source=<agent role> AND a task_id. Keyed off event_source it fell
    // through into the agent's task and rendered "Crew Completed" as a row
    // mid-task, before the trailing memory-save LLM calls.
    const traces: Trace[] = [
      makeTrace({ event_source: 'crew', event_type: 'crew_started', event_context: 'crew',
                  span_id: 'C1', created_at: at(0) }),
      makeTrace({ event_source: 'AgentOne', event_type: 'task_started', task_id: 'T1',
                  event_context: 'Do it', span_id: 'T1S', parent_span_id: 'C1',
                  created_at: at(100) }),
      makeTrace({ event_source: 'AgentOne', event_type: 'task_completed', task_id: 'T1',
                  event_context: 'Do it', span_id: 'T1E', parent_span_id: 'C1',
                  output: { content: 'done' }, created_at: at(200) }),
      // The mis-attributed boundary.
      makeTrace({ event_source: 'AgentOne', event_type: 'crew_completed', task_id: 'T1',
                  event_context: 'Do it', span_id: 'C1E', parent_span_id: 'C1',
                  created_at: at(300) }),
      // Post-crew memory-save LLM calls that used to appear AFTER "Crew Completed".
      makeTrace({ event_source: 'AgentOne', event_type: 'llm_call', task_id: 'T1',
                  span_id: 'L1', parent_span_id: 'T1S', created_at: at(400) }),
      makeTrace({ event_source: 'AgentOne', event_type: 'memory_write', task_id: 'T1',
                  output: { content: 'saved' }, span_id: 'M1', parent_span_id: 'T1S',
                  created_at: at(500) }),
    ];

    const result = processTraces(traces);

    // It is the section footer, not an agent row.
    expect(result.crewSections).toHaveLength(1);
    expect(result.crewSections[0].end?.span_id).toBe('C1E');

    const rowTypes = result.agents.flatMap(a => a.tasks.flatMap(t => t.events.map(e => e.type)));
    expect(rowTypes).not.toContain('crew_completed');
    expect(rowTypes).not.toContain('crew_started');

    // And the spine footer renders last, after everything the crew did.
    const shape = result.timelineItems.map(i => i.kind);
    expect(shape[shape.length - 1]).toBe('crew-end');
  });

  it('drops the "Starting: <task>" row that duplicated the task header', () => {
    const result = processTraces([
      makeTrace({ event_source: 'Worker', event_type: 'task_started', task_id: 'T1',
                  event_context: 'Review CNN.com', created_at: at(0) }),
      makeTrace({ event_source: 'Worker', event_type: 'llm_call', task_id: 'T1',
                  created_at: at(100) }),
      makeTrace({ event_source: 'Worker', event_type: 'task_completed', task_id: 'T1',
                  event_context: 'Review CNN.com', output: { content: 'done' },
                  created_at: at(200) }),
    ]);

    const task = result.agents[0].tasks[0];
    // The task is still NAMED from the task_started trace...
    expect(task.taskName).toBe('Review CNN.com');
    // ...but no row repeats that name.
    expect(task.events.map(e => e.type)).not.toContain('task_start');
    expect(task.events.some(e => e.description.startsWith('Starting:'))).toBe(false);
    // The completion row survives (it carries the output) without echoing it.
    const done = task.events.find(e => e.type === 'task_complete');
    expect(done?.description).toBe('Task Completed');
  });

  it('a light/chat run with no crew events yields one headerless section', () => {
    const result = processTraces([
      makeTrace({ event_type: 'response_run', event_context: 'hi' }),
    ]);
    expect(result.crewSections).toHaveLength(1);
    expect(result.crewSections[0].start).toBeUndefined();
    expect(result.crewSections[0].agentIdxs).toEqual([0]);
    // No banner → the card is not indented, i.e. renders exactly as before.
    expect(result.timelineItems).toEqual([{ kind: 'agent', agentIdx: 0, nested: false }]);
  });

  it('a crew run with no flow events still gets its crew section', () => {
    const result = processTraces([
      makeTrace({ event_source: 'crew', event_type: 'crew_started', event_context: 'crew',
                  span_id: 'C1', created_at: at(0) }),
      makeTrace({ event_source: 'Worker', event_type: 'task_completed', task_id: 'T1',
                  event_context: 'Do it', span_id: 'W', parent_span_id: 'C1',
                  output: { content: 'r' }, created_at: at(100) }),
      makeTrace({ event_source: 'crew', event_type: 'crew_completed', event_context: 'crew',
                  span_id: 'C1E', created_at: at(200) }),
    ]);
    expect(result.globalEvents.start).toHaveLength(0);
    expect(result.crewSections).toHaveLength(1);
    expect(result.crewSections[0].start?.event_type).toBe('crew_started');
    expect(result.crewSections[0].end?.event_type).toBe('crew_completed');
    expect(result.agents[result.crewSections[0].agentIdxs[0]].agent).toBe('Worker');
  });
});

describe('processTraces — crew run (has task ids)', () => {
  it('keeps a stray no-task trace under "Unassigned" (not flagged)', () => {
    const traces: Trace[] = [
      // A real task trace → the run HAS task ids.
      makeTrace({ event_source: 'Worker', event_type: 'task_completed',
                  event_context: 'Do the work', task_id: 'T1',
                  output: { content: 'result' } }),
      // A stray trace with no task id under the same agent.
      makeTrace({ event_source: 'Worker', event_type: 'sometool_run',
                  event_context: 'stray activity',
                  output: { tool_name: 'SomeTool' } }),
    ];

    const result = processTraces(traces);
    const agent = result.agents.find(a => a.agent === 'Worker');
    expect(agent).toBeDefined();
    const stray = agent!.tasks.find(t => t.taskName === 'Unassigned');
    // Crew runs still get an "Unassigned" bucket for unattributed traces,
    // and it is NOT flagged (so the UI keeps the task framing there).
    expect(stray).toBeDefined();
    expect(stray!.unassigned).toBe(false);
  });
});

describe('processTraces — task descriptions are never truncated', () => {
  const longDescription =
    'Answer this question about the conversations you have read. ' +
    'Give only the specific detail asked for, in a few words. '.repeat(4);

  it('keeps the whole task name (the row renderer clips, the data does not)', () => {
    const result = processTraces([
      makeTrace({ event_source: 'Worker', event_type: 'task_started', task_id: 'T1',
                  event_context: longDescription }),
      makeTrace({ event_source: 'Worker', event_type: 'task_completed', task_id: 'T1',
                  event_context: longDescription, output: { content: 'done' } }),
    ]);

    const task = result.agents[0].tasks[0];
    expect(task.taskName).toBe(longDescription);
    expect(task.taskName).not.toContain('...');
  });

  it('carries the crew config description and the tasks-table id', () => {
    const result = processTraces([
      makeTrace({ event_source: 'crew', event_type: 'crew_started', event_context: 'crew',
                  span_id: 'C1',
                  trace_metadata: {
                    crew_agents: [{ role: 'Worker' }],
                    crew_tasks: [{ id: 'T1', description: longDescription }],
                  } }),
      makeTrace({ event_source: 'Worker', event_type: 'task_started',
                  // Mirrors production: the per-event copy is capped, the id on
                  // the trace is the engine's runtime uuid, and the tasks-table
                  // id rides along as frontend_task_id.
                  event_context: longDescription.slice(0, 80),
                  trace_metadata: { task_id: 'T1', frontend_task_id: 'db-task-1' } }),
    ]);

    const task = result.agents.find(a => a.agent === 'Worker')!.tasks[0];
    expect(task.fullDescription).toBe(longDescription);
    expect(task.configTaskId).toBe('db-task-1');
    expect(task.taskId).toBe('T1');
  });
});
