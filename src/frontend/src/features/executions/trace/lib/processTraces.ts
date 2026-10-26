import type { CrewSection, ProcessedTraces, RunConfig, RunConfigAgent, RunConfigTask, TimelineItem, Trace } from '../../../../types/execution/trace';
import { processTraceEvent, extractOutputForDisplay, extractExtraData } from './traceEventProcessors';
import { formatDurationMs } from '../../../../utils/formatDuration';

// Helper function to extract task_id from trace
const getTaskId = (trace: Trace): string | null => {
  if (trace.task_id) return trace.task_id;
  if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
    const metadata = trace.trace_metadata as Record<string, unknown>;
    if (metadata.task_id) return metadata.task_id as string;
  }
  if (trace.extra_data && typeof trace.extra_data === 'object') {
    const extraData = trace.extra_data as Record<string, unknown>;
    if (extraData.task_id) return extraData.task_id as string;
  }
  return null;
};

/**
 * Process raw traces into hierarchical structure for display.
 *
 * Exported for unit testing (the agent/task grouping, including the task-less
 * light-agent path, is non-trivial enough to test in isolation).
 */
export function processTraces(rawTraces: Trace[]): ProcessedTraces {
  const filteredTraces = rawTraces.filter(trace =>
    trace.event_source !== 'Task Orchestrator' &&
    trace.event_context !== 'task_management'
  );

  const timestamps = new Map(filteredTraces.map(trace =>
    [trace, parseTraceTime(trace.created_at).getTime()] as const
  ));
  const sorted = filteredTraces.sort((a, b) => timestamps.get(a)! - timestamps.get(b)!);

  if (sorted.length === 0) {
    return { agents: [], globalEvents: { start: [], end: [] }, crewSections: [], timelineItems: [] };
  }

  const globalStart = parseTraceTime(sorted[0].created_at);
  const globalEnd = parseTraceTime(sorted[sorted.length - 1].created_at);
  const totalDuration = globalEnd.getTime() - globalStart.getTime();

  // Flow-level rows only. Crew starts/completions are NOT global — they belong to
  // their own section of the spine (see crewSections below). Rendering them here
  // is what produced N stacked "CREW STARTED" banners with no run structure.
  //
  // Collapsed to a single row each: one JOB can kick the flow off more than once
  // — a HITL gate pauses the flow, then checkpoint-resume rebuilds and kicks it
  // off again, so the run legitimately emits several flow_started events. The
  // timeline shows one run, so it shows the FIRST start and the LAST completion.
  //
  // Classified by event_type ALONE, never by event_source. A crew/flow boundary
  // is run-level by definition, but the backend used to stamp it with whatever
  // agent/task was last active (the ambient event context leaked onto it), so
  // rows exist with event_source=<agent role>. Keying off event_source let those
  // fall through into the agent's task and render "Crew Completed" as an event
  // row mid-task. Type is the reliable signal, and it also fixes runs already
  // recorded that way.
  const isFlowStart = (t: Trace) => t.event_type === 'flow_started';
  const isFlowEnd = (t: Trace) => t.event_type === 'flow_completed';
  const isCrewStart = (t: Trace) =>
    t.event_type === 'crew_started' || t.event_type === 'execution_started';
  const isCrewEnd = (t: Trace) =>
    t.event_type === 'crew_completed' || t.event_type === 'execution_completed';
  // Work a resume RESTORED rather than executed — a CREW in a flow, a TASK in
  // a crew. Run-level like the crew boundaries: it belongs to the spine, not
  // inside an agent's task, and would otherwise be dropped by the agent pass.
  const isCrewRestored = (t: Trace) =>
    t.event_type === 'crew_checkpoint_restored' ||
    t.event_type === 'task_checkpoint_restored';
  // A checkpoint WRITE. Run-level for the same reason as a restore: it belongs
  // to the flow's spine, not inside an agent's task, and the agent pass would
  // otherwise drop it.
  const isCheckpointSaved = (t: Trace) =>
    t.event_type === 'flow_checkpoint_saved' || t.event_type === 'checkpoint_unit_saved';
  const isRunLevel = (t: Trace) =>
    isFlowStart(t) || isFlowEnd(t) || isCrewStart(t) || isCrewEnd(t) ||
    isCrewRestored(t) || isCheckpointSaved(t);

  const flowStarts = sorted.filter(isFlowStart);
  const flowEnds = sorted.filter(isFlowEnd);
  const globalEvents = {
    start: flowStarts.length > 0 ? [flowStarts[0]] : [],
    end: flowEnds.length > 0 ? [flowEnds[flowEnds.length - 1]] : []
  };

  const crewStarts = sorted.filter(isCrewStart);
  const crewEnds = sorted.filter(isCrewEnd);
  const crewRestored = sorted.filter(isCrewRestored);
  // Collected, not just classified. `isRunLevel` already excluded these from
  // the agent pass; without this they were excluded from everything.
  const checkpointSaved = sorted.filter(isCheckpointSaved);

  // span_id -> parent_span_id, for walking the DAG up to an owning crew span.
  const spanToParent = new Map<string, string>();
  sorted.forEach(t => {
    if (t.span_id && t.parent_span_id) spanToParent.set(t.span_id, t.parent_span_id);
  });
  const crewSpanToIdx = new Map<string, number>();
  crewStarts.forEach((t, i) => { if (t.span_id) crewSpanToIdx.set(t.span_id, i); });

  /**
   * Which crew a trace belongs to. Walks span ancestry first (authoritative —
   * this is the engine's causality DAG). Falls back to "the most recent crew
   * that had started by then" for spans outside our bridge's hierarchy (e.g. the
   * openinference instrumentor's own spans, which we don't parent).
   */
  const resolvedSpans = new Map<string, number | undefined>();
  const crewStartTimes = crewStarts.map(trace => timestamps.get(trace)!);
  const resolveCrewIdx = (trace: Trace): number | undefined => {
    let sid: string | undefined = trace.span_id;
    const seen = new Set<string>();
    let crew: number | undefined;
    while (sid && !seen.has(sid)) {
      if (resolvedSpans.has(sid)) {
        crew = resolvedSpans.get(sid);
        break;
      }
      seen.add(sid);
      crew = crewSpanToIdx.get(sid);
      if (crew !== undefined) break;
      sid = spanToParent.get(sid);
    }
    seen.forEach(span => resolvedSpans.set(span, crew));
    if (crew !== undefined) return crew;
    if (crewStarts.length === 0) return undefined;
    // The fallback depends on the trace time, not just its span.
    const at = timestamps.get(trace)!;
    let low = 0;
    let high = crewStartTimes.length;
    while (low < high) {
      const mid = (low + high) >>> 1;
      if (crewStartTimes[mid] <= at) low = mid + 1;
      else high = mid;
    }
    return Math.max(0, low - 1);
  };

  // Group by agent
  const agentMap = new Map<string, Trace[]>();

  // OTel span hierarchy maps
  const spanIdToTaskId = new Map<string, string>();
  const spanIdToAgent = new Map<string, string>();
  const taskIdToName = new Map<string, string>();
  const taskIdToAgent = new Map<string, string>();
  // Runtime task id -> the task's id in the tasks table (stamped by the engine
  // as `frontend_task_id`). The task id on a trace is the ENGINE's per-run Task
  // uuid, which has no row in `tasks` — fetching /tasks/{that} is a guaranteed
  // 404, so only this id is ever safe to look up.
  const taskIdToConfigId = new Map<string, string>();
  // Runtime task id -> the FULL, untruncated description. Every per-event copy
  // is capped server-side (event_context at 500 chars, trace_metadata.task_name
  // at 200), so the complete text only exists in two places: the task's own
  // task_started span (`task_description_full`, written once per task) and the
  // crew config captured on the crew boundary (`crew_tasks[].description`).
  const taskIdToFullDescription = new Map<string, string>();

  const rememberFullDescription = (taskId: string, description: string) => {
    const known = taskIdToFullDescription.get(taskId);
    if (!known || description.length > known.length) {
      taskIdToFullDescription.set(taskId, description);
    }
  };

  sorted.forEach(trace => {
    const meta = trace.trace_metadata && typeof trace.trace_metadata === 'object'
      ? trace.trace_metadata as Record<string, unknown>
      : null;
    const own = meta?.task_description_full;
    const ownTaskId = getTaskId(trace);
    if (own && ownTaskId) {
      rememberFullDescription(ownTaskId, String(own));
    }

    const crewTasks = meta?.crew_tasks;
    if (!Array.isArray(crewTasks)) return;
    crewTasks.forEach(entry => {
      if (!entry || typeof entry !== 'object') return;
      const { id, description } = entry as { id?: unknown; description?: unknown };
      if (id && description) {
        rememberFullDescription(String(id), String(description));
      }
    });
  });

  // First pass: build span hierarchy and task name index
  sorted.forEach(trace => {
    // A mis-attributed crew boundary carries a task_id; indexing its span would
    // map the CREW span to a task and drag that task's children onto the crew.
    if (isRunLevel(trace)) return;

    const taskId = getTaskId(trace);
    const meta = trace.trace_metadata && typeof trace.trace_metadata === 'object'
      ? trace.trace_metadata as Record<string, unknown>
      : null;

    if (trace.span_id && taskId) {
      spanIdToTaskId.set(trace.span_id, taskId);
    }
    if (trace.span_id && trace.event_source && trace.event_source !== 'Unknown Agent') {
      spanIdToAgent.set(trace.span_id, trace.event_source);
    }

    if (taskId && !taskIdToName.has(taskId)) {
      // Longest wins: task_name is capped at 200 chars server-side while
      // event_context keeps 500, so neither is reliably the fuller copy.
      // Never truncated here — the row renderer clips for display, and the
      // description dialog needs whatever text we actually have.
      const candidates = [
        meta?.task_name as string | undefined,
        trace.event_type === 'task_started' ? trace.event_context : undefined,
      ].filter((v): v is string => !!v);
      const name = candidates.sort((a, b) => b.length - a.length)[0];
      if (name) {
        taskIdToName.set(taskId, name);
      }
    }

    if (taskId && !taskIdToConfigId.has(taskId)) {
      const configId = meta?.frontend_task_id;
      if (configId) {
        taskIdToConfigId.set(taskId, String(configId));
      }
    }

    if (taskId && !taskIdToAgent.has(taskId)) {
      const agent = trace.event_source;
      if (agent && agent !== 'Unknown Agent' && agent !== 'task' && agent !== 'crew') {
        taskIdToAgent.set(taskId, agent);
      }
    }
  });

  // Second pass: group traces by agent
  sorted.forEach(trace => {
    // Run-level boundaries belong to the spine and are never agent work — drop
    // them here whatever event_source they carry, so a mis-attributed
    // crew_completed cannot surface as a row inside an agent's task.
    if (isRunLevel(trace)) return;

    const isErrorEvent = trace.event_type?.includes('failed') || trace.event_type?.includes('error');
    const src = trace.event_source?.toLowerCase();
    if (!isErrorEvent && (
        src === 'crew' ||
        src === 'flow' ||
        src === 'task' ||
        src === 'system' ||
        trace.event_source === 'Task Orchestrator' ||
        trace.event_context === 'task_management')) {
      return;
    }

    let agent = trace.event_source || 'Unknown Agent';

    if (trace.parent_span_id && spanIdToAgent.has(trace.parent_span_id)) {
      agent = spanIdToAgent.get(trace.parent_span_id)!;
    }
    if (trace.span_id && agent !== 'Unknown Agent') {
      spanIdToAgent.set(trace.span_id, agent);
    }

    const traceTaskId = getTaskId(trace);
    if (traceTaskId && taskIdToAgent.has(traceTaskId)) {
      agent = taskIdToAgent.get(traceTaskId)!;
    }

    if ((trace.event_type === 'llm_call' || isErrorEvent) && trace.extra_data && typeof trace.extra_data === 'object') {
      const extraData = trace.extra_data as Record<string, unknown>;
      const agentRole = extraData.agent_role as string;
      if (agentRole && agentRole !== 'UnknownAgent-str' && agentRole !== 'Unknown Agent') {
        agent = agentRole;
      }
    }
    if (isErrorEvent && (agent === 'crew' || agent === 'task' || agent === 'system' || agent === 'Unknown Agent')) {
      const meta = trace.trace_metadata && typeof trace.trace_metadata === 'object'
        ? trace.trace_metadata as Record<string, unknown>
        : null;
      const metaRole = meta?.agent_role as string;
      if (metaRole && metaRole !== 'Unknown Agent') {
        agent = metaRole;
      }
    }

    if (!agentMap.has(agent)) {
      agentMap.set(agent, []);
    }
    agentMap.get(agent)!.push(trace);
  });

  // Process each agent's traces
  const agents: ProcessedTraces['agents'] = [];
  // Owning crew per agent group, parallel to `agents` (index-aligned).
  const agentCrewIdx: (number | undefined)[] = [];

  // A run with NO task ids anywhere is the single light/chat agent path
  // (Agent.kickoff_async) — there is no crew task, so its traces must not be
  // framed as an "Unassigned" task in the timeline. Crew runs always carry task
  // ids, so a stray unattributed trace there still shows under "Unassigned".
  const runHasTaskIds = sorted.some(t => !!getTaskId(t));

  agentMap.forEach((agentTraces, agentName) => {
    if (agentTraces.length === 0) return;

    const agentStart = parseTraceTime(agentTraces[0].created_at);
    const agentEnd = parseTraceTime(agentTraces[agentTraces.length - 1].created_at);

    const taskMap = new Map<string, Trace[]>();
    const taskIdToUniqueKey = new Map<string, string>();
    let taskCounter = 0;

    agentTraces.forEach(trace => {
      const traceTaskId = getTaskId(trace)
        || (trace.parent_span_id ? spanIdToTaskId.get(trace.parent_span_id) : undefined)
        || undefined;

      let taskKey = 'Unassigned';
      if (traceTaskId) {
        if (taskIdToUniqueKey.has(traceTaskId)) {
          taskKey = taskIdToUniqueKey.get(traceTaskId)!;
        } else {
          const baseName = taskIdToName.get(traceTaskId)
            || (trace.event_context && trace.event_context !== trace.event_type
                ? trace.event_context
                : 'Task');
          taskKey = taskMap.has(baseName) ? `${baseName} (${++taskCounter})` : baseName;
          taskIdToUniqueKey.set(traceTaskId, taskKey);
          if (!taskIdToName.has(traceTaskId)) {
            taskIdToName.set(traceTaskId, baseName);
          }
        }
      }

      if (!taskMap.has(taskKey)) {
        taskMap.set(taskKey, []);
      }
      taskMap.get(taskKey)!.push(trace);
    });

    const tasks = Array.from(taskMap.entries()).map(([taskName, taskTraces]) => {
      // The light/chat agent produces only the synthetic "Unassigned" bucket (no
      // crew task). Relabel it to the user's request (the trace's event_context)
      // — falling back to the agent name — and flag it so the UI drops the crew
      // "task" framing. Left as "Unassigned" for crew runs (runHasTaskIds), where
      // a stray bucket really is an unattributed task.
      const isUnassignedRun = taskName === 'Unassigned' && !runHasTaskIds;
      let displayName = taskName;
      if (isUnassignedRun) {
        const ctx = taskTraces.find(
          t => t.event_context && t.event_context !== t.event_type
        )?.event_context;
        const trimmedCtx = ctx?.trim();
        displayName = trimmedCtx || agentName;
      }
      const taskStart = parseTraceTime(taskTraces[0].created_at);
      const taskEnd = parseTraceTime(taskTraces[taskTraces.length - 1].created_at);

      // Exhaustive wall-time accounting: build the VISIBLE rows first, then
      // give each row the slice from its own timestamp to the NEXT visible
      // row's (raw traces filtered out by the processors donate their time to
      // the preceding visible row); the last row runs to the task end. By
      // construction the row durations sum to the task span — no time is
      // swallowed invisibly. Intrinsic op times (memory query/save, MCP call)
      // are carried separately as detail — never as the column value.
      const visibleEvents = taskTraces.map((trace) => {
        const timestamp = parseTraceTime(trace.created_at);
        const processed = processTraceEvent(trace);
        if (!processed) return null;

        return {
          type: processed.type,
          description: processed.description,
          timestamp,
          traceId: typeof trace.id === 'number' ? trace.id : undefined,
          intrinsicMs: processed.durationMs,
          output: extractOutputForDisplay(trace.output),
          extraData: extractExtraData(trace)
        };
      }).filter((event): event is NonNullable<typeof event> => event !== null);

      const collapsed = collapseAdjacentPlanRows(visibleEvents);

      const events = collapsed.map((event, idx) => ({
        ...event,
        duration: idx + 1 < collapsed.length
          ? collapsed[idx + 1].timestamp.getTime() - event.timestamp.getTime()
          : taskEnd.getTime() - event.timestamp.getTime(),
      }));

      const resolvedTaskId = getTaskId(taskTraces[0]) || undefined;

      return {
        taskName: displayName,
        taskId: resolvedTaskId,
        configTaskId: resolvedTaskId ? taskIdToConfigId.get(resolvedTaskId) : undefined,
        // Full text from the crew config when the run carried one; the capped
        // per-event copy otherwise.
        fullDescription: resolvedTaskId
          ? taskIdToFullDescription.get(resolvedTaskId)
          : undefined,
        startTime: taskStart,
        endTime: taskEnd,
        duration: taskEnd.getTime() - taskStart.getTime(),
        events,
        unassigned: isUnassignedRun
      };
    });

    agents.push({
      agent: agentName,
      startTime: agentStart,
      endTime: agentEnd,
      duration: agentEnd.getTime() - agentStart.getTime(),
      tasks
    });

    // Attribute the group to a crew by majority vote over its traces. A single
    // trace can be mis-parented (instrumentor spans we don't own); the bulk of a
    // group's traces reliably descend from one crew kickoff.
    const votes = new Map<number, number>();
    agentTraces.forEach(t => {
      const idx = resolveCrewIdx(t);
      if (idx !== undefined) votes.set(idx, (votes.get(idx) || 0) + 1);
    });
    let winner: number | undefined;
    let best = 0;
    votes.forEach((count, idx) => {
      if (count > best) { best = count; winner = idx; }
    });
    agentCrewIdx.push(winner);
  });

  // ── Build the crew spine ────────────────────────────────────────────────
  // Pair each crew start with the first not-yet-claimed completion after it.
  // Crews within a flow run sequentially, so order pairing is correct and
  // survives a crew that never completed (it simply has no footer).
  const crewSections: CrewSection[] = [];
  if (crewStarts.length === 0) {
    // Light/chat run, or a run whose crew events never made it to the trace:
    // one headerless section so the timeline renders exactly as before.
    crewSections.push({ agentIdxs: agents.map((_, i) => i) });
  } else {
    const agentsByCrew = new Map<number | undefined, number[]>();
    agentCrewIdx.forEach((crew, idx) => {
      const indices = agentsByCrew.get(crew) ?? [];
      indices.push(idx);
      agentsByCrew.set(crew, indices);
    });
    let nextEnd = 0;
    crewStarts.forEach((start, i) => {
      const startAt = timestamps.get(start)!;
      while (nextEnd < crewEnds.length && !(timestamps.get(crewEnds[nextEnd])! >= startAt)) {
        nextEnd++;
      }
      const end = crewEnds[nextEnd++];
      crewSections.push({
        crewName: start.trace_metadata && typeof start.trace_metadata === 'object'
          ? ((start.trace_metadata as Record<string, unknown>).crew_name as string | undefined)
          : undefined,
        start,
        end,
        agentIdxs: agentsByCrew.get(i) ?? [],
      });
    });
    // Never drop an agent group: anything unattributed goes to the last section
    // so the timeline stays a complete view of the run.
    const placed = new Set(crewSections.flatMap(s => s.agentIdxs));
    const orphans = agents.map((_, i) => i).filter(i => !placed.has(i));
    if (orphans.length > 0) {
      crewSections[crewSections.length - 1].agentIdxs.push(...orphans);
    }
  }

  // Flatten the spine into render order.
  const timelineItems: TimelineItem[] = [];

  // Restored crews first: a resume replays them before running anything, and
  // they are what makes the timeline read as the whole flow rather than only
  // the tail that re-executed.
  crewRestored.forEach(trace => {
    const meta = trace.trace_metadata && typeof trace.trace_metadata === 'object'
      ? trace.trace_metadata as Record<string, unknown>
      : null;
    timelineItems.push({
      kind: 'crew-restored',
      trace,
      // A crew run names the restored unit by TASK; a flow names it by CREW.
      crewName:
        (meta?.crew_name as string | undefined) ||
        (meta?.task_name as string | undefined) ||
        trace.event_source ||
        undefined,
    });
  });

  // Checkpoint writes close out the spine: they are bookkeeping ABOUT the run
  // rather than steps of it, and each carries its own timestamp so the order it
  // happened in stays legible.
  const asRecord = (v: unknown): Record<string, unknown> =>
    v && typeof v === 'object' ? (v as Record<string, unknown>) : {};
  const checkpointItems: TimelineItem[] = checkpointSaved.map((trace) => {
    // The JSON column arrives as a dict over SSE and as a STRING over the REST
    // polling fallback; both have to yield the same item.
    let raw: unknown = trace.output;
    if (typeof raw === 'string') {
      try { raw = JSON.parse(raw); } catch { raw = {}; }
    }
    const out = asRecord(raw);
    const extra = asRecord(out.extra_data);
    return {
      kind: 'checkpoint-saved' as const,
      trace,
      unit:
        (extra.unit_key as string | undefined) ||
        (extra.method_name as string | undefined) ||
        (extra.crew_name as string | undefined),
      failed: Boolean(extra.error),
    };
  });

  crewSections.forEach(section => {
    if (section.start) {
      timelineItems.push({ kind: 'crew-start', trace: section.start, crewName: section.crewName });
    }
    section.agentIdxs.forEach(agentIdx => {
      timelineItems.push({ kind: 'agent', agentIdx, nested: !!section.start });
    });
    if (section.end) {
      timelineItems.push({ kind: 'crew-end', trace: section.end });
    }
  });

  timelineItems.push(...checkpointItems);

  // Extract run configuration from crew_started/execution_started trace metadata
  let runConfig: RunConfig | undefined;
  for (const trace of sorted) {
    if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
      const meta = trace.trace_metadata as Record<string, unknown>;
      if (meta.crew_agents && Array.isArray(meta.crew_agents)) {
        runConfig = {
          crew_key: meta.crew_key as string | undefined,
          crew_id: meta.crew_id as string | undefined,
          crew_agents: meta.crew_agents as RunConfigAgent[],
          crew_tasks: (meta.crew_tasks || []) as RunConfigTask[],
          crew_inputs: meta.crew_inputs as Record<string, unknown> | undefined,
        };
        break;
      }
    }
  }

  return {
    globalStart,
    globalEnd,
    totalDuration,
    agents,
    globalEvents,
    crewSections,
    timelineItems,
    runConfig,
  };
}

/**
 * One plan update, one line.
 *
 * A single update writes up to three rows: the engine's own `plan_updated`
 * event and the `todo` tool call's start/finish pair. All three now describe
 * the plan, so without this the timeline shows the same "Plan — 2/5 done" three
 * times in a row and the actual work scrolls out of view.
 *
 * The LAST of a run wins: it reflects the state after the call, and it is the
 * one whose progress matches what happens next.
 */
function collapseAdjacentPlanRows<T extends { type: string }>(events: T[]): T[] {
  const out: T[] = [];
  for (const event of events) {
    const prev = out[out.length - 1];
    if (event.type === 'plan_updated' && prev?.type === 'plan_updated') {
      out[out.length - 1] = event;
      continue;
    }
    out.push(event);
  }
  return out;
}

export const formatTraceDuration = (ms: number): string => {
  // Offsets/chips deal in whole-ms timestamp deltas — show a literal zero
  // instead of the helper's "<1 ms" (which is meant for measured sub-ms times).
  if (ms <= 0) return '0 ms';
  return formatDurationMs(ms);
};

// Offset column: ONE format everywhere — seconds with one decimal ("+0.0s",
// "+7.0s", "+11.8s") so offsets line up and read as a single system.
export const formatTraceOffset = (start: Date, timestamp: Date): string => {
  const deltaSec = Math.max(0, timestamp.getTime() - start.getTime()) / 1000;
  return `+${deltaSec.toFixed(1)}s`;
};


/** Already carries a zone: trailing Z, or ±HH:MM after the time part. */
const HAS_TIMEZONE_RE = /(?:Z|[+-]\d{2}:?\d{2})$/;

/**
 * One clock for every trace row, whichever transport delivered it.
 *
 * The timeline merges two sources that disagree about timezones. Rows read from
 * the database carry NAIVE UTC ("2026-07-27T23:16:27.868") — and JavaScript
 * reads a date-time without an offset as LOCAL time. Rows delivered as live SSE
 * frames carry the engine's `datetime.now(timezone.utc)`, which serializes WITH
 * "+00:00" and is therefore read correctly.
 *
 * So the same event arrives twice, hours apart: at UTC+2 a run showed a
 * 120-minute span, a 119.8-minute gap between adjacent rows, and an order that
 * put "Task Completed" above the memory read that preceded it — the whole
 * timeline sorted into two clusters two hours apart.
 *
 * Treating a zoneless timestamp as UTC is correct for every producer here: the
 * database column is UTC, and the engine stamps UTC.
 */
export const parseTraceTime = (value: string | undefined | null): Date => {
  if (!value) return new Date(NaN);
  const iso = value.includes('T') ? value : value.replace(' ', 'T');
  return new Date(HAS_TIMEZONE_RE.test(iso) ? iso : `${iso}Z`);
};
