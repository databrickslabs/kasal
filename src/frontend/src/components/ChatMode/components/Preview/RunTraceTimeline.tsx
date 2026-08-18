/**
 * A run's activity, rendered as the Execution Trace Timeline is: crew → agent →
 * task → the exact steps that ran.
 *
 * The chat previously narrated each step in the first person ("I'm working
 * through this step") over its own step derivation. That reading was invented
 * per step, so an event its labeller did not know fell through to the raw JSON
 * frame — which is what showed up in the panel. This renders the same
 * `ProcessedTraces` the Jobs timeline does, so the rows are the events, named
 * once, in one place.
 *
 * Only the presentation is local: ChatMode is Tailwind + CSS variables, and the
 * Jobs timeline is MUI. The model, the labels and the durations are shared.
 */
import React, { useState } from 'react';
import {
  ChevronDown,
  ChevronRight,
  CheckCircle2,
  Clock,
  Database,
  History,
  PlayCircle,
  Save,
  Terminal,
  Sparkles,
  AlertCircle,
} from 'lucide-react';
import { isEventClickable } from '../../../Jobs/traceEventProcessors';
import {
  formatTraceDuration,
  formatTraceOffset,
  parseTraceTime,
} from '../../../../hooks/global/useTraceData';
import type {
  GroupedTrace,
  ProcessedTraces,
  TraceEvent,
} from '../../../../types/execution/trace';

/** Rows below this are noise in a duration column — the timeline leaves them blank. */
const MIN_ROW_DURATION_MS = 50;

/** Durations and offsets are formatted by the SAME helpers the trace dialog
 *  uses — two surfaces showing one run must not disagree about how long a step
 *  took, or about where the "<1 ms" threshold sits. */
function duration(ms?: number): string {
  return ms == null ? '' : formatTraceDuration(ms);
}

function offsetFrom(start: Date | undefined, at: Date): string {
  return start ? formatTraceOffset(start, at) : '';
}

function clockTime(at: Date): string {
  return Number.isNaN(at.getTime()) ? '' : at.toLocaleTimeString();
}

/**
 * The event's icon, by the same families the trace dialog groups on — tools,
 * LLM calls, memory, completion, checkpoints — drawn with ChatMode's icon set.
 */
function EventIcon({ type }: { type: string }): JSX.Element {
  const common = { size: 13, className: 'flex-shrink-0' } as const;
  if (type.startsWith('memory') || type === 'knowledge_operation') {
    return <Database {...common} style={{ color: 'var(--accent)' }} />;
  }
  if (type.includes('failed') || type.includes('error')) {
    return <AlertCircle {...common} style={{ color: 'var(--error, #dc2626)' }} />;
  }
  if (type.startsWith('llm')) {
    return <PlayCircle {...common} style={{ color: 'var(--accent)' }} />;
  }
  if (type.includes('checkpoint_restored')) {
    return <History {...common} style={{ color: 'var(--text-secondary)' }} />;
  }
  if (type.includes('checkpoint')) {
    return <Save {...common} style={{ color: 'var(--text-secondary)' }} />;
  }
  if (type.includes('complete') || type === 'completed') {
    return <CheckCircle2 {...common} style={{ color: 'var(--success, #16a34a)' }} />;
  }
  if (type.includes('plan')) {
    return <Sparkles {...common} style={{ color: 'var(--text-secondary)' }} />;
  }
  return <Terminal {...common} style={{ color: 'var(--text-secondary)' }} />;
}

interface EventRowProps {
  event: TraceEvent;
  onSelect?: (event: TraceEvent) => void;
}

const EventRow: React.FC<EventRowProps> = ({ event, onSelect }) => {
  const clickable = Boolean(onSelect) && isEventClickable(event.type, !!event.output);
  const shown =
    event.duration != null && event.duration >= MIN_ROW_DURATION_MS
      ? duration(event.duration)
      : '';
  return (
    <div
      role={clickable ? 'button' : undefined}
      tabIndex={clickable ? 0 : undefined}
      onClick={clickable ? () => onSelect?.(event) : undefined}
      onKeyDown={
        clickable
          ? (e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                onSelect?.(event);
              }
            }
          : undefined
      }
      className={`flex items-center gap-2 py-1 pl-3 text-xs ${clickable ? 'cursor-pointer hover:opacity-80' : ''}`}
      style={{ borderLeft: '2px solid var(--border-color)' }}
    >
      <EventIcon type={event.type} />
      <span
        className="flex-1 min-w-0 truncate"
        style={{ color: clickable ? 'var(--accent)' : 'var(--text-primary)' }}
        title={event.description}
      >
        {event.description}
      </span>
      <span className="tabular-nums flex-shrink-0" style={{ color: 'var(--text-secondary)' }}>
        {shown}
      </span>
      <ChevronRight
        size={12}
        className="flex-shrink-0"
        style={{ color: 'var(--text-secondary)', visibility: clickable ? 'visible' : 'hidden' }}
      />
    </div>
  );
};

interface AgentBlockProps {
  agent: GroupedTrace;
  agentIdx: number;
  globalStart?: Date;
  onSelectEvent?: (event: TraceEvent) => void;
}

const AgentBlock: React.FC<AgentBlockProps> = ({ agent, agentIdx, globalStart, onSelectEvent }) => {
  const [open, setOpen] = useState(true);
  // Keyed by index within this agent: a run can repeat a task name, and the
  // name alone would collapse both rows together.
  const [closedTasks, setClosedTasks] = useState<Set<number>>(() => new Set());
  const taskCount = agent.tasks.filter((t) => !t.unassigned).length;

  return (
    <div className="mb-2">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-left text-xs"
        style={{ backgroundColor: 'var(--bg-secondary)' }}
      >
        {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <span className="font-medium truncate" style={{ color: 'var(--text-primary)' }}>
          {agent.agent}
        </span>
        <span
          className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-full flex-shrink-0"
          style={{ border: '1px solid var(--border-color)', color: 'var(--text-secondary)' }}
        >
          <Clock size={10} />
          {duration(agent.duration)}
        </span>
        {taskCount > 0 && (
          <span className="ml-auto flex-shrink-0" style={{ color: 'var(--text-secondary)' }}>
            {taskCount} {taskCount === 1 ? 'task' : 'tasks'}
          </span>
        )}
      </button>

      {open && (
        <div className="pl-4 mt-1">
          {agent.tasks.map((task, taskIdx) => {
            const taskOpen = !closedTasks.has(taskIdx);
            return (
              <div key={`${agentIdx}-${taskIdx}`} className="mb-1">
                {/* The task is its NAME only — the full description belongs in
                    the step content, not in a row that has to stay scannable. */}
                <button
                  type="button"
                  onClick={() =>
                    setClosedTasks((prev) => {
                      const next = new Set(prev);
                      if (next.has(taskIdx)) next.delete(taskIdx);
                      else next.add(taskIdx);
                      return next;
                    })
                  }
                  className="w-full flex items-center gap-2 px-2 py-1 rounded-md text-left text-xs"
                  title={task.fullDescription || task.taskName}
                >
                  {taskOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                  <span className="truncate" style={{ color: 'var(--text-primary)' }}>
                    {task.taskName}
                  </span>
                  <span
                    className="px-1.5 py-0.5 rounded-full flex-shrink-0"
                    style={{ border: '1px solid var(--border-color)', color: 'var(--text-secondary)' }}
                  >
                    {duration(task.duration)}
                  </span>
                  {globalStart && !task.unassigned && (
                    <span className="flex-shrink-0" style={{ color: 'var(--text-secondary)' }}>
                      starts {offsetFrom(globalStart, task.startTime)}
                    </span>
                  )}
                </button>
                {taskOpen && (
                  <div className="pl-4 mt-0.5">
                    {task.events.map((event, eventIdx) => (
                      <EventRow key={eventIdx} event={event} onSelect={onSelectEvent} />
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

/** A run-level banner (crew started/completed/restored, a checkpoint write). */
const SpineRow: React.FC<{ label: string; detail?: string; at?: Date; icon: JSX.Element }> = ({
  label,
  detail,
  at,
  icon,
}) => (
  <div className="flex items-center gap-2 py-1 text-xs">
    {icon}
    <span className="uppercase tracking-wide" style={{ color: 'var(--text-secondary)' }}>
      {label}
    </span>
    {detail && (
      <span className="font-medium truncate" style={{ color: 'var(--text-primary)' }}>
        {detail}
      </span>
    )}
    {at && <span style={{ color: 'var(--text-secondary)' }}>{clockTime(at)}</span>}
  </div>
);

export interface RunTraceTimelineProps {
  processed: ProcessedTraces | null;
  loading?: boolean;
  /** Open a step's content. Rows the trace model marks as having no output are
   *  inert, exactly as they are in the trace dialog. */
  onSelectEvent?: (event: TraceEvent) => void;
  /** True while the run is still going — an empty timeline then means the first
   *  rows have not been written yet, not that nothing happened. */
  live?: boolean;
}

const RunTraceTimeline: React.FC<RunTraceTimelineProps> = ({ processed, loading, onSelectEvent, live }) => {
  const items = processed?.timelineItems ?? [];

  if (loading && !processed) {
    return (
      <div className="text-xs py-2" style={{ color: 'var(--text-secondary)' }}>
        Loading run activity…
      </div>
    );
  }
  if (!processed || items.length === 0) {
    return (
      <div className="text-xs py-2" style={{ color: 'var(--text-secondary)' }}>
        {live ? 'Getting started…' : 'No activity recorded for this run.'}
      </div>
    );
  }

  return (
    <div>
      {items.map((item, idx) => {
        switch (item.kind) {
          case 'crew-start':
            return (
              <SpineRow
                key={`crew-start-${idx}`}
                label="Crew started"
                detail={item.crewName}
                at={parseTraceTime(item.trace.created_at)}
                icon={<PlayCircle size={13} style={{ color: 'var(--accent)' }} />}
              />
            );
          case 'crew-end':
            return (
              <SpineRow
                key={`crew-end-${idx}`}
                label="Crew completed"
                at={parseTraceTime(item.trace.created_at)}
                icon={<CheckCircle2 size={13} style={{ color: 'var(--success, #16a34a)' }} />}
              />
            );
          case 'crew-restored':
            return (
              <SpineRow
                key={`crew-restored-${idx}`}
                label="Restored from checkpoint"
                detail={item.crewName}
                at={parseTraceTime(item.trace.created_at)}
                icon={<History size={13} style={{ color: 'var(--text-secondary)' }} />}
              />
            );
          case 'checkpoint-saved':
            return (
              <SpineRow
                key={`checkpoint-${idx}`}
                label={item.failed ? 'Checkpoint failed' : 'Checkpoint saved'}
                detail={item.unit}
                at={parseTraceTime(item.trace.created_at)}
                icon={<Save size={13} style={{ color: 'var(--text-secondary)' }} />}
              />
            );
          case 'agent': {
            const agent = processed.agents[item.agentIdx];
            if (!agent) return null;
            return (
              <div key={`agent-${item.agentIdx}`} className={item.nested ? 'pl-3' : ''}>
                <AgentBlock
                  agent={agent}
                  agentIdx={item.agentIdx}
                  globalStart={processed.globalStart}
                  onSelectEvent={onSelectEvent}
                />
              </div>
            );
          }
          default:
            return null;
        }
      })}
    </div>
  );
};

export default RunTraceTimeline;
