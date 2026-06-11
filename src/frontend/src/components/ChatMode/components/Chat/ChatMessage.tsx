import React, { useState } from 'react';
import { ChatMessage as ChatMessageType } from '../../types/chat';
import {
  CatalogListResult,
  CatalogLoadResult,
  FlowListResult,
  FlowLoadResult,
  GeneratedAgent,
  GeneratedTask,
} from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
import { GenerationCompleteData } from '../../hooks/useGenerationStream';
import { useAppStore } from '../../store/appStore';
import { isGenieToolRef, CrewNameConflictError } from '../../api/crews';
import { TraceDetail, findInlineTraceRenderer } from './traces';
import MessageContent from './MessageContent';
import AgentCard from '../Cards/AgentCard';
import TaskCard from '../Cards/TaskCard';
import CrewListCard from '../Cards/CrewListCard';
import FlowListCard from '../Cards/FlowListCard';
import CrewDetailCard from '../Cards/CrewDetailCard';
import FlowDetailCard from '../Cards/FlowDetailCard';
import HelpCard from '../Cards/HelpCard';
import GenieSpaceSelector from '../Cards/GenieSpaceSelector';
import { useSessionStore } from '../../store/sessionStore';
import InputVariablesPrompt from '../Cards/InputVariablesPrompt';
import GenieSpacePrompt from '../Cards/GenieSpacePrompt';
import CrewActionsBar from '../Cards/CrewActionsBar';
import { DetectedVariable } from '../../utils/variableDetector';

interface ChatMessageProps {
  message: ChatMessageType;
  onCommand?: (command: string) => void;
  onExecuteCrew?: (plan: PlanData) => void;
  onExecuteFlow?: (flow: FlowData) => void;
  onExecuteGenerated?: (data: GenerationCompleteData, spaceId?: string) => void;
  /** Save this generated crew's plan to the catalog. Resolves to the saved name. */
  onSaveCrew?: (data: GenerationCompleteData, opts?: { overwrite?: boolean; spaceId?: string }) => Promise<{ id: string; name: string }>;
  /** Run the parked execution with the user-provided {variable} inputs. */
  onSubmitVariables?: (messageId: string, inputs: Record<string, string>) => void;
}

/** Resolve a tool identifier (ID or name) to its display name */
function resolveToolName(tool: unknown, nameMap: Record<string, string>): string {
  const key = String(tool);
  return nameMap[key] || key;
}

const ChatMessageComponent: React.FC<ChatMessageProps> = ({
  message,
  onCommand,
  onExecuteCrew,
  onExecuteFlow,
  onExecuteGenerated,
  onSaveCrew,
  onSubmitVariables,
}) => {
  const isUser = message.role === 'user';
  const isSystem = message.role === 'system';

  const renderRichContent = () => {
    if (!message.resultType || !message.resultData) return null;

    switch (message.resultType) {
      case 'agent':
        return <AgentCard agent={message.resultData as GeneratedAgent} />;
      case 'task':
        return <TaskCard task={message.resultData as GeneratedTask} />;
      case 'catalog_list':
        return (
          <CrewListCard
            data={message.resultData as CatalogListResult}
            onCommand={onCommand}
          />
        );
      case 'catalog_load': {
        // Render a loaded crew with the SAME business-friendly card as a freshly
        // generated crew (agents + tasks with descriptions), not the technical
        // Process/Memory/RPM summary.
        const loadData = message.resultData as CatalogLoadResult;
        if (loadData.plan) {
          // No save bookmark + no Run button: a loaded crew is already in the
          // catalog, and it's run via the chat submit button.
          return (
            <GenerationCompleteCard
              data={planToGenerationData(loadData.plan)}
              messageId={message.id}
            />
          );
        }
        return <CrewDetailCard data={loadData} />;
      }
      case 'flow_list':
        return (
          <FlowListCard
            data={message.resultData as FlowListResult}
            onCommand={onCommand}
          />
        );
      case 'flow_load':
        return (
          <FlowDetailCard
            data={message.resultData as FlowLoadResult}
            onExecute={
              (message.resultData as FlowLoadResult).flow
                ? () =>
                    onExecuteFlow?.(
                      (message.resultData as FlowLoadResult).flow!
                    )
                : undefined
            }
          />
        );
      case 'execute_crew': {
        const execData = message.resultData as {
          plan?: CatalogLoadResult['plan'];
        };
        if (execData.plan) {
          return (
            <CrewDetailCard
              data={{ type: 'catalog_load', plan: execData.plan, message: '' }}
              onExecute={() => onExecuteCrew?.(execData.plan!)}
            />
          );
        }
        return null;
      }
      case 'execute_flow': {
        const execFlowData = message.resultData as {
          flow?: FlowLoadResult['flow'];
        };
        if (execFlowData.flow) {
          return (
            <FlowDetailCard
              data={{ type: 'flow_load', flow: execFlowData.flow, message: '' }}
              onExecute={() => onExecuteFlow?.(execFlowData.flow!)}
            />
          );
        }
        return null;
      }
      case 'generation_complete': {
        // NEW generations no longer produce these messages (steps fold into
        // the run-activity element; genie crews get a slim space prompt).
        // The card remains only for LEGACY persisted messages and for crews
        // explicitly loaded from the catalog (catalog_load above).
        return (
          <GenerationCompleteCard
            data={message.resultData as GenerationCompleteData}
            messageId={message.id}
            onExecute={onExecuteGenerated}
            onSaveCrew={onSaveCrew}
          />
        );
      }
      case 'crew_actions': {
        return (
          <CrewActionsBar
            data={message.resultData as GenerationCompleteData}
            messageId={message.id}
            onSaveCrew={onSaveCrew}
          />
        );
      }
      case 'genie_space_prompt': {
        return (
          <GenieSpacePrompt
            data={message.resultData as GenerationCompleteData}
            messageId={message.id}
            onExecute={onExecuteGenerated}
          />
        );
      }
      case 'input_variables': {
        const varsData = message.resultData as { variables?: DetectedVariable[] };
        if (!varsData.variables?.length) return null;
        return (
          <InputVariablesPrompt
            variables={varsData.variables}
            messageId={message.id}
            onSubmit={(inputs) => onSubmitVariables?.(message.id, inputs)}
          />
        );
      }
      case 'help':
        return (
          <HelpCard
            content={
              (message.resultData as { message: string }).message ||
              message.content
            }
          />
        );
      default:
        return null;
    }
  };

  if (message.resultType === 'trace') {
    return <TraceMessage data={message.resultData as TraceEntryData} />;
  }

  if (isSystem) {
    return (
      <div className="flex justify-center my-4 animate-fade-in">
        <span
          className="text-[11px] px-3 py-1.5 rounded-full font-medium"
          style={{
            color: 'var(--text-muted)',
            backgroundColor: 'var(--bg-secondary)',
          }}
        >
          {message.content}
        </span>
      </div>
    );
  }

  if (isUser) {
    const attachments = message.attachments || [];
    return (
      <div className="flex justify-end mb-5 px-4 animate-fade-in">
        <div className="max-w-[75%] flex flex-col items-end gap-1.5">
          <div
            className="rounded-3xl rounded-br-lg px-5 py-3"
            style={{ backgroundColor: 'var(--bg-user-msg)' }}
          >
            <div className="text-[15px] leading-relaxed" style={{ color: 'var(--text-primary)' }}>
              {message.content}
            </div>
          </div>
          {attachments.length > 0 && (
            <div className="flex flex-wrap gap-1.5 justify-end">
              {attachments.map((name, i) => (
                <span
                  key={`${name}-${i}`}
                  className="inline-flex items-center gap-1 rounded-lg px-2 py-1 text-[11px] font-medium"
                  style={{ backgroundColor: 'var(--bg-secondary)', color: 'var(--text-secondary)' }}
                  title={name}
                >
                  <svg className="w-3 h-3 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.7}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                  </svg>
                  <span className="max-w-[160px] truncate">{name}</span>
                </span>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  }

  // Assistant message
  return (
    <div className="mb-5 px-4 animate-fade-in">
      <div className="max-w-[85%]">
        {/* Loading indicator */}
        {message.isStreaming && (
          <div className="flex items-center gap-1.5 mb-2 ml-1">
            <div
              className="w-1.5 h-1.5 rounded-full animate-bounce"
              style={{ backgroundColor: 'var(--accent)' }}
            />
            <div
              className="w-1.5 h-1.5 rounded-full animate-bounce"
              style={{ backgroundColor: 'var(--accent)', animationDelay: '0.15s' }}
            />
            <div
              className="w-1.5 h-1.5 rounded-full animate-bounce"
              style={{ backgroundColor: 'var(--accent)', animationDelay: '0.3s' }}
            />
          </div>
        )}

        {message.content && (
          <div className="text-[15px] leading-[1.7]" style={{ color: 'var(--text-primary)' }}>
            <MessageContent content={message.content} />
          </div>
        )}

        {renderRichContent()}
      </div>
    </div>
  );
};

/**
 * Normalize generation data by searching for agents/tasks arrays at multiple
 * nesting levels.  The backend may wrap them differently depending on the model
 * or code-path (streaming SSE vs synchronous dispatch).
 */
function normalizeGenerationData(raw: unknown): { agents: Record<string, unknown>[]; tasks: Record<string, unknown>[] } {
  if (!raw || typeof raw !== 'object') return { agents: [], tasks: [] };

  const obj = raw as Record<string, unknown>;

  const findArray = (key: string): Record<string, unknown>[] => {
    // Direct top-level
    if (Array.isArray(obj[key]) && obj[key].length > 0) return obj[key] as Record<string, unknown>[];
    // Nested under "result"
    if (obj.result && typeof obj.result === 'object') {
      const nested = obj.result as Record<string, unknown>;
      if (Array.isArray(nested[key]) && nested[key].length > 0) return nested[key] as Record<string, unknown>[];
    }
    // Nested under "data"
    if (obj.data && typeof obj.data === 'object') {
      const nested = obj.data as Record<string, unknown>;
      if (Array.isArray(nested[key]) && nested[key].length > 0) return nested[key] as Record<string, unknown>[];
    }
    // Nested under "generation_result"
    if (obj.generation_result && typeof obj.generation_result === 'object') {
      const nested = obj.generation_result as Record<string, unknown>;
      if (Array.isArray(nested[key]) && nested[key].length > 0) return nested[key] as Record<string, unknown>[];
    }
    return [];
  };

  return { agents: findArray('agents'), tasks: findArray('tasks') };
}

/** Check whether any agent or task in the generation data references GenieTool.
 *  Uses the shared, toolNameMap-independent detector so the space selector shows
 *  whenever the crew uses Genie — by name, alias, or tool id — regardless of the
 *  chosen output format (Auto, Data answer (Genie), …) or whether the tool list
 *  has loaded yet. */
function hasGenieTool(
  agents: Record<string, unknown>[],
  tasks: Record<string, unknown>[],
  toolNameMap: Record<string, string>,
): boolean {
  const isGenie = (t: unknown) => isGenieToolRef(t, toolNameMap);
  for (const a of agents) {
    if (Array.isArray(a.tools) && a.tools.some(isGenie)) return true;
  }
  for (const t of tasks) {
    if (Array.isArray(t.tools) && t.tools.some(isGenie)) return true;
  }
  return false;
}

type IndexedTask = { task: Record<string, unknown>; taskIndex: number };
type AgentGroup = { agent: Record<string, unknown>; agentIndex: number; tasks: IndexedTask[] };

/** Group each task under the agent it's assigned to (by assigned_agent/agent
 *  name or agent_id), so the crew card reads as "agent → its tasks" instead of
 *  two disconnected lists. Tasks with no resolvable agent fall into `orphanTasks`
 *  (rendered separately), so nothing is ever dropped. */
function groupTasksUnderAgents(
  agents: Record<string, unknown>[],
  tasks: Record<string, unknown>[],
): { groups: AgentGroup[]; orphanTasks: IndexedTask[] } {
  const norm = (s: unknown) => String(s ?? '').trim().toLowerCase();
  const indexFor = new Map<string, number>();
  agents.forEach((a, i) => {
    for (const key of [norm(a.name), norm(a.role), norm(a.id)]) {
      if (key && !indexFor.has(key)) indexFor.set(key, i);
    }
  });
  const groups: AgentGroup[] = agents.map((agent, agentIndex) => ({ agent, agentIndex, tasks: [] }));
  const orphanTasks: IndexedTask[] = [];
  tasks.forEach((task, taskIndex) => {
    const idx = [norm(task.assigned_agent), norm(task.agent), norm(task.agent_id)]
      .map((k) => (k ? indexFor.get(k) : undefined))
      .find((v) => v !== undefined);
    if (idx !== undefined) groups[idx].tasks.push({ task, taskIndex });
    else orphanTasks.push({ task, taskIndex });
  });
  return { groups, orphanTasks };
}

/**
 * Convert a loaded catalog plan (ReactFlow nodes/edges) into the same
 * { agents, tasks } shape a freshly generated crew uses, so a loaded crew renders
 * with the identical business-friendly card (agents + tasks with descriptions),
 * not a technical Process/Memory/RPM summary. Task→agent links come from the
 * agent→task edges.
 */
function planToGenerationData(
  plan: { nodes?: unknown[]; edges?: unknown[] } | undefined | null,
): GenerationCompleteData {
  const nodes = (plan?.nodes || []) as Array<{ id?: string; type?: string; data?: Record<string, unknown> }>;
  const edges = (plan?.edges || []) as Array<{ source?: string; target?: string }>;
  const agentNodeName = new Map<string, string>();
  const agents: Record<string, unknown>[] = [];
  const tasks: Record<string, unknown>[] = [];

  for (const n of nodes) {
    if (n.type !== 'agentNode') continue;
    const d = (n.data || {}) as Record<string, unknown>;
    const name = String(d.label ?? d.name ?? d.role ?? '');
    if (n.id) agentNodeName.set(n.id, name);
    agents.push({
      id: d.agentId ?? d.id,
      name,
      role: d.role ?? '',
      goal: d.goal ?? '',
      backstory: d.backstory ?? '',
      tools: Array.isArray(d.tools) ? d.tools : [],
    });
  }
  for (const n of nodes) {
    if (n.type !== 'taskNode') continue;
    const d = (n.data || {}) as Record<string, unknown>;
    const owningEdge = edges.find((e) => e.target === n.id && String(e.source || '').startsWith('agent-'));
    const ownerName = owningEdge?.source ? agentNodeName.get(owningEdge.source) : undefined;
    tasks.push({
      id: d.taskId ?? d.id,
      name: d.label ?? d.name ?? '',
      description: d.description ?? '',
      expected_output: d.expected_output ?? '',
      tools: Array.isArray(d.tools) ? d.tools : [],
      ...(ownerName ? { agent: ownerName } : {}),
    });
  }
  return { agents, tasks };
}

// Persist the Genie-space selection (+ whether the crew was already run) per
// generation_complete message, so the choice survives the card remounting when
// the run streams messages in / opens the preview pane (local useState alone
// resets, which looked like "I lost the space I selected" after running).
const genieSelectionStore = new Map<string, { spaceId: string; ran: boolean }>();

/** Sub-component for the generation_complete card with optional Genie Space selector.
 *  Exported so the ChatContainer can mount it inside the run-activity container. */
export const GenerationCompleteCard: React.FC<{
  data: GenerationCompleteData;
  messageId: string;
  onExecute?: (data: GenerationCompleteData, spaceId?: string) => void;
  onSaveCrew?: (data: GenerationCompleteData, opts?: { overwrite?: boolean; spaceId?: string }) => Promise<{ id: string; name: string }>;
}> = ({ data, messageId, onExecute, onSaveCrew }) => {
  const { agents, tasks } = normalizeGenerationData(data);
  const toolNameMap = useAppStore((s) => s.toolNameMap);
  // Genie selection hydrates from (1) the in-memory store (survives remounts
  // within the session) then (2) the persisted message resultData (survives
  // session switches and reloads — stored server-side with the message).
  const persisted = data as GenerationCompleteData & { genieSpaceId?: string; genieRan?: boolean };
  const [selectedSpaceId, setSelectedSpaceId] = useState(
    () => genieSelectionStore.get(messageId)?.spaceId ?? persisted.genieSpaceId ?? '',
  );
  const [ran, setRan] = useState(
    () => genieSelectionStore.get(messageId)?.ran ?? persisted.genieRan ?? false,
  );

  const persistGenie = (next: { spaceId?: string; ran?: boolean }) => {
    const prev = genieSelectionStore.get(messageId) ?? { spaceId: selectedSpaceId, ran };
    const merged = { ...prev, ...next };
    genieSelectionStore.set(messageId, merged);
    // Write through to the message itself (server-side) so the selection and
    // ran-state survive leaving the session. resultType must be re-sent: the
    // backend replaces generation_result wholesale on update.
    try {
      useSessionStore.getState().updateMessage(messageId, {
        resultType: 'generation_complete',
        resultData: { ...data, genieSpaceId: merged.spaceId, genieRan: merged.ran },
      });
    } catch {
      /* persistence is best-effort; the in-memory store still covers the session */
    }
  };
  // idle → saving → saved (terminal). 'exists' offers Overwrite; error → hint.
  const [saveState, setSaveState] = useState<'idle' | 'saving' | 'saved' | 'exists' | 'error'>('idle');
  const [savedName, setSavedName] = useState('');

  const canSave = Boolean(onSaveCrew) && (agents.length > 0 || tasks.length > 0);

  const handleSave = async (overwrite = false) => {
    // The button only renders when canSave (onSaveCrew present) and is disabled
    // while saving/once saved, so re-entry is prevented at the DOM level.
    setSaveState('saving');
    try {
      // Carry the picked Genie space so the saved crew runs against it.
      const result = await onSaveCrew!(data, {
        ...(overwrite ? { overwrite: true } : {}),
        ...(selectedSpaceId ? { spaceId: selectedSpaceId } : {}),
      });
      setSavedName(result.name);
      setSaveState('saved');
    } catch (err) {
      // Name already taken → offer to overwrite instead of a dead-end error.
      setSaveState(err instanceof CrewNameConflictError ? 'exists' : 'error');
    }
  };

  const showGenieSelector = hasGenieTool(agents, tasks, toolNameMap);
  const { groups, orphanTasks } = groupTasksUnderAgents(agents, tasks);

  return (
    <div className="mt-3 space-y-3">
      {/* Header: agent count + subtle "save to catalog" bookmark */}
      <div className="flex items-center justify-between px-1">
        {agents.length > 0 || tasks.length > 0 ? (
          <div
            className="text-[10px] font-semibold uppercase tracking-wider"
            style={{ color: 'var(--text-muted)' }}
          >
            {[
              agents.length > 0 ? `${agents.length} Agent${agents.length > 1 ? 's' : ''}` : '',
              tasks.length > 0 ? `${tasks.length} Task${tasks.length > 1 ? 's' : ''}` : '',
            ].filter(Boolean).join(' · ')}
          </div>
        ) : <span />}

        {canSave && (
          <div className="flex items-center gap-1.5">
            {saveState === 'saved' && (
              <span className="text-[10px]" style={{ color: 'var(--text-muted)' }}>
                Saved “{savedName}” to catalog
              </span>
            )}
            {saveState === 'exists' && (
              <span className="text-[10px] flex items-center gap-1.5" style={{ color: 'var(--text-muted)' }}>
                Already in catalog
                <button
                  type="button"
                  onClick={() => handleSave(true)}
                  className="font-medium transition-opacity hover:opacity-70"
                  style={{ color: 'var(--accent)' }}
                >
                  Overwrite
                </button>
              </span>
            )}
            {saveState === 'error' && (
              <span className="text-[10px]" style={{ color: 'var(--bad, #fb7185)' }}>
                Couldn’t save — try again
              </span>
            )}
            <button
              type="button"
              onClick={() => handleSave()}
              disabled={saveState === 'saving' || saveState === 'saved'}
              title={saveState === 'saved' ? 'Saved to catalog' : 'Save this crew to the catalog'}
              aria-label={saveState === 'saved' ? 'Saved to catalog' : 'Save crew to catalog'}
              className="w-6 h-6 rounded-md flex items-center justify-center transition-colors hover:opacity-70 disabled:cursor-default"
              style={{ color: saveState === 'saved' ? 'var(--accent)' : 'var(--text-muted)' }}
            >
              {saveState === 'saving' ? (
                <div
                  className="w-3.5 h-3.5 rounded-full border-2 border-t-transparent animate-spin"
                  style={{ borderColor: 'var(--border-color)', borderTopColor: 'var(--accent)' }}
                />
              ) : (
                <svg
                  className="w-4 h-4"
                  viewBox="0 0 24 24"
                  fill={saveState === 'saved' ? 'currentColor' : 'none'}
                  stroke="currentColor"
                  strokeWidth={2}
                >
                  <path strokeLinecap="round" strokeLinejoin="round" d="M17.593 3.322c1.1.128 1.907 1.077 1.907 2.185V21L12 17.25 4.5 21V5.507c0-1.108.806-2.057 1.907-2.185a48.507 48.507 0 0111.186 0z" />
                </svg>
              )}
            </button>
          </div>
        )}
      </div>

      {/* Crew: each agent with its assigned task(s) nested beneath it */}
      {groups.map(({ agent, agentIndex, tasks: agentTasks }) => (
        <div key={`agent-${agentIndex}`} className="space-y-2">
          <AgentRow agent={agent} index={agentIndex} toolNameMap={toolNameMap} taskCount={agentTasks.length} />
          {agentTasks.length > 0 && (
            <div className="ml-3 pl-3 space-y-2" style={{ borderLeft: '2px solid var(--border-color)' }}>
              {agentTasks.map(({ task, taskIndex }) => (
                <TaskRow key={`task-${taskIndex}`} task={task} index={taskIndex} toolNameMap={toolNameMap} />
              ))}
            </div>
          )}
        </div>
      ))}

      {/* Tasks with no resolvable agent (or a tasks-only crew) — never dropped */}
      {orphanTasks.length > 0 && (
        <>
          <div
            className="text-[10px] font-semibold uppercase tracking-wider px-1 mt-1"
            style={{ color: 'var(--text-muted)' }}
          >
            {agents.length > 0
              ? 'Other tasks'
              : `${orphanTasks.length} Task${orphanTasks.length > 1 ? 's' : ''}`}
          </div>
          {orphanTasks.map(({ task, taskIndex }) => (
            <TaskRow key={`orphan-${taskIndex}`} task={task} index={taskIndex} toolNameMap={toolNameMap} />
          ))}
        </>
      )}

      {/* Genie Space selector + Run — only when GenieTool is in the crew's tools.
          Genie crews aren't auto-run; the user picks a space here then runs. */}
      {showGenieSelector && onExecute && (
        <div className="pt-1 space-y-2">
          <GenieSpaceSelector
            value={selectedSpaceId}
            onChange={(v) => {
              setSelectedSpaceId(v);
              persistGenie({ spaceId: v });
            }}
          />
          <button
            type="button"
            onClick={() => {
              // Button is disabled until a space is chosen and after it runs,
              // so a fired click always has a space and hasn't run yet.
              setRan(true);
              persistGenie({ ran: true });
              onExecute(data, selectedSpaceId);
            }}
            disabled={!selectedSpaceId || ran}
            className="w-full flex items-center justify-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed"
            style={{ backgroundColor: 'var(--bg-secondary)', color: 'var(--text-secondary)', border: '1px solid var(--border-color)' }}
          >
            <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
              <path d="M8 5v14l11-7z" />
            </svg>
            {ran ? 'Running…' : selectedSpaceId ? 'Run crew' : 'Select a Genie space to run'}
          </button>
        </div>
      )}
    </div>
  );
};

export interface TraceEntryData {
  label: string;
  sublabel?: string;
  durationMs?: number;
  source?: string;
  kind: 'tool_call' | 'tool_result' | 'event';
  detail?: string;
  timestamp: number;
}

/** Format a duration in ms as "2.93s" (>=1s) or "640ms". */
export function formatDurationMs(durationMs: number | undefined): string | null {
  if (typeof durationMs !== 'number') return null;
  return durationMs >= 1000
    ? `${(durationMs / 1000).toFixed(2)}s`
    : `${Math.round(durationMs)}ms`;
}


const TraceMessage: React.FC<{ data: TraceEntryData }> = ({ data }) => {
  const [open, setOpen] = useState(false);
  const hasDetail = Boolean(data.detail && data.detail.trim().length > 0);

  // Some tools render their RESULT directly in the chat instead of behind the
  // collapsed pill (e.g. Genie shows its answer Perplexity-style, with the
  // question + SQL tucked into a collapsible). When a tool_result has such an
  // inline renderer, use it.
  const Inline =
    data.kind === 'tool_result' && hasDetail
      ? findInlineTraceRenderer(data.detail as string, data.label)
      : undefined;
  if (Inline) {
    return (
      <div className="px-4 my-1 animate-fade-in">
        <Inline
          detail={data.detail as string}
          label={data.label}
          sublabel={data.sublabel}
          durationMs={data.durationMs}
        />
      </div>
    );
  }

  const isPending = data.kind === 'tool_call' && data.durationMs === undefined;
  const durationLabel =
    typeof data.durationMs === 'number'
      ? data.durationMs >= 1000
        ? `${(data.durationMs / 1000).toFixed(2)}s`
        : `${Math.round(data.durationMs)}ms`
      : null;

  return (
    <div className="px-4 my-1 animate-fade-in">
      <button
        type="button"
        onClick={() => hasDetail && setOpen((v) => !v)}
        className="flex items-center gap-2 text-left rounded-lg px-3 py-1.5 w-full max-w-[85%] transition-colors hover:opacity-90"
        disabled={!hasDetail}
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
          cursor: hasDetail ? 'pointer' : 'default',
        }}
      >
        {isPending ? (
          <div
            className="w-3 h-3 rounded-full border-2 border-t-transparent animate-spin flex-shrink-0"
            style={{ borderColor: 'var(--border-color)', borderTopColor: 'var(--accent)' }}
          />
        ) : (
          <svg
            className="w-3.5 h-3.5 flex-shrink-0"
            style={{ color: data.kind === 'event' ? 'var(--text-muted)' : 'var(--accent)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            {data.kind === 'event' ? (
              <circle cx="12" cy="12" r="3" />
            ) : (
              <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
            )}
          </svg>
        )}
        <span className="text-xs font-medium" style={{ color: 'var(--text-primary)' }}>
          {data.label}
        </span>
        {data.sublabel && (
          <span className="text-xs font-mono truncate max-w-[280px]" style={{ color: 'var(--text-muted)' }}>
            {data.sublabel}
          </span>
        )}
        <div className="flex items-center gap-2 ml-auto flex-shrink-0">
          {durationLabel && (
            <span className="text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
              · {durationLabel}
            </span>
          )}
          {hasDetail && (
            <svg
              className="w-3 h-3 flex-shrink-0 transition-transform"
              style={{
                color: 'var(--text-muted)',
                transform: open ? 'rotate(90deg)' : 'rotate(0deg)',
              }}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </svg>
          )}
        </div>
      </button>
      {open && data.detail && <TraceDetail detail={data.detail} label={data.label} indentClass="ml-3" />}
    </div>
  );
};

/** One row inside an expanded trace group: the call's query + duration, with
 *  an optional expandable detail (the tool's raw result). Compact — the tool
 *  name lives once on the group header, not repeated per row. */
const TraceGroupChild: React.FC<{ data: TraceEntryData }> = ({ data }) => {
  const [open, setOpen] = useState(false);
  const hasDetail = Boolean(data.detail && data.detail.trim().length > 0);
  const durationLabel = formatDurationMs(data.durationMs);
  const text = data.sublabel || data.label;

  return (
    <div>
      <button
        type="button"
        onClick={() => hasDetail && setOpen((v) => !v)}
        disabled={!hasDetail}
        className="flex items-center gap-2 text-left rounded-md px-2 py-1 w-full transition-colors hover:opacity-90"
        style={{ cursor: hasDetail ? 'pointer' : 'default' }}
      >
        <span
          className="text-xs font-mono truncate flex-1"
          style={{ color: 'var(--text-primary)' }}
        >
          {text}
        </span>
        {durationLabel && (
          <span className="text-[10px] font-mono flex-shrink-0" style={{ color: 'var(--text-muted)' }}>
            · {durationLabel}
          </span>
        )}
        {hasDetail && (
          <svg
            className="w-3 h-3 flex-shrink-0 transition-transform"
            style={{ color: 'var(--text-muted)', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </svg>
        )}
      </button>
      {open && data.detail && <TraceDetail detail={data.detail} label={data.label} indentClass="ml-2" />}
    </div>
  );
};

/** Collapses a run of consecutive same-tool traces into ONE line
 *  ("PerplexityTool · 10 calls · 27.34s") that expands to list every call.
 *  Keeps the chat readable when an agent fires a tool many times in a row. */
export const TraceGroupMessage: React.FC<{ label: string; traces: TraceEntryData[] }> = ({
  label,
  traces,
}) => {
  const [open, setOpen] = useState(false);
  const count = traces.length;
  const totalMs = traces.reduce(
    (sum, t) => sum + (typeof t.durationMs === 'number' ? t.durationMs : 0),
    0,
  );
  const totalLabel = formatDurationMs(totalMs);
  const anyPending = traces.some((t) => t.kind === 'tool_call' && t.durationMs === undefined);

  return (
    <div className="px-4 my-1 animate-fade-in">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-2 text-left rounded-lg px-3 py-1.5 w-full max-w-[85%] transition-colors hover:opacity-90"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          border: '1px solid var(--border-color)',
          cursor: 'pointer',
        }}
      >
        {anyPending ? (
          <div
            className="w-3 h-3 rounded-full border-2 border-t-transparent animate-spin flex-shrink-0"
            style={{ borderColor: 'var(--border-color)', borderTopColor: 'var(--accent)' }}
          />
        ) : (
          <svg
            className="w-3.5 h-3.5 flex-shrink-0"
            style={{ color: 'var(--accent)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
          </svg>
        )}
        <span className="text-xs font-medium" style={{ color: 'var(--text-primary)' }}>
          {label}
        </span>
        <div className="flex items-center gap-2 ml-auto flex-shrink-0">
          <span className="text-xs font-mono" style={{ color: 'var(--text-muted)' }}>
            · {count} calls
          </span>
          {totalMs > 0 && totalLabel && (
            <span className="text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
              · {totalLabel}
            </span>
          )}
          <svg
            className="w-3 h-3 flex-shrink-0 transition-transform"
            style={{ color: 'var(--text-muted)', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </svg>
        </div>
      </button>
      {open && (
        <div
          className="mt-1 ml-3 flex flex-col gap-0.5 max-w-[85%] rounded-lg p-1"
          style={{ backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
        >
          {traces.map((t, i) => (
            <TraceGroupChild key={i} data={t} />
          ))}
        </div>
      )}
    </div>
  );
};

const Chevron: React.FC<{ open: boolean }> = ({ open }) => (
  <svg
    className="w-3.5 h-3.5 flex-shrink-0 transition-transform"
    style={{ color: 'var(--text-muted)', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
  >
    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
  </svg>
);

const AgentRow: React.FC<{
  agent: Record<string, unknown>;
  index: number;
  toolNameMap: Record<string, string>;
  taskCount?: number;
}> = ({ agent, index, toolNameMap, taskCount }) => {
  const [open, setOpen] = useState(false);
  const name = (agent.name as string) || (agent.role as string) || `Agent ${index + 1}`;
  const role = (agent.role as string) || '';
  const goal = (agent.goal as string) || '';
  const backstory = (agent.backstory as string) || '';
  const tools = (agent.tools as string[]) || [];
  const hasDetails = Boolean(goal || backstory || tools.length > 0);

  return (
    <div
      className="rounded-xl p-3"
      style={{ backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
    >
      <button
        type="button"
        onClick={() => hasDetails && setOpen((v) => !v)}
        className="flex items-center gap-2 w-full text-left"
        disabled={!hasDetails}
        style={{ cursor: hasDetails ? 'pointer' : 'default' }}
      >
        {hasDetails && <Chevron open={open} />}
        <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0zM4.501 20.118a7.5 7.5 0 0114.998 0" />
        </svg>
        <span className="flex flex-col min-w-0">
          <span className="text-sm font-semibold leading-snug" style={{ color: 'var(--text-primary)' }}>{name}</span>
          {role && role !== name && (
            <span className="text-xs leading-snug" style={{ color: 'var(--text-muted)' }}>{role}</span>
          )}
        </span>
        {taskCount != null && taskCount > 0 && (
          <span
            className="text-[10px] px-2 py-0.5 rounded-full ml-auto flex-shrink-0 self-start"
            style={{ color: 'var(--text-muted)', backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}
          >
            {taskCount} task{taskCount > 1 ? 's' : ''}
          </span>
        )}
      </button>
      {open && (
        <div className="mt-2 ml-6 space-y-1">
          {goal && (
            <div>
              <span className="text-[10px] font-semibold uppercase" style={{ color: 'var(--text-muted)' }}>Goal: </span>
              <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>{goal}</span>
            </div>
          )}
          {backstory && (
            <div>
              <span className="text-[10px] font-semibold uppercase" style={{ color: 'var(--text-muted)' }}>Backstory: </span>
              <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>{backstory}</span>
            </div>
          )}
          {tools.length > 0 && (
            <div className="flex flex-wrap gap-1 pt-1">
              {tools.map((t, j) => (
                <span key={j} className="text-[10px] px-1.5 py-0.5 rounded" style={{ backgroundColor: 'var(--bg-primary)', color: 'var(--text-muted)' }}>
                  {resolveToolName(t, toolNameMap)}
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

const TaskRow: React.FC<{
  task: Record<string, unknown>;
  index: number;
  toolNameMap: Record<string, string>;
}> = ({ task, index, toolNameMap }) => {
  const [open, setOpen] = useState(false);
  const name = (task.name as string) || `Task ${index + 1}`;
  const description = (task.description as string) || '';
  const expectedOutput = (task.expected_output as string) || '';
  const taskTools = (task.tools as string[]) || [];
  const hasDetails = Boolean(description || expectedOutput || taskTools.length > 0);

  return (
    <div
      className="rounded-xl p-3"
      style={{ backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
    >
      <button
        type="button"
        onClick={() => hasDetails && setOpen((v) => !v)}
        className="flex items-center gap-2 w-full text-left"
        disabled={!hasDetails}
        style={{ cursor: hasDetails ? 'pointer' : 'default' }}
      >
        {hasDetails && <Chevron open={open} />}
        <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--text-muted)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15a2.25 2.25 0 012.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V19.5a2.25 2.25 0 002.25 2.25h.75" />
        </svg>
        <span className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{name}</span>
      </button>
      {open && (
        <div className="mt-2 ml-6 space-y-1">
          {description && (
            <div>
              <span className="text-[10px] font-semibold uppercase" style={{ color: 'var(--text-muted)' }}>Description: </span>
              <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>{description}</span>
            </div>
          )}
          {expectedOutput && (
            <div>
              <span className="text-[10px] font-semibold uppercase" style={{ color: 'var(--text-muted)' }}>Expected output: </span>
              <span className="text-xs" style={{ color: 'var(--text-secondary)' }}>{expectedOutput}</span>
            </div>
          )}
          {taskTools.length > 0 && (
            <div className="flex flex-wrap gap-1 pt-1">
              {taskTools.map((t, j) => (
                <span key={j} className="text-[10px] px-1.5 py-0.5 rounded" style={{ backgroundColor: 'var(--bg-primary)', color: 'var(--text-muted)' }}>
                  {resolveToolName(t, toolNameMap)}
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default ChatMessageComponent;
