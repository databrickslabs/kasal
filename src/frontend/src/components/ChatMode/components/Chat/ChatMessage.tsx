import React, { useState } from 'react';
import Box from '@mui/material/Box';
import { buttonResetSx, fadeInSx } from '../../chatSx';
import { ChatMessage as ChatMessageType } from '../../types/chat';
import {
  CatalogListResult,
  CatalogLoadResult,
  FlowListResult,
  FlowLoadResult,
  GeneratedAgent,
  GeneratedTask,
  GenerationCompleteData,
} from '../../types/dispatcher';
import { PlanData, FlowData } from '../../hooks/useDispatcher';
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
import { type Surface } from '../../../../shared/a2ui';
import { useExecutionStore } from '../../store/executionStore';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import A2uiSurface from './A2uiSurface';
import { downloadSurfacePdf } from '../../utils/surfacePdf';

/**
 * 12px / 14px ring spinner used in the card "save" button + the trace pills.
 * The Tailwind `border-t-transparent` utility is `!important` (the chat scopes
 * every utility with `important: '.kasal-chat-root'`), so it always beat the
 * inline accent top-border — i.e. the top has always rendered transparent;
 * reproduced faithfully here. Spread into a `<Box>`'s sx and add width/height
 * (+ `flexShrink: 0` for the trace pills).
 */
const cardSpinnerSx = {
  borderRadius: '9999px',
  border: '2px solid',
  borderColor: 'divider',
  borderTopColor: 'transparent',
  animation: 'kasalSpin 1s linear infinite',
  '@keyframes kasalSpin': { to: { transform: 'rotate(360deg)' } },
} as const;

/**
 * Tailwind `animate-bounce`, reproduced for the assistant "typing" dots (no
 * shared atom exists for bounce). Spread into each dot's sx; the 2nd/3rd dots
 * add their own `animationDelay`.
 */
const bounceDotSx = {
  width: 6,
  height: 6,
  borderRadius: '9999px',
  backgroundColor: 'primary.main',
  animation: 'kasalBounce 1s infinite',
  '@keyframes kasalBounce': {
    '0%, 100%': { transform: 'translateY(-25%)', animationTimingFunction: 'cubic-bezier(0.8, 0, 1, 1)' },
    '50%': { transform: 'none', animationTimingFunction: 'cubic-bezier(0, 0, 0.2, 1)' },
  },
} as const;

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
  // When THIS message's surface is open in the side pane, its inline copy hides
  // (it shows a compact "opened in panel" note instead) so it isn't visible twice.
  const previewPaneOpen = useExecutionStore((s) => s.previewPaneOpen);
  const previewSourceMessageId = useExecutionStore((s) => s.previewSourceMessageId);
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
            executionId={message.executionId}
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
      case 'a2ui': {
        // Generative-UI surface composed by the backend, rendered INLINE in the
        // chat (preview pane is opt-in). Themed + drawn by the shared A2uiSurface
        // wrapper — the ONE implementation used by every host and the export. The
        // corner "expand" control opens THIS surface in the side preview pane.
        const a2uiSurface = message.resultData as Surface;
        // While it's open in the pane, hide the inline copy — a compact note with
        // a "Show here" action (which closes the pane) stands in for it.
        if (previewPaneOpen && previewSourceMessageId === message.id) {
          return (
            <Box
              sx={{
                mt: 1.5,
                display: 'flex',
                alignItems: 'center',
                gap: 1,
                borderRadius: '8px',
                px: 1.5,
                py: 1,
                fontSize: 12,
                backgroundColor: 'background.default',
                border: 1,
                borderColor: 'divider',
                color: 'text.secondary',
              }}
            >
              <OpenInFullIcon sx={{ fontSize: 15, color: 'primary.main' }} />
              <span>Opened in the side panel</span>
              <Box
                component="button"
                type="button"
                onClick={() => useExecutionStore.getState().clearPreview()}
                sx={{
                  ...buttonResetSx,
                  ml: 'auto',
                  fontWeight: 500,
                  transition: 'color 0.15s, background-color 0.15s',
                  color: 'primary.main',
                  '&:hover': { opacity: 0.8 },
                }}
              >
                Show here
              </Box>
            </Box>
          );
        }
        return (
          <Box sx={{ mt: 1.5 }}>
            <A2uiSurface
              surface={a2uiSurface}
              onExpand={() =>
                useExecutionStore.getState().openPreviewPane(
                  { type: 'ui', data: JSON.stringify(a2uiSurface) },
                  message.id,
                )
              }
              // PDF rasterization is host-specific (Kasal's preflight restore), so
              // the chat host supplies it; the deck's "Download" menu then offers
              // PDF alongside the shared PowerPoint export.
              onDownloadPdf={() => downloadSurfacePdf(a2uiSurface, 'presentation')}
            />
          </Box>
        );
      }
      default:
        return null;
    }
  };

  if (message.resultType === 'trace') {
    return <TraceMessage data={message.resultData as TraceEntryData} />;
  }

  if (isSystem) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', my: 2, ...fadeInSx }}>
        <Box
          component="span"
          sx={{
            fontSize: 11,
            px: 1.5,
            py: 0.75,
            borderRadius: '9999px',
            fontWeight: 500,
            color: 'text.disabled',
            backgroundColor: (t) => t.chat.bgSecondary,
          }}
        >
          {message.content}
        </Box>
      </Box>
    );
  }

  if (isUser) {
    const attachments = message.attachments || [];
    return (
      <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 2.5, px: 2, ...fadeInSx }}>
        <Box sx={{ maxWidth: '75%', display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 0.75 }}>
          <Box
            sx={{
              borderRadius: '24px',
              borderBottomRightRadius: '8px',
              px: 2.5,
              py: 1.5,
              backgroundColor: (t) => t.chat.bgUserMsg,
            }}
          >
            <Box sx={{ fontSize: 15, lineHeight: 1.625, color: 'text.primary' }}>
              {message.content}
            </Box>
          </Box>
          {attachments.length > 0 && (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75, justifyContent: 'flex-end' }}>
              {attachments.map((name, i) => (
                <Box
                  component="span"
                  key={`${name}-${i}`}
                  sx={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 0.5,
                    borderRadius: '8px',
                    px: 1,
                    py: 0.5,
                    fontSize: 11,
                    fontWeight: 500,
                    backgroundColor: (t) => t.chat.bgSecondary,
                    color: 'text.secondary',
                  }}
                  title={name}
                >
                  <Box component="svg" sx={{ width: 12, height: 12, flexShrink: 0, color: 'primary.main' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.7}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                  </Box>
                  <Box component="span" sx={{ maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{name}</Box>
                </Box>
              ))}
            </Box>
          )}
        </Box>
      </Box>
    );
  }

  // Assistant message
  return (
    <Box sx={{ mb: 2.5, px: 2, ...fadeInSx }}>
      <Box sx={{ maxWidth: '85%' }}>
        {/* Loading indicator */}
        {message.isStreaming && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 1, ml: 0.5 }}>
            <Box sx={{ ...bounceDotSx }} />
            <Box sx={{ ...bounceDotSx, animationDelay: '0.15s' }} />
            <Box sx={{ ...bounceDotSx, animationDelay: '0.3s' }} />
          </Box>
        )}

        {message.content && (
          <Box sx={{ fontSize: 15, lineHeight: 1.7, color: 'text.primary' }}>
            <MessageContent content={message.content} />
          </Box>
        )}

        {renderRichContent()}
      </Box>
    </Box>
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
    <Box sx={{ mt: 1.5, '& > * + *': { mt: 1.5 } }}>
      {/* Header: agent count + subtle "save to catalog" bookmark */}
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', px: 0.5 }}>
        {agents.length > 0 || tasks.length > 0 ? (
          <Box
            sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', color: 'text.disabled' }}
          >
            {[
              agents.length > 0 ? `${agents.length} Agent${agents.length > 1 ? 's' : ''}` : '',
              tasks.length > 0 ? `${tasks.length} Task${tasks.length > 1 ? 's' : ''}` : '',
            ].filter(Boolean).join(' · ')}
          </Box>
        ) : <span />}

        {canSave && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
            {saveState === 'saved' && (
              <Box component="span" sx={{ fontSize: 10, color: 'text.disabled' }}>
                Saved “{savedName}” to catalog
              </Box>
            )}
            {saveState === 'exists' && (
              <Box component="span" sx={{ fontSize: 10, display: 'flex', alignItems: 'center', gap: 0.75, color: 'text.disabled' }}>
                Already in catalog
                <Box
                  component="button"
                  type="button"
                  onClick={() => handleSave(true)}
                  sx={{ ...buttonResetSx, fontWeight: 500, transition: 'opacity 0.15s', color: 'primary.main', '&:hover': { opacity: 0.7 } }}
                >
                  Overwrite
                </Box>
              </Box>
            )}
            {saveState === 'error' && (
              <Box component="span" sx={{ fontSize: 10, color: '#fb7185' }}>
                Couldn’t save — try again
              </Box>
            )}
            <Box
              component="button"
              type="button"
              onClick={() => handleSave()}
              disabled={saveState === 'saving' || saveState === 'saved'}
              title={saveState === 'saved' ? 'Saved to catalog' : 'Save this crew to the catalog'}
              aria-label={saveState === 'saved' ? 'Saved to catalog' : 'Save crew to catalog'}
              sx={{
                ...buttonResetSx,
                width: 24,
                height: 24,
                borderRadius: '6px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transition: 'color 0.15s, background-color 0.15s',
                color: saveState === 'saved' ? 'primary.main' : 'text.disabled',
                '&:hover': { opacity: 0.7 },
              }}
            >
              {saveState === 'saving' ? (
                <Box sx={{ ...cardSpinnerSx, width: 14, height: 14 }} />
              ) : (
                <Box
                  component="svg"
                  sx={{ width: 16, height: 16 }}
                  viewBox="0 0 24 24"
                  fill={saveState === 'saved' ? 'currentColor' : 'none'}
                  stroke="currentColor"
                  strokeWidth={2}
                >
                  <path strokeLinecap="round" strokeLinejoin="round" d="M17.593 3.322c1.1.128 1.907 1.077 1.907 2.185V21L12 17.25 4.5 21V5.507c0-1.108.806-2.057 1.907-2.185a48.507 48.507 0 0111.186 0z" />
                </Box>
              )}
            </Box>
          </Box>
        )}
      </Box>

      {/* Crew: each agent with its assigned task(s) nested beneath it */}
      {groups.map(({ agent, agentIndex, tasks: agentTasks }) => (
        <Box key={`agent-${agentIndex}`} sx={{ '& > * + *': { mt: 1 } }}>
          <AgentRow agent={agent} index={agentIndex} toolNameMap={toolNameMap} taskCount={agentTasks.length} />
          {agentTasks.length > 0 && (
            <Box sx={{ ml: 1.5, pl: 1.5, '& > * + *': { mt: 1 }, borderLeft: 2, borderColor: 'divider' }}>
              {agentTasks.map(({ task, taskIndex }) => (
                <TaskRow key={`task-${taskIndex}`} task={task} index={taskIndex} toolNameMap={toolNameMap} />
              ))}
            </Box>
          )}
        </Box>
      ))}

      {/* Tasks with no resolvable agent (or a tasks-only crew) — never dropped */}
      {orphanTasks.length > 0 && (
        <>
          <Box
            sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', px: 0.5, mt: 0.5, color: 'text.disabled' }}
          >
            {agents.length > 0
              ? 'Other tasks'
              : `${orphanTasks.length} Task${orphanTasks.length > 1 ? 's' : ''}`}
          </Box>
          {orphanTasks.map(({ task, taskIndex }) => (
            <TaskRow key={`orphan-${taskIndex}`} task={task} index={taskIndex} toolNameMap={toolNameMap} />
          ))}
        </>
      )}

      {/* Genie Space selector + Run — only when GenieTool is in the crew's tools.
          Genie crews aren't auto-run; the user picks a space here then runs. */}
      {showGenieSelector && onExecute && (
        <Box sx={{ pt: 0.5, '& > * + *': { mt: 1 } }}>
          <GenieSpaceSelector
            value={selectedSpaceId}
            onChange={(v) => {
              setSelectedSpaceId(v);
              persistGenie({ spaceId: v });
            }}
          />
          <Box
            component="button"
            type="button"
            onClick={() => {
              // Button is disabled until a space is chosen and after it runs,
              // so a fired click always has a space and hasn't run yet.
              setRan(true);
              persistGenie({ ran: true });
              onExecute(data, selectedSpaceId);
            }}
            disabled={!selectedSpaceId || ran}
            sx={{
              ...buttonResetSx,
              width: '100%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 1,
              borderRadius: '8px',
              px: 1.5,
              py: 1,
              fontSize: 14,
              fontWeight: 500,
              transition: 'all 0.15s',
              backgroundColor: (t) => t.chat.bgSecondary,
              color: 'text.secondary',
              border: 1,
              borderColor: 'divider',
              '&:hover': { opacity: 0.8 },
              '&:disabled': { opacity: 0.5, cursor: 'not-allowed' },
            }}
          >
            <Box component="svg" sx={{ width: 16, height: 16 }} fill="currentColor" viewBox="0 0 24 24">
              <path d="M8 5v14l11-7z" />
            </Box>
            {ran ? 'Running…' : selectedSpaceId ? 'Run crew' : 'Select a Genie space to run'}
          </Box>
        </Box>
      )}
    </Box>
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
      <Box sx={{ px: 2, my: 0.5, ...fadeInSx }}>
        <Inline
          detail={data.detail as string}
          label={data.label}
          sublabel={data.sublabel}
          durationMs={data.durationMs}
        />
      </Box>
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
    <Box sx={{ px: 2, my: 0.5, ...fadeInSx }}>
      <Box
        component="button"
        type="button"
        onClick={() => hasDetail && setOpen((v) => !v)}
        disabled={!hasDetail}
        sx={{
          ...buttonResetSx,
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          textAlign: 'left',
          borderRadius: '8px',
          px: 1.5,
          py: 0.75,
          width: '100%',
          maxWidth: '85%',
          transition: 'color 0.15s, background-color 0.15s',
          backgroundColor: (t) => t.chat.bgSecondary,
          border: 1,
          borderColor: 'divider',
          cursor: hasDetail ? 'pointer' : 'default',
          '&:hover': { opacity: 0.9 },
        }}
      >
        {isPending ? (
          <Box sx={{ ...cardSpinnerSx, width: 12, height: 12, flexShrink: 0 }} />
        ) : (
          <Box
            component="svg"
            sx={{ width: 14, height: 14, flexShrink: 0, color: data.kind === 'event' ? 'text.disabled' : 'primary.main' }}
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
          </Box>
        )}
        <Box component="span" sx={{ fontSize: 12, fontWeight: 500, color: 'text.primary' }}>
          {data.label}
        </Box>
        {data.sublabel && (
          <Box component="span" sx={{ fontSize: 12, fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 280, color: 'text.disabled' }}>
            {data.sublabel}
          </Box>
        )}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, ml: 'auto', flexShrink: 0 }}>
          {durationLabel && (
            <Box component="span" sx={{ fontSize: 10, fontFamily: 'monospace', color: 'text.disabled' }}>
              · {durationLabel}
            </Box>
          )}
          {hasDetail && (
            <Box
              component="svg"
              sx={{ width: 12, height: 12, flexShrink: 0, transition: 'transform 0.15s', color: 'text.disabled', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </Box>
          )}
        </Box>
      </Box>
      {open && data.detail && <TraceDetail detail={data.detail} label={data.label} indent={1.5} />}
    </Box>
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
      <Box
        component="button"
        type="button"
        onClick={() => hasDetail && setOpen((v) => !v)}
        disabled={!hasDetail}
        sx={{
          ...buttonResetSx,
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          textAlign: 'left',
          borderRadius: '6px',
          px: 1,
          py: 0.5,
          width: '100%',
          transition: 'color 0.15s, background-color 0.15s',
          cursor: hasDetail ? 'pointer' : 'default',
          '&:hover': { opacity: 0.9 },
        }}
      >
        <Box
          component="span"
          sx={{ fontSize: 12, fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1, color: 'text.primary' }}
        >
          {text}
        </Box>
        {durationLabel && (
          <Box component="span" sx={{ fontSize: 10, fontFamily: 'monospace', flexShrink: 0, color: 'text.disabled' }}>
            · {durationLabel}
          </Box>
        )}
        {hasDetail && (
          <Box
            component="svg"
            sx={{ width: 12, height: 12, flexShrink: 0, transition: 'transform 0.15s', color: 'text.disabled', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </Box>
        )}
      </Box>
      {open && data.detail && <TraceDetail detail={data.detail} label={data.label} indent={1} />}
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
    <Box sx={{ px: 2, my: 0.5, ...fadeInSx }}>
      <Box
        component="button"
        type="button"
        onClick={() => setOpen((v) => !v)}
        sx={{
          ...buttonResetSx,
          display: 'flex',
          alignItems: 'center',
          gap: 1,
          textAlign: 'left',
          borderRadius: '8px',
          px: 1.5,
          py: 0.75,
          width: '100%',
          maxWidth: '85%',
          transition: 'color 0.15s, background-color 0.15s',
          backgroundColor: (t) => t.chat.bgSecondary,
          border: 1,
          borderColor: 'divider',
          cursor: 'pointer',
          '&:hover': { opacity: 0.9 },
        }}
      >
        {anyPending ? (
          <Box data-testid="trace-group-spinner" sx={{ ...cardSpinnerSx, width: 12, height: 12, flexShrink: 0 }} />
        ) : (
          <Box
            component="svg"
            sx={{ width: 14, height: 14, flexShrink: 0, color: 'primary.main' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
          </Box>
        )}
        <Box component="span" sx={{ fontSize: 12, fontWeight: 500, color: 'text.primary' }}>
          {label}
        </Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, ml: 'auto', flexShrink: 0 }}>
          <Box component="span" sx={{ fontSize: 12, fontFamily: 'monospace', color: 'text.disabled' }}>
            · {count} calls
          </Box>
          {totalMs > 0 && totalLabel && (
            <Box component="span" sx={{ fontSize: 10, fontFamily: 'monospace', color: 'text.disabled' }}>
              · {totalLabel}
            </Box>
          )}
          <Box
            component="svg"
            sx={{ width: 12, height: 12, flexShrink: 0, transition: 'transform 0.15s', color: 'text.disabled', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </Box>
        </Box>
      </Box>
      {open && (
        <Box
          sx={{ mt: 0.5, ml: 1.5, display: 'flex', flexDirection: 'column', gap: 0.25, maxWidth: '85%', borderRadius: '8px', p: 0.5, backgroundColor: (t) => t.chat.bgSecondary, border: 1, borderColor: 'divider' }}
        >
          {traces.map((t, i) => (
            <TraceGroupChild key={i} data={t} />
          ))}
        </Box>
      )}
    </Box>
  );
};

const Chevron: React.FC<{ open: boolean }> = ({ open }) => (
  <Box
    component="svg"
    sx={{ width: 14, height: 14, flexShrink: 0, transition: 'transform 0.15s', color: 'text.disabled', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
  >
    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
  </Box>
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
    <Box
      sx={{ borderRadius: '12px', p: 1.5, backgroundColor: (t) => t.chat.bgSecondary, border: 1, borderColor: 'divider' }}
    >
      <Box
        component="button"
        type="button"
        onClick={() => hasDetails && setOpen((v) => !v)}
        disabled={!hasDetails}
        sx={{ ...buttonResetSx, display: 'flex', alignItems: 'center', gap: 1, width: '100%', textAlign: 'left', cursor: hasDetails ? 'pointer' : 'default' }}
      >
        {hasDetails && <Chevron open={open} />}
        <Box component="svg" sx={{ width: 16, height: 16, flexShrink: 0, color: 'primary.main' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M15.75 6a3.75 3.75 0 11-7.5 0 3.75 3.75 0 017.5 0zM4.501 20.118a7.5 7.5 0 0114.998 0" />
        </Box>
        <Box component="span" sx={{ display: 'flex', flexDirection: 'column', minWidth: 0 }}>
          <Box component="span" sx={{ fontSize: 14, fontWeight: 600, lineHeight: 1.375, color: 'text.primary' }}>{name}</Box>
          {role && role !== name && (
            <Box component="span" sx={{ fontSize: 12, lineHeight: 1.375, color: 'text.disabled' }}>{role}</Box>
          )}
        </Box>
        {taskCount != null && taskCount > 0 && (
          <Box
            component="span"
            sx={{ fontSize: 10, px: 1, py: 0.25, borderRadius: '9999px', ml: 'auto', flexShrink: 0, alignSelf: 'flex-start', color: 'text.disabled', backgroundColor: 'background.default', border: 1, borderColor: 'divider' }}
          >
            {taskCount} task{taskCount > 1 ? 's' : ''}
          </Box>
        )}
      </Box>
      {open && (
        <Box sx={{ mt: 1, ml: 3, '& > * + *': { mt: 0.5 } }}>
          {goal && (
            <div>
              <Box component="span" sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', color: 'text.disabled' }}>Goal: </Box>
              <Box component="span" sx={{ fontSize: 12, color: 'text.secondary' }}>{goal}</Box>
            </div>
          )}
          {backstory && (
            <div>
              <Box component="span" sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', color: 'text.disabled' }}>Backstory: </Box>
              <Box component="span" sx={{ fontSize: 12, color: 'text.secondary' }}>{backstory}</Box>
            </div>
          )}
          {tools.length > 0 && (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, pt: 0.5 }}>
              {tools.map((t, j) => (
                <Box component="span" key={j} sx={{ fontSize: 10, px: 0.75, py: 0.25, borderRadius: '4px', backgroundColor: 'background.default', color: 'text.disabled' }}>
                  {resolveToolName(t, toolNameMap)}
                </Box>
              ))}
            </Box>
          )}
        </Box>
      )}
    </Box>
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
    <Box
      sx={{ borderRadius: '12px', p: 1.5, backgroundColor: (t) => t.chat.bgSecondary, border: 1, borderColor: 'divider' }}
    >
      <Box
        component="button"
        type="button"
        onClick={() => hasDetails && setOpen((v) => !v)}
        disabled={!hasDetails}
        sx={{ ...buttonResetSx, display: 'flex', alignItems: 'center', gap: 1, width: '100%', textAlign: 'left', cursor: hasDetails ? 'pointer' : 'default' }}
      >
        {hasDetails && <Chevron open={open} />}
        <Box component="svg" sx={{ width: 16, height: 16, flexShrink: 0, color: 'text.disabled' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15a2.25 2.25 0 012.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V19.5a2.25 2.25 0 002.25 2.25h.75" />
        </Box>
        <Box component="span" sx={{ fontSize: 14, fontWeight: 600, color: 'text.primary' }}>{name}</Box>
      </Box>
      {open && (
        <Box sx={{ mt: 1, ml: 3, '& > * + *': { mt: 0.5 } }}>
          {description && (
            <div>
              <Box component="span" sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', color: 'text.disabled' }}>Description: </Box>
              <Box component="span" sx={{ fontSize: 12, color: 'text.secondary' }}>{description}</Box>
            </div>
          )}
          {expectedOutput && (
            <div>
              <Box component="span" sx={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', color: 'text.disabled' }}>Expected output: </Box>
              <Box component="span" sx={{ fontSize: 12, color: 'text.secondary' }}>{expectedOutput}</Box>
            </div>
          )}
          {taskTools.length > 0 && (
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, pt: 0.5 }}>
              {taskTools.map((t, j) => (
                <Box component="span" key={j} sx={{ fontSize: 10, px: 0.75, py: 0.25, borderRadius: '4px', backgroundColor: 'background.default', color: 'text.disabled' }}>
                  {resolveToolName(t, toolNameMap)}
                </Box>
              ))}
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
};

export default ChatMessageComponent;
