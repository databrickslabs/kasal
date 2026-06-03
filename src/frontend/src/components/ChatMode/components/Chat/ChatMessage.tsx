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
import MessageContent from './MessageContent';
import AgentCard from '../Cards/AgentCard';
import TaskCard from '../Cards/TaskCard';
import CrewListCard from '../Cards/CrewListCard';
import FlowListCard from '../Cards/FlowListCard';
import CrewDetailCard from '../Cards/CrewDetailCard';
import FlowDetailCard from '../Cards/FlowDetailCard';
import HelpCard from '../Cards/HelpCard';
import GenieSpaceSelector from '../Cards/GenieSpaceSelector';

interface ChatMessageProps {
  message: ChatMessageType;
  onCommand?: (command: string) => void;
  onExecuteCrew?: (plan: PlanData) => void;
  onExecuteFlow?: (flow: FlowData) => void;
  onExecuteGenerated?: (data: GenerationCompleteData, spaceId?: string) => void;
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
      case 'catalog_load':
        return (
          <CrewDetailCard
            data={message.resultData as CatalogLoadResult}
            onExecute={
              (message.resultData as CatalogLoadResult).plan
                ? () =>
                    onExecuteCrew?.(
                      (message.resultData as CatalogLoadResult).plan!
                    )
                : undefined
            }
          />
        );
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
        return (
          <GenerationCompleteCard
            data={message.resultData as GenerationCompleteData}
            onExecute={onExecuteGenerated}
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
    return (
      <div className="flex justify-end mb-5 px-4 animate-fade-in">
        <div
          className="max-w-[75%] rounded-3xl rounded-br-lg px-5 py-3"
          style={{ backgroundColor: 'var(--bg-user-msg)' }}
        >
          <div className="text-[15px] leading-relaxed" style={{ color: 'var(--text-primary)' }}>
            {message.content}
          </div>
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

        <div className="text-[15px] leading-[1.7]" style={{ color: 'var(--text-primary)' }}>
          <MessageContent content={message.content} />
        </div>

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

/** Check whether any agent or task in the generation data references GenieTool */
function hasGenieTool(
  agents: Record<string, unknown>[],
  tasks: Record<string, unknown>[],
  toolNameMap: Record<string, string>,
): boolean {
  const isGenie = (t: unknown) => {
    const name = String(t);
    const resolved = toolNameMap[name] || name;
    return resolved === 'GenieTool' || name === 'GenieTool';
  };
  for (const a of agents) {
    if (Array.isArray(a.tools) && a.tools.some(isGenie)) return true;
  }
  for (const t of tasks) {
    if (Array.isArray(t.tools) && t.tools.some(isGenie)) return true;
  }
  return false;
}

/** Sub-component for the generation_complete card with optional Genie Space selector */
const GenerationCompleteCard: React.FC<{
  data: GenerationCompleteData;
  onExecute?: (data: GenerationCompleteData, spaceId?: string) => void;
}> = ({ data }) => {
  const { agents, tasks } = normalizeGenerationData(data);
  const toolNameMap = useAppStore((s) => s.toolNameMap);
  const [selectedSpaceId, setSelectedSpaceId] = useState('');

  const showGenieSelector = hasGenieTool(agents, tasks, toolNameMap);

  return (
    <div className="mt-3 space-y-3">
      {/* Section label */}
      {agents.length > 0 && (
        <div
          className="text-[10px] font-semibold uppercase tracking-wider px-1"
          style={{ color: 'var(--text-muted)' }}
        >
          {agents.length} Agent{agents.length > 1 ? 's' : ''}
        </div>
      )}

      {/* Agent details */}
      {agents.map((agent, i) => (
        <AgentRow key={i} agent={agent} index={i} toolNameMap={toolNameMap} />
      ))}

      {/* Tasks section label */}
      {tasks.length > 0 && (
        <div
          className="text-[10px] font-semibold uppercase tracking-wider px-1 mt-1"
          style={{ color: 'var(--text-muted)' }}
        >
          {tasks.length} Task{tasks.length > 1 ? 's' : ''}
        </div>
      )}

      {/* Task details */}
      {tasks.map((task, i) => (
        <TaskRow key={i} task={task} index={i} toolNameMap={toolNameMap} />
      ))}

      {/* Genie Space selector — only shown when GenieTool is in the crew's tools */}
      {showGenieSelector && (
        <div className="pt-1">
          <GenieSpaceSelector value={selectedSpaceId} onChange={setSelectedSpaceId} />
        </div>
      )}
    </div>
  );
};

interface TraceEntryData {
  label: string;
  sublabel?: string;
  durationMs?: number;
  source?: string;
  kind: 'tool_call' | 'tool_result' | 'event';
  detail?: string;
  timestamp: number;
}

const TraceMessage: React.FC<{ data: TraceEntryData }> = ({ data }) => {
  const [open, setOpen] = useState(false);
  const hasDetail = Boolean(data.detail && data.detail.trim().length > 0);
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
        className="flex items-center gap-2 text-left rounded-lg px-3 py-1.5 max-w-[85%] transition-colors hover:opacity-90"
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
        {durationLabel && (
          <span className="text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
            · {durationLabel}
          </span>
        )}
        {hasDetail && (
          <svg
            className="w-3 h-3 flex-shrink-0 transition-transform ml-1"
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
      </button>
      {open && data.detail && (
        <pre
          className="mt-1 ml-3 text-[11px] whitespace-pre-wrap break-words rounded p-2 max-w-[85%] max-h-72 overflow-y-auto"
          style={{
            color: 'var(--text-primary)',
            backgroundColor: 'var(--bg-primary)',
            border: '1px solid var(--border-color)',
          }}
        >
          {data.detail}
        </pre>
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
}> = ({ agent, index, toolNameMap }) => {
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
        <span className="text-sm font-semibold" style={{ color: 'var(--text-primary)' }}>{name}</span>
        {role && role !== name && (
          <span className="text-xs px-1.5 py-0.5 rounded" style={{ color: 'var(--text-muted)', backgroundColor: 'var(--bg-primary)' }}>{role}</span>
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
