import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import { ExecutionStatus } from './types/execution';
import { createExecution, stopExecution, listExecutions, getExecutionStatus, getJobTraces } from './api/executions';
import { saveGeneratedCrew, synthesizeCrewFromConversation, CrewNameConflictError } from './api/crews';
import { useSessionStore } from './store/sessionStore';
import { useExecutionStore } from './store/executionStore';
import { readActiveExecution, clearActiveExecution } from './store/activeExecutionMarker';
import { useAppStore } from './store/appStore';
import { useDispatcher, PlanData, FlowData } from './hooks/useDispatcher';
import { useExecutionStream } from './hooks/useExecutionStream';
import { startGenerationStream, stopAllGenerationStreams } from './utils/generationStreamManager';
import { GenerationCompleteData } from './types/dispatcher';
import { generateId } from './utils/markdown';
import { buildCrewConfig, buildFlowConfig, buildCrewConfigFromGenerated } from './utils/crewConfigBuilder';
import { detectVariablesFromNodes, detectVariablesFromGenerated } from './utils/variableDetector';
import ChatContainer from './components/Chat/ChatContainer';
import CatalogLibrary from './components/CatalogLibrary';
import PreviewPanel, { parsePreviewContent, PreviewContent } from './components/Preview/PreviewPanel';
import PreviewSkeleton, { shouldShowPreviewSkeleton } from './components/Preview/PreviewSkeleton';
import type { RunStep } from './components/Preview/RunTimeline';
import { parseUiDocument, extractDocSummary } from './utils/surfaceAdapter';
import type { Surface } from '../../shared/a2ui';
import { getSessionPreview } from './db/sessionApi';
import { useThemeStore } from '../../store/theme';
import ChatMcpDialog from './components/Chat/ChatMcpDialog';
import './chat.css';

export interface TraceEntry {
  label: string;
  sublabel?: string;
  durationMs?: number;
  source?: string;
  kind: 'tool_call' | 'tool_result' | 'event';
  detail?: string;
  timestamp: number;
  matchKey?: string;
}

/**
 * Build a stable key that pairs a tool_usage (start) event with its matching
 * `<tool>_run` (result) event, so they can be rendered as a single pill.
 */
export function toolMatchKey(name: unknown, args: unknown): string {
  const n = String(name || '').toLowerCase().replace(/[_\s]/g, '');
  let parsed: Record<string, unknown> = {};
  try {
    if (typeof args === 'string' && args.trim()) {
      parsed = JSON.parse(args.replace(/'/g, '"'));
    } else if (args && typeof args === 'object') {
      parsed = args as Record<string, unknown>;
    }
  } catch { /* ignore */ }
  const vals = Object.values(parsed)
    .filter((v) => typeof v === 'string' || typeof v === 'number')
    .join('|');
  return `${n}::${vals}`;
}

export function summarizeArgs(args: unknown): string | undefined {
  if (!args) return undefined;
  const clip = (s: string): string => (s.length > 80 ? `${s.slice(0, 80)}…` : s);
  let raw: unknown = args;
  if (typeof args === 'string') {
    try {
      raw = JSON.parse(args.replace(/'/g, '"'));
    } catch {
      // A non-JSON string is already a plain value (e.g. a bare query).
      const s = args.trim();
      return s ? clip(s) : undefined;
    }
  }
  // A bare list of strings (e.g. the URLs a reader visited) → "N pages".
  if (Array.isArray(raw)) {
    const items = raw.filter((x) => typeof x === 'string' && String(x).trim()).map((x) => String(x).trim());
    if (items.length === 1) return clip(items[0]);
    if (items.length > 1) return `${items.length} pages`;
    return undefined;
  }
  if (!raw || typeof raw !== 'object') return undefined;
  const parsed = raw as Record<string, unknown>;
  // Surface the ONE meaningful field a human cares about — the query / question /
  // topic / url — instead of dumping EVERY argument value as a CSV (which reads as
  // ", 10, 30, Switzerland news today, CH, moderate, …" to a non-technical user).
  const lower: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(parsed)) lower[k.toLowerCase()] = v;
  const PREFERRED = ['query', 'search_query', 'searchquery', 'q', 'question', 'prompt', 'search', 'topic', 'text', 'url', 'urls', 'website_url', 'task'];
  for (const key of PREFERRED) {
    const v = lower[key];
    if (typeof v === 'string' && v.trim()) return clip(v.trim());
    if (Array.isArray(v)) {
      const items = v.filter((x) => typeof x === 'string' && String(x).trim()).map((x) => String(x).trim());
      if (items.length === 1) return clip(items[0]);
      if (items.length > 1) return `${items.length} pages`;
    }
  }
  // Fallback: the single longest string value (the substantive one), not a CSV.
  const strings = Object.values(parsed).filter((v) => typeof v === 'string' && (v as string).trim()) as string[];
  if (!strings.length) return undefined;
  return clip(strings.reduce((a, b) => (b.length > a.length ? b : a)).trim());
}

/**
 * Turn a raw SSE trace event into a clean entry for the chat. Returns null
 * when the event is pure noise (LLM retries, token fragments, internal IDs).
 */
export function buildTraceEntry(
  message: string,
  data?: Record<string, unknown>,
): TraceEntry | null {
  const eventType = (data?.event_type as string) || '';
  const eventSource = (data?.event_source as string) || '';
  // `output` (and its nested `extra_data`) is a JSON column. Over the SSE path
  // (local dev) it arrives as a dict; over the REST polling fallback on
  // Postgres/asyncpg (Lakebase, deployed) the same column comes back as a JSON
  // STRING. Parse strings so memory + tool content render identically on both —
  // otherwise polled traces (the only transport on Databricks Apps, where SSE
  // is dead) would show no memory/tool results.
  const asObject = (v: unknown): Record<string, unknown> => {
    if (typeof v === 'string') {
      try {
        const parsed = JSON.parse(v);
        return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : {};
      } catch {
        return {};
      }
    }
    return (v as Record<string, unknown>) || {};
  };
  const output = asObject(data?.output);
  const extra = asObject(output.extra_data);
  const metadata = asObject(data?.trace_metadata);
  const num = (o: Record<string, unknown>, k: string): number | undefined =>
    typeof o[k] === 'number' ? (o[k] as number) : undefined;
  // Prefer the backend's explicit duration; for MEMORY recall the real time lives
  // in trace_metadata (query_time_ms / retrieval_time_ms), matching the Job-History
  // trace. Without this, long memory reads (10–16s) showed as 0.0s in the timeline.
  const durationMs =
    num(output, 'duration_ms')
    ?? num(metadata, 'duration_ms')
    ?? num(metadata, 'query_time_ms')
    ?? num(metadata, 'retrieval_time_ms')
    ?? num(metadata, 'save_time_ms');
  const now = Date.now();

  // Hard-filter known noise events.
  if (eventType === 'llm_retry' || eventType === 'task_started') return null;

  // Tool/MCP failures (e.g. HTTP 403 connecting to a selected MCP server).
  // The run continues without those tools, so surface the error in the
  // activity instead of leaving it buried in backend logs.
  if (eventType === 'tool_error') {
    const errorText =
      (typeof output.content === 'string' && output.content.trim()) ||
      (typeof output.error === 'string' && output.error.trim()) ||
      message.trim() ||
      'Tool error';
    return {
      kind: 'event',
      label: errorText.length > 80 ? `⚠ ${errorText.slice(0, 77)}…` : `⚠ ${errorText}`,
      detail: errorText.length > 80 ? errorText : undefined,
      source: eventSource || undefined,
      timestamp: now,
    };
  }

  // Tool invocation (start of a call).
  if (eventType === 'tool_usage') {
    const toolName = (extra.tool_name as string) || 'tool';
    const rawArgs = (extra.tool_args as string) || '';
    return {
      kind: 'tool_call',
      label: toolName,
      sublabel: summarizeArgs(rawArgs),
      source: eventSource || undefined,
      detail: rawArgs || undefined,
      timestamp: now,
      matchKey: toolMatchKey(toolName, rawArgs),
    };
  }

  // Tool result event — backend names them like `perplexitytool_run`,
  // `scrapewebsitetool_run`, etc.
  if (eventType.endsWith('_run')) {
    const toolName = (output.tool_name as string) || eventType.replace(/_run$/, '');
    const content = typeof output.content === 'string' ? (output.content as string) : '';
    const input = output.input;
    return {
      kind: 'tool_result',
      label: toolName,
      sublabel: summarizeArgs(input),
      durationMs,
      source: eventSource || undefined,
      detail: content || undefined,
      timestamp: now,
      matchKey: toolMatchKey(toolName, input),
    };
  }

  // Memory retrieval — surface the RETRIEVED memories (the context), not just a
  // "searching…" ping. CrewAI's recall emits these with the matched memories in
  // output.content; without this branch they fall to the generic handler below
  // and the context is hidden (or dropped as JSON noise). The matching "Search
  // memory" tool result still shows the empty case on its own.
  if (eventType === 'memory_retrieval' || eventType === 'memory_retrieval_completed') {
    const content = typeof output.content === 'string' ? (output.content as string).trim() : '';
    const foundNothing = !content || /no relevant memories|no memories found|^\[\]$/i.test(content);
    if (foundNothing) return null; // nothing retrieved — don't add a redundant pill
    // For memory recall the REAL time is the query/retrieval time in metadata —
    // output.duration_ms is a tiny unrelated value, so it must NOT win here (that
    // made long recalls show 0.0s). Matches the Job-History trace's "Memory Read".
    const memoryDurationMs =
      num(metadata, 'query_time_ms') ?? num(metadata, 'retrieval_time_ms') ?? durationMs;
    return {
      kind: 'tool_result',
      // Same label so consecutive recalls group under one "Memory" line.
      label: 'Memory',
      sublabel: 'context retrieved',
      durationMs: memoryDurationMs,
      source: eventSource || undefined,
      detail: content,
      timestamp: now,
    };
  }

  // Strip raw JSON dumps and single-token fragments that have no useful label.
  const trimmed = message.trim();
  if (!trimmed) return null;
  // Raw JSON event payload — useless without parsing.
  if (trimmed.startsWith('{') && /"id":\s*\d+/.test(trimmed)) return null;
  // Single-word / sub-token fragments from agent streaming ("_usage", "s.", "Read").
  if (trimmed.length < 12 && !/\s/.test(trimmed)) return null;
  // Generic "Calling tools." status pings.
  if (/^calling tool/i.test(trimmed) && trimmed.length <= 30) return null;

  return {
    kind: 'event',
    label: trimmed.length > 80 ? trimmed.slice(0, 80) + '…' : trimmed,
    detail: trimmed.length > 80 ? trimmed : undefined,
    timestamp: now,
  };
}

/**
 * Rebuild the run-activity steps from the DURABLE execution traces (the
 * /traces/job rows), running each through {@link buildTraceEntry} — the SAME
 * mapping the live SSE path uses. This lets a refreshed session restore the full
 * tool context (memory recalls, SQL/Genie result tables, …) straight from the
 * database, independent of whatever the per-message copy retained.
 */
export function tracesToRunSteps(
  traces: { id?: number; event_type?: string; output?: unknown; trace_metadata?: unknown; event_source?: string }[],
): { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] {
  // Keep EVERY labeled kind (tool_result / event / tool_call) — anything the
  // user watched stream by must survive the restore. Unlike the live message
  // path (where a pending tool_call is PROMOTED in place to its result), the
  // durable trace rows keep BOTH the tool_usage start row and its `*_run`
  // result row — so suppress a tool_call whose label also has a result row,
  // keeping only genuinely dangling calls (tool started, no result recorded).
  const entries: { entry: NonNullable<ReturnType<typeof buildTraceEntry>>; id: string }[] = [];
  traces.forEach((t, idx) => {
    const entry = buildTraceEntry('', t as unknown as Record<string, unknown>);
    if (!entry || !entry.label) return;
    entries.push({ entry, id: `trace-${t.id ?? idx}` });
  });
  const resultLabels = new Set(
    entries.filter(({ entry }) => entry.kind === 'tool_result').map(({ entry }) => entry.label),
  );
  const steps: { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] = [];
  entries.forEach(({ entry, id }, idx) => {
    if (entry.kind === 'tool_call' && resultLabels.has(entry.label)) return;
    steps.push({
      id,
      label: entry.label,
      sublabel: entry.sublabel,
      detail: entry.detail,
      durationMs: entry.durationMs,
      timestamp: idx,
    });
  });
  return steps;
}

/**
 * The LATEST run segment's activity steps from the persistent chat trace
 * messages (everything after the last user message). Shows EVERY labeled step —
 * tool results AND tool calls / events — with no pruning/dedup: the user wants
 * each step the agent ran, even when two look similar (repeated memory recalls
 * or Genie queries sharing a SQL prefix), and anything shown while the run
 * streamed must not vanish when it ends (a pending tool_call message is
 * promoted in place to its result, so a call and its result never appear twice).
 */
export function deriveMessageActivitySteps(
  messages: { id?: string; role: string; resultType?: string; resultData?: unknown }[],
): { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] {
  let start = 0;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === 'user') { start = i + 1; break; }
  }
  const steps: { id: string; label: string; sublabel?: string; detail?: string; durationMs?: number; timestamp: number }[] = [];
  messages.slice(start).forEach((m, idx) => {
    if (m.resultType !== 'trace') return;
    const t = m.resultData as Partial<TraceEntry> | undefined;
    if (!t || !t.label) return;
    steps.push({
      id: m.id || `step-${idx}`,
      label: t.label,
      sublabel: t.sublabel,
      detail: t.detail,
      durationMs: t.durationMs,
      timestamp: t.timestamp ?? idx,
    });
  });
  return steps;
}

/**
 * Which step list the activity views show: the durable DB-restored steps when
 * available (complete + survives a refresh), else the per-message steps (the
 * live source during a run) — but NEVER a swap that SHRINKS the visible list.
 * The messages hold everything the user watched stream by, so if the restored
 * rows reproduce fewer steps, the richer message-derived set wins.
 */
export function pickRunActivitySteps<T>(restored: T[] | undefined, messageSteps: T[]): T[] {
  if (!restored || !restored.length) return messageSteps;
  return restored.length >= messageSteps.length ? restored : messageSteps;
}

/**
 * Distill a task output into a concise chat-message body. Returns null when
 * the output is pure progress-noise that shouldn't appear in chat.
 */
/**
 * Produce a concise, human-friendly label for a task-output chat message.
 *
 * Traces report the task *name* as the full (interpolated) task description, so
 * a refine surfaces the entire "Improve the artifact below… CURRENT ARTIFACT:
 * <!DOCTYPE html>…" prompt — which dumps the whole artifact into the chat. We
 * special-case the refine prompt to "Refined artifact" and otherwise collapse
 * any over-long description to its first line, truncated.
 */
export function cleanTaskLabel(taskName: string): string {
  const name = (taskName || '').trim();
  if (!name) return 'Task';
  // The refine editor task description always starts with this sentinel.
  if (/^Improve the artifact below based on this instruction/i.test(name)) {
    return 'Refined artifact';
  }
  const firstLine = name.split('\n')[0].trim();
  return firstLine.length > 80 ? `${firstLine.slice(0, 80).trim()}…` : firstLine;
}

export function summarizeTaskOutput(
  raw: string,
  preview: PreviewContent | null,
): string | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;

  // Status noise like "Calling tools.", "Thinking...", "Using tool X" — skip.
  if (
    trimmed.length <= 120 &&
    /^(calling tool|thinking|processing|searching|using tool|executing|agent (started|thinking|finished)|tool call)/i.test(trimmed)
  ) {
    return null;
  }

  if (preview) {
    // Prefer the model's own one-liner (top-level "summary" in the A2UI doc);
    // fall back to the generic line when it didn't supply one.
    return extractDocSummary(trimmed) || 'Generated an app. View it in the preview pane.';
  }

  // Belt-and-suspenders: even if the surface wasn't extracted as a preview,
  // NEVER dump raw A2UI JSON into the chat. Strip any embedded UI document and
  // keep only the surrounding prose; if the doc markers still remain (it didn't
  // parse cleanly), collapse to the friendly line instead of the JSON blob.
  if (trimmed.includes('createSurface') || trimmed.includes('updateComponents')) {
    const prose = stripEmbeddedUiDocument(trimmed);
    if (prose.includes('createSurface') || prose.includes('updateComponents')) {
      return 'Generated an app. View it in the preview pane.';
    }
    return prose.length > 400 ? `${prose.slice(0, 300).trim()}…` : prose;
  }

  // Long plain-text outputs get collapsed too, otherwise they take over the chat.
  if (trimmed.length > 400) {
    return `${trimmed.slice(0, 300).trim()}…`;
  }

  return trimmed;
}

/**
 * Strip an embedded A2UI document (createSurface/updateComponents payload)
 * from a final-answer text. The surface is rendered in the preview pane —
 * dumping its raw JSON into the chat hard-confuses business users. Keeps the
 * surrounding prose; falls back to a friendly line when nothing else remains.
 */
export function stripEmbeddedUiDocument(text: string): string {
  if (!text || (!text.includes('createSurface') && !text.includes('updateComponents'))) {
    return text;
  }
  // The model's own chat one-liner (top-level "summary" in the doc), if present;
  // else the generic line. Used for both the whole-doc and embedded-doc cases.
  const friendly = extractDocSummary(text) || 'Generated an app. View it in the preview pane.';
  // The common case: the WHOLE payload IS the UI document (no surrounding prose).
  // The preview pane renders it, so collapse the chat to the friendly line rather
  // than surgically excising brackets — brittle for a weak model's slightly
  // malformed JSON. parseUiDocument is repair-tolerant (coerceJson rebalances
  // mismatched brackets), so e.g. gpt-5-nano's invalid A2UI is recognised here
  // too. Gated on the text STARTING with the document so a genuine prose answer
  // that merely embeds a doc keeps its prose (handled by the per-block logic below).
  const whole = text.trim();
  if ((whole.startsWith('{') || whole.startsWith('[')) && parseUiDocument(whole)) {
    return friendly;
  }
  let cleaned = text;
  // 1. Remove fenced ```json blocks that parse as a UI document.
  cleaned = cleaned.replace(/```(?:json)?\s*([\s\S]*?)```/g, (match, inner) =>
    parseUiDocument(inner.trim()) ? '' : match,
  );
  // 2. Remove a bare (unfenced) JSON document — from the first '{' that opens
  //    a parseable UI document through its matching closing brace.
  if (cleaned.includes('createSurface') || cleaned.includes('updateComponents')) {
    const start = cleaned.indexOf('{');
    if (start >= 0) {
      let depth = 0;
      for (let i = start; i < cleaned.length; i++) {
        if (cleaned[i] === '{') depth++;
        else if (cleaned[i] === '}') {
          depth--;
          if (depth === 0) {
            const candidate = cleaned.slice(start, i + 1);
            if (parseUiDocument(candidate)) {
              cleaned = cleaned.slice(0, start) + cleaned.slice(i + 1);
            }
            break;
          }
        }
      }
    }
  }
  // Drop a now-orphaned "json" fence label and tidy whitespace.
  cleaned = cleaned.replace(/(^|\n)\s*json\s*(\n|$)/g, '$1').replace(/\n{3,}/g, '\n\n').trim();
  return cleaned || friendly;
}

/**
 * Extract the final answer text from the various result shapes the backend
 * returns ("text", {result}, {result:{result}}, {content}, {output}, {value}…).
 * Shared by the live SSE completion path AND the REST polling fallback so both
 * render the answer identically.
 */
export function extractResultText(data: Record<string, unknown>): string {
  let resultText = '';
  try {
    const rawResult = data.result;
    if (typeof rawResult === 'string') {
      try {
        const parsed = JSON.parse(rawResult);
        if (parsed && typeof parsed === 'object') {
          resultText = (typeof parsed.result === 'string' ? parsed.result : '')
            || (typeof parsed.content === 'string' ? parsed.content : '')
            || (typeof parsed.value === 'string' ? parsed.value : '')
            // The composed light/crew envelope is { text, a2ui } — the chat shows
            // `text`; the a2ui surface is pulled out separately (extractA2uiSurface).
            || (typeof parsed.text === 'string' ? parsed.text : '')
            || rawResult;
        } else {
          resultText = rawResult;
        }
      } catch {
        resultText = rawResult;
      }
    } else if (rawResult && typeof rawResult === 'object') {
      const nested = rawResult as Record<string, unknown>;
      // `nested.text` covers the composed { text, a2ui } envelope (the a2ui surface
      // is extracted separately by extractA2uiSurface).
      const inner = nested.result ?? nested.content ?? nested.raw ?? nested.value ?? nested.text;
      if (typeof inner === 'string') {
        resultText = inner;
      } else if (inner && typeof inner === 'object') {
        const deepContent = (inner as Record<string, unknown>).content;
        if (typeof deepContent === 'string') {
          resultText = deepContent;
        } else {
          resultText = JSON.stringify(inner);
        }
      } else {
        resultText = JSON.stringify(nested);
      }
    }
    if (!resultText && typeof data.content === 'string') {
      resultText = data.content;
    }
    if (!resultText) {
      const output = data.output;
      resultText = typeof output === 'string' ? output : '';
    }
  } catch {
    resultText = '';
  }
  // The preview pane renders A2UI surfaces — never show their raw JSON in chat.
  return stripEmbeddedUiDocument(resultText);
}

/**
 * Pull the composed A2UI surface out of a completion payload, if any. The
 * light/crew runners persist a rich answer as { text, a2ui } (a plain chat turn
 * stays a bare string), so this returns the surface for inline rendering or null
 * when the turn is plain prose. Tolerates the result arriving as a JSON string
 * or nested one level (result.result), matching extractResultText's unwrapping.
 */
export function extractA2uiSurface(data: Record<string, unknown>): Surface | null {
  try {
    let result: unknown = data.result;
    if (typeof result === 'string') {
      try {
        result = JSON.parse(result);
      } catch {
        return null;
      }
    }
    if (!result || typeof result !== 'object') return null;
    const obj = result as Record<string, unknown>;
    const nested =
      obj.result && typeof obj.result === 'object'
        ? (obj.result as Record<string, unknown>)
        : null;
    const candidate = obj.a2ui ?? nested?.a2ui;
    if (
      candidate &&
      typeof candidate === 'object' &&
      'surfaceKind' in candidate &&
      'components' in candidate
    ) {
      return candidate as Surface;
    }
  } catch {
    /* a malformed surface must never break completion */
  }
  return null;
}

const ChatWorkspace: React.FC = () => {
  // --- Zustand Stores ---
  const sessions = useSessionStore((s) => s.sessions);
  const currentSessionId = useSessionStore((s) => s.currentSessionId);
  const messages = useSessionStore((s) => s.messages);
  // True until init() finishes restoring a persisted session — holds the empty
  // "new chat" greeting so a refresh doesn't flash it before the chat loads.
  const hydrating = useSessionStore((s) => s.hydrating);
  const addMessage = useSessionStore((s) => s.addMessage);
  const addMessageToTargetSession = useSessionStore((s) => s.addMessageToTargetSession);
  const updateMessage = useSessionStore((s) => s.updateMessage);
  const updateMessageInTargetSession = useSessionStore((s) => s.updateMessageInTargetSession);
  const clearMessages = useSessionStore((s) => s.clearMessages);

  const isExecuting = useExecutionStore((s) => s.isExecuting);
  const isGenerating = useExecutionStore((s) => s.isGenerating);
  const isLoading = useExecutionStore((s) => s.isLoading);
  const executionContext = useExecutionStore((s) => s.executionContext);
  const rawPreviewContent = useExecutionStore((s) => s.previewContent);
  const previewOwnerSessionId = useExecutionStore((s) => s.previewOwnerSessionId);
  const previewHistory = useExecutionStore((s) => s.previewHistory);
  const previewIndex = useExecutionStore((s) => s.previewIndex);
  const navigatePreview = useExecutionStore((s) => s.navigatePreview);
  // The side preview pane is opt-in: closed by default even when a deliverable
  // exists (it renders inline in the chat), opened via a surface's "expand"
  // control or the reopen pill.
  const previewPaneOpen = useExecutionStore((s) => s.previewPaneOpen);
  const chatCollapsed = useExecutionStore((s) => s.chatCollapsed);
  // Where the run activity ("thinking" stream) is shown: the preview pane (default)
  // or collapsed into the chat's "Working…" bar (expandable). A persisted choice.
  const activityPlacement = useExecutionStore((s) => s.activityPlacement);
  const activityInChat = activityPlacement === 'chat';
  // When the user opens a SPECIFIC run in the pane via its "Show in panel" icon,
  // these are that run's steps — shown in the pane instead of the latest run's, so
  // a historical run's pane shows ITS OWN activity. Cleared on close / session
  // switch / when a new live run starts (so the pane tracks the live run again).
  const [focusedRunSteps, setFocusedRunSteps] = useState<RunStep[] | null>(null);
  // …and when the user clicks an individual step ROW in a run's expanded
  // timeline, the pane opens directly on THAT step's content (master→detail
  // pre-selected). Cleared together with focusedRunSteps.
  const [focusedRunStep, setFocusedRunStep] = useState<RunStep | null>(null);
  // Memory mode (workspace vs session) is owned by the store so it persists
  // across the empty→conversation input swap (local state would reset to ON).
  const memoryEnabled = useExecutionStore((s) => s.memoryEnabled);
  const setMemoryEnabled = useExecutionStore((s) => s.setMemoryEnabled);

  // Render-time isolation guard: only show a preview that belongs to the
  // session currently on screen. This is the backstop that prevents a preview
  // produced by a job in another session (e.g. a late SSE completion after the
  // user switched chats) from leaking into the session being viewed.
  const previewContent =
    rawPreviewContent && previewOwnerSessionId === currentSessionId
      ? rawPreviewContent
      : null;

  // Execution UI (the "Running crew…" banner, generation spinner, loading
  // state) belongs to the session that OWNS the run. A run started in one
  // session must never surface in whatever session is on screen now — e.g. you
  // submit in chat A, switch to B, and A's crew starts: it must stay in A.
  // Strict equality so a run owned by another session never leaks here.
  const executionOwnerSessionId = useExecutionStore((s) => s.executionOwnerSessionId);
  const ownsExecution = executionOwnerSessionId === currentSessionId;
  const viewIsExecuting = isExecuting && ownsExecution;
  const viewIsGenerating = isGenerating && ownsExecution;
  const viewIsLoading = isLoading && ownsExecution;
  const viewExecutionContext = ownsExecution ? executionContext : null;

  // Drop the focused-run pin when the viewed session changes (the pinned steps
  // belong to the other session's run).
  useEffect(() => {
    setFocusedRunSteps(null);
    setFocusedRunStep(null);
  }, [currentSessionId]);
  // …and when a NEW live run starts (rising edge), so the pane stops pinning a
  // past run and tracks the fresh one. A run finishing does NOT clear the pin.
  const prevExecutingRef = useRef(false);
  useEffect(() => {
    if (viewIsExecuting && !prevExecutingRef.current) {
      setFocusedRunSteps(null);
      setFocusedRunStep(null);
    }
    prevExecutingRef.current = viewIsExecuting;
  }, [viewIsExecuting]);

  // Open a specific run in the side preview pane: its deliverable (A2UI surface or
  // the plain-text answer) with that run's activity. The pane is opt-in — this is
  // the ONLY way it opens for a chat run, and it fires only on the user's click.
  const handleShowRunInPane = useCallback((deliverable: PreviewContent | undefined, steps: RunStep[], focusStep?: RunStep) => {
    const st = useExecutionStore.getState();
    setFocusedRunSteps(steps.length ? steps : null);
    // A step ROW click opens the pane directly on that step's content; the
    // per-run pane icon (no focusStep) opens the run normally.
    setFocusedRunStep(focusStep ?? null);
    st.setActivityPlacement('preview');
    if (deliverable) {
      st.openPreviewPane(deliverable);
    } else {
      // No previewable deliverable (a run that only has activity): show the
      // activity alone — clear any stale content so the skeleton, not a prior
      // run's deliverable, fills the pane.
      st.setPreviewContent(null);
      st.openPreviewPane();
    }
  }, []);

  // The run-activity timeline shown in the preview pane (live skeleton AND
  // collapsed above the finished result). Sourced from the PERSISTENT chat trace
  // messages — the latest run's steps — so it survives the run finishing (unlike
  // the ephemeral live feed). Each trace message's resultData carries the
  // label / query / context the step pulled in.
  const messageActivitySteps = useMemo(() => deriveMessageActivitySteps(messages), [messages]);

  // The latest run's job id (a crew_actions / result message carries `executionId`)
  // — used to restore the activity from the durable execution traces on refresh.
  const latestRunJobId = useMemo(() => {
    for (let i = messages.length - 1; i >= 0; i--) {
      const e = messages[i].executionId;
      if (e) return e;
    }
    return undefined;
  }, [messages]);

  // Run activity restored from the PERSISTED execution traces, keyed by job id —
  // the durable, complete source (a refresh can lose the per-message copy).
  const [restoredStepsByJob, setRestoredStepsByJob] = useState<Record<string, ReturnType<typeof tracesToRunSteps>>>({});
  const fetchedTraceJobsRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    // Only restore for a FINISHED run we're viewing — a live run streams its own
    // steps into the messages; the traces are fetched once it has settled.
    if (!latestRunJobId || viewIsExecuting) return;
    if (fetchedTraceJobsRef.current.has(latestRunJobId)) return;
    fetchedTraceJobsRef.current.add(latestRunJobId);
    let cancelled = false;
    (async () => {
      try {
        const traces = await getJobTraces(latestRunJobId);
        if (cancelled) return;
        const steps = tracesToRunSteps(traces);
        if (steps.length) setRestoredStepsByJob((prev) => ({ ...prev, [latestRunJobId]: steps }));
      } catch {
        /* best-effort: fall back to the per-message steps */
      }
    })();
    return () => { cancelled = true; };
  }, [latestRunJobId, viewIsExecuting]);

  // Durable restored steps vs the per-message live source — see
  // pickRunActivitySteps (the swap may never shrink the visible list).
  const runActivitySteps = useMemo(
    () => pickRunActivitySteps(latestRunJobId ? restoredStepsByJob[latestRunJobId] : undefined, messageActivitySteps),
    [restoredStepsByJob, latestRunJobId, messageActivitySteps],
  );

  // When the user routes activity to the pane ('preview' placement), the pane
  // shows the run-activity surface. It appears immediately during a live run
  // (shouldShowPreviewSkeleton) instead of staying blank, AND it survives the
  // prompt ending: once the run finishes we still have its steps, so the
  // expanded activity keeps showing rather than vanishing. In 'chat' placement
  // the activity lives in the chat's Working bar, so the pane stays out until a
  // real deliverable exists. A finished deliverable (previewContent) always wins
  // the pane — the skeleton never competes with it.
  // The pane is OPT-IN: it expands only when the user has opened it
  // (previewPaneOpen). A live run no longer force-expands the pane — run
  // activity stays in the chat's Working bar until the user opens the pane.
  const showPreviewSkeleton =
    previewPaneOpen &&
    !activityInChat &&
    !previewContent &&
    (shouldShowPreviewSkeleton({ runActive: viewIsExecuting, hasPreview: !!previewContent }) ||
      (focusedRunSteps ?? runActivitySteps).length > 0);
  // Opt-in: the deliverable pane shows only when the user opened it (a deliverable
  // alone no longer forces it open). The run skeleton still shows when activity is
  // routed to the pane.
  const previewPaneVisible = (previewPaneOpen && !!previewContent) || showPreviewSkeleton;

  const models = useAppStore((s) => s.models);
  const selectedModel = useAppStore((s) => s.selectedModel);
  const sidebarOpen = useAppStore((s) => s.sidebarOpen);

  // --- Local UI state (sidebar-only concerns) ---
  const [contextMenu, setContextMenu] = useState<{ sessionId: string; x: number; y: number } | null>(null);
  // Saved-catalog library shown in the rail (replaces /list crews & /list flows).
  // Lives in the Zustand appStore so it's shared + refreshed consistently.
  const libraryCrews = useAppStore((s) => s.savedCrews);
  const libraryFlows = useAppStore((s) => s.savedFlows);
  const refreshLibrary = useAppStore((s) => s.loadCatalog);
  // A crew/flow loaded from the catalog that the chat submit button will run.
  // Session-scoped so it only applies to the session it was loaded into.
  const [pendingRun, setPendingRun] = useState<{ sessionId: string | null; label: string; run: () => void } | null>(null);
  const [renamingSessionId, setRenamingSessionId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState('');
  // MCP config dialog opened from the composer's "+" picker ("Connect a tool").
  const [mcpConfigOpen, setMcpConfigOpen] = useState(false);

  // Input variables dialog state
  const [pendingExecution, setPendingExecution] = useState<{
    type: 'crew' | 'generated';
    plan?: PlanData;
    data?: GenerationCompleteData;
    spaceId?: string;
    originSession?: string | null;
  } | null>(null);

  // Sync chat theme from Kasal's theme store (dark-mode toggle).
  const kasalIsDarkMode = useThemeStore((s) => s.isDarkMode);
  // The chat-scoped theme (drives the sidebar dark-mode toggle — flips instantly,
  // no page reload, persisted to localStorage by appStore).
  const chatThemeIsDark = useAppStore((s) => s.theme) === 'dark';
  useEffect(() => {
    useAppStore.getState().setTheme(kasalIsDarkMode ? 'dark' : 'light');
  }, [kasalIsDarkMode]);

  // --- Initialize stores on mount ---
  useEffect(() => {
    useAppStore.getState().init();
    // Apply Kasal's current theme to the chat container immediately on mount.
    useAppStore.getState().setTheme(useThemeStore.getState().isDarkMode ? 'dark' : 'light');
    useAppStore.getState().loadModels();
    useAppStore.getState().loadTools();
    useSessionStore.getState().init().then(() => {
      const sessionId = useSessionStore.getState().currentSessionId;
      if (sessionId) {
        useExecutionStore.getState().restoreSessionState(sessionId);
      }
    });
  }, []);

  // Chat sessions are per workspace. When the user switches workspace (the
  // group store fires 'group-changed'), re-list sessions for the new group and
  // rehydrate that group's active session — so the sidebar + chat only ever
  // show the current workspace's conversations.
  useEffect(() => {
    const onGroupChange = () => {
      void useSessionStore.getState().reloadForGroup().then(() => {
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) {
          useExecutionStore.getState().restoreSessionState(sid);
        } else {
          useExecutionStore.getState().resetForSession();
        }
      });
      void refreshLibrary();
    };
    window.addEventListener('group-changed', onGroupChange);
    return () => window.removeEventListener('group-changed', onGroupChange);
  }, [refreshLibrary]);

  // Populate the catalog library (rail) on mount. It's refreshed on workspace
  // change (above), after each chat save (handleSaveCrew / /save), and after
  // agent-builder saves (SaveCrew calls useAppStore.getState().loadCatalog()).
  useEffect(() => {
    void refreshLibrary();
  }, [refreshLibrary]);

  // Collapse paired tool events (tool_usage + matching *_run) into a single
  // chat pill keyed by matchKey, regardless of which order they arrive in.
  const traceMessageIdsRef = useRef<Map<string, { messageId: string; resolved: boolean }>>(
    new Map(),
  );
  // Trace DB ids already rendered this run. The live SSE stream and the REST
  // polling fallback (Job-History style) can both deliver the same trace; this
  // guarantees each trace renders exactly once regardless of transport.
  const seenTraceIdsRef = useRef<Set<number>>(new Set());
  // jobId → the session that STARTED that job. Every trace/output carries its
  // job_id, so we attribute it to the right session even when runs overlap or
  // the user switched away — instead of trusting the single global "current
  // owner" slot, which mis-routes one session's output into another.
  const jobOwnerRef = useRef<Map<string, string>>(new Map());
  // The job currently bound to the single live SSE stream (startStream replaces,
  // so only the latest foreground run streams here). The SSE onComplete/onError
  // carry no job id, so we stamp completion with this; backgrounded runs finalize
  // via the REST poller's window events, which DO carry an explicit job id.
  const sseJobIdRef = useRef<string | undefined>(undefined);
  // Origin session per in-flight generation, keyed by generationId. Generations
  // run as concurrent streams, so every trace / completion / execution-start
  // routes by the generation's OWN origin — never a single global owner, which
  // cross-contaminated run-activity traces between parallel sessions.
  const genOriginRef = useRef<Map<string, string>>(new Map());
  // The most recent generated crew in this session — the target for `/save`.
  // (The bookmark on each crew card saves its own specific crew directly.)
  const lastGeneratedRef = useRef<GenerationCompleteData | null>(null);
  // The chat prompt that triggered the in-flight generation — attached to the
  // generation result so the executed run answers the user's actual request.
  const lastUserPromptRef = useRef<string>('');

  // Render a task's output: surface previewable content in the preview pane
  // (scoped to the owning session) and append a concise chat message. Shared by
  // the live SSE stream and the REST polling fallback.
  const handleTaskOutput = useCallback((taskName: string, output: string, ownerSession: string | null) => {
    const execState = useExecutionStore.getState();
    const sessionStore = useSessionStore.getState();

    // Try to extract renderable content from various result shapes
    let displayContent = output;
    try {
      // The output may be JSON with a "content" field wrapping the actual result
      const parsed = typeof output === 'string' && output.trim().startsWith('{')
        ? JSON.parse(output) as Record<string, unknown>
        : null;
      if (parsed?.content && typeof parsed.content === 'string') {
        displayContent = parsed.content;
      }
    } catch { /* not JSON, use raw */ }

    // Check if the content is previewable (HTML, structured markdown, A2UI
    // surface, …). A UI document renders as a dashboard in the PREVIEW pane.
    const preview = parsePreviewContent(displayContent);
    const currentSession = sessionStore.currentSessionId;
    if (preview) {
      if (currentSession === ownerSession) {
        execState.setPreviewContent(preview);
      } else if (ownerSession) {
        // This run's session is off screen — park the preview into ITS snapshot
        // (not the live slot, which belongs to whatever's on screen now) so it's
        // there on switch-back. Without this the preview reached only IndexedDB
        // and a later null-preview completion snapshot hid it.
        execState.stashSessionPreview(ownerSession, preview);
      }
      // NOT persisted per task output: each PUT re-uploaded the full artifact
      // (multi-100KB surfaces) mid-run. Durable persistence happens ONCE at
      // completion (executionStore.completeExecution) — and after a mid-run
      // refresh the deliverable derives from execution.result anyway.
    }

    // Build a concise chat-message body. Raw task output (HTML dumps, status
    // pings like "Calling tools.", or echoed task descriptions) clutters the
    // chat; the real content lives in the preview pane.
    const chatBody = summarizeTaskOutput(displayContent, preview);
    if (chatBody !== null) {
      const msg = `**${cleanTaskLabel(taskName)}** — ${chatBody}`;
      if (ownerSession) {
        sessionStore.addMessageToTargetSession(ownerSession, 'assistant', msg);
      } else {
        sessionStore.addMessage('assistant', msg);
      }
    }

    // NOTE: we intentionally do NOT auto-complete the execution on a timer.
    // The crew keeps running in the UI (appending each task's output to the
    // preview) until the real completion/error arrives — via SSE or, when the
    // Databricks Apps HTTP/2 proxy kills SSE, via the polling fallback's
    // 'jobCompleted'/'jobFailed' window events.
  }, []);

  // Render a single trace event (tool pill / memory / task output) into the
  // chat. Deduped by trace DB id so the live SSE stream and the REST polling
  // fallback never render the same trace twice. This is THE seam that lets
  // memory + Genie + tool traces appear in chat even when SSE is dead
  // (Databricks Apps), exactly like crew-mode Job History does via polling.
  const processTrace = useCallback((message: string, data?: Record<string, unknown>) => {
    const traceId = data?.id;
    if (typeof traceId === 'number') {
      if (seenTraceIdsRef.current.has(traceId)) return;
      seenTraceIdsRef.current.add(traceId);
    }

    // Attribute this trace to the session that STARTED its job (from job_id),
    // not the session currently on screen — so a still-running run's output
    // never lands in the session you switched to.
    const jobId = data?.job_id as string | undefined;
    const ownerSession =
      (jobId && jobOwnerRef.current.get(jobId)) ||
      useExecutionStore.getState().executionOwnerSessionId;

    const trace = buildTraceEntry(message, data);
    if (trace) {
      // The run-activity timeline (preview pane) is derived from these trace
      // messages — see `runActivitySteps`. tool_result traces carry the step's
      // label / query / context the agent pulled in.
      const sessionStore = useSessionStore.getState();
      let handled = false;

      if (trace.matchKey) {
        const existing = traceMessageIdsRef.current.get(trace.matchKey);
        if (existing) {
          // Already have a pill for this key. The tool_result is the richer
          // event (has duration + content), so promote the pill to it; drop
          // any later tool_call for an already-resolved key.
          if (trace.kind === 'tool_result' && !existing.resolved) {
            // Re-send resultType too: the persistence layer OVERWRITES
            // generation_result with packExtras(updates), so omitting resultType
            // would drop it from the stored row — and on refresh the promoted
            // tool step would no longer be a 'trace' (its context vanishes from
            // the run activity). Keeping it here makes the tool context survive
            // a reload (it's restored from the persisted message, not just live).
            const updates = { resultType: 'trace', resultData: trace };
            if (ownerSession) {
              sessionStore.updateMessageInTargetSession(ownerSession, existing.messageId, updates);
            } else {
              sessionStore.updateMessage(existing.messageId, updates);
            }
            traceMessageIdsRef.current.set(trace.matchKey, {
              messageId: existing.messageId,
              resolved: true,
            });
          }
          handled = true;
        }
      }

      if (!handled) {
        const extra = { resultType: 'trace', resultData: trace };
        const id = ownerSession
          ? sessionStore.addMessageToTargetSession(ownerSession, 'assistant', '', extra)
          : sessionStore.addMessage('assistant', '', extra);
        if (trace.matchKey) {
          traceMessageIdsRef.current.set(trace.matchKey, {
            messageId: id,
            resolved: trace.kind === 'tool_result',
          });
        }
      }
    }

    // task_completed carries a task's full output — render it (preview + chat).
    if ((data?.event_type as string) === 'task_completed') {
      const metadata = data?.trace_metadata as Record<string, unknown> | undefined;
      const taskName = (metadata?.task_name as string) || (data?.event_context as string) || 'Task';
      const rawOutput = data?.output ?? data?.result ?? message;
      const taskOutput = typeof rawOutput === 'string' ? rawOutput : JSON.stringify(rawOutput);
      handleTaskOutput(taskName, taskOutput, ownerSession);
    }
  }, [handleTaskOutput]);

  // De-dupe completion across the SSE path and the polling fallback so a run is
  // finished exactly once (completeExecution/failExecution are NOT idempotent —
  // a double call double-posts the result and to the wrong session). Keyed by
  // job id; a no-op when there is no active job, so the callbacks still fire in
  // isolation.
  const finishedJobsRef = useRef<Set<string>>(new Set());
  const finishOnce = useCallback((jobId: string | undefined, run: () => void) => {
    // Key by the COMPLETING job's id (passed in), not the global slot's active
    // job — with parallel sessions the slot may hold a different (foreground)
    // run, and keying off it would let a backgrounded job slip the de-dupe or
    // block the wrong one.
    if (jobId) {
      if (finishedJobsRef.current.has(jobId)) return;
      finishedJobsRef.current.add(jobId);
    }
    run();
  }, []);
  // The bookmark/feedback actions row for the latest generated crew, parked
  // until that crew's run finishes — feedback only makes sense once the
  // result is visible. Cleared on post; a refine run never sets it.
  const pendingActionsRef = useRef<{ data: GenerationCompleteData; ownerSession: string | null; mode?: string; usedWorkspaceMemory?: boolean } | null>(null);
  const postPendingActionsRow = useCallback((jobId?: string) => {
    const pending = pendingActionsRef.current;
    if (!pending) return;
    pendingActionsRef.current = null;
    const sessionStore = useSessionStore.getState();
    // Anchor the row to this run's execution id so the actions bar can offer a
    // "Memory graph" link scoped to exactly this run's cognitive memory. Carry
    // the run's answer mode so the bar can hide crew-catalog actions for plain
    // 'chat' turns (there is no crew worth cataloging).
    const extra = {
      id: generateId(),
      resultType: 'crew_actions',
      // Carry the run's session so an answer-mode (chat) bar can distill a crew
      // from THIS conversation, even after the user switches sessions.
      resultData: {
        ...pending.data,
        chatModeType: pending.mode,
        sessionId: pending.ownerSession ?? useSessionStore.getState().currentSessionId,
      },
      executionId: jobId,
      // Per-run snapshot (captured at generation, not the live toggle) so the
      // "Memory graph" action only appears for runs that used workspace memory.
      usedWorkspaceMemory: pending.usedWorkspaceMemory,
    };
    if (pending.ownerSession) sessionStore.addMessageToTargetSession(pending.ownerSession, 'assistant', '', extra);
    else sessionStore.addMessage('assistant', '', extra);
  }, []);

  const completeExecutionOnce = useCallback((jobId: string | undefined, resultText: string, surface?: Surface | null) => {
    finishOnce(jobId, () => {
      const store = useExecutionStore.getState();
      // Only thread the surface arg when a rich one was composed — a plain chat
      // turn calls with the original (text, jobId) shape.
      if (surface) store.completeExecution(resultText, jobId, surface);
      else store.completeExecution(resultText, jobId);
      // Result is in — now surface the bookmark/feedback row beneath it.
      postPendingActionsRow(jobId);
    });
  }, [finishOnce, postPendingActionsRow]);
  const failExecutionOnce = useCallback((jobId: string | undefined, error: string) => {
    finishOnce(jobId, () => useExecutionStore.getState().failExecution(error, jobId));
  }, [finishOnce]);

  // --- Execution Stream ---
  const executionStream = useExecutionStream({
    onTrace: processTrace,
    onStatusChange: (status) => {
      useExecutionStore.getState().updateExecutionStatus(status as ExecutionStatus);
    },
    onComplete: (data) => {
      completeExecutionOnce(sseJobIdRef.current, extractResultText(data), extractA2uiSurface(data));
    },
    onError: (error) => {
      failExecutionOnce(sseJobIdRef.current, error);
    },
  });

  // REST polling fallback (Job-History style). The globally-mounted
  // useTracePolling (SSEConnectionManager) polls /traces + /executions for any
  // job announced via the 'jobCreated' event (dispatched in
  // handleStartExecutionStream) and re-emits the results as window events.
  // ChatMode consumes them here so memory/Genie/tool traces + completion show
  // up even when the Databricks Apps HTTP/2 proxy kills the SSE stream.
  useEffect(() => {
    const onTraceUpdate = (e: Event) => {
      const { jobId, trace } = (e as CustomEvent).detail || {};
      // Route by the job's OWNER, not the single live slot — like the job
      // completion events below. A backgrounded session's task output (which
      // carries the rendered deliverable) arrives here when SSE is dead
      // (Databricks Apps) or after its stream was closed by a newer run; gating
      // on the live `activeExecution` dropped it, so the preview was never
      // stashed into its snapshot and vanished on switch-back. jobOwnerOf returns
      // null once a job finalizes, so a late re-poll is still dropped.
      if (!jobId || !trace || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      const msg =
        (trace.message as string) ||
        (trace.trace as string) ||
        JSON.stringify(trace);
      processTrace(msg, trace as Record<string, unknown>);
    };
    // Route by the job's OWNER, not the single live slot: a backgrounded
    // session's run must still finalize (and land in ITS session) even though
    // the slot currently holds a different, foreground run. jobOwnerOf returns
    // null once a job has finalized, so this also drops late duplicates.
    const onJobCompleted = (e: Event) => {
      const { jobId, result } = (e as CustomEvent).detail || {};
      if (!jobId || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      completeExecutionOnce(jobId, extractResultText({ result }), extractA2uiSurface({ result }));
    };
    const onJobFailed = (e: Event) => {
      const { jobId, error } = (e as CustomEvent).detail || {};
      if (!jobId || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      failExecutionOnce(jobId, (error as string) || 'Execution failed');
    };
    const onJobStopped = (e: Event) => {
      const { jobId } = (e as CustomEvent).detail || {};
      if (!jobId || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      failExecutionOnce(jobId, 'Execution stopped');
    };
    // The poller hit a definitive 404 loop: the job's row no longer exists for
    // this workspace (deleted, or a different group). Abandon it — drop the
    // running banner + the durable reconnect marker — so neither the poller nor a
    // refresh resurrects it. Routed by owner like the completion events, and a
    // no-op once the job is untracked (e.g. the reconnect backstop already
    // abandoned it). Deliberately posts NO chat message: the run isn't a failure.
    const onJobNotFound = (e: Event) => {
      const { jobId } = (e as CustomEvent).detail || {};
      if (!jobId || !useExecutionStore.getState().jobOwnerOf(jobId)) return;
      jobOwnerRef.current.delete(jobId);
      useExecutionStore.getState().abandonExecution(jobId);
    };
    window.addEventListener('traceUpdate', onTraceUpdate as EventListener);
    window.addEventListener('jobCompleted', onJobCompleted as EventListener);
    window.addEventListener('jobFailed', onJobFailed as EventListener);
    window.addEventListener('jobStopped', onJobStopped as EventListener);
    window.addEventListener('jobNotFound', onJobNotFound as EventListener);
    return () => {
      window.removeEventListener('traceUpdate', onTraceUpdate as EventListener);
      window.removeEventListener('jobCompleted', onJobCompleted as EventListener);
      window.removeEventListener('jobFailed', onJobFailed as EventListener);
      window.removeEventListener('jobStopped', onJobStopped as EventListener);
      window.removeEventListener('jobNotFound', onJobNotFound as EventListener);
    };
  }, [processTrace, completeExecutionOnce, failExecutionOnce]);

  // --- Generation Stream ---
  // Generation steps fold into the SAME collapsible run-activity element as
  // tool calls (no crew card in the conversation): each step posts a trace
  // entry, and the only interactive remnant is the Genie-space prompt when a
  // crew needs one. Final output renders in the preview pane as usual.
  const addGenerationTrace = useCallback((ownerSession: string | undefined, label: string, sublabel?: string) => {
    const sessionStore = useSessionStore.getState();
    const extra = {
      resultType: 'trace',
      resultData: {
        label,
        ...(sublabel ? { sublabel } : {}),
        source: 'generation',
        kind: 'event',
        timestamp: Date.now(),
      },
    };
    if (ownerSession) sessionStore.addMessageToTargetSession(ownerSession, 'assistant', '', extra);
    else sessionStore.addMessage('assistant', '', extra);
  }, []);

  // Post a rich crew-detail card into the chat as each agent/task is generated,
  // so the chatbox shows the FULL details (agent goal + backstory, task
  // description + expected output) — not just a terse "ready" tick. ChatMessage
  // renders resultType 'agent'/'task' as AgentCard/TaskCard. Routes to the
  // generating session like addGenerationTrace.
  const addGenerationCard = useCallback(
    (ownerSession: string | undefined, resultType: 'agent' | 'task', resultData: unknown) => {
      if (!resultData) return;
      const sessionStore = useSessionStore.getState();
      const extra = { resultType, resultData };
      if (ownerSession) sessionStore.addMessageToTargetSession(ownerSession, 'assistant', '', extra);
      else sessionStore.addMessage('assistant', '', extra);
    },
    [],
  );

  // The origin session of a generation. handleStartGenerationStream always
  // registers it before any event arrives, so the map is the source of truth;
  // the global-owner fallback is a safety net only (it never fires in the real
  // flow, where genId is always registered).
  const ownerForGen = useCallback(
    (generationId: string) =>
      genOriginRef.current.get(generationId)
      ?? useExecutionStore.getState().executionOwnerSessionId
      ?? undefined,
    [],
  );

  // The plan produced by each in-flight generation, keyed by generationId, so
  // execution-start can show the right crew even when several runs overlap.
  const genDataRef = useRef<Map<string, GenerationCompleteData>>(new Map());

  // --- Execution handlers ---
  const handleStartExecutionStream = useCallback(
    (jobId: string, sessionId?: string, opts?: { preservePreview?: boolean }) => {
      const origin = sessionId || useSessionStore.getState().currentSessionId;
      // Remember which session owns this job, so its traces/output are routed
      // back to it by job_id even if the user switches sessions mid-run.
      if (origin) jobOwnerRef.current.set(jobId, origin);
      useExecutionStore.getState().startExecution(jobId, origin || undefined, opts);
      // Only seize the single live SSE stream when the run's OWNER is on screen.
      // A backgrounded run (a generation that finished for another session while
      // you're elsewhere) must not take over the viewed session's stream — its
      // traces/completion still arrive via the global poller (jobCreated below),
      // routed back by job owner.
      const viewingOwner = !origin || origin === useSessionStore.getState().currentSessionId;
      if (viewingOwner) {
        traceMessageIdsRef.current.clear();
        seenTraceIdsRef.current.clear();
        // This job now owns the single live SSE stream.
        sseJobIdRef.current = jobId;
        executionStream.startStream(jobId);
      }
      // Announce the job so the globally-mounted useTracePolling
      // (SSEConnectionManager) starts its REST polling fallback for it. When the
      // Databricks Apps HTTP/2 proxy kills SSE, the poller delivers traces +
      // completion via 'traceUpdate'/'jobCompleted' window events (see the
      // listener effect above), so memory/Genie/tool traces still render.
      // groupId is required: runStatus drops jobCreated events without it
      // (workspace-isolation check), and a dropped run never enters activeRuns
      // — so the 10s reconciliation loop can't finalize it if the poller is
      // retargeted to a newer job before the first status flip is observed.
      window.dispatchEvent(new CustomEvent('jobCreated', {
        detail: { jobId, groupId: localStorage.getItem('selectedGroupId') || undefined },
      }));
    },
    [executionStream],
  );

  const handleStartGenerationStream = useCallback(
    (generationId: string, sessionId: string) => {
      const origin = sessionId || useSessionStore.getState().currentSessionId;
      // Tie this generation to its origin so all its events route there, even if
      // the user switches sessions (or starts other generations) before it ends.
      if (origin) genOriginRef.current.set(generationId, origin);
      useExecutionStore.getState().startGeneration(origin || undefined);
      // Observe via the module-level manager (not a React hook): concurrent-safe
      // and independent of this component's render lifecycle, like the execution
      // side. Callbacks are passed per-call and route by the generation's origin.
      startGenerationStream(generationId, {
        onPlanReady: (genId, plan) => {
          const owner = ownerForGen(genId);
          const agents = Array.isArray(plan?.agents) ? (plan.agents as unknown[]).length : 0;
          const tasks = Array.isArray(plan?.tasks) ? (plan.tasks as unknown[]).length : 0;
          addGenerationTrace(owner, 'Crew planned', `${agents} agent${agents === 1 ? '' : 's'} · ${tasks} task${tasks === 1 ? '' : 's'}`);
        },
        onAgentDetail: (genId, agent) => {
          // Render the full agent card (role · goal · backstory · tools) in chat.
          addGenerationCard(ownerForGen(genId), 'agent', agent);
        },
        onTaskDetail: (genId, task) => {
          // Render the full task card (description · expected output · tools) in chat.
          addGenerationCard(ownerForGen(genId), 'task', task);
        },
        onComplete: (genId, raw: GenerationCompleteData) => {
          // Route by THIS generation's own origin — never a global owner, which a
          // parallel session's run may hold. The crew is generated AND run on the
          // BACKEND now (auto-execute); the frontend just records the plan and the
          // backend folds the execution id into this event (see onExecutionStarted).
          const ownerSession = ownerForGen(genId);
          const data = raw;
          genDataRef.current.set(genId, data);
          // Park the actions row (bookmark + thumbs feedback) — it posts only
          // AFTER the run's result comes back, so users rate what they've seen.
          pendingActionsRef.current = {
            data,
            ownerSession: ownerSession ?? null,
            mode: useExecutionStore.getState().chatModeType,
            // memoryEnabled === true means the run used Workspace memory (false =
            // session-only). Snapshot it now so a later toggle can't change it.
            usedWorkspaceMemory: useExecutionStore.getState().memoryEnabled,
          };
          dispatcher.setLastGenerated(data);
          lastGeneratedRef.current = data; // /save target
          useExecutionStore.getState().completeGeneration(ownerSession ?? undefined);
        },
        onExecutionStarted: (genId, executionId) => {
          // The backend launched the run; observe it under the session that asked
          // for it (origin), even if the user has since switched sessions.
          const ownerSession = ownerForGen(genId);
          const data = genDataRef.current.get(genId);
          // Only drive the live crew display when the owner is on screen — a
          // backgrounded run must not overwrite the viewed session's context.
          const viewingOwner = !ownerSession
            || ownerSession === useSessionStore.getState().currentSessionId;
          if (data && viewingOwner) {
            useExecutionStore.getState().setExecutionContext({
              crewName: 'Generated Crew',
              agents: (data.agents || []).map((a) => ({
                name: (a.name as string) || (a.role as string) || 'Agent',
                role: (a.role as string) || undefined,
              })),
              tasks: (data.tasks || []).map((t) => ({
                name: (t.name as string) || (t.description as string)?.slice(0, 40) || 'Task',
              })),
            });
          }
          handleStartExecutionStream(executionId, ownerSession ?? undefined);
          genOriginRef.current.delete(genId);
          genDataRef.current.delete(genId);
        },
        onFailed: (genId, error) => {
          useExecutionStore.getState().failGeneration(error, ownerForGen(genId));
          genOriginRef.current.delete(genId);
          genDataRef.current.delete(genId);
        },
      });
    },
    // `dispatcher` is intentionally not a dep: useDispatcher consumes this
    // callback (onStartGenerationStream), so depending on it here would be a
    // declaration cycle. Its methods (setLastGenerated) are stable useCallbacks,
    // and this runs only after dispatcher is initialized.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [ownerForGen, addGenerationTrace, addGenerationCard, handleStartExecutionStream],
  );

  // Close any open generation streams when ChatMode is fully torn down. It's kept
  // mounted across app-mode switches (so streams survive those), so this fires
  // only on a real unmount (leaving the workspace) — not on mode toggles.
  useEffect(() => () => stopAllGenerationStreams(), []);

  // Reconnect to a still-running crew after a page refresh. The in-memory store
  // is wiped on reload, so without this the Stop button (and live updates)
  // vanish even though the backend job is still running. The running job is
  // persisted per session in IndexedDB; when a session becomes active we read
  // it (async), re-attach the SSE stream + execution state, and verify status.
  // Attempted once per session id (covers refresh on the running session, and
  // switching to it afterwards).
  // In-flight guard so a re-render can't fire a duplicate reconnect for the SAME
  // session while its (async) marker read is pending. Unlike a once-ever Set,
  // this RESETS after each attempt, so switching BACK to a still-running session
  // re-detects and restores it — the bug where switch-away/return lost the
  // monitoring while a refresh (fresh component) brought it back.
  const reconnectingRef = useRef<string | null>(null);
  // Job ids proven gone (a 404 during the reconnect backstop, or finalized as
  // already-finished). Without this the effect would re-attach the SAME dead job
  // on every re-render: handleStartExecutionStream re-persists the IndexedDB
  // marker, abandonExecution clears activeExecution (removing the re-entry guard
  // below) and async-clears the marker — so a re-run that reads the not-yet-
  // cleared marker re-attaches → 404 → abandon → loop (tight render loop, screen
  // flicker, 404 storm). A dead job id never returns (UUIDs), so we never clear it.
  const deadJobsRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    const sid = currentSessionId;
    if (!sid || reconnectingRef.current === sid) return;
    const st = useExecutionStore.getState();
    // Already showing THIS session's run as active (snapshot restore handled it)
    // — nothing to reconnect.
    if (st.executionOwnerSessionId === sid && (st.isExecuting || st.isGenerating)) return;
    // A DIFFERENT run holds the live slot — don't clobber it.
    if (st.activeExecution) return;
    // Detect a running job for this session: the Zustand store first (survives
    // session switches in memory), then the IndexedDB marker (covers refresh,
    // where the in-memory store was wiped).
    const known = st.runningJobBySession[sid];
    reconnectingRef.current = sid;
    // Cancelled on unmount so an in-flight reconnect never re-attaches after the
    // component is gone (also keeps tests isolated: a prior render's pending async
    // can't fire startExecution into the next test).
    let cancelled = false;
    (async () => {
      const jobId = known || (await readActiveExecution(sid));
      // Re-check after the async read: still on this session, still nothing active.
      if (!jobId || useSessionStore.getState().currentSessionId !== sid) {
        reconnectingRef.current = null;
        return;
      }
      if (useExecutionStore.getState().activeExecution) {
        reconnectingRef.current = null;
        return;
      }
      // Already proven gone this session — don't re-attach it (would loop: the
      // marker clear is async, so a stale re-read could resurrect it otherwise).
      if (deadJobsRef.current.has(jobId)) {
        reconnectingRef.current = null;
        return;
      }

      // Restore the running state OPTIMISTICALLY so the Stop button reappears
      // immediately, and re-attach the SSE stream (its replay buffer + future
      // events drive live updates and completion). We deliberately do NOT gate
      // this on a status fetch — if that fetch failed or returned an unexpected
      // shape, the Stop button would vanish even though the crew is still
      // running, which is exactly the bug we're fixing.
      if (cancelled || useSessionStore.getState().currentSessionId !== sid) {
        reconnectingRef.current = null;
        return;
      }
      handleStartExecutionStream(jobId, sid, { preservePreview: true });

      // Backstop: if the job had ALREADY finished before the refresh, drop the
      // (now stale) running state so the Stop button doesn't linger. Only acts
      // on a definitively-terminal status; anything else keeps the optimistic
      // state and lets the SSE stream resolve it.
      try {
        const exec = await getExecutionStatus(jobId);
        const status = String(exec?.status || '').toLowerCase();
        const finished = ['completed', 'failed', 'stopped', 'cancelled', 'error'].includes(status);
        if (finished && useExecutionStore.getState().activeExecution?.jobId === jobId) {
          deadJobsRef.current.add(jobId);
          executionStream.stopStream();
          useExecutionStore.setState({
            isExecuting: false,
            isLoading: false,
            activeExecution: null,
            executionOwnerSessionId: null,
          });
          clearActiveExecution(sid);
          // We finalized this job directly (it was already done) — drop its
          // owner mapping so a late poller event can't re-post a completion, and
          // drop the Zustand switch-back entry so we don't re-detect a dead run.
          useExecutionStore.getState().clearJobOwner(jobId);
          useExecutionStore.setState((s) => {
            if (!(sid in s.runningJobBySession)) return {};
            const next = { ...s.runningJobBySession };
            delete next[sid];
            return { runningJobBySession: next };
          });
        }
      } catch (err) {
        // A 404 means the run no longer exists for this workspace (deleted, or it
        // belongs to a group you no longer have selected). Without this, the
        // optimistic running state + the IndexedDB reconnect marker persist, so
        // the global poller hammers /executions + /traces every 2s with 404s and
        // the NEXT refresh re-detects the dead job and resumes the storm. Treat it
        // as terminal: stop the stream and abandon the job (clears the marker +
        // the running banner). Any OTHER error (offline / 5xx / transient) keeps
        // the optimistic state — the SSE stream / next poll stays the source of truth.
        const httpStatus = (err as { response?: { status?: number } })?.response?.status;
        if (httpStatus === 404) {
          deadJobsRef.current.add(jobId);
          executionStream.stopStream();
          jobOwnerRef.current.delete(jobId);
          useExecutionStore.getState().abandonExecution(jobId);
          // handleStartExecutionStream above dispatched 'jobCreated', arming the
          // global poller's grace timer. Tell it to stand down now so it never
          // starts hammering this dead job (no residual 404 tail).
          window.dispatchEvent(new CustomEvent('jobNotFound', { detail: { jobId } }));
        }
        // else: offline / transient — keep optimistic state; SSE/next poll resolves it.
      } finally {
        // Allow a future switch-back to re-detect (the guard is per-attempt, not
        // once-ever) — this is what makes switching away and returning restore
        // the monitoring, like a refresh does.
        reconnectingRef.current = null;
      }
    })();
    return () => { cancelled = true; reconnectingRef.current = null; };
  }, [currentSessionId, handleStartExecutionStream, executionStream]);

  const doExecuteCrew = useCallback(
    async (plan: PlanData, inputs?: Record<string, string>) => {
      // Capture the session ID NOW, before the async createExecution call.
      // If the user switches sessions during the API call, currentSessionId
      // will have changed, but originSessionId preserves the correct owner.
      const originSessionId = useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        const nodes = (plan.nodes || []) as { type: string; data: Record<string, unknown> }[];
        const agentNames = nodes
          .filter((n) => n.type === 'agentNode' || n.type === 'agent')
          .map((n) => ({ name: (n.data.role as string) || (n.data.name as string) || 'Agent' }));
        const taskNames = nodes
          .filter((n) => n.type === 'taskNode' || n.type === 'task')
          .map((n) => ({ name: (n.data.name as string) || (n.data.description as string)?.slice(0, 40) || 'Task' }));
        execStore.setExecutionContext({
          crewName: plan.name || 'Crew',
          agents: agentNames,
          tasks: taskNames,
        });

        // Reflect the chat's current memory toggle on the loaded plan, so
        // disabling memory in the chat runs the loaded crew without memory.
        const crewConfig = buildCrewConfig(
          plan,
          selectedModel || undefined,
          inputs,
          useExecutionStore.getState().memoryEnabled,
          // Agent Bricks endpoints picked in the "+" menu — equip + configure the
          // tool on this loaded crew so it has the endpoint (else "not configured").
          useExecutionStore.getState().selectedAgentBricksEndpoints,
        );
        const execution = await createExecution(crewConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  const handleExecuteCrew = useCallback(
    async (plan: PlanData) => {
      const vars = detectVariablesFromNodes(plan.nodes || []);
      if (vars.length > 0) {
        setPendingExecution({ type: 'crew', plan });
        addMessage('assistant', 'This crew needs input variables before it can run.', {
          resultType: 'input_variables',
          resultData: { variables: vars },
        });
        return;
      }
      doExecuteCrew(plan);
    },
    [doExecuteCrew],
  );

  const doExecuteGenerated = useCallback(
    async (
      data: GenerationCompleteData,
      spaceId?: string,
      inputs?: Record<string, string>,
      opts?: { preservePreview?: boolean; originSession?: string | null },
    ) => {
      // The run belongs to the session that started it. On auto-run after
      // generation, that's the generation's origin (passed via opts) — NOT the
      // session currently on screen, which the user may have switched to.
      const originSessionId = opts?.originSession || useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        const agentNames = (data.agents || []).map((a) => ({
          name: (a.name as string) || (a.role as string) || 'Agent',
          role: (a.role as string) || undefined,
        }));
        const taskNames = (data.tasks || []).map((t) => ({
          name: (t.name as string) || (t.description as string)?.slice(0, 40) || 'Task',
        }));
        execStore.setExecutionContext({
          crewName: 'Generated Crew',
          agents: agentNames,
          tasks: taskNames,
        });

        // If a Genie space was selected, pass tool_configs with the spaceId
        // (the selector only shows when GenieTool is already in the crew's tools)
        const agents = data.agents;
        const taskList = data.tasks;
        // Agent Bricks mirrors the Genie mechanism EXACTLY: configure the tool by
        // NAME in tool_configs, and buildCrewConfigFromGenerated → applicableToolConfigs
        // attaches it to whichever agent/task already lists the tool (the generator
        // equips AgentBricksTool, just like GenieTool). The backend skips the tool if
        // no endpoint resolves, so an unselected/empty pick never aborts the run.
        const selectedAgentBricks =
          useExecutionStore.getState().selectedAgentBricksEndpoints || [];
        const toolConfigs: Record<string, Record<string, unknown>> = {};
        if (spaceId) toolConfigs.GenieTool = { spaceId };
        if (selectedAgentBricks.length > 0) {
          toolConfigs.AgentBricksTool = { endpointName: selectedAgentBricks };
        }
        const toolConfigsArg =
          Object.keys(toolConfigs).length > 0 ? toolConfigs : undefined;

        // NOTE: "Predefined UI" emission is enforced in the BACKEND
        // (crew_preparation → ui_emission) so every channel — chat, Crew mode,
        // API, schedules — behaves the same. No frontend injection here.
        const crewConfig = buildCrewConfigFromGenerated(
          agents,
          taskList,
          selectedModel || undefined,
          toolConfigsArg,
          inputs,
          useAppStore.getState().toolNameMap,
          originSessionId,
          // Read the recall scope from the store at execution time so the value
          // is always the user's current choice (not a stale closure capture).
          useExecutionStore.getState().workspaceMemory,
          // "No memory" mode → agents are built without memory.
          useExecutionStore.getState().memoryEnabled,
          // MCP servers picked via the chat input's "+" menu.
          useExecutionStore.getState().selectedMcpServers,
          // The chat prompt that asked for this crew — appended to the task
          // descriptions so the run answers it (instead of running a generic
          // mission with no actual question).
          data.user_request,
          // Agent Bricks endpoints picked via the chat input's "+" menu.
          useExecutionStore.getState().selectedAgentBricksEndpoints,
          // Answer mode → reasoning (research/deep) + planning (+planning_llm, deep)
          // so a manually re-run crew matches what the mode (and save) produced.
          useExecutionStore.getState().chatModeType === 'research' ||
            useExecutionStore.getState().chatModeType === 'deep',
          useExecutionStore.getState().chatModeType === 'deep',
          useExecutionStore.getState().chatModeType === 'deep' ? (selectedModel || undefined) : undefined,
        );
        const execution = await createExecution(crewConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined, opts);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  const handleExecuteGenerated = useCallback(
    async (data: GenerationCompleteData, spaceId?: string, originSession?: string | null) => {
      const vars = detectVariablesFromGenerated(data.agents || [], data.tasks || []);
      if (vars.length > 0) {
        setPendingExecution({ type: 'generated', data, spaceId, originSession });
        addMessage('assistant', 'This crew needs input variables before it can run.', {
          resultType: 'input_variables',
          resultData: { variables: vars },
        });
        return;
      }
      doExecuteGenerated(data, spaceId, undefined, { originSession });
    },
    [doExecuteGenerated],
  );

  // --- Refine the current artifact instead of generating a brand-new crew ---
  // Builds a single "editor" agent + task whose input is the previous artifact
  // plus the user's instruction, then runs it through the normal execution path
  // so the revised artifact streams straight back into the preview pane.
  const handleRefine = useCallback(
    async (instruction: string) => {
      const trimmed = instruction.trim();
      if (!trimmed) return;

      // Resolve the artifact currently shown (or last persisted) for this session.
      let artifact = useExecutionStore.getState().previewContent?.data || '';
      if (!artifact) {
        const sid = useSessionStore.getState().currentSessionId;
        if (sid) {
          const stored = await getSessionPreview(sid);
          artifact = stored?.data || '';
        }
      }
      if (!artifact) {
        addMessage(
          'assistant',
          'There is no result to refine yet. Run a crew first, then use the Refine button or `/refine <instruction>`.',
        );
        return;
      }

      addMessage('user', `Refine: ${trimmed}`);
      // Give the refine run its own activity section — same treatment as a
      // regular prompt: this trace anchors the collapsible run container
      // right under the Refine message, ABOVE the refined result, and it
      // persists there after the run finishes.
      useSessionStore.getState().addMessage('assistant', '', {
        resultType: 'trace',
        resultData: {
          label: 'Refining artifact',
          sublabel: trimmed.length > 80 ? `${trimmed.slice(0, 77)}…` : trimmed,
          source: 'refine',
          kind: 'event',
          timestamp: Date.now(),
        },
      });

      const editorAgents = [
        {
          id: 'refiner',
          role: 'Content Editor',
          goal: 'Revise the provided artifact according to the user instruction, preserving correctness and returning the complete updated artifact.',
          backstory:
            'You are an expert editor and front-end developer who refines documents and HTML, keeping the output valid, self-contained and ready to render.',
          tools: [],
          // Pin the editor to the user-selected model. Without an explicit llm
          // the backend defaults this hand-built agent to gpt-4o, which fails in
          // Databricks environments with no OpenAI key.
          ...(selectedModel ? { llm: selectedModel } : {}),
          // A refine is a single-shot edit, not a research crew. Disabling memory
          // (the only agent → disables crew memory entirely) skips the cognitive
          // memory search/save flow; no delegation keeps it to one LLM pass.
          memory: false,
          allow_delegation: false,
        },
      ];
      const editorTasks = [
        {
          id: 'refine_task',
          name: 'Refine artifact',
          agent_id: 'refiner',
          // The instruction and artifact are passed as crew inputs (below) and
          // referenced via {instruction}/{artifact} placeholders. CrewAI runs a
          // single-pass {var} interpolation over the description, so the artifact
          // must NOT be inlined: an HTML/CSS/JS artifact routinely contains brace
          // tokens (e.g. a JS template literal `${spread}` → `{spread}`) that
          // CrewAI would otherwise read as missing template variables and fail
          // ("Template variable 'spread' not found in inputs dictionary"). The
          // substituted input values are not re-scanned, so their braces are safe.
          description:
            `Improve the artifact below based on this instruction.\n\n` +
            `INSTRUCTION:\n{instruction}\n\n` +
            `CURRENT ARTIFACT:\n{artifact}\n\n` +
            `Return ONLY the complete revised artifact (e.g. the full HTML document) with no commentary and no markdown code fences.`,
          expected_output: 'The complete revised artifact, ready to render.',
          tools: [],
        },
      ];

      // doExecuteGenerated runs immediately (no variable-detection dialog).
      // preservePreview keeps the current artifact + history visible so the
      // refined version is appended (scroll back to compare), not wiped.
      doExecuteGenerated(
        { agents: editorAgents, tasks: editorTasks },
        undefined,
        { instruction: trimmed, artifact },
        { preservePreview: true },
      );
    },
    [addMessage, doExecuteGenerated, selectedModel],
  );

  // --- Save a generated crew's plan to the catalog ---
  // Used by the bookmark on each crew card (it owns its own saved-state UI), so
  // this just performs the save and resolves to the created crew.
  const handleSaveCrew = useCallback(
    (data: GenerationCompleteData, opts?: { overwrite?: boolean; spaceId?: string }) => {
      // Capture the chat's current memory choice so the saved crew matches what
      // the user sees here (no-memory mode → saved crew has memory disabled).
      // opts carries overwrite + the picked Genie space from the crew card.
      // Answer mode → persist reasoning/planning so a Research/Deep crew reloads
      // with the same behaviour (planning_llm only matters for Deep).
      const mode = useExecutionStore.getState().chatModeType;
      return saveGeneratedCrew(data, undefined, {
        ...opts,
        memoryEnabled: useExecutionStore.getState().memoryEnabled,
        // Persist the MCP servers selected for the run so the saved crew keeps them.
        mcpServers: useExecutionStore.getState().selectedMcpServers,
        // Persist the Agent Bricks endpoint picked in the "+" so the saved crew
        // reloads with the agent assigned and runs against it.
        agentBricksEndpoints: useExecutionStore.getState().selectedAgentBricksEndpoints,
        reasoning: mode === 'research' || mode === 'deep',
        planning: mode === 'deep',
        planningLlm: mode === 'deep' ? (selectedModel || undefined) : undefined,
      }).then((r) => {
        // Surface the freshly saved crew in the rail library.
        void refreshLibrary();
        return r;
      });
    },
    [refreshLibrary, selectedModel],
  );

  // --- Answer mode: distill a reusable crew from the conversation and SAVE it ---
  // ChatMode 'chat' turns run a generic single assistant, so bookmarking that
  // saves nothing specific. Instead we ask the backend to read the conversation
  // and synthesize an agent + task, then save it to the catalog in one shot and
  // show what was saved (read-only) — no second confirmation click.
  const handleSaveAnswerToCatalog = useCallback(
    async (sessionId?: string) => {
      const sid = sessionId || useSessionStore.getState().currentSessionId;
      if (!sid) {
        addMessage('assistant', 'There is no active chat session to build a crew from.');
        return;
      }
      const thinkingId = addMessage(
        'assistant',
        'Distilling a reusable crew from this conversation and saving it…',
        { isStreaming: true },
      );
      try {
        const data = await synthesizeCrewFromConversation(sid, selectedModel || undefined);
        if (data.agents.length === 0 && data.tasks.length === 0) {
          updateMessage(thinkingId, {
            content:
              'I could not distill a reusable crew from this conversation yet — try again after a few more messages.',
            isStreaming: false,
          });
          return;
        }
        // Save automatically (no second click). On a name clash, overwrite the
        // same-named crew rather than dead-ending — this one-shot save re-derives
        // the same distilled crew, so overwrite is the intended outcome.
        const saved = await handleSaveCrew(data).catch((e) => {
          if (e instanceof CrewNameConflictError) return handleSaveCrew(data, { overwrite: true });
          throw e;
        });
        updateMessage(thinkingId, {
          content: `✓ Saved **${saved.name}** to the catalog — find it in the **Crews** library on the left.`,
          isStreaming: false,
          // Read-only display of exactly what was saved (no save bookmark, no Run).
          // Carry the saved crew id/name so the card can offer "Open in Agent/Flow
          // Builder" without re-saving.
          resultType: 'saved_crew',
          resultData: { ...data, savedCrewId: saved.id, savedName: saved.name },
        });
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to save a crew';
        updateMessage(thinkingId, {
          content: `I couldn't save a crew from this conversation: ${errMsg}`,
          isStreaming: false,
        });
      }
    },
    [addMessage, updateMessage, selectedModel, handleSaveCrew],
  );

  const handleExecuteFlow = useCallback(
    async (flow: FlowData) => {
      // Capture the session ID NOW, before the async createExecution call.
      const originSessionId = useSessionStore.getState().currentSessionId;

      const execStore = useExecutionStore.getState();
      execStore.setIsLoading(true);
      try {
        execStore.setExecutionContext({
          crewName: flow.name || 'Flow',
          agents: [],
          tasks: [],
        });

        const flowConfig = buildFlowConfig(flow, selectedModel || undefined);
        const execution = await createExecution(flowConfig);
        const jobId = execution.job_id || execution.execution_id;
        if (jobId) {
          handleStartExecutionStream(jobId, originSessionId || undefined);
        } else {
          addMessage('assistant', 'Execution started but no job ID was returned.');
          execStore.setExecutionContext(null);
          execStore.setIsLoading(false);
        }
      } catch (error) {
        const errMsg = error instanceof Error ? error.message : 'Failed to start execution';
        addMessage('assistant', `Execution failed: ${errMsg}`);
        execStore.setExecutionContext(null);
        execStore.setIsLoading(false);
      }
    },
    [addMessage, handleStartExecutionStream, selectedModel],
  );

  // --- Inline input-variables prompt (genie-style, in the chat flow) ---
  const handleVariablesSubmit = useCallback(
    (_messageId: string, inputs: Record<string, string>) => {
      const pending = pendingExecution;
      setPendingExecution(null);

      if (!pending) {
        // Prompt outlived its parked run (e.g. page reload) — ask for a re-run.
        addMessage('assistant', 'This run prompt has expired — run the crew again to use these variables.');
        return;
      }
      // pending is always a crew (carrying a plan) or a generated crew (carrying data).
      if (pending.type === 'crew') {
        doExecuteCrew(pending.plan as PlanData, inputs);
      } else {
        doExecuteGenerated(pending.data as GenerationCompleteData, pending.spaceId, inputs, { originSession: pending.originSession });
      }
    },
    [pendingExecution, doExecuteCrew, doExecuteGenerated, addMessage],
  );

  // --- Dispatcher ---
  const dispatcher = useDispatcher({
    addMessage,
    addMessageToTargetSession,
    updateMessage,
    updateMessageInTargetSession,
    onStartGenerationStream: handleStartGenerationStream,
    onStartExecutionStream: handleStartExecutionStream,
    onExecuteCrew: handleExecuteCrew,
    onExecuteFlow: handleExecuteFlow,
    onExecuteGenerated: handleExecuteGenerated,
    onCrewLoaded: (plan, sessionId) =>
      setPendingRun({ sessionId, label: plan.name || 'crew', run: () => handleExecuteCrew(plan) }),
    onFlowLoaded: (flow, sessionId) =>
      setPendingRun({ sessionId, label: flow.name || 'flow', run: () => handleExecuteFlow(flow) }),
    getCurrentSessionId: () => useSessionStore.getState().currentSessionId,
    ensureSession: () => useSessionStore.getState().ensureSession(),
  });

  // --- Local command handling ---
  const handleLocalCommand = useCallback(
    async (message: string): Promise<boolean> => {
      const lower = message.toLowerCase().trim();
      const execStore = useExecutionStore.getState();

      if (lower === '/clear') {
        clearMessages();
        execStore.resetForSession();
        return true;
      }

      if (lower === '/jobs' || lower === '/list jobs' || lower === '/list executions') {
        addMessage('user', message);
        execStore.setIsLoading(true);
        try {
          const executions = await listExecutions(20);
          if (executions.length === 0) {
            addMessage('assistant', 'No recent executions found.');
          } else {
            let msg = `**Recent Executions** (${executions.length})\n\n`;
            msg += '| # | Job ID | Status | Created |\n';
            msg += '|---|--------|--------|---------|\n';
            executions.forEach((exec, i) => {
              const shortId = exec.job_id?.slice(0, 8) || exec.id?.slice(0, 8) || '\u2014';
              const status = exec.status || 'unknown';
              const created = exec.created_at
                ? new Date(exec.created_at).toLocaleString()
                : '\u2014';
              msg += `| ${i + 1} | \`${shortId}\` | ${status} | ${created} |\n`;
            });
            msg += '\nUse `/stop <job_id>` to stop a running execution.';
            addMessage('assistant', msg);
          }
        } catch (error) {
          const errMsg = error instanceof Error ? error.message : 'Failed to list executions';
          addMessage('assistant', `Failed to list executions: ${errMsg}`);
        }
        execStore.setIsLoading(false);
        return true;
      }

      if (lower === '/stop' || lower.startsWith('/stop ')) {
        const jobId = message.trim().slice(5).trim();
        if (!jobId) {
          addMessage('assistant', 'Usage: `/stop <job_id>`');
          return true;
        }
        addMessage('user', message);
        try {
          await stopExecution(jobId);
          addMessage('assistant', `Execution \`${jobId.slice(0, 8)}...\` stop requested.`);
          const currentExec = execStore.activeExecution;
          if (currentExec?.jobId === jobId || currentExec?.jobId.startsWith(jobId)) {
            executionStream.stopStream();
            execStore.updateExecutionStatus('stopped');
          }
        } catch (error) {
          const errMsg = error instanceof Error ? error.message : 'Failed to stop execution';
          addMessage('assistant', `Failed to stop: ${errMsg}`);
        }
        return true;
      }

      if (lower === '/dismiss' || lower === '/close') {
        execStore.resetForSession();
        return true;
      }

      if (lower === '/refine' || lower.startsWith('/refine ')) {
        const instruction = message.trim().slice(7).trim();
        if (!instruction) {
          addMessage('assistant', 'Usage: `/refine <how to improve the current result>`');
          return true;
        }
        handleRefine(instruction);
        return true;
      }

      if (lower === '/save' || lower.startsWith('/save ')) {
        const data = lastGeneratedRef.current;
        if (!data) {
          addMessage(
            'assistant',
            'There is no generated crew to save yet. Generate a crew first, then `/save` it or use the bookmark on the crew card.',
          );
          return true;
        }
        addMessage('user', message);
        // Allow "/save overwrite [name]" to replace an existing same-named crew.
        let arg = message.trim().slice(5).trim();
        let overwrite = false;
        if (arg.toLowerCase() === 'overwrite' || arg.toLowerCase().startsWith('overwrite ')) {
          overwrite = true;
          arg = arg.slice('overwrite'.length).trim();
        }
        const name = arg;
        const memoryEnabled = useExecutionStore.getState().memoryEnabled;
        const mcpServers = useExecutionStore.getState().selectedMcpServers;
        const saveMode = useExecutionStore.getState().chatModeType;
        try {
          const agentBricksEndpoints = useExecutionStore.getState().selectedAgentBricksEndpoints;
          const saved = await saveGeneratedCrew(data, name || undefined, {
            overwrite, memoryEnabled, mcpServers, agentBricksEndpoints,
            reasoning: saveMode === 'research' || saveMode === 'deep',
            planning: saveMode === 'deep',
            planningLlm: saveMode === 'deep' ? (selectedModel || undefined) : undefined,
          });
          void refreshLibrary();
          addMessage(
            'assistant',
            `✓ ${overwrite ? 'Updated' : 'Saved'} **${saved.name}** ${overwrite ? 'in' : 'to'} the catalog — find it in the **Crews** library on the left.`,
          );
        } catch (error) {
          if (error instanceof CrewNameConflictError) {
            addMessage(
              'assistant',
              `**${error.crewName}** is already in the catalog. Type \`/save overwrite\` to replace it, or \`/save <a different name>\`.`,
            );
          } else {
            const errMsg = error instanceof Error ? error.message : 'Failed to save crew';
            addMessage('assistant', `Failed to save crew: ${errMsg}`);
          }
        }
        return true;
      }

      return false;
    },
    [addMessage, clearMessages, executionStream, handleRefine, selectedModel],
  );

  const handleSend = useCallback(
    async (
      message: string,
      meta?: { tools?: string[]; dispatchSuffix?: string; attachments?: string[]; displayAs?: string; knowledgeFilePaths?: string[] },
    ) => {
      // A genuine user message supersedes any pending loaded-crew run (the rail's
      // own "/load …" send is exempt — it's what arms the pending run).
      if (!message.startsWith('/load ')) setPendingRun(null);

      const handled = await handleLocalCommand(message);
      if (handled) return;

      // Remember the prompt: if this message triggers a crew generation, the
      // executed run is grounded with it (see onComplete / doExecuteGenerated).
      lastUserPromptRef.current = message;

      useExecutionStore.getState().setIsLoading(true);
      try {
        // Send the picker selection; when none is set the backend falls back to a
        // working default (gpt-5.3-codex), so we don't force a model here.
        await dispatcher.sendMessage(
          message,
          selectedModel || undefined,
          meta?.tools,
          meta?.dispatchSuffix,
          meta?.attachments,
          meta?.displayAs,
          meta?.knowledgeFilePaths,
        );
      } finally {
        useExecutionStore.getState().setIsLoading(false);
      }
    },
    [dispatcher, handleLocalCommand, selectedModel],
  );

  // Load a saved crew/flow from the rail library into a FRESH chat session (so it
  // never clobbers the current conversation). Reuses the deterministic /load
  // command under the hood but shows a friendly label in the transcript.
  const handleLoadFromLibrary = useCallback(
    async (kind: 'crew' | 'flow', name: string) => {
      if (currentSessionId) {
        useExecutionStore.getState().saveSessionState(currentSessionId);
      }
      const newId = await useSessionStore.getState().createNewSession();
      useExecutionStore.getState().restoreSessionState(newId);
      void handleSend(`/load ${kind} ${name}`, { displayAs: `Open ${kind}: ${name}` });
    },
    [handleSend, currentSessionId],
  );

  const handleStopExecution = useCallback(async () => {
    const execStore = useExecutionStore.getState();
    const activeExec = execStore.activeExecution;
    if (!activeExec?.jobId) return;
    try {
      await stopExecution(activeExec.jobId);
      executionStream.stopStream();
      addMessage('assistant', 'Execution stopped.');
      execStore.failExecution('Stopped by user', activeExec.jobId);
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : 'Failed to stop execution';
      addMessage('assistant', `Failed to stop: ${errMsg}`);
    }
  }, [addMessage, executionStream]);

  // --- Session switching ---
  const handleNewChat = useCallback(() => {
    if (currentSessionId) {
      useExecutionStore.getState().saveSessionState(currentSessionId);
    }
    // Reset to a blank chat WITHOUT persisting a row — the session is created
    // (and titled) lazily on the first message. Eagerly creating here is what
    // left an empty "New Chat" sitting in the Recent rail beside the button.
    useSessionStore.getState().startNewChat();
    useExecutionStore.getState().resetForSession();
  }, [currentSessionId]);

  const handleSwitchSession = useCallback(async (sessionId: string) => {
    if (currentSessionId) {
      useExecutionStore.getState().saveSessionState(currentSessionId);
    }
    setPendingRun(null);
    await useSessionStore.getState().switchSession(sessionId);
    useExecutionStore.getState().restoreSessionState(sessionId);
  }, [currentSessionId]);

  const handleDeleteSession = useCallback(async (sessionId: string) => {
    await useSessionStore.getState().deleteSession(sessionId);
    setContextMenu(null);
    useExecutionStore.getState().resetForSession();
  }, []);

  const handleStartRename = useCallback((sessionId: string, currentTitle: string) => {
    setRenamingSessionId(sessionId);
    setRenameValue(currentTitle);
    setContextMenu(null);
  }, []);

  const handleFinishRename = useCallback(async () => {
    if (renamingSessionId && renameValue.trim()) {
      await useSessionStore.getState().renameSession(renamingSessionId, renameValue.trim());
    }
    setRenamingSessionId(null);
    setRenameValue('');
  }, [renamingSessionId, renameValue]);

  return (
    <div id="kasal-chat-root" className="kasal-chat-root h-full w-full flex" style={{ backgroundColor: 'var(--bg-primary)' }}>
      {/* Sidebar */}
      {sidebarOpen && (
        <aside
          className="w-64 flex flex-col flex-shrink-0"
          style={{ backgroundColor: 'var(--bg-rail)' }}
        >
          {/* New Chat button — soft filled chip */}
          <div className="px-3 pt-3 pb-1">
            <button
              onClick={handleNewChat}
              className="kasal-newchat w-full flex items-center gap-2.5 px-3 py-2.5 rounded-xl text-sm font-medium"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                color: 'var(--text-primary)',
              }}
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
              </svg>
              New Chat
            </button>
          </div>

          {/* Saved catalog library (Crews / Flows) — replaces /list commands */}
          <CatalogLibrary
            crews={libraryCrews}
            flows={libraryFlows}
            onLoadCrew={(name) => handleLoadFromLibrary('crew', name)}
            onLoadFlow={(name) => handleLoadFromLibrary('flow', name)}
          />

          {/* Section label */}
          {sessions.length > 0 && (
            <div className="px-3 pt-4 pb-1.5">
              <span
                className="text-[11px] font-semibold uppercase tracking-[0.08em]"
                style={{ color: 'var(--text-muted)' }}
              >
                Recent
              </span>
            </div>
          )}

          {/* Session list — generous bottom padding so the last row keeps a bit of
              breathing room and never sits flush against the sidebar's edge. */}
          <div className="flex-1 overflow-y-auto px-2 pb-6">
            {sessions.map((s) => {
              const isActive = s.id === currentSessionId;
              return (
              <div key={s.id} className="relative">
                {renamingSessionId === s.id ? (
                  <input
                    autoFocus
                    value={renameValue}
                    onChange={(e) => setRenameValue(e.target.value)}
                    onBlur={handleFinishRename}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') handleFinishRename();
                      if (e.key === 'Escape') { setRenamingSessionId(null); setRenameValue(''); }
                    }}
                    className="kasal-rename-input w-full pl-5 pr-3 py-1.5 my-0.5 rounded-lg text-[13px]"
                    style={{
                      backgroundColor: 'var(--bg-input)',
                      color: 'var(--text-primary)',
                      border: '1px solid var(--border-color)',
                    }}
                  />
                ) : (
                  <div
                    className="kasal-session flex items-center rounded-lg group my-0.5"
                    style={{
                      backgroundColor: isActive ? 'var(--bg-active-chip)' : 'transparent',
                    }}
                  >
                    <button
                      onClick={() => handleSwitchSession(s.id)}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        setContextMenu({ sessionId: s.id, x: e.clientX, y: e.clientY });
                      }}
                      className="flex-1 flex items-center gap-2 text-left min-w-0"
                      // Padding is set INLINE, not via Tailwind `pl-*`/`py-*`: the
                      // global `#kasal-chat-root button { padding: 0 }` reset uses an
                      // ID selector that out-specifies the class-scoped utilities, so
                      // a `pl-5` on a <button> is silently overridden. Inline wins.
                      style={{
                        color: isActive ? 'var(--text-primary)' : 'var(--text-secondary)',
                        padding: '6px 4px 6px 14px',
                      }}
                      title={s.title}
                    >
                      <SessionSpinner sessionId={s.id} />
                      <span className={`kasal-session-title truncate text-[13px] ${isActive ? 'font-semibold' : 'font-medium'}`}>{s.title}</span>
                    </button>
                    {/* Kebab menu button */}
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        const rect = (e.target as HTMLElement).getBoundingClientRect();
                        setContextMenu({ sessionId: s.id, x: rect.right, y: rect.bottom });
                      }}
                      className="flex-shrink-0 w-6 h-6 mr-1.5 rounded-md flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-[var(--bg-rail-hover)]"
                      style={{ color: 'var(--text-muted)' }}
                      title="Options"
                    >
                      <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
                        <circle cx="12" cy="6" r="1.5" />
                        <circle cx="12" cy="12" r="1.5" />
                        <circle cx="12" cy="18" r="1.5" />
                      </svg>
                    </button>
                  </div>
                )}
              </div>
              );
            })}
          </div>

          {/* Sidebar footer — dark-mode toggle, pinned at the bottom. A divider +
              padding above it leaves clear space between the scrolling session
              list and the toggle. */}
          <div
            className="flex-shrink-0 px-2 pt-2 pb-3 mt-1"
            style={{ borderTop: '1px solid var(--border-color)' }}
          >
            <button
              onClick={() => useAppStore.getState().toggleTheme()}
              className="w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-[13px] font-medium transition-colors hover:bg-[var(--bg-rail-hover)]"
              style={{ color: 'var(--text-secondary)' }}
              title={chatThemeIsDark ? 'Switch to light mode' : 'Switch to dark mode'}
              aria-label={chatThemeIsDark ? 'Switch to light mode' : 'Switch to dark mode'}
            >
              {chatThemeIsDark ? (
                // Sun — currently dark, click for light
                <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                  <circle cx="12" cy="12" r="4" />
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M4.93 19.07l1.41-1.41m11.32-11.32l1.41-1.41" />
                </svg>
              ) : (
                // Moon — currently light, click for dark
                <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
                </svg>
              )}
              {chatThemeIsDark ? 'Light mode' : 'Dark mode'}
            </button>
          </div>

          {/* Context menu */}
          {contextMenu && (
            <>
              <div data-testid="context-menu-backdrop" className="fixed inset-0 z-40" onClick={() => setContextMenu(null)} />
              <div
                className="kasal-popover fixed z-50 rounded-xl overflow-hidden p-1 shadow-lg"
                style={{
                  left: contextMenu.x,
                  top: contextMenu.y,
                  minWidth: 168,
                  backgroundColor: 'var(--bg-input)',
                  border: '1px solid var(--border-color)',
                }}
              >
                <button
                  onClick={() => {
                    const session = sessions.find((s) => s.id === contextMenu.sessionId);
                    if (session) handleStartRename(session.id, session.title);
                  }}
                  className="w-full flex items-center gap-2.5 text-left px-3 py-2 text-sm rounded-lg transition-colors hover:bg-[var(--bg-rail-hover)]"
                  style={{ color: 'var(--text-primary)' }}
                >
                  <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M16.862 4.487l1.687-1.688a1.875 1.875 0 112.652 2.652L10.582 16.07a4.5 4.5 0 01-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 011.13-1.897l8.932-8.931zM19.5 7.125L16.875 4.5" />
                  </svg>
                  Rename
                </button>
                <div className="my-1 h-px" style={{ backgroundColor: 'var(--border-color)' }} />
                <button
                  onClick={() => handleDeleteSession(contextMenu.sessionId)}
                  className="w-full flex items-center gap-2.5 text-left px-3 py-2 text-sm rounded-lg transition-colors hover:bg-[rgba(239,68,68,0.10)]"
                  style={{ color: '#ef4444' }}
                >
                  <svg className="w-4 h-4 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
                  </svg>
                  Delete
                </button>
              </div>
            </>
          )}
        </aside>
      )}

      {/* Main content — chat panel */}
      {/* Chat hides full-screen ONLY for a real deliverable the user collapsed to;
          the build skeleton never hides chat — the activity must stay visible. */}
      {!(chatCollapsed && previewPaneOpen && previewContent) && (
        <main className="flex-1 flex flex-col overflow-hidden relative" style={{ flex: previewPaneVisible ? '1 1 50%' : '1 1 100%' }}>
          {/* The sidebar toggle + Databricks wordmark now live in the app top bar
              (ChatModeHeaderSlot), so the main area no longer renders its own
              header — this keeps it vertically stable when the sidebar toggles. */}

          {/* Chat container — the reopen-preview pill is rendered inside it,
              anchored above the composer, so it never overlaps the input. */}
          <div className="flex-1 overflow-hidden">
            <ChatContainer
              messages={messages}
              hydrating={hydrating}
              onSend={handleSend}
              onCommand={handleSend}
              onExecuteCrew={handleExecuteCrew}
              onExecuteFlow={handleExecuteFlow}
              onExecuteGenerated={handleExecuteGenerated}
              onSaveCrew={handleSaveCrew}
              onSaveAnswerToCatalog={handleSaveAnswerToCatalog}
              onSubmitVariables={handleVariablesSubmit}
              onStopExecution={handleStopExecution}
              isLoading={viewIsLoading}
              isExecuting={viewIsExecuting}
              isGenerating={viewIsGenerating}
              executionContext={viewExecutionContext}
              hideLiveTimeline={!activityInChat && previewPaneVisible}
              runSteps={runActivitySteps}
              onShowRunInPane={handleShowRunInPane}
              models={models}
              selectedModel={selectedModel}
              onModelChange={(m) => useAppStore.getState().setSelectedModel(m)}
              sessionId={currentSessionId}
              memoryEnabled={memoryEnabled}
              onMemoryEnabledChange={setMemoryEnabled}
              pendingRunLabel={pendingRun && pendingRun.sessionId === currentSessionId ? pendingRun.label : undefined}
              onRunPending={() => {
                if (pendingRun && pendingRun.sessionId === currentSessionId) {
                  const run = pendingRun.run;
                  setPendingRun(null);
                  run();
                }
              }}
              onOpenMcpConfig={() => setMcpConfigOpen(true)}
            />
          </div>
        </main>
      )}

      {/* Preview panel — right side. Opt-in: shown only when the user opened it. */}
      {previewPaneOpen && previewContent && (
        <PreviewPanel
          key={currentSessionId}
          content={previewContent}
          onClose={() => { setFocusedRunSteps(null); setFocusedRunStep(null); useExecutionStore.getState().clearPreview(); }}
          chatCollapsed={chatCollapsed}
          onToggleChat={() => useExecutionStore.getState().toggleChatCollapsed()}
          onRefine={handleRefine}
          onStyleChange={(data) => useExecutionStore.getState().updatePreviewData(data)}
          history={previewHistory}
          index={previewIndex}
          onNavigate={navigatePreview}
          // A focused run (opened via its "Show in panel" icon) shows ITS steps;
          // otherwise the latest run's, hidden when activity lives in the chat bar.
          runSteps={focusedRunSteps ?? (activityInChat ? [] : runActivitySteps)}
          // A clicked step ROW pre-opens that step's content in the pane.
          focusStep={focusedRunStep}
          onMoveActivityToChat={() => { setFocusedRunSteps(null); setFocusedRunStep(null); useExecutionStore.getState().setActivityPlacement('chat'); }}
        />
      )}

      {/* Preview skeleton — the single run monitor (a clickable step timeline)
          shown WHILE the viewed session's run builds its deliverable (no preview
          yet). Mutually exclusive with PreviewPanel. */}
      {showPreviewSkeleton && (
        <PreviewSkeleton
          steps={focusedRunSteps ?? runActivitySteps}
          running={viewIsExecuting}
          focusStep={focusedRunStep}
          onMoveActivityToChat={() => {
            // Collapse activity back to the chat bar AND close the pane — the
            // skeleton only ever shows when there's no deliverable, so leaving the
            // pane "open" would let the next deliverable auto-expand it (the pane
            // must open only on a manual expand click).
            const st = useExecutionStore.getState();
            setFocusedRunSteps(null);
            setFocusedRunStep(null);
            st.setActivityPlacement('chat');
            st.clearPreview();
          }}
        />
      )}

      {/* Chat-native MCP dialog — opened from the composer picker's "Connect a
          tool" action. Styled with chat tokens (not the MUI config dialog, which
          stays for the Agent Builder). The picker refetches its list on reopen,
          so a server enabled here shows up next time without extra wiring. */}
      <ChatMcpDialog
        open={mcpConfigOpen}
        onClose={() => setMcpConfigOpen(false)}
      />

    </div>
  );
};

/** Tiny component to show spinner for sessions with active executions */
const SessionSpinner: React.FC<{ sessionId: string }> = ({ sessionId }) => {
  const hasActive = useExecutionStore((s) => s.hasActiveExecution(sessionId));
  if (!hasActive) return null;
  // A clearly-visible accent ring (was an 8px hairline that read as a static dot)
  // so an in-progress session is obvious at a glance in the list.
  return (
    <span
      role="status"
      aria-label="Running"
      title="Running…"
      className="w-3.5 h-3.5 rounded-full border-2 border-t-transparent animate-spin flex-shrink-0"
      style={{ borderColor: 'var(--accent)', borderTopColor: 'transparent' }}
    />
  );
};

export default ChatWorkspace;
