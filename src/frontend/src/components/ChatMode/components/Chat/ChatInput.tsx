import React, { useState, useRef, useEffect, useLayoutEffect } from 'react';
import { ModelConfigResponse } from '../../types/dispatcher';
import { uploadKnowledgeFile } from '../../api/knowledge';
import McpPicker from './McpPicker';
import TrifectaNotice from './TrifectaNotice';
import SharedWorkspaceNotice from './SharedWorkspaceNotice';
import { useExecutionStore } from '../../store/executionStore';

// The crew tool that searches uploaded knowledge. Passed to the dispatcher so a
// generated crew can read files attached in chat.
const KNOWLEDGE_TOOL = 'DatabricksKnowledgeSearchTool';

interface Attachment {
  id: string;
  name: string;
  size: number;
  status: 'uploading' | 'ready' | 'error';
  path?: string;
  error?: string;
}

/** Metadata sent alongside a chat message (e.g. tools the crew should include). */
export interface SendMeta {
  tools?: string[];
  /**
   * Text appended to the DISPATCH payload only (not shown in the chat). Used to
   * steer the crew (e.g. "search the attached knowledge") without cluttering the
   * visible user message.
   */
  dispatchSuffix?: string;
  /** Attached knowledge-file names, shown as chips on the user's message. */
  attachments?: string[];
}

function formatBytes(bytes: number): string {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return `${val >= 10 || i === 0 ? Math.round(val) : val.toFixed(1)} ${units[i]}`;
}

const SLASH_COMMANDS = [
  { command: '/help', description: 'Show available commands' },
  { command: '/jobs', description: 'List recent executions' },
  { command: '/run crew ', description: 'Run a crew by name' },
  { command: '/run flow ', description: 'Run a flow by name' },
  { command: '/stop ', description: 'Stop an execution by job ID' },
  { command: '/delete crew ', description: 'Delete a crew by name' },
  { command: '/delete flow ', description: 'Delete a flow by name' },
  { command: '/dismiss', description: 'Dismiss execution panel' },
  { command: '/clear', description: 'Clear chat history' },
];

// Answer modes shown in the composer's mode pill. 'chat' runs a single light
// agent (fast); 'research'/'deep' build a crew (with reasoning, +planning).
// `label` is the full name (dropdown rows + aria); `short` is the compact label
// shown on the collapsed trigger pill so the composer's control row stays tidy.
const MODES = [
  { id: 'chat', label: 'Chat', short: 'Chat', hint: 'Quick answer from a single agent' },
  { id: 'research', label: 'Research', short: 'Research', hint: 'Full crew with reasoning' },
  { id: 'deep', label: 'Deep Research', short: 'Deep', hint: 'Deep tools with planning & reasoning' },
] as const;

// Memory modes shown in the composer's memory pill — same labelled-dropdown
// pattern as the answer-mode pill so the three states are explicit (no blind
// cycling). Mapped to the (memoryEnabled, workspaceMemory) prop pair below.
type MemoryModeId = 'workspace' | 'session' | 'off';
const MEMORY_MODES: { id: MemoryModeId; label: string; short: string; hint: string }[] = [
  { id: 'workspace', label: 'Workspace memory', short: 'Workspace', hint: 'Recall context across the whole workspace' },
  { id: 'session', label: 'Session memory', short: 'Session', hint: 'Recall only this chat session' },
  { id: 'off', label: 'No memory', short: 'Off', hint: 'Run without memory — nothing recalled or persisted' },
];

// NOTE: there is deliberately NO per-message output-format picker. The
// deliverable type (presentation, dashboard, quiz, …) is derived from the
// request's content by crew generation + deliverable inference (backend
// ui_emission.py); an enumerated format list would not scale as output
// varieties grow.

interface ChatInputProps {
  onSend: (message: string, meta?: SendMeta) => void;
  disabled?: boolean;
  models: ModelConfigResponse[];
  selectedModel: string;
  onModelChange: (model: string) => void;
  /** Active chat session id — pending (uploaded, unsent) attachments persist per session. */
  sessionId?: string | null;
  /** This session owns a crew that is currently running — the Send button becomes Stop. */
  isExecuting?: boolean;
  /** This session is generating a crew — the Send button shows a busy spinner. */
  isGenerating?: boolean;
  /** Stop the running execution (only meaningful while isExecuting). */
  onStopExecution?: () => void;
  /**
   * "Workspace memory" toggle — owned by the parent so it persists across the
   * empty→conversation input swap and remounts. true (default) = recall
   * workspace-wide; false = restrict recall to this chat session only.
   */
  workspaceMemory?: boolean;
  onWorkspaceMemoryChange?: (value: boolean) => void;
  /**
   * "No memory" toggle — owned by the parent for the same persistence reason.
   * true (default) = crews keep memory (scope governed by workspaceMemory);
   * false = agents run without memory (nothing recalled or persisted).
   */
  memoryEnabled?: boolean;
  onMemoryEnabledChange?: (value: boolean) => void;
  /** A crew/flow loaded from the catalog; when set and the input is empty, the
   *  submit button runs it instead of sending a message. */
  pendingRunLabel?: string;
  onRunPending?: () => void;
  /**
   * Where the composer's pop-up menus open relative to their trigger. The input
   * is centered on the empty/landing screen (room below → 'down') and pinned to
   * the bottom once a conversation starts (no room below → 'up'). Defaults to
   * 'up' for the bottom-pinned case.
   */
  menuPlacement?: 'up' | 'down';
}

const attachmentsKey = (sessionId: string) => `kasal-chat-attachments-${sessionId}`;

// Width of the composer pop-up menus (matches Tailwind `w-72` = 18rem).
const MENU_WIDTH = 288;

/**
 * Anchored fixed-position style for a composer pop-up menu.
 *
 * The pills sit inside the chat's `overflow-hidden` layout containers (the
 * <main> column + the chat scroll wrapper). An `absolute` menu that extends
 * past those bounds — which happens once the sidebar narrows <main> — gets
 * CLIPPED, so the menu appears to vanish "behind" the sidebar. `position: fixed`
 * is positioned against the viewport and is NOT clipped by an ancestor's
 * overflow, while keeping the menu a DOM child of the picker wrapper (so the
 * outside-click `contains()` checks and the #kasal-chat-root theme/Tailwind
 * scope both still apply). We compute the coords from the trigger: right-edge
 * aligned to it, opening up or down per `placement`, clamped to the viewport.
 */
function useAnchoredFixedStyle(
  open: boolean,
  anchorRef: React.RefObject<HTMLElement>,
  placement: 'up' | 'down',
): React.CSSProperties {
  const [style, setStyle] = useState<React.CSSProperties>({ position: 'fixed' });
  useLayoutEffect(() => {
    const el = anchorRef.current;
    if (!open || !el) return;
    const update = () => {
      const r = el.getBoundingClientRect();
      const left = Math.max(8, Math.min(r.right - MENU_WIDTH, window.innerWidth - MENU_WIDTH - 8));
      setStyle(
        placement === 'down'
          ? { position: 'fixed', left, top: r.bottom + 8, width: MENU_WIDTH }
          : { position: 'fixed', left, bottom: window.innerHeight - r.top + 8, width: MENU_WIDTH },
      );
    };
    update();
    window.addEventListener('resize', update);
    window.addEventListener('scroll', update, true);
    return () => {
      window.removeEventListener('resize', update);
      window.removeEventListener('scroll', update, true);
    };
  }, [open, placement, anchorRef]);
  return style;
}

const ChatInput: React.FC<ChatInputProps> = ({
  onSend,
  disabled = false,
  models,
  selectedModel,
  onModelChange,
  sessionId,
  isExecuting = false,
  isGenerating = false,
  workspaceMemory = true,
  onWorkspaceMemoryChange,
  memoryEnabled = true,
  onMemoryEnabledChange,
  pendingRunLabel,
  onRunPending,
  menuPlacement = 'up',
}) => {
  // Entrance animation for the pop-up menus, matching the open direction. The
  // menus are positioned with `position: fixed` (see useAnchoredFixedStyle) so
  // they escape the chat's overflow-hidden containers; this class only drives the
  // slide-in motion, not the placement.
  const menuAnimClass =
    menuPlacement === 'down' ? 'animate-slide-down' : 'animate-slide-up';
  const [value, setValue] = useState('');
  const [showCommands, setShowCommands] = useState(false);
  const [showModelPicker, setShowModelPicker] = useState(false);
  const [showModePicker, setShowModePicker] = useState(false);
  const [showMemoryPicker, setShowMemoryPicker] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const modelPickerRef = useRef<HTMLDivElement>(null);
  const modePickerRef = useRef<HTMLDivElement>(null);
  const memoryPickerRef = useRef<HTMLDivElement>(null);
  // Viewport-anchored fixed coords for each pop-up menu (escape overflow-hidden).
  const modeMenuStyle = useAnchoredFixedStyle(showModePicker, modePickerRef, menuPlacement);
  const memoryMenuStyle = useAnchoredFixedStyle(showMemoryPicker, memoryPickerRef, menuPlacement);
  const modelMenuStyle = useAnchoredFixedStyle(showModelPicker, modelPickerRef, menuPlacement);
  // Answer mode (chat|research|deep) lives in the store so the choice persists
  // and is consistent across ChatInput's dual mount (read store-direct, not props).
  const chatModeType = useExecutionStore((s) => s.chatModeType);
  const setChatModeType = useExecutionStore((s) => s.setChatModeType);
  const activeMode = MODES.find((m) => m.id === chatModeType) ?? MODES[0];
  // Current memory mode derived from the (memoryEnabled, workspaceMemory) pair.
  const memoryModeId: MemoryModeId = !memoryEnabled
    ? 'off'
    : workspaceMemory
      ? 'workspace'
      : 'session';
  const activeMemory = MEMORY_MODES.find((m) => m.id === memoryModeId) ?? MEMORY_MODES[0];
  const selectMemoryMode = (id: MemoryModeId) => {
    if (id === 'off') {
      onMemoryEnabledChange?.(false);
    } else {
      onMemoryEnabledChange?.(true);
      onWorkspaceMemoryChange?.(id === 'workspace');
    }
    setShowMemoryPicker(false);
    inputRef.current?.focus();
  };
  const fileInputRef = useRef<HTMLInputElement>(null);
  const dragDepth = useRef(0);
  const hydratedRef = useRef(false);
  // Stable per-mount id that scopes uploaded files in the Volume; group_id (from
  // the shared api client) is what scopes knowledge search, so any stable id works.
  const uploadScopeId = useRef(
    `chat-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`
  );

  // Restore pending (uploaded, unsent) attachments for the active session so an
  // uploaded file survives a page refresh / session switch.
  useEffect(() => {
    hydratedRef.current = false;
    if (sessionId) {
      try {
        const raw = localStorage.getItem(attachmentsKey(sessionId));
        const stored = raw ? (JSON.parse(raw) as Attachment[]) : [];
        setAttachments(Array.isArray(stored) ? stored : []);
      } catch {
        setAttachments([]);
      }
    }
    hydratedRef.current = true;
  }, [sessionId]);

  // Persist only the ready (fully uploaded) attachments; transient
  // uploading/error chips are not restored.
  useEffect(() => {
    if (!hydratedRef.current || !sessionId) return;
    const ready = attachments.filter((a) => a.status === 'ready');
    try {
      if (ready.length > 0) {
        localStorage.setItem(attachmentsKey(sessionId), JSON.stringify(ready));
      } else {
        localStorage.removeItem(attachmentsKey(sessionId));
      }
    } catch {
      /* ignore storage failures */
    }
  }, [attachments, sessionId]);

  const setAttachment = (id: string, patch: Partial<Attachment>) =>
    setAttachments((prev) => prev.map((a) => (a.id === id ? { ...a, ...patch } : a)));

  const uploadAttachment = async (id: string, file: File) => {
    try {
      const result = await uploadKnowledgeFile(file, sessionId || uploadScopeId.current);
      setAttachment(id, { status: 'ready', path: result.path });
    } catch (err) {
      setAttachment(id, {
        status: 'error',
        error: err instanceof Error ? err.message : 'Upload failed',
      });
    }
  };

  const addFiles = (files: File[]) => {
    files.forEach((file) => {
      const id = `${file.name}-${file.size}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      setAttachments((prev) => [
        ...prev,
        { id, name: file.name, size: file.size, status: 'uploading' },
      ]);
      void uploadAttachment(id, file);
    });
  };

  const removeAttachment = (id: string) =>
    setAttachments((prev) => prev.filter((a) => a.id !== id));

  const handleFilesSelected = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      addFiles(Array.from(e.target.files));
    }
    // Reset so selecting the same file again re-triggers change.
    e.target.value = '';
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    dragDepth.current = 0;
    setIsDragging(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      addFiles(Array.from(e.dataTransfer.files));
    }
  };

  const handleDragEnter = (e: React.DragEvent) => {
    if (Array.from(e.dataTransfer.types).includes('Files')) {
      dragDepth.current += 1;
      setIsDragging(true);
    }
  };

  const handleDragLeave = () => {
    dragDepth.current = Math.max(0, dragDepth.current - 1);
    if (dragDepth.current === 0) setIsDragging(false);
  };

  const readyAttachments = attachments.filter((a) => a.status === 'ready');
  // Block sending while any file is still uploading (don't drop in-flight files).
  const isUploading = attachments.some((a) => a.status === 'uploading');

  const filteredCommands = SLASH_COMMANDS.filter((cmd) =>
    cmd.command.toLowerCase().startsWith(value.toLowerCase())
  );

  useEffect(() => {
    if (value.startsWith('/') && value.length > 0) {
      setShowCommands(filteredCommands.length > 0);
      setSelectedIndex(0);
    } else {
      setShowCommands(false);
    }
  }, [value, filteredCommands.length]);

  // Close the model picker on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (modelPickerRef.current && !modelPickerRef.current.contains(e.target as Node)) {
        setShowModelPicker(false);
      }
    };
    if (showModelPicker) {
      document.addEventListener('mousedown', handleClick);
      return () => document.removeEventListener('mousedown', handleClick);
    }
  }, [showModelPicker]);

  // Close the mode picker on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (modePickerRef.current && !modePickerRef.current.contains(e.target as Node)) {
        setShowModePicker(false);
      }
    };
    if (showModePicker) {
      document.addEventListener('mousedown', handleClick);
      return () => document.removeEventListener('mousedown', handleClick);
    }
  }, [showModePicker]);

  // Close the memory picker on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (memoryPickerRef.current && !memoryPickerRef.current.contains(e.target as Node)) {
        setShowMemoryPicker(false);
      }
    };
    if (showMemoryPicker) {
      document.addEventListener('mousedown', handleClick);
      return () => document.removeEventListener('mousedown', handleClick);
    }
  }, [showMemoryPicker]);

  const handleSend = () => {
    const trimmed = value.trim();
    if (!trimmed || disabled || isUploading) return;

    setCommandHistory((prev) => [...prev.slice(-50), trimmed]);
    setHistoryIndex(-1);
    // Slash commands are literal; only natural-language prompts get extras.
    const isSlash = trimmed.startsWith('/');
    // The knowledge note is not appended to the VISIBLE message — the chat shows
    // only what the user typed. It rides along with the dispatch payload via
    // dispatchSuffix so it still steers the crew.
    let dispatchSuffix = '';
    const tools: string[] = [];
    let attachments: string[] | undefined;

    // Attach uploaded knowledge: include the knowledge-search tool + a steering note.
    if (!isSlash && readyAttachments.length > 0) {
      attachments = readyAttachments.map((a) => a.name);
      tools.push(KNOWLEDGE_TOOL);
      dispatchSuffix += `\n\n[Knowledge files attached: ${attachments.join(', ')}. Use the ${KNOWLEDGE_TOOL} to search the uploaded documents before answering.]`;
    }

    // MCP servers picked via the "+" menu: steer the crew GENERATION around
    // them. The picker only equips tools at run time — without this note an
    // ambiguous prompt ("what can I ask here?") generates a crew unrelated to
    // the selected data sources, which then never queries them.
    const selectedMcp = useExecutionStore.getState().selectedMcpServers;
    if (!isSlash && selectedMcp.length > 0) {
      dispatchSuffix += `\n\n[MCP data sources attached: ${selectedMcp.join(', ')}. Design the crew to answer using these sources — references like "here" or "this data" mean them.]`;
    }

    // The "Workspace memory" scope is owned by the store (read at execution
    // time), so meta only carries per-message extras. Omit the arg entirely
    // when there are none, so a plain message is a clean single-arg send.
    const meta: SendMeta = {
      ...(tools.length ? { tools } : {}),
      ...(dispatchSuffix ? { dispatchSuffix } : {}),
      ...(attachments ? { attachments } : {}),
    };
    if (Object.keys(meta).length) {
      onSend(trimmed, meta);
    } else {
      onSend(trimmed);
    }
    setValue('');
    setShowCommands(false);
    // Keep attachments after sending so follow-up prompts in the same session
    // can reuse the uploaded knowledge (remove via the chip's × when done).
    // Reset the auto-grown height after sending. The textarea is always mounted
    // when this runs, so the ref is non-null.
    inputRef.current!.style.height = 'auto';
  };

  const handleSelectCommand = (command: string) => {
    setValue(command);
    setShowCommands(false);
    inputRef.current?.focus();

    const needsParam = command.endsWith(' ');
    if (!needsParam) {
      onSend(command);
      setValue('');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (showCommands) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setSelectedIndex((prev) =>
          prev < filteredCommands.length - 1 ? prev + 1 : 0
        );
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSelectedIndex((prev) =>
          prev > 0 ? prev - 1 : filteredCommands.length - 1
        );
        return;
      }
      // Inside this block showCommands is true, which already implies
      // filteredCommands is non-empty.
      if (e.key === 'Tab' || e.key === 'Enter') {
        e.preventDefault();
        handleSelectCommand(filteredCommands[selectedIndex].command);
        return;
      }
      if (e.key === 'Escape') {
        setShowCommands(false);
        return;
      }
    }

    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
      return;
    }

    if (!showCommands && commandHistory.length > 0) {
      // Allow ArrowUp from an empty input, or to keep walking further back
      // while already navigating history (terminal-style recall).
      if (e.key === 'ArrowUp' && (!value || historyIndex !== -1)) {
        e.preventDefault();
        const newIndex =
          historyIndex === -1
            ? commandHistory.length - 1
            : Math.max(0, historyIndex - 1);
        setHistoryIndex(newIndex);
        setValue(commandHistory[newIndex]);
        return;
      }
      if (e.key === 'ArrowDown' && historyIndex !== -1) {
        e.preventDefault();
        if (historyIndex >= commandHistory.length - 1) {
          setHistoryIndex(-1);
          setValue('');
        } else {
          const newIndex = historyIndex + 1;
          setHistoryIndex(newIndex);
          setValue(commandHistory[newIndex]);
        }
        return;
      }
    }
  };

  const selectedModelObj = models.find((m) => m.key === selectedModel);
  const modelDisplayName = selectedModelObj?.name || selectedModel || 'Default';

  return (
    <div className="relative px-4 pb-5 pt-2">
      {/* Slash command autocomplete */}
      {showCommands && (
        <div
          className="kasal-popover absolute bottom-full mb-2 left-4 right-4 rounded-xl overflow-hidden z-10 animate-slide-up"
          style={{
            backgroundColor: 'var(--bg-input)',
            border: '1px solid var(--border-color)',
          }}
        >
          <div className="px-3 py-2">
            <span
              className="text-[10px] font-semibold uppercase tracking-wider"
              style={{ color: 'var(--text-muted)' }}
            >
              Commands
            </span>
          </div>
          {filteredCommands.map((cmd, i) => (
            <button
              key={cmd.command}
              onClick={() => handleSelectCommand(cmd.command)}
              className="w-full text-left px-3 py-2.5 text-sm flex items-center justify-between transition-colors"
              style={{
                backgroundColor:
                  i === selectedIndex ? 'var(--bg-active-chip)' : 'transparent',
              }}
              onMouseEnter={() => setSelectedIndex(i)}
            >
              <span
                className="font-mono text-[13px] font-medium"
                style={{ color: 'var(--accent)' }}
              >
                {cmd.command}
              </span>
              <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
                {cmd.description}
              </span>
            </button>
          ))}
        </div>
      )}

      {/* Hidden file input driving the attach button + drag-drop */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={handleFilesSelected}
        data-testid="chat-file-input"
        aria-hidden="true"
      />

      {/* Shared-workspace data-exposure notice. Shown once per shared (team)
          workspace: runs/results/memory here are visible to all members. */}
      <SharedWorkspaceNotice />

      {/* Lethal-trifecta heads-up — inline, non-blocking. Shows when the picked
          MCP servers / Agent Bricks endpoints combine an internal data source
          with a channel that could reach the internet or untrusted content. */}
      <TrifectaNotice />

      {/* Input container — two-row layout, also a drop target.
          No `overflow-hidden`: the MCP picker's popover is an absolutely-
          positioned menu that must escape the container's bounds (same
          decision as the run-activity container's Genie dropdown). The
          rounded border + bg already round the corners without clipping. */}
      <div
        className="kasal-input-shell rounded-3xl relative transition-all"
        style={{
          backgroundColor: 'var(--bg-input)',
          border: `1px solid ${isDragging ? 'var(--accent)' : 'var(--border-color)'}`,
          boxShadow: isDragging
            ? '0 0 0 3px color-mix(in srgb, var(--accent) 22%, transparent)'
            : 'var(--shadow-input)',
        }}
        onDrop={handleDrop}
        onDragOver={(e) => e.preventDefault()}
        onDragEnter={handleDragEnter}
        onDragLeave={handleDragLeave}
      >
        {/* Drag overlay */}
        {isDragging && (
          <div
            className="absolute inset-0 z-20 flex items-center justify-center rounded-3xl pointer-events-none animate-fade-in"
            style={{ backgroundColor: 'color-mix(in srgb, var(--bg-input) 86%, transparent)' }}
          >
            <div className="flex items-center gap-2 text-sm font-medium" style={{ color: 'var(--accent)' }}>
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 16.5V9.75m0 0l-3 3m3-3l3 3M6.75 19.5a4.5 4.5 0 01-1.41-8.775 5.25 5.25 0 0110.233-2.33 3 3 0 013.758 3.848A3.752 3.752 0 0118 19.5H6.75z" />
              </svg>
              Drop files to attach as knowledge
            </div>
          </div>
        )}

        {/* Top row — textarea */}
        <div className="flex items-start px-5 pt-4 pb-1">
          <textarea
            ref={inputRef}
            value={value}
            onChange={(e) => {
              setValue(e.target.value);
              setHistoryIndex(-1);
            }}
            onKeyDown={handleKeyDown}
            placeholder="Ask a question..."
            disabled={disabled}
            rows={2}
            className="w-full resize-none bg-transparent text-[15px] outline-none disabled:opacity-50 max-h-40 overflow-y-auto leading-relaxed"
            style={{
              color: 'var(--text-primary)',
              minHeight: '52px',
            }}
            onInput={(e) => {
              const target = e.target as HTMLTextAreaElement;
              target.style.height = 'auto';
              target.style.height = Math.min(target.scrollHeight, 160) + 'px';
            }}
          />
        </div>

        {/* Attachment chips */}
        {attachments.length > 0 && (
          <div className="flex flex-wrap gap-2 px-5 pb-1.5">
            {attachments.map((a) => (
              <div
                key={a.id}
                className="group/chip flex items-center gap-1.5 max-w-[230px] rounded-lg pl-2 pr-1 py-1 animate-slide-up"
                style={{
                  backgroundColor: 'var(--bg-secondary)',
                  border: `1px solid ${a.status === 'error' ? '#ef4444' : 'var(--border-color)'}`,
                }}
                title={a.status === 'error' ? a.error : a.name}
              >
                {a.status === 'uploading' ? (
                  <svg className="w-3.5 h-3.5 animate-spin flex-shrink-0" style={{ color: 'var(--text-muted)' }} fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                  </svg>
                ) : a.status === 'error' ? (
                  <svg className="w-3.5 h-3.5 flex-shrink-0" style={{ color: '#ef4444' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
                  </svg>
                ) : (
                  <svg className="w-3.5 h-3.5 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.7}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                  </svg>
                )}
                <span className="truncate text-xs font-medium" style={{ color: a.status === 'error' ? '#ef4444' : 'var(--text-primary)' }}>
                  {a.name}
                </span>
                <span className="text-[10px] flex-shrink-0" style={{ color: 'var(--text-muted)' }}>
                  {a.status === 'ready' ? formatBytes(a.size) : a.status === 'error' ? 'failed' : '…'}
                </span>
                <button
                  onClick={() => removeAttachment(a.id)}
                  className="flex-shrink-0 w-5 h-5 rounded-md flex items-center justify-center transition-colors hover:opacity-100 opacity-60"
                  style={{ color: 'var(--text-muted)' }}
                  title="Remove"
                  aria-label={`Remove ${a.name}`}
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            ))}
          </div>
        )}

        {/* Bottom row — controls + attach + send, all right-aligned */}
        <div className="flex items-center justify-end px-4 py-2.5">
          {/* mode + memory + model selector + attach + send */}
          <div className="flex items-center gap-2">
            {/* Answer-mode pill + dropdown. The dropdown is anchored to THIS
                pill (right-0, under the chat controls) and opens up/down per
                menuPlacement — never off at the screen's side. Chat = single
                light agent; Research = crew + reasoning; Deep Research = crew +
                planning + reasoning. Store-owned so it persists. */}
            <div className="relative" ref={modePickerRef}>
              <button
                type="button"
                onClick={() => {
                  setShowModePicker(!showModePicker);
                  setShowMemoryPicker(false);
                  setShowModelPicker(false);
                  setShowCommands(false);
                }}
                aria-label={`Answer mode: ${activeMode.label}`}
                title={activeMode.hint}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
                style={{ color: 'var(--text-secondary)', backgroundColor: 'transparent', border: 'none' }}
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
                </svg>
                <span>{activeMode.short}</span>
                <svg
                  className={`w-3 h-3 transition-transform ${showModePicker ? 'rotate-180' : ''}`}
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={2.5}
                >
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                </svg>
              </button>
              {showModePicker && (
                <div
                  className={`kasal-popover ${menuAnimClass} w-72 rounded-xl overflow-hidden z-50`}
                  style={{ ...modeMenuStyle, backgroundColor: 'var(--bg-input)', border: '1px solid var(--border-color)' }}
                >
                  <div className="px-3 py-2">
                    <span className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>
                      Answer mode
                    </span>
                  </div>
                  <div className="px-1.5 pb-1.5">
                    {MODES.map((m) => (
                      <button
                        key={m.id}
                        onClick={() => {
                          setChatModeType(m.id);
                          setShowModePicker(false);
                          inputRef.current?.focus();
                        }}
                        aria-label={`Answer mode: ${m.label}`}
                        className={`w-full text-left !px-2.5 !py-2 my-0.5 rounded-lg flex items-center justify-between transition-colors ${m.id === chatModeType ? 'bg-[var(--bg-active-chip)]' : 'hover:bg-[var(--bg-rail-hover)]'}`}
                      >
                        <div>
                          <div className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>{m.label}</div>
                          <div className="text-[11px] mt-0.5" style={{ color: 'var(--text-muted)' }}>{m.hint}</div>
                        </div>
                        {m.id === chatModeType && (
                          <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                          </svg>
                        )}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* Memory pill + dropdown — same labelled-dropdown pattern as the
                answer mode (no blind cycling): Workspace / Session / No memory.
                Owned by the parent (props) so the choice survives the
                empty→conversation remount. */}
            <div className="relative" ref={memoryPickerRef}>
              <button
                type="button"
                onClick={() => {
                  setShowMemoryPicker(!showMemoryPicker);
                  setShowModePicker(false);
                  setShowModelPicker(false);
                  setShowCommands(false);
                }}
                aria-label={`Memory mode: ${activeMemory.label}`}
                title={activeMemory.hint}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
                style={{
                  color: memoryEnabled ? 'var(--text-secondary)' : 'var(--text-muted)',
                  backgroundColor: 'transparent',
                  border: 'none',
                }}
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 6.042A8.967 8.967 0 006 3.75c-1.052 0-2.062.18-3 .512v14.25A8.987 8.987 0 016 18c2.305 0 4.408.867 6 2.292m0-14.25a8.966 8.966 0 016-2.292c1.052 0 2.062.18 3 .512v14.25A8.987 8.987 0 0018 18a8.967 8.967 0 00-6 2.292m0-14.25v14.25" />
                  {!memoryEnabled && (
                    <path strokeLinecap="round" strokeLinejoin="round" d="M4 4l16 16" />
                  )}
                </svg>
                <span>{activeMemory.short}</span>
                <svg
                  className={`w-3 h-3 transition-transform ${showMemoryPicker ? 'rotate-180' : ''}`}
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={2.5}
                >
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                </svg>
              </button>
              {showMemoryPicker && (
                <div
                  className={`kasal-popover ${menuAnimClass} w-72 rounded-xl overflow-hidden z-50`}
                  style={{ ...memoryMenuStyle, backgroundColor: 'var(--bg-input)', border: '1px solid var(--border-color)' }}
                >
                  <div className="px-3 py-2">
                    <span className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>
                      Memory
                    </span>
                  </div>
                  <div className="px-1.5 pb-1.5">
                    {MEMORY_MODES.map((m) => (
                      <button
                        key={m.id}
                        onClick={() => selectMemoryMode(m.id)}
                        aria-label={`Memory mode: ${m.label}`}
                        className={`w-full text-left !px-2.5 !py-2 my-0.5 rounded-lg flex items-center justify-between transition-colors ${m.id === memoryModeId ? 'bg-[var(--bg-active-chip)]' : 'hover:bg-[var(--bg-rail-hover)]'}`}
                      >
                        <div>
                          <div className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>{m.label}</div>
                          <div className="text-[11px] mt-0.5" style={{ color: 'var(--text-muted)' }}>{m.hint}</div>
                        </div>
                        {m.id === memoryModeId && (
                          <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                          </svg>
                        )}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* Model pill + dropdown — same anchored up/down pattern as the
                answer-mode & memory pills: opens DOWN when the composer is
                centered (empty state) and UP once it's docked at the bottom
                (menuPlacement), so it never renders off-screen. */}
            {models.length > 0 && (
              <div className="relative" ref={modelPickerRef}>
                <button
                  onClick={() => {
                    setShowModelPicker(!showModelPicker);
                    setShowModePicker(false);
                    setShowMemoryPicker(false);
                    setShowCommands(false);
                  }}
                  className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
                  style={{
                    color: 'var(--text-secondary)',
                    backgroundColor: 'transparent',
                    border: 'none',
                  }}
                  title="Select model"
                >
                  <svg
                    className="w-3.5 h-3.5 flex-shrink-0"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={2}
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      d="M9.75 3.104v5.714a2.25 2.25 0 01-.659 1.591L5 14.5M9.75 3.104c-.251.023-.501.05-.75.082m.75-.082a24.301 24.301 0 014.5 0m0 0v5.714c0 .597.237 1.17.659 1.591L19.8 15.3M14.25 3.104c.251.023.501.05.75.082M19.8 15.3l-1.57.393A9.065 9.065 0 0112 15a9.065 9.065 0 00-6.23.693L5 14.5m14.8.8l1.402 1.402c1.232 1.232.65 3.318-1.067 3.611A48.309 48.309 0 0112 21c-2.773 0-5.491-.235-8.135-.687-1.718-.293-2.3-2.379-1.067-3.61L5 14.5"
                    />
                  </svg>
                  <span className="max-w-[140px] truncate">{modelDisplayName}</span>
                  <svg
                    className={`w-3 h-3 transition-transform ${showModelPicker ? 'rotate-180' : ''}`}
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={2.5}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                  </svg>
                </button>
                {showModelPicker && (
                  <div
                    className={`kasal-popover ${menuAnimClass} w-72 rounded-xl overflow-hidden z-50`}
                    style={{ ...modelMenuStyle, backgroundColor: 'var(--bg-input)', border: '1px solid var(--border-color)' }}
                  >
                    <div className="px-3 py-2">
                      <span className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: 'var(--text-muted)' }}>
                        Model
                      </span>
                    </div>
                    <div className="max-h-64 overflow-y-auto px-1.5 pb-1.5">
                      {models.map((m) => (
                        <button
                          key={m.key}
                          onClick={() => {
                            onModelChange(m.key);
                            setShowModelPicker(false);
                            inputRef.current?.focus();
                          }}
                          className={`w-full text-left !px-2.5 !py-2 my-0.5 rounded-lg flex items-center justify-between transition-colors ${m.key === selectedModel ? 'bg-[var(--bg-active-chip)]' : 'hover:bg-[var(--bg-rail-hover)]'}`}
                        >
                          <div>
                            <div className="text-sm font-medium" style={{ color: 'var(--text-primary)' }}>{m.name}</div>
                            {m.provider && (
                              <div className="text-[11px] mt-0.5" style={{ color: 'var(--text-muted)' }}>{m.provider}</div>
                            )}
                          </div>
                          {m.key === selectedModel && (
                            <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                            </svg>
                          )}
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Attach knowledge files — sits just left of Send. */}
            <button
              onClick={() => fileInputRef.current?.click()}
              disabled={disabled}
              className="relative flex-shrink-0 w-8 h-8 rounded-xl flex items-center justify-center transition-colors hover:opacity-80 disabled:opacity-40 disabled:cursor-not-allowed"
              style={{ color: 'var(--text-secondary)', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
              title="Attach files as knowledge for the crew to search"
              aria-label="Attach files"
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M18.375 12.739l-7.693 7.693a4.5 4.5 0 01-6.364-6.364l10.94-10.94A3 3 0 1119.5 7.372L8.552 18.32m.009-.01l-.01.01m5.699-9.941l-7.81 7.81a1.5 1.5 0 002.112 2.13" />
              </svg>
              {attachments.length > 0 && (
                <span
                  className="absolute -top-1 -right-1 text-[9px] tabular-nums rounded-full min-w-[14px] h-[14px] flex items-center justify-center px-0.5"
                  style={{ backgroundColor: 'var(--accent)', color: '#fff' }}
                >
                  {attachments.length}
                </span>
              )}
            </button>

            {/* MCP picker ("+") — select the MCP servers (Kasal-configured and
                Databricks managed) the next crew gets equipped with. */}
            <McpPicker disabled={disabled} menuPlacement={menuPlacement} />

            {/* Send — submit only. Stop lives in the run-activity container above.
                When a catalog crew/flow is loaded and the input is empty, the
                submit button RUNS it (play icon) instead of sending a message. */}
            {(() => {
              const runMode = !value.trim() && !!pendingRunLabel && !isExecuting && !isGenerating && !disabled && !isUploading;
              return (
            <button
              onClick={runMode ? onRunPending : handleSend}
              disabled={disabled || isUploading || isGenerating || isExecuting || (!value.trim() && !pendingRunLabel)}
              title={
                isUploading
                  ? 'Waiting for attachments to finish uploading…'
                  : runMode
                    ? `Run “${pendingRunLabel}”`
                    : undefined
              }
              aria-label={runMode ? `Run ${pendingRunLabel}` : 'Send message'}
              className="flex-shrink-0 w-8 h-8 rounded-xl flex items-center justify-center transition-all hover:opacity-80 disabled:cursor-not-allowed"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                color:
                  disabled || isUploading || isGenerating || isExecuting
                    ? 'var(--text-muted)'
                    : runMode || value.trim()
                      ? 'var(--text-secondary)'
                      : 'var(--text-muted)',
                border: '1px solid var(--border-color)',
              }}
            >
              {disabled || isUploading || isGenerating ? (
                <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
              ) : runMode ? (
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M8 5v14l11-7z" />
                </svg>
              ) : (
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 10.5L12 3m0 0l7.5 7.5M12 3v18" />
                </svg>
              )}
            </button>
              );
            })()}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ChatInput;
