import React, { useState, useRef, useEffect } from 'react';
import { ModelConfigResponse } from '../../types/dispatcher';
import { uploadKnowledgeFile } from '../../api/knowledge';

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
  { command: '/list crews', description: 'List saved crews' },
  { command: '/list flows', description: 'List saved flows' },
  { command: '/jobs', description: 'List recent executions' },
  { command: '/load crew ', description: 'Load a crew by name' },
  { command: '/load flow ', description: 'Load a flow by name' },
  { command: '/run crew ', description: 'Run a crew by name' },
  { command: '/run flow ', description: 'Run a flow by name' },
  { command: '/stop ', description: 'Stop an execution by job ID' },
  { command: '/delete crew ', description: 'Delete a crew by name' },
  { command: '/delete flow ', description: 'Delete a flow by name' },
  { command: '/dismiss', description: 'Dismiss execution panel' },
  { command: '/clear', description: 'Clear chat history' },
];

// Per-message output format. "auto" lets the crew infer; the others append a
// directive so the generated crew + the structured-UI renderer target that type.
type FormatKey = 'auto' | 'presentation' | 'quiz' | 'dashboard' | 'album' | 'mindmap' | 'report' | 'genie';
const FORMAT_OPTIONS: { key: FormatKey; label: string; directive: string }[] = [
  { key: 'auto', label: 'Auto format', directive: '' },
  { key: 'presentation', label: 'Presentation', directive: 'Produce the final result as a slide presentation (a deck of slides).' },
  { key: 'quiz', label: 'Interactive quiz', directive: 'Produce the final result as an interactive multiple-choice quiz that tracks the score.' },
  { key: 'dashboard', label: 'Dashboard', directive: 'Produce the final result as a metrics dashboard with KPI tiles and charts.' },
  { key: 'album', label: 'Album', directive: 'Produce the final result as an album: a responsive image gallery built from the EXISTING image links (an Album with images:[{url, caption}]). Use the real image URLs — do not invent images.' },
  { key: 'mindmap', label: 'Mindmap', directive: 'Produce the final result as a mindmap: a central topic that branches into sub-topics and details (a Mindmap whose nested root has label + children).' },
  { key: 'report', label: 'Report', directive: 'Produce the final result as a structured, readable report.' },
  { key: 'genie', label: 'Data answer (Genie)', directive: 'This is a structured-data question answered from a Genie space. Use the GenieTool to query the data — assign GenieTool to the task(s) that gather or analyze data. Do NOT use web-search tools (PerplexityTool, ScrapeWebsiteTool) and do NOT add web-research tasks: ALL data comes from Genie. Keep the crew minimal (ideally a single agent that queries Genie). Produce the final result as a data answer: a short answer, the result Table from the Genie query, and a chart when useful.' },
];

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
}

const attachmentsKey = (sessionId: string) => `kasal-chat-attachments-${sessionId}`;

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
}) => {
  const [value, setValue] = useState('');
  const [showCommands, setShowCommands] = useState(false);
  const [showModelPicker, setShowModelPicker] = useState(false);
  const [showFormatPicker, setShowFormatPicker] = useState(false);
  const [format, setFormat] = useState<FormatKey>('auto');
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const modelPickerRef = useRef<HTMLDivElement>(null);
  const formatPickerRef = useRef<HTMLDivElement>(null);
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

  // Close model / format pickers on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (modelPickerRef.current && !modelPickerRef.current.contains(e.target as Node)) {
        setShowModelPicker(false);
      }
      if (formatPickerRef.current && !formatPickerRef.current.contains(e.target as Node)) {
        setShowFormatPicker(false);
      }
    };
    if (showModelPicker || showFormatPicker) {
      document.addEventListener('mousedown', handleClick);
      return () => document.removeEventListener('mousedown', handleClick);
    }
  }, [showModelPicker, showFormatPicker]);

  const handleSend = () => {
    const trimmed = value.trim();
    if (!trimmed || disabled || isUploading) return;

    setCommandHistory((prev) => [...prev.slice(-50), trimmed]);
    setHistoryIndex(-1);
    // Slash commands are literal; only natural-language prompts get a format hint.
    const isSlash = trimmed.startsWith('/');
    const directive = isSlash
      ? ''
      : (FORMAT_OPTIONS.find((f) => f.key === format)?.directive || '');
    // Neither the output-format hint nor the knowledge note is appended to the
    // VISIBLE message — the chat shows only what the user typed. Both ride along
    // with the dispatch payload via dispatchSuffix so they still steer the crew.
    let dispatchSuffix = '';
    const tools: string[] = [];
    let attachments: string[] | undefined;

    if (directive) {
      dispatchSuffix += `\n\n[Output format: ${directive}]`;
    }
    // Attach uploaded knowledge: include the knowledge-search tool + a steering note.
    if (!isSlash && readyAttachments.length > 0) {
      attachments = readyAttachments.map((a) => a.name);
      tools.push(KNOWLEDGE_TOOL);
      dispatchSuffix += `\n\n[Knowledge files attached: ${attachments.join(', ')}. Use the ${KNOWLEDGE_TOOL} to search the uploaded documents before answering.]`;
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

      {/* Model picker dropdown — anchored to the right */}
      {showModelPicker && models.length > 0 && (
        <div
          ref={modelPickerRef}
          className="kasal-popover absolute top-full mt-2 right-4 w-72 rounded-xl overflow-hidden z-10 animate-slide-up"
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
              Model
            </span>
          </div>
          <div className="max-h-64 overflow-y-auto pb-1">
            {models.map((m) => (
              <button
                key={m.key}
                onClick={() => {
                  onModelChange(m.key);
                  setShowModelPicker(false);
                  inputRef.current?.focus();
                }}
                className="w-full text-left px-3 py-2 flex items-center justify-between transition-colors"
                style={{
                  backgroundColor:
                    m.key === selectedModel ? 'var(--bg-active-chip)' : 'transparent',
                }}
              >
                <div>
                  <div
                    className="text-sm font-medium"
                    style={{ color: 'var(--text-primary)' }}
                  >
                    {m.name}
                  </div>
                  {m.provider && (
                    <div
                      className="text-[11px] mt-0.5"
                      style={{ color: 'var(--text-muted)' }}
                    >
                      {m.provider}
                    </div>
                  )}
                </div>
                {m.key === selectedModel && (
                  <svg
                    className="w-4 h-4 flex-shrink-0"
                    style={{ color: 'var(--accent)' }}
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={2.5}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                  </svg>
                )}
              </button>
            ))}
          </div>
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

      {/* Input container — two-row layout, also a drop target */}
      <div
        className="kasal-input-shell rounded-3xl overflow-hidden relative transition-all"
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

        {/* Top row — textarea with sparkle icon */}
        <div className="flex items-start gap-3 px-5 pt-4 pb-1">
          {/* Sparkle icon */}
          <svg
            className="w-5 h-5 mt-0.5 flex-shrink-0"
            style={{ color: 'var(--text-muted)' }}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={1.5}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456zM16.894 20.567L16.5 21.75l-.394-1.183a2.25 2.25 0 00-1.423-1.423L13.5 18.75l1.183-.394a2.25 2.25 0 001.423-1.423l.394-1.183.394 1.183a2.25 2.25 0 001.423 1.423l1.183.394-1.183.394a2.25 2.25 0 00-1.423 1.423z" />
          </svg>
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
          {/* format + model selector + attach + send */}
          <div className="flex items-center gap-2">
            {/* Workspace-memory toggle — ON (default): recall context across the
                whole workspace; OFF: only this chat session. */}
            <button
              type="button"
              onClick={() => onWorkspaceMemoryChange?.(!workspaceMemory)}
              aria-pressed={workspaceMemory}
              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
              style={{
                color: workspaceMemory ? 'var(--accent)' : 'var(--text-muted)',
                backgroundColor: 'var(--bg-secondary)',
              }}
              title={
                workspaceMemory
                  ? 'Workspace memory ON — recall context from across the workspace. Click to use only this chat session.'
                  : 'Workspace memory OFF — recall only this chat session. Click to use workspace-wide context.'
              }
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M12 6.042A8.967 8.967 0 006 3.75c-1.052 0-2.062.18-3 .512v14.25A8.987 8.987 0 016 18c2.305 0 4.408.867 6 2.292m0-14.25a8.966 8.966 0 016-2.292c1.052 0 2.062.18 3 .512v14.25A8.987 8.987 0 0018 18a8.967 8.967 0 00-6 2.292m0-14.25v14.25" />
              </svg>
              <span>{workspaceMemory ? 'Workspace memory' : 'Session only'}</span>
            </button>

            {/* Output format selector */}
            <div className="relative" ref={formatPickerRef}>
              <button
                onClick={() => {
                  setShowFormatPicker((v) => !v);
                  setShowModelPicker(false);
                  setShowCommands(false);
                }}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
                style={{
                  color: format === 'auto' ? 'var(--text-secondary)' : 'var(--accent)',
                  backgroundColor: 'var(--bg-secondary)',
                }}
                title="Choose the output format the crew should produce"
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
                </svg>
                <span className="max-w-[120px] truncate">
                  {FORMAT_OPTIONS.find((f) => f.key === format)?.label}
                </span>
                <svg className={`w-3 h-3 transition-transform ${showFormatPicker ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                </svg>
              </button>
              {showFormatPicker && (
                <div
                  className="absolute bottom-full right-0 mb-1 w-56 rounded-lg shadow-lg overflow-hidden z-50 py-1"
                  style={{ backgroundColor: 'var(--bg-input)', border: '1px solid var(--border-color)' }}
                >
                  {FORMAT_OPTIONS.map((opt) => (
                    <button
                      key={opt.key}
                      onClick={() => { setFormat(opt.key); setShowFormatPicker(false); }}
                      className="w-full text-left px-3 py-2 text-xs transition-colors hover:opacity-80 flex items-center justify-between gap-2"
                      style={{
                        color: 'var(--text-primary)',
                        backgroundColor: opt.key === format ? 'var(--bg-secondary)' : 'transparent',
                      }}
                    >
                      <span>{opt.label}</span>
                      {opt.key === format && (
                        <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--accent)' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                        </svg>
                      )}
                    </button>
                  ))}
                </div>
              )}
            </div>

            {/* Model selector button */}
            {models.length > 0 && (
              <button
                onClick={() => {
                  setShowModelPicker(!showModelPicker);
                  setShowCommands(false);
                }}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
                style={{
                  color: 'var(--text-secondary)',
                  backgroundColor: 'var(--bg-secondary)',
                }}
                title="Select model"
              >
                <svg
                  className="w-3.5 h-3.5"
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

            {/* Send — submit only. Stop lives in the run-activity container above. */}
            <button
              onClick={handleSend}
              disabled={disabled || isUploading || isGenerating || isExecuting || !value.trim()}
              title={isUploading ? 'Waiting for attachments to finish uploading…' : undefined}
              aria-label="Send message"
              className="flex-shrink-0 w-8 h-8 rounded-xl flex items-center justify-center transition-all hover:opacity-80 disabled:cursor-not-allowed"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                color:
                  disabled || isUploading || isGenerating || isExecuting
                    ? 'var(--text-muted)'
                    : value.trim()
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
              ) : (
                <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 10.5L12 3m0 0l7.5 7.5M12 3v18" />
                </svg>
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ChatInput;
