import React, { useState, useRef, useEffect } from 'react';
import { AssetService } from '../../../../api/chat/AssetService';
import AssetThumb from './AssetThumb';
import { isImageFile, measureImage } from '../../utils/imageFiles';
import type { ImageRef } from '../../types/chat';
import { ModelConfigResponse } from '../../types/dispatcher';
import { modelLacksReasoning } from '../../utils/answerModes';
import HeldConversationPill from './HeldConversationPill';
import { forgetKnowledgeFile, uploadKnowledgeFile } from '../../api/knowledge';
import { improveChatPrompt } from '../../api/prompt';
import ComposerMenu from './ComposerMenu';
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
  /** A document goes to the knowledge index; an image is stored whole (assets). */
  kind?: 'file' | 'image';
  path?: string;
  /** Image only: the stored asset's id and measured size. */
  assetId?: string;
  width?: number;
  height?: number;
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
  /** Attached images (ids + sizes) — the run tells the agents how to place them. */
  images?: ImageRef[];
  /** Attached knowledge-file PATHS — scopes the knowledge search to these files. */
  knowledgeFilePaths?: string[];
}

function formatBytes(bytes: number): string {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return `${val >= 10 || i === 0 ? Math.round(val) : val.toFixed(1)} ${units[i]}`;
}

// The palette shown while typing "/". `command` is what gets inserted (a
// trailing space means "then type an argument"); `arg` names that argument.
// Deliberately short: everything else the chat can do has a button or a
// plain-language path now, and a long palette buried the three that matter.
const SLASH_COMMANDS: { command: string; arg?: string; description: string }[] = [
  { command: '/skill ', arg: 'topic', description: 'Draft a skill — bare /skill captures this conversation' },
  { command: '/refine ', arg: 'instruction', description: 'Refine the current result' },
  { command: '/clear', description: 'Clear chat history' },
];

// Answer modes shown in the composer's mode pill. 'chat' runs a single light
// agent (fast); 'research'/'deep' build a crew, with progressively deeper model
// reasoning ON MODELS THAT HAVE A REASONING BUDGET.
// `label` is the full name (dropdown rows + aria); `short` is the compact label
// shown on the collapsed trigger pill so the composer's control row stays tidy.
// Hints are resolved per model at render (see utils/answerModes): on a model
// with no budget, Research is still a real crew but Deep Research would be
// identical to it, so its promise — and the mode itself — is withdrawn.

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
   * Memory mode toggle — owned by the parent so it persists across the
   * empty→conversation input swap and remounts.
   *   true            = "Workspace memory": semantic memory on (workspace-scoped)
   *   false (default) = "Session memory": semantic memory off; recall comes only
   *                     from this chat's history (the light-agent preamble).
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
  /**
   * Drop text into the composer without sending it — used by the empty-state
   * suggestion chips. `nonce` changes on every request so re-picking the SAME
   * suggestion (identical text) still re-applies; the value is set, the textarea
   * focused/auto-grown, and the caret moved to the end so the user can edit or
   * just hit Enter.
   */
  prefill?: { text: string; nonce: number };
  /** Open the MCP config dialog — forwarded to the "+" picker's admin-only
   *  "Connect a tool" action. */
  onOpenMcpConfig?: () => void;
  /** True for the empty/landing composer — enables the rotating placeholder that
   *  advertises deliverable types (dashboard, presentation, quiz, …). The
   *  conversation composer keeps the stable base placeholder. */
  isLanding?: boolean;
}

const attachmentsKey = (sessionId: string) => `kasal-chat-attachments-${sessionId}`;

// The stable placeholder shown while typing/focused and in the conversation composer.
const BASE_PLACEHOLDER = 'Ask a question...';

// Landing-only rotating hints that advertise what Kasal can build WITHOUT adding a
// format picker (the deliverable is inferred from the request text by the backend).
// Dashboard leads; each hint uses the keyword the inference matches on, so adopting
// the phrasing yields the intended deliverable.
const LANDING_HINTS = [
  'Create a dashboard of last quarter’s sales…',
  'Make a presentation on the product roadmap…',
  'Write a quiz to test the team on onboarding…',
  'Turn these notes into flashcards…',
  'Build a photo album from the launch event…',
  'Map out a mindmap of the strategy…',
];
const HINT_INTERVAL_MS = 3200;

const prefersReducedMotion = () =>
  typeof window !== 'undefined' &&
  typeof window.matchMedia === 'function' &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches;

const ChatInput: React.FC<ChatInputProps> = ({
  onSend,
  disabled = false,
  models,
  selectedModel,
  onModelChange,
  sessionId,
  isExecuting = false,
  isGenerating = false,
  memoryEnabled = false,
  onMemoryEnabledChange,
  pendingRunLabel,
  onRunPending,
  menuPlacement = 'up',
  prefill,
  onOpenMcpConfig,
  isLanding = false,
}) => {
  // Entrance animation for the pop-up menus, matching the open direction. The
  // menus are positioned with `position: fixed` (see useAnchoredFixedStyle) so
  // they escape the chat's overflow-hidden containers; this class only drives the
  // slide-in motion, not the placement.
  const menuAnimClass =
    menuPlacement === 'down' ? 'animate-slide-down' : 'animate-slide-up';
  const [value, setValue] = useState('');
  // Rotating landing placeholder: advance a hint index while idle (empty + blurred)
  // on the landing composer; freeze on focus/typing; honor reduced-motion.
  const [focused, setFocused] = useState(false);
  const [hintIndex, setHintIndex] = useState(0);
  const [reducedMotion] = useState(prefersReducedMotion);
  const [showCommands, setShowCommands] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  // Viewport-anchored fixed coords for each pop-up menu (escape overflow-hidden).
  // Answer mode (chat|research|deep) lives in the store so the choice persists
  // and is consistent across ChatInput's dual mount (read store-direct, not props).
  const chatModeType = useExecutionStore((s) => s.chatModeType);
  const setChatModeType = useExecutionStore((s) => s.setChatModeType);
  // The SOURCE axis. Only read here to grey the answer-mode pill — the control
  // itself owns its own state in SourcePill. chatModeType is deliberately left
  // alone while this is on: the user gets their selection back on switching
  // back, and it is what the "build one instead" offer runs at.
  const preferExisting = useExecutionStore((s) => s.preferExisting);
  // A capability that holds a conversation keeps the next turn even when the
  // message is a fragment. Shown, and leavable — stickiness the user cannot see
  // or refuse is indistinguishable from a bug.
  const heldConversation = useExecutionStore((s) => s.heldConversation);
  // Whether the SELECTED model can spend a reasoning budget. Drives the mode
  // hints and disables Deep Research, which on such a model is byte-for-byte
  // identical to Research (the engine drops the effort).
  const lacksReasoning = modelLacksReasoning(models, selectedModel);
  // The mode persists in the store, so a Deep selection made under a
  // reasoning-capable model would otherwise stick after switching to one
  // without a budget — silently running as Research while the pill still reads
  // "Deep". Fall back explicitly instead.
  useEffect(() => {
    if (chatModeType === 'deep' && lacksReasoning) {
      setChatModeType('research');
    }
  }, [chatModeType, lacksReasoning, setChatModeType]);
  // Memory mode is a single binary toggle: workspace (semantic memory on) vs
  // session (semantic memory off — recall comes only from this chat's history).
  const toggleMemoryMode = () => {
    onMemoryEnabledChange?.(!memoryEnabled);
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
      if (isImageFile(file)) {
        // An image is SHOWN, not searched: stored whole, referenced by id.
        const { width, height } = await measureImage(file);
        const asset = await AssetService.upload(file, { sessionId: sessionId || uploadScopeId.current, width, height });
        setAttachment(id, { status: 'ready', assetId: asset.id, width: asset.width || width, height: asset.height || height });
        return;
      }
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
        { id, name: file.name, size: file.size, status: 'uploading', kind: isImageFile(file) ? 'image' : 'file' },
      ]);
      void uploadAttachment(id, file);
    });
  };

  const removeAttachment = (id: string) => {
    // Delete the embedded chunks too. Dropping only the chip left the file
    // searchable until the TTL expired it, which is not what "remove" means to
    // the person who clicked it. Fire-and-forget: the chip goes either way, and
    // a failed cleanup is still caught by the retention sweep.
    const attachment = attachments.find((a) => a.id === id);
    if (attachment?.status === 'ready' && attachment.kind === 'image') {
      if (attachment.assetId) void AssetService.delete(attachment.assetId).catch(() => {});
    } else if (attachment?.status === 'ready') {
      void forgetKnowledgeFile(sessionId || uploadScopeId.current, attachment.name);
    }
    setAttachments((prev) => prev.filter((a) => a.id !== id));
  };

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

  const readyAll = attachments.filter((a) => a.status === 'ready');

  const readyAttachments = readyAll.filter((a) => a.kind !== 'image');

  const readyImages = readyAll.filter((a) => a.kind === 'image' && a.assetId);
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

  // Apply an empty-state suggestion chip's text: populate the composer, focus it,
  // auto-grow to fit, and drop the caret at the end so the user can edit or send.
  const lastPrefillNonce = useRef<number | undefined>(undefined);
  useEffect(() => {
    if (!prefill || prefill.nonce === lastPrefillNonce.current) return;
    lastPrefillNonce.current = prefill.nonce;
    setValue(prefill.text);
    setHistoryIndex(-1);
    const el = inputRef.current;
    if (!el) return;
    el.focus();
    // Grow + caret placement after the value paints (the textarea is controlled,
    // so scrollHeight is only correct once React has committed the new value).
    requestAnimationFrame(() => {
      el.style.height = 'auto';
      el.style.height = Math.min(el.scrollHeight, 160) + 'px';
      el.setSelectionRange(el.value.length, el.value.length);
    });
  }, [prefill]);

  // Rotate the landing placeholder through the deliverable hints while the composer
  // is idle (empty + blurred). Frozen on focus/typing and when reduced-motion is on.
  const rotating = isLanding && !value && !focused && !reducedMotion;
  useEffect(() => {
    if (!rotating) return;
    const id = setInterval(
      () => setHintIndex((i) => (i + 1) % LANDING_HINTS.length),
      HINT_INTERVAL_MS,
    );
    return () => clearInterval(id);
  }, [rotating]);

  // The visible placeholder: rotating hint while idle on the landing composer (or a
  // static dashboard-first hint under reduced-motion); the stable base otherwise.
  const placeholderText = !isLanding
    ? BASE_PLACEHOLDER
    : focused || value
      ? BASE_PLACEHOLDER
      : LANDING_HINTS[hintIndex % LANDING_HINTS.length];

  // Close the model picker on outside click

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
    let knowledgeFilePaths: string[] | undefined;
    let images: ImageRef[] | undefined;

    // Attach uploaded knowledge: include the knowledge-search tool + a steering note.
    if (!isSlash && readyAttachments.length > 0) {
      attachments = readyAttachments.map((a) => a.name);
      // Scope the knowledge search to THESE files so the run grounds on the
      // just-uploaded document instead of group-wide search picking another file.
      knowledgeFilePaths = readyAttachments
        .map((a) => a.path)
        .filter((p): p is string => Boolean(p));
      tools.push(KNOWLEDGE_TOOL);
      dispatchSuffix += `\n\n[Knowledge files attached: ${attachments.join(', ')}. Use the ${KNOWLEDGE_TOOL} to search the uploaded documents before answering.]`;
    }

    // Attached images: their ids ride along so the run can tell the agents how
    // to place them (<img src="asset:<id>">); the generator is told they exist
    // so a deck or page is planned WITH them.
    if (!isSlash && readyImages.length > 0) {
      images = readyImages.map((a) => ({
        id: a.assetId as string,
        name: a.name,
        ...(a.width ? { width: a.width } : {}),
        ...(a.height ? { height: a.height } : {}),
      }));
      dispatchSuffix += `\n\n[Images attached: ${readyImages.map((a) => a.name).join(', ')}. Use them where the request calls for a picture.]`;
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
      ...(knowledgeFilePaths && knowledgeFilePaths.length ? { knowledgeFilePaths } : {}),
      ...(images && images.length ? { images } : {}),
    };
    if (Object.keys(meta).length) {
      onSend(trimmed, meta);
    } else {
      onSend(trimmed);
    }
    // A document stays for follow-ups (the knowledge search keeps its scope);
    // an image belongs to the message it was attached to. Left in place it
    // rode along with every later question — a relic nobody re-attached.
    if (images && images.length) setAttachments((prev) => prev.filter((a) => a.kind !== 'image'));
    setValue('');
    setShowCommands(false);
    // Keep attachments after sending so follow-up prompts in the same session
    // can reuse the uploaded knowledge (remove via the chip's × when done).
    // Reset the auto-grown height after sending. The textarea is always mounted
    // when this runs, so the ref is non-null.
    inputRef.current!.style.height = 'auto';
  };

  // On-demand prompt improvement (the sparkle button) — rewrites the typed
  // request in place; nothing is sent until the user hits Send, so the rewrite
  // stays editable/discardable. Slash commands are literal and never rewritten.
  const [isImproving, setIsImproving] = useState(false);
  const canImprove = Boolean(value.trim()) && !value.trim().startsWith('/') && !disabled && !isImproving;

  const handleImprovePrompt = async () => {
    if (!canImprove) return;
    setIsImproving(true);
    try {
      const improved = await improveChatPrompt(value.trim(), selectedModel);
      // Only apply if the user hasn't kept typing while the rewrite ran.
      if (improved && inputRef.current) {
        setValue(improved);
        const el = inputRef.current;
        el.focus();
        requestAnimationFrame(() => {
          el.style.height = 'auto';
          el.style.height = Math.min(el.scrollHeight, 160) + 'px';
          el.setSelectionRange(el.value.length, el.value.length);
        });
      }
    } finally {
      setIsImproving(false);
    }
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
          {/* Two aligned columns, read left to right: the command (plus the
              argument it expects) and what it does. Neutral text — --accent
              is the theme's alert red, not a highlight. */}
          {filteredCommands.map((cmd, i) => (
            <button
              key={cmd.command}
              onClick={() => handleSelectCommand(cmd.command)}
              className="w-full text-left !px-3 !py-1.5 text-sm flex items-baseline gap-4 transition-colors"
              style={{
                backgroundColor:
                  i === selectedIndex ? 'var(--bg-active-chip)' : 'transparent',
              }}
              onMouseEnter={() => setSelectedIndex(i)}
            >
              <span className="font-mono text-[13px] shrink-0 w-44 truncate">
                <span style={{ color: 'var(--text-primary)' }}>{cmd.command.trimEnd()}</span>
                {cmd.arg && (
                  <span style={{ color: 'var(--text-muted)' }}> ‹{cmd.arg}›</span>
                )}
              </span>
              <span className="text-xs truncate" style={{ color: 'var(--text-secondary)' }}>
                {cmd.description}
              </span>
            </button>
          ))}
          <div
            className="px-3 py-1.5 text-[11px] border-t"
            style={{ color: 'var(--text-muted)', borderColor: 'var(--border-color)' }}
          >
            ↑↓ to choose · Tab or Enter to insert · Esc to close
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
        {heldConversation && (
          <div className="px-5 pt-3">
            <HeldConversationPill
              capability={heldConversation}
              onLeave={() => {
                const store = useExecutionStore.getState();
                store.setHeldConversation(null);
                // The next turn goes to the router on its own words.
                store.setSkipContinuation(true);
                inputRef.current?.focus();
              }}
            />
          </div>
        )}

        <div className="flex items-start px-5 pt-4 pb-1">
          <textarea
            ref={inputRef}
            value={value}
            // A pasted screenshot is the main way an image arrives.
            onPaste={(e) => {
              const files = Array.from(e.clipboardData?.files || []).filter(isImageFile);
              if (files.length > 0) {
                e.preventDefault();
                addFiles(files);
              }
            }}
            onChange={(e) => {
              setValue(e.target.value);
              setHistoryIndex(-1);
            }}
            onKeyDown={handleKeyDown}
            onFocus={() => setFocused(true)}
            onBlur={() => setFocused(false)}
            placeholder={placeholderText}
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
                {a.kind === 'image' && a.status === 'ready' && a.assetId ? (
                  <AssetThumb id={a.assetId} name={a.name} size={22} />
                ) : a.status === 'uploading' ? (
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
            {/* Improve prompt — rewrites the typed request with prompt-
                engineering best practices before sending. */}
            <button
              onClick={handleImprovePrompt}
              disabled={!canImprove}
              className="flex-shrink-0 w-8 h-8 rounded-xl flex items-center justify-center transition-colors hover:opacity-80 disabled:opacity-40 disabled:cursor-not-allowed"
              style={{ color: 'var(--text-secondary)', backgroundColor: 'var(--bg-secondary)', border: '1px solid var(--border-color)' }}
              title="Improve this prompt with AI"
              aria-label="Improve prompt"
            >
              {isImproving ? (
                <svg className="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
              ) : (
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.456 2.456L21.75 6l-1.035.259a3.375 3.375 0 00-2.456 2.456z" />
                </svg>
              )}
            </button>

            {/* The "+" menu — source, answer mode, memory, model, attach and
                the MCP tools all live here now; the bar stays sparkle + send. */}
            <ComposerMenu
              disabled={disabled}
              menuPlacement={menuPlacement}
              menuAnimClass={menuAnimClass}
              onPicked={() => inputRef.current?.focus()}
              chatModeType={chatModeType}
              setChatModeType={setChatModeType}
              models={models}
              selectedModel={selectedModel}
              onModelChange={onModelChange}
              memoryEnabled={memoryEnabled}
              onToggleMemory={toggleMemoryMode}
              attachmentCount={attachments.length}
              onAttachFiles={() => fileInputRef.current?.click()}
              onOpenMcpConfig={onOpenMcpConfig}
            />

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
