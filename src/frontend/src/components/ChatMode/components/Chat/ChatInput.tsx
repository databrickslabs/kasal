import React, { useState, useRef, useEffect } from 'react';
import { ModelConfigResponse } from '../../types/dispatcher';

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
type FormatKey = 'auto' | 'presentation' | 'quiz' | 'dashboard' | 'report' | 'genie';
const FORMAT_OPTIONS: { key: FormatKey; label: string; directive: string }[] = [
  { key: 'auto', label: 'Auto format', directive: '' },
  { key: 'presentation', label: 'Presentation', directive: 'Produce the final result as a slide presentation (a deck of slides).' },
  { key: 'quiz', label: 'Interactive quiz', directive: 'Produce the final result as an interactive multiple-choice quiz that tracks the score.' },
  { key: 'dashboard', label: 'Dashboard', directive: 'Produce the final result as a metrics dashboard with KPI tiles and charts.' },
  { key: 'report', label: 'Report', directive: 'Produce the final result as a structured, readable report.' },
  { key: 'genie', label: 'Data answer (Genie)', directive: 'Produce the final result as a data answer: a short answer, the result Table, and a chart when useful.' },
];

interface ChatInputProps {
  onSend: (message: string) => void;
  disabled?: boolean;
  models: ModelConfigResponse[];
  selectedModel: string;
  onModelChange: (model: string) => void;
}

const ChatInput: React.FC<ChatInputProps> = ({
  onSend,
  disabled = false,
  models,
  selectedModel,
  onModelChange,
}) => {
  const [value, setValue] = useState('');
  const [showCommands, setShowCommands] = useState(false);
  const [showModelPicker, setShowModelPicker] = useState(false);
  const [showFormatPicker, setShowFormatPicker] = useState(false);
  const [format, setFormat] = useState<FormatKey>('auto');
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [commandHistory, setCommandHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const modelPickerRef = useRef<HTMLDivElement>(null);
  const formatPickerRef = useRef<HTMLDivElement>(null);

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
    if (!trimmed || disabled) return;

    setCommandHistory((prev) => [...prev.slice(-50), trimmed]);
    setHistoryIndex(-1);
    // Slash commands are literal; only natural-language prompts get a format hint.
    const directive = trimmed.startsWith('/')
      ? ''
      : (FORMAT_OPTIONS.find((f) => f.key === format)?.directive || '');
    onSend(directive ? `${trimmed}\n\n[Output format: ${directive}]` : trimmed);
    setValue('');
    setShowCommands(false);
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

      {/* Input container — two-row layout */}
      <div
        className="kasal-input-shell rounded-3xl overflow-hidden"
        style={{
          backgroundColor: 'var(--bg-input)',
          border: '1px solid var(--border-color)',
          boxShadow: 'var(--shadow-input)',
        }}
      >
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

        {/* Bottom row — format + model selector + send button */}
        <div className="flex items-center justify-end px-4 py-2.5">
          {/* Right side — format selector + model selector + send */}
          <div className="flex items-center gap-2">
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

            {/* Send button */}
            <button
              onClick={handleSend}
              disabled={disabled || !value.trim()}
              className="flex-shrink-0 w-8 h-8 rounded-xl flex items-center justify-center transition-all disabled:cursor-not-allowed"
              style={{
                backgroundColor:
                  disabled
                    ? 'transparent'
                    : value.trim()
                      ? 'var(--accent)'
                      : 'transparent',
                color:
                  disabled
                    ? 'var(--text-muted)'
                    : value.trim()
                      ? '#ffffff'
                      : 'var(--text-muted)',
              }}
            >
              {disabled ? (
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
