import React from 'react';
import { useExecutionStore } from '../../store/executionStore';
import { useUILayoutStore } from '../../../../store/uiLayout';
import { useFlowConfigStore } from '../../../../store/flowConfig';

/**
 * First-run launchpad shown BELOW the composer when a chat has no messages (the
 * greeting sits above the input; these are the secondary "if you're not sure"
 * affordances — the placement every major LLM chat product uses so the input
 * stays the hero and starter chips are a fallback the eye finds next):
 *
 *  1. Three chips mirror the composer's answer modes (Chat / Research / Deep
 *     Research). Clicking one SELECTS that mode (same store the composer's mode
 *     pill uses) and drops an editable starter prompt into the composer — so the
 *     user discovers the modes exist and the blank page isn't a dead end. They
 *     don't auto-send.
 *  2. A quiet footer surfaces the Agent Builder / Flow Builder (otherwise hidden
 *     behind an unlabeled grid icon) and a link to the docs.
 *
 * Connecting a tool (MCP) lives in the composer's "+" picker, not here — see
 * McpPicker — so the launchpad stays light.
 *
 * Styling note: buttons inside `#kasal-chat-root` are reset to `padding: 0` by an
 * ID-specificity rule in chat.css that beats Tailwind `px-*`/`py-*` utilities, so
 * interactive elements here set their padding INLINE (inline styles win over the
 * ID selector).
 */
export interface ChatEmptyStateProps {
  /** Drop a starter prompt into the composer (does not send). */
  onPrefill: (text: string) => void;
}

// Answer-mode ids as used by the composer's mode pill (executionStore.chatModeType).
type ModeId = 'chat' | 'research' | 'deep';

interface ModeChip {
  id: ModeId;
  label: string;
  /** Short subtitle — kept concise so it fits a 3-across row without truncating. */
  hint: string;
  /** Editable starter prompt dropped into the composer when the chip is clicked. */
  prompt: string;
  icon: React.ReactNode;
}

const iconClass = 'w-[18px] h-[18px]';
const MODE_CHIPS: ModeChip[] = [
  {
    id: 'chat',
    label: 'Chat',
    hint: 'Fast single-agent answer',
    prompt: 'Give me a quick summary of [topic].',
    icon: (
      <svg className={iconClass} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M8 12h8m-8-4h8m-4 8H8m-4.5 1.5V6.75A2.25 2.25 0 015.75 4.5h12.5a2.25 2.25 0 012.25 2.25v6a2.25 2.25 0 01-2.25 2.25H9l-4.5 3.75z" />
      </svg>
    ),
  },
  {
    id: 'research',
    label: 'Research',
    hint: 'Crew with reasoning',
    prompt: 'Research [topic] and write a concise brief with sources.',
    icon: (
      <svg className={iconClass} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-4.35-4.35m1.35-5.15a7 7 0 11-14 0 7 7 0 0114 0z" />
      </svg>
    ),
  },
  {
    id: 'deep',
    label: 'Deep Research',
    hint: 'Planning + deep tools',
    prompt:
      'Do a deep-dive analysis of [topic], comparing multiple sources and reasoning through the trade-offs.',
    icon: (
      <svg className={iconClass} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.456 2.456L21.75 6l-1.035.259a3.375 3.375 0 00-2.456 2.456z" />
      </svg>
    ),
  },
];

const ChatEmptyState: React.FC<ChatEmptyStateProps> = ({ onPrefill }) => {
  const chatModeType = useExecutionStore((s) => s.chatModeType);
  const setChatModeType = useExecutionStore((s) => s.setChatModeType);
  const setAppMode = useUILayoutStore((s) => s.setAppMode);
  const crewAIFlowEnabled = useFlowConfigStore((s) => s.crewAIFlowEnabled);

  // Picking a chip selects that answer mode (so the composer's mode pill matches)
  // and seeds an editable starter prompt.
  const pickMode = (chip: ModeChip) => {
    setChatModeType(chip.id);
    onPrefill(chip.prompt);
  };

  // Neutral, theme-agnostic styling: the launchpad stays white/greyscale so it
  // never clashes with the app's blue chrome (Agent Builder, MCP config). No
  // accent colour here — icon tiles + emphasis use ink tones only.
  const tileStyle: React.CSSProperties = {
    backgroundColor: 'var(--bg-active-chip)',
    color: 'var(--text-secondary)',
  };

  return (
    <div className="w-full mt-4" data-testid="chat-empty-state">
      {/* Answer-mode chips — one row. Selects the mode + seeds a starter prompt.
          Padding is inline: the #kasal-chat-root reset zeroes Tailwind px/py on
          <button>. The active mode is highlighted so this doubles as showing the
          composer's current selection. */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5 mb-3">
        {MODE_CHIPS.map((chip) => {
          const active = chip.id === chatModeType;
          return (
            <button
              key={chip.id}
              type="button"
              onClick={() => pickMode(chip)}
              aria-pressed={active}
              className="kasal-suggest group flex items-center gap-3 text-left rounded-xl transition-colors"
              style={{
                padding: '12px 14px',
                backgroundColor: active ? 'var(--bg-active-chip)' : 'var(--bg-secondary)',
                border: `1px solid ${active ? 'var(--text-muted)' : 'var(--border-color)'}`,
              }}
            >
              <span
                className="flex items-center justify-center w-9 h-9 rounded-lg flex-shrink-0"
                style={tileStyle}
              >
                {chip.icon}
              </span>
              <span className="flex-1 min-w-0">
                <span className="block text-sm font-medium truncate" style={{ color: 'var(--text-primary)' }}>
                  {chip.label}
                </span>
                <span className="block text-xs truncate" style={{ color: 'var(--text-muted)' }}>
                  {chip.hint}
                </span>
              </span>
            </button>
          );
        })}
      </div>

      {/* Builder bridge — Agent/Flow Builder are otherwise hidden behind the
          top-bar grid icon; surface them with a hint about WHEN to reach for each
          (crews vs sequenced multi-crew orchestration). Two tidy lines: the
          builder guidance, then a separate docs line, so neither reads run-on. */}
      <div className="text-center text-xs leading-relaxed space-y-1" style={{ color: 'var(--text-muted)' }}>
        <div>
          Want to design it yourself? Build a crew in the{' '}
          <button
            type="button"
            onClick={() => setAppMode('crew')}
            title="Visually design and run a crew of agents"
            className="font-medium underline underline-offset-2 hover:opacity-80"
            style={{ color: 'var(--text-secondary)' }}
          >
            Agent Builder
          </button>
          {crewAIFlowEnabled && (
            <>
              {' '}or sequence crews into a workflow in the{' '}
              <button
                type="button"
                onClick={() => setAppMode('flow')}
                title="Chain multiple crews into a step-by-step workflow"
                className="font-medium underline underline-offset-2 hover:opacity-80"
                style={{ color: 'var(--text-secondary)' }}
              >
                Flow Builder
              </button>
            </>
          )}
          .
        </div>
        <div>
          New to Kasal?{' '}
          <a
            href="/docs"
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => {
              // Kasal runs inside the Databricks Apps sandboxed iframe, where a bare
              // <a target="_blank"> degrades to same-frame navigation (react-router
              // then resolves /docs in place). Open an ABSOLUTE URL imperatively so
              // it escapes the iframe into a real new tab. href/target are kept for
              // middle-click and keyboard activation.
              e.preventDefault();
              window.open(`${window.location.origin}/docs`, '_blank', 'noopener,noreferrer');
            }}
            className="font-medium underline underline-offset-2 hover:opacity-80"
            style={{ color: 'var(--text-secondary)' }}
          >
            Check the docs
          </a>
        </div>
      </div>
    </div>
  );
};

export default ChatEmptyState;
