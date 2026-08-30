import React from 'react';
import { useExecutionStore } from '../../store/executionStore';
import { usePermissionStore } from '../../../../store/permissions';
import { useAppStore } from '../../store/appStore';
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

type ModeId = 'chat' | 'research' | 'deep';

const ChatEmptyState: React.FC<ChatEmptyStateProps> = ({ onPrefill }) => {
  const setAppMode = useUILayoutStore((s) => s.setAppMode);
  const kasalFlowEnabled = useFlowConfigStore((s) => s.kasalFlowEnabled);
  // Read the model straight from the store, as this component already does for
  // every other piece of state, so the chips and the composer's mode pill agree
  // about what the selected model can actually do.

  // Chat-only users (operators) get no builder bridge — the canvases they
  // cannot enter must not be advertised.
  const allowAgentBuilder = usePermissionStore((s) => s.allowAgentBuilder);
  const allowFlowBuilder = usePermissionStore((s) => s.allowFlowBuilder);
  const canUseBuilders = allowAgentBuilder || allowFlowBuilder;

  return (
    <div className="w-full mt-4" data-testid="chat-empty-state">
      {/* Builder bridge — Agent/Flow Builder are otherwise hidden behind the
          top-bar grid icon; surface them with a hint about WHEN to reach for each
          (crews vs sequenced multi-crew orchestration). Two tidy lines: the
          builder guidance, then a separate docs line, so neither reads run-on. */}
      <div className="text-center text-xs leading-relaxed space-y-1" style={{ color: 'var(--text-muted)' }}>
        {canUseBuilders && (
        <div>
          Want to design it yourself?{' '}
          {allowAgentBuilder && (<>Build a crew in the{' '}
          <button
            type="button"
            onClick={() => setAppMode('crew')}
            title="Visually design and run a crew of agents"
            className="font-medium underline underline-offset-2 hover:opacity-80"
            style={{ color: 'var(--text-secondary)' }}
          >
            Agent Builder
          </button>
          </>)}
          {kasalFlowEnabled && allowFlowBuilder && (
            <>
              {allowAgentBuilder ? ' or sequence' : 'Sequence'} crews into a workflow in the{' '}
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
        )}
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
