/**
 * "Continuing <capability>" — and the way out of it.
 *
 * A capability that holds a conversation keeps the next turn even when the
 * message is a fragment the router would not have matched on its own words.
 * That is the point of it, and it is also why it needs to be visible: a user
 * whose follow-ups keep going somewhere they did not choose, with no sign of
 * where or why, has no way to tell the feature from a bug.
 *
 * So the rule this component exists for: stickiness the user cannot see, and
 * cannot refuse, is a trap. The × sends the next turn to the router with
 * continuation switched off, and the router decides it on the message alone.
 */

import React from 'react';

interface HeldConversationPillProps {
  /** The capability currently holding the conversation, or null for none. */
  capability: string | null;
  /** Leave it: the next turn is routed on its own words. */
  onLeave: () => void;
}

const HeldConversationPill: React.FC<HeldConversationPillProps> = ({
  capability,
  onLeave,
}) => {
  if (!capability) return null;

  return (
    <div
      className="kasal-held-pill"
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
        maxWidth: '100%',
        padding: '2px 6px 2px 10px',
        marginBottom: 6,
        borderRadius: 999,
        border: '1px solid var(--kasal-border, rgba(127,127,127,0.28))',
        background: 'var(--kasal-surface-2, rgba(127,127,127,0.08))',
        fontSize: 12,
        lineHeight: 1.6,
        color: 'var(--kasal-text-muted, #6b7280)',
      }}
    >
      <span
        aria-hidden
        style={{
          width: 6,
          height: 6,
          borderRadius: '50%',
          background: 'var(--kasal-accent, #10b981)',
          flex: '0 0 auto',
        }}
      />
      <span
        style={{
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
        title={`Follow-up messages continue ${capability}`}
      >
        Continuing <strong style={{ fontWeight: 600 }}>{capability}</strong>
      </span>
      <button
        type="button"
        onClick={onLeave}
        aria-label={`Stop continuing ${capability}`}
        title="Route the next message on its own"
        style={{
          border: 'none',
          background: 'transparent',
          cursor: 'pointer',
          color: 'inherit',
          fontSize: 14,
          lineHeight: 1,
          padding: '2px 4px',
          borderRadius: 999,
        }}
      >
        ×
      </button>
    </div>
  );
};

export default HeldConversationPill;
