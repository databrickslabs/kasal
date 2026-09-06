import React from 'react';

/**
 * "Use existing" found nothing to run, and offers the build instead.
 *
 * The reason this card exists at all: with the source control set to reuse the
 * user has explicitly said *run what we have*. Silently generating a crew would
 * run work they did not ask for and bill a full crew run for it — so nothing
 * runs, and the next move is one click that is unambiguously theirs.
 *
 * One line and one button, deliberately. This is a dead end being turned into a
 * signpost, not a screen.
 */
interface NoMatchPromptProps {
  message: string;
  /** 'nothing_published' | 'no_match' | 'unresolved'. */
  reason: string;
  /** Flips the source back to "Build new" and re-sends at the stored answer mode. */
  onBuildInstead?: () => void;
}

const NoMatchPrompt: React.FC<NoMatchPromptProps> = ({
  message,
  reason,
  onBuildInstead,
}) => (
  <div
    className="mt-3 rounded-lg px-3 py-2.5 text-sm"
    style={{
      backgroundColor: 'var(--bg-primary)',
      border: '1px solid var(--border-color)',
    }}
  >
    <div style={{ color: 'var(--text-secondary)' }}>{message}</div>
    {onBuildInstead && (
      <button
        type="button"
        onClick={onBuildInstead}
        className="mt-2 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors hover:opacity-80"
        style={{ color: 'var(--accent)', backgroundColor: 'transparent', border: 'none' }}
      >
        {/* An empty workspace needs to be sent to the publish dialog; a genuine
            miss just needs the build offer. Same button, honest label. */}
        {reason === 'nothing_published'
          ? 'Build a new crew for this instead'
          : 'Build a new crew instead'}
      </button>
    )}
  </div>
);

export default NoMatchPrompt;
