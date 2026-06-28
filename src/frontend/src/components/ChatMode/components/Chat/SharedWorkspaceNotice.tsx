import React, { useEffect, useState } from 'react';
import { useGroupStore } from '../../../../store/groups';

/**
 * Shared-workspace data-exposure notice for ChatMode.
 *
 * A shared (team) workspace is powerful: members build on each other's chats,
 * runs and agent memory. The caveat is that this same sharing exposes whatever
 * an agent read from Databricks under one member's permissions to every other
 * member, even if they lack access to that data in Unity Catalog. We don't try
 * to isolate per-user (provenance is lost once data is materialised into memory
 * and logs); instead we make the trade-off explicit via informed consent.
 *
 * Personal workspaces (group id starting with "user_") are single-member, so the
 * notice never shows there. Shown ONCE per shared workspace: dismissal is
 * remembered in localStorage keyed by group id, so it informs without nagging.
 */
const DISMISS_PREFIX = 'kasal_shared_ws_notice_dismissed:';
const ACCENT = 'var(--accent)';

const SharedWorkspaceNotice: React.FC = () => {
  const currentGroupId = useGroupStore((s) => s.currentGroupId);
  const groups = useGroupStore((s) => s.groups);

  const isShared = !!currentGroupId && !currentGroupId.startsWith('user_');
  const [dismissed, setDismissed] = useState(true);

  // Re-evaluate dismissal whenever the active workspace changes.
  useEffect(() => {
    if (!isShared || !currentGroupId) {
      setDismissed(true);
      return;
    }
    try {
      setDismissed(localStorage.getItem(DISMISS_PREFIX + currentGroupId) === '1');
    } catch {
      setDismissed(false);
    }
  }, [currentGroupId, isShared]);

  if (!isShared || dismissed || !currentGroupId) return null;

  const group = groups.find((g) => g.id === currentGroupId);
  const memberCount = group?.user_count;
  const audience = memberCount && memberCount > 0 ? `all ${memberCount} members` : 'all members';

  const onDismiss = () => {
    try {
      localStorage.setItem(DISMISS_PREFIX + currentGroupId, '1');
    } catch {
      /* localStorage unavailable; just hide for this view */
    }
    setDismissed(true);
  };

  return (
    <div
      role="status"
      data-testid="shared-workspace-notice"
      className="mb-2 rounded-xl px-3 py-2 flex items-start gap-2 text-[11px] leading-snug animate-slide-up"
      style={{
        color: ACCENT,
        backgroundColor: `color-mix(in srgb, ${ACCENT} 10%, transparent)`,
        border: `1px solid color-mix(in srgb, ${ACCENT} 35%, transparent)`,
      }}
    >
      <svg className="w-4 h-4 mt-0.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z"
        />
      </svg>
      <div className="flex-1 min-w-0">
        <span style={{ fontWeight: 600 }}>Shared workspace. </span>
        Your team can build on each other's work here. Chats, runs, results, logs, and agent memory in this
        workspace are visible to {audience}, including data your agents read from Databricks that teammates may
        not otherwise have access to. Avoid queries whose results shouldn't be shared with the team.
      </div>
      <button
        type="button"
        onClick={onDismiss}
        aria-label="Dismiss shared workspace notice"
        className="flex-shrink-0 -mr-1 -mt-0.5 p-0.5 rounded hover:opacity-70 transition-opacity"
        style={{ color: ACCENT }}
      >
        <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
};

export default SharedWorkspaceNotice;
