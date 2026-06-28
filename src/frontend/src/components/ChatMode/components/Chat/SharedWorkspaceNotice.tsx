import React, { useEffect, useState } from 'react';
import Box from '@mui/material/Box';
import { useGroupStore } from '../../../../store/groups';
import { buttonResetSx, slideUpSx } from '../../chatSx';

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
    <Box
      role="status"
      data-testid="shared-workspace-notice"
      sx={{
        mb: 1,
        borderRadius: '12px',
        px: 1.5,
        py: 1,
        display: 'flex',
        alignItems: 'flex-start',
        gap: 1,
        fontSize: 11,
        lineHeight: 1.375,
        color: 'primary.main',
        backgroundColor: (t) => `color-mix(in srgb, ${t.palette.primary.main} 10%, transparent)`,
        border: (t) => `1px solid color-mix(in srgb, ${t.palette.primary.main} 35%, transparent)`,
        ...slideUpSx,
      }}
    >
      <Box component="svg" sx={{ width: 16, height: 16, mt: 0.25, flexShrink: 0 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198l.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0z"
        />
      </Box>
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Box component="span" sx={{ fontWeight: 600 }}>Shared workspace. </Box>
        Your team can build on each other's work here. Chats, runs, results, logs, and agent memory in this
        workspace are visible to {audience}, including data your agents read from Databricks that teammates may
        not otherwise have access to. Avoid queries whose results shouldn't be shared with the team.
      </Box>
      <Box
        component="button"
        type="button"
        onClick={onDismiss}
        aria-label="Dismiss shared workspace notice"
        sx={{
          ...buttonResetSx,
          flexShrink: 0,
          mr: -0.5,
          mt: -0.25,
          p: 0.25,
          borderRadius: '4px',
          transition: 'opacity 0.15s',
          color: 'primary.main',
          '&:hover': { opacity: 0.7 },
        }}
      >
        <Box component="svg" sx={{ width: 14, height: 14 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
        </Box>
      </Box>
    </Box>
  );
};

export default SharedWorkspaceNotice;
