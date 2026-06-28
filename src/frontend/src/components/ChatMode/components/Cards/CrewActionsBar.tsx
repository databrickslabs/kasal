import React, { useState } from 'react';
import Box from '@mui/material/Box';
import { GenerationCompleteData } from '../../types/dispatcher';
import { postCrewFeedback, CrewNameConflictError } from '../../api/crews';
import { useSessionStore } from '../../store/sessionStore';
import { useExecutionStore } from '../../store/executionStore';
import { MemoryRecordsBrowser } from '../../../MemoryBackend/MemoryRecordsBrowser';
import { buttonResetSx, inputResetSx } from '../../chatSx';

/**
 * Slim post-generation actions row (no crew card): bookmark the crew into the
 * catalog, and rate the result. Thumbs-down requires a short comment — both
 * are stored against the cataloged crew and surfaced in the Agent Builder
 * catalog for the AI engineer.
 */

interface PersistedActions {
  savedCrewId?: string;
  savedName?: string;
  voted?: 'up' | 'down';
}

interface CrewActionsBarProps {
  data: GenerationCompleteData;
  messageId: string;
  onSaveCrew?: (
    data: GenerationCompleteData,
    opts?: { overwrite?: boolean },
  ) => Promise<{ id: string; name: string }>;
  /** Execution (job) id of the run this message anchors — scopes the memory graph. */
  executionId?: string;
}

// Shared pill-button styling for the actions row (the chat MUI replacement for
// the old `ICON_BTN` Tailwind class). Per-button colour is added at each site.
const iconBtnSx = {
  ...buttonResetSx,
  display: 'flex',
  alignItems: 'center',
  gap: 0.75,
  px: 1.25,
  py: 0.75,
  borderRadius: '8px',
  fontSize: 12,
  fontWeight: 500,
  transition: 'all 0.15s',
  backgroundColor: (t: { chat: { bgSecondary: string } }) => t.chat.bgSecondary,
  border: 1,
  borderColor: 'divider',
  '&:hover': { opacity: 0.8 },
  '&:disabled': { opacity: 0.5, cursor: 'not-allowed' },
};

const CrewActionsBar: React.FC<CrewActionsBarProps> = ({ data, messageId, onSaveCrew, executionId }) => {
  const persisted = data as GenerationCompleteData & PersistedActions;
  const [savedId, setSavedId] = useState<string | undefined>(persisted.savedCrewId);
  const [savedName, setSavedName] = useState<string | undefined>(persisted.savedName);
  const [voted, setVoted] = useState<'up' | 'down' | undefined>(persisted.voted);
  const [busy, setBusy] = useState(false);
  const [showDownForm, setShowDownForm] = useState(false);
  const [downComment, setDownComment] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [graphOpen, setGraphOpen] = useState(false);

  // The memory graph is only meaningful when this run actually wrote memory.
  const memoryEnabled = useExecutionStore((s) => s.memoryEnabled);
  const canShowGraph = memoryEnabled && Boolean(executionId);

  // Plain 'chat' turns run a single default assistant — there is no generated
  // crew worth saving or rating, so the catalog actions (Save to catalog +
  // feedback, which also catalogs the crew) are hidden. The run's answer mode is
  // carried on the message data so this stays correct even if the user later
  // switches modes. Memory graph still applies when this run wrote memory.
  const isChat = (data as GenerationCompleteData & { chatModeType?: string }).chatModeType === 'chat';
  if (isChat && !canShowGraph) return null;

  const persist = (patch: PersistedActions) => {
    try {
      useSessionStore.getState().updateMessage(messageId, {
        resultType: 'crew_actions',
        resultData: {
          ...data,
          savedCrewId: patch.savedCrewId ?? savedId,
          savedName: patch.savedName ?? savedName,
          voted: patch.voted ?? voted,
        },
      });
    } catch {
      /* best-effort */
    }
  };

  /** Save to the catalog if not already saved; returns the crew id. */
  const ensureSaved = async (overwrite = false): Promise<string> => {
    if (savedId) return savedId;
    if (!onSaveCrew) throw new Error('Saving is unavailable');
    const saved = await onSaveCrew(data, overwrite ? { overwrite: true } : undefined);
    setSavedId(saved.id);
    setSavedName(saved.name);
    persist({ savedCrewId: saved.id, savedName: saved.name });
    return saved.id;
  };

  const handleBookmark = async () => {
    setBusy(true);
    setError(null);
    try {
      await ensureSaved();
    } catch (e) {
      if (e instanceof CrewNameConflictError) {
        try {
          await ensureSaved(true);
        } catch {
          setError('Could not save the crew');
        }
      } else {
        setError('Could not save the crew');
      }
    } finally {
      setBusy(false);
    }
  };

  const handleVote = async (rating: 'up' | 'down', comment?: string) => {
    setBusy(true);
    setError(null);
    try {
      const crewId = await ensureSaved().catch(async (e) => {
        if (e instanceof CrewNameConflictError) return ensureSaved(true);
        throw e;
      });
      await postCrewFeedback(crewId, rating, comment);
      setVoted(rating);
      setShowDownForm(false);
      persist({ voted: rating });
    } catch {
      setError('Could not record the feedback');
    } finally {
      setBusy(false);
    }
  };

  return (
    <Box sx={{ px: 2, my: 0.5, maxWidth: '48rem' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
        {!isChat && (
        <>
        {/* Bookmark — save the generated crew to the catalog */}
        <Box
          component="button"
          type="button"
          onClick={handleBookmark}
          disabled={busy || Boolean(savedId)}
          title={savedId ? `Saved as “${savedName}”` : 'Save this crew to the catalog'}
          sx={{ ...iconBtnSx, color: savedId ? 'primary.main' : 'text.secondary' }}
        >
          <Box component="svg" sx={{ width: 14, height: 14 }} fill={savedId ? 'currentColor' : 'none'} viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M17.593 3.322c1.1.128 1.907 1.077 1.907 2.185V21L12 17.25 4.5 21V5.507c0-1.108.806-2.057 1.907-2.185a48.507 48.507 0 0111.186 0z" />
          </Box>
          {savedId ? `Saved${savedName ? ` — ${savedName}` : ''}` : 'Save to catalog'}
        </Box>

        {/* Thumbs up */}
        <Box
          component="button"
          type="button"
          onClick={() => handleVote('up')}
          disabled={busy || Boolean(voted)}
          title="Good result"
          aria-label="Thumbs up"
          sx={{ ...iconBtnSx, color: voted === 'up' ? 'primary.main' : 'text.secondary' }}
        >
          <Box component="svg" sx={{ width: 14, height: 14 }} fill={voted === 'up' ? 'currentColor' : 'none'} viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M6.633 10.5c.806 0 1.533-.446 2.031-1.08a9.041 9.041 0 012.861-2.4c.723-.384 1.35-.956 1.653-1.715a4.498 4.498 0 00.322-1.672V3a.75.75 0 01.75-.75A2.25 2.25 0 0116.5 4.5c0 1.152-.26 2.243-.723 3.218-.266.558.107 1.282.725 1.282h3.126c1.026 0 1.945.694 2.054 1.715.045.422.068.85.068 1.285a11.95 11.95 0 01-2.649 7.521c-.388.482-.987.729-1.605.729H13.48c-.483 0-.964-.078-1.423-.23l-3.114-1.04a4.501 4.501 0 00-1.423-.23H5.904" />
          </Box>
        </Box>

        {/* Thumbs down — opens the comment form */}
        <Box
          component="button"
          type="button"
          onClick={() => setShowDownForm((v) => !v)}
          disabled={busy || Boolean(voted)}
          title="Something was wrong"
          aria-label="Thumbs down"
          sx={{ ...iconBtnSx, color: voted === 'down' ? '#ef4444' : 'text.secondary' }}
        >
          <Box component="svg" sx={{ width: 14, height: 14 }} fill={voted === 'down' ? 'currentColor' : 'none'} viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M7.498 15.25H4.372c-1.026 0-1.945-.694-2.054-1.715a12.137 12.137 0 01-.068-1.285c0-2.848.992-5.464 2.649-7.521C5.287 4.247 5.886 4 6.504 4h4.016a4.5 4.5 0 011.423.23l3.114 1.04a4.5 4.5 0 001.423.23h1.294M7.498 15.25c.618 0 .991.724.725 1.282A7.471 7.471 0 007.5 19.75 2.25 2.25 0 009.75 22a.75.75 0 00.75-.75v-.633c0-.573.11-1.14.322-1.672.304-.76.93-1.33 1.653-1.715a9.04 9.04 0 002.86-2.4c.498-.634 1.226-1.08 2.032-1.08h.384" />
          </Box>
        </Box>
        </>
        )}

        {/* Memory graph — concept graph of what this run wrote to memory */}
        {canShowGraph && (
          <Box
            component="button"
            type="button"
            onClick={() => setGraphOpen(true)}
            title="View this run's memory graph"
            aria-label="View memory graph"
            sx={{ ...iconBtnSx, color: 'text.secondary' }}
          >
            <Box component="svg" sx={{ width: 14, height: 14 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <circle cx="6" cy="6" r="2.5" />
              <circle cx="18" cy="7" r="2.5" />
              <circle cx="12" cy="17" r="2.5" />
              <path strokeLinecap="round" d="M7.8 7.4l2.6 7.4M16.6 8.7l-3 6.4M8.3 6.4l7.2.4" />
            </Box>
            Memory graph
          </Box>
        )}

        {voted === 'down' && (
          <Box component="span" sx={{ fontSize: 11, color: 'text.disabled' }}>
            Feedback recorded — thank you
          </Box>
        )}
        {error && (
          <Box component="span" sx={{ fontSize: 11, color: '#ef4444' }}>{error}</Box>
        )}
      </Box>

      {/* Thumbs-down requires telling the AI engineer what went wrong */}
      {showDownForm && !voted && (
        <Box sx={{ mt: 1, display: 'flex', flexDirection: 'column', gap: 1 }}>
          <Box
            component="textarea"
            value={downComment}
            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setDownComment(e.target.value)}
            placeholder="What went wrong? This is stored with the crew for the AI engineer."
            rows={2}
            sx={{
              ...inputResetSx,
              width: '100%',
              borderRadius: '8px',
              px: 1.5,
              py: 1,
              fontSize: 14,
              resize: 'none',
              backgroundColor: 'background.paper',
              color: 'text.primary',
              border: 1,
              borderColor: 'divider',
            }}
          />
          <Box
            component="button"
            type="button"
            onClick={() => handleVote('down', downComment.trim())}
            disabled={busy || !downComment.trim()}
            sx={{ ...iconBtnSx, color: 'text.secondary' }}
          >
            Submit feedback
          </Box>
        </Box>
      )}

      {canShowGraph && graphOpen && (
        <MemoryRecordsBrowser
          open={graphOpen}
          onClose={() => setGraphOpen(false)}
          initialRunId={executionId}
          initialView="graph"
        />
      )}
    </Box>
  );
};

export default CrewActionsBar;
