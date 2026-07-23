import React, { useState } from 'react';
import { GenerationCompleteData } from '../../types/dispatcher';
import { postCrewFeedback, CrewNameConflictError } from '../../api/crews';
import { useSessionStore } from '../../store/sessionStore';
import { MemoryRecordsBrowser } from '../../../MemoryBackend/MemoryRecordsBrowser';
import OpenOnCanvasButtons from './OpenOnCanvasButtons';

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
  /**
   * Answer ("chat") mode: distill a reusable crew from the conversation and
   * save it to the catalog in one shot (showing what was saved). Used INSTEAD
   * of onSaveCrew for chat turns, where the run is a generic single assistant
   * (nothing specific worth cataloging directly).
   */
  onSaveAnswerToCatalog?: (sessionId?: string) => void | Promise<void>;
  /** Execution (job) id of the run this message anchors — scopes the memory graph. */
  executionId?: string;
  /**
   * Whether THIS run used workspace memory (captured at run time). The memory
   * graph only exists for runs that wrote workspace memory, so a session-only
   * run must NOT show it even after the user later toggles workspace memory on.
   */
  usedWorkspaceMemory?: boolean;
}

const ICON_BTN =
  'flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed';

const CrewActionsBar: React.FC<CrewActionsBarProps> = ({ data, messageId, onSaveCrew, onSaveAnswerToCatalog, executionId, usedWorkspaceMemory }) => {
  const persisted = data as GenerationCompleteData & PersistedActions & {
    chatModeType?: string;
    sessionId?: string;
  };
  const [savedId, setSavedId] = useState<string | undefined>(persisted.savedCrewId);
  const [savedName, setSavedName] = useState<string | undefined>(persisted.savedName);
  const [voted, setVoted] = useState<'up' | 'down' | undefined>(persisted.voted);
  const [busy, setBusy] = useState(false);
  const [proposing, setProposing] = useState(false);
  const [answerSaved, setAnswerSaved] = useState(false);
  const [showDownForm, setShowDownForm] = useState(false);
  const [downComment, setDownComment] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [graphOpen, setGraphOpen] = useState(false);

  // Answer ("chat") mode runs a generic single assistant, so there is no crew
  // worth cataloging directly. "Save to catalog" instead distills a reusable
  // crew from THIS conversation and proposes it (the user bookmarks that to
  // finish saving), and the crew-feedback votes are hidden (nothing cataloged
  // to rate yet).
  const isChatMode = persisted.chatModeType === 'chat' && Boolean(onSaveAnswerToCatalog);

  const handleSaveFromConversation = async () => {
    setProposing(true);
    setError(null);
    try {
      // Saves automatically and posts a read-only card showing what was saved.
      await onSaveAnswerToCatalog?.(persisted.sessionId);
      setAnswerSaved(true);
    } catch {
      setError('Could not save a crew from this conversation');
    } finally {
      setProposing(false);
    }
  };

  // The memory graph only exists for runs that actually used WORKSPACE memory.
  // This is a per-run snapshot (taken when the run was dispatched) — NOT the live
  // toggle — so a session-only run never shows the graph even after the user
  // later switches the composer to workspace memory.
  const canShowGraph = Boolean(usedWorkspaceMemory) && Boolean(executionId);

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
    <div className="px-4 my-1 max-w-3xl">
      <div className="flex items-center gap-2 flex-wrap">
        {/* Bookmark — save the generated crew to the catalog. In answer ("chat")
            mode it distills a reusable crew from the conversation and saves it in
            one shot (the generic chat assistant isn't worth cataloging), then
            shows what was saved below. */}
        <button
          type="button"
          onClick={isChatMode ? handleSaveFromConversation : handleBookmark}
          disabled={busy || proposing || (isChatMode ? answerSaved : Boolean(savedId))}
          title={
            isChatMode
              ? answerSaved
                ? 'Saved to catalog'
                : 'Distill a reusable crew from this conversation and save it'
              : savedId
                ? `Saved as “${savedName}”`
                : 'Save this crew to the catalog'
          }
          className={ICON_BTN}
          style={{
            color: (isChatMode ? answerSaved : savedId) ? 'var(--accent)' : 'var(--text-secondary)',
            backgroundColor: 'transparent',
            border: 'none',
          }}
        >
          <svg className="w-3.5 h-3.5" fill={(isChatMode ? answerSaved : savedId) ? 'currentColor' : 'none'} viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M17.593 3.322c1.1.128 1.907 1.077 1.907 2.185V21L12 17.25 4.5 21V5.507c0-1.108.806-2.057 1.907-2.185a48.507 48.507 0 0111.186 0z" />
          </svg>
          {isChatMode
            ? (proposing ? 'Saving…' : answerSaved ? 'Saved to catalog' : 'Save to catalog')
            : savedId ? `Saved${savedName ? ` — ${savedName}` : ''}` : 'Save to catalog'}
        </button>

        {/* Open the generated crew on a builder canvas. Only for actual crews —
            the answer-mode single assistant has no crew graph to load. */}
        {!isChatMode && (
          <OpenOnCanvasButtons
            data={data}
            savedCrewId={savedId}
            savedName={savedName}
            ensureSaved={() => ensureSaved()}
            disabled={busy || proposing}
          />
        )}

        {/* Crew-result feedback — only for actual cataloged crews, not the
            generic answer-mode assistant. */}
        {!isChatMode && (
          <>
            {/* Thumbs up */}
            <button
              type="button"
              onClick={() => handleVote('up')}
              disabled={busy || Boolean(voted)}
              title="Good result"
              aria-label="Thumbs up"
              className={ICON_BTN}
              style={{
                color: voted === 'up' ? 'var(--accent)' : 'var(--text-secondary)',
                backgroundColor: 'transparent',
                border: 'none',
              }}
            >
              <svg className="w-3.5 h-3.5" fill={voted === 'up' ? 'currentColor' : 'none'} viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6.633 10.5c.806 0 1.533-.446 2.031-1.08a9.041 9.041 0 012.861-2.4c.723-.384 1.35-.956 1.653-1.715a4.498 4.498 0 00.322-1.672V3a.75.75 0 01.75-.75A2.25 2.25 0 0116.5 4.5c0 1.152-.26 2.243-.723 3.218-.266.558.107 1.282.725 1.282h3.126c1.026 0 1.945.694 2.054 1.715.045.422.068.85.068 1.285a11.95 11.95 0 01-2.649 7.521c-.388.482-.987.729-1.605.729H13.48c-.483 0-.964-.078-1.423-.23l-3.114-1.04a4.501 4.501 0 00-1.423-.23H5.904" />
              </svg>
            </button>

            {/* Thumbs down — opens the comment form */}
            <button
              type="button"
              onClick={() => setShowDownForm((v) => !v)}
              disabled={busy || Boolean(voted)}
              title="Something was wrong"
              aria-label="Thumbs down"
              className={ICON_BTN}
              style={{
                color: voted === 'down' ? '#ef4444' : 'var(--text-secondary)',
                backgroundColor: 'transparent',
                border: 'none',
              }}
            >
              <svg className="w-3.5 h-3.5" fill={voted === 'down' ? 'currentColor' : 'none'} viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M7.498 15.25H4.372c-1.026 0-1.945-.694-2.054-1.715a12.137 12.137 0 01-.068-1.285c0-2.848.992-5.464 2.649-7.521C5.287 4.247 5.886 4 6.504 4h4.016a4.5 4.5 0 011.423.23l3.114 1.04a4.5 4.5 0 001.423.23h1.294M7.498 15.25c.618 0 .991.724.725 1.282A7.471 7.471 0 007.5 19.75 2.25 2.25 0 009.75 22a.75.75 0 00.75-.75v-.633c0-.573.11-1.14.322-1.672.304-.76.93-1.33 1.653-1.715a9.04 9.04 0 002.86-2.4c.498-.634 1.226-1.08 2.032-1.08h.384" />
              </svg>
            </button>
          </>
        )}

        {/* Memory graph — concept graph of what this run wrote to memory */}
        {canShowGraph && (
          <button
            type="button"
            onClick={() => setGraphOpen(true)}
            title="View this run's memory graph"
            aria-label="View memory graph"
            className={ICON_BTN}
            style={{
              color: 'var(--text-secondary)',
              backgroundColor: 'transparent',
              border: 'none',
            }}
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <circle cx="6" cy="6" r="2.5" />
              <circle cx="18" cy="7" r="2.5" />
              <circle cx="12" cy="17" r="2.5" />
              <path strokeLinecap="round" d="M7.8 7.4l2.6 7.4M16.6 8.7l-3 6.4M8.3 6.4l7.2.4" />
            </svg>
            Memory graph
          </button>
        )}

        {voted === 'down' && (
          <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>
            Feedback recorded — thank you
          </span>
        )}
        {error && (
          <span className="text-[11px]" style={{ color: '#ef4444' }}>{error}</span>
        )}
      </div>

      {/* Thumbs-down requires telling the AI engineer what went wrong */}
      {showDownForm && !voted && (
        <div className="mt-2 space-y-2">
          <textarea
            value={downComment}
            onChange={(e) => setDownComment(e.target.value)}
            placeholder="What went wrong? This is stored with the crew for the AI engineer."
            rows={2}
            className="w-full rounded-lg px-3 py-2 text-sm outline-none resize-none"
            style={{
              backgroundColor: 'var(--bg-input)',
              color: 'var(--text-primary)',
              border: '1px solid var(--border-color)',
            }}
          />
          <button
            type="button"
            onClick={() => handleVote('down', downComment.trim())}
            disabled={busy || !downComment.trim()}
            className={ICON_BTN}
            style={{
              color: 'var(--text-secondary)',
              backgroundColor: 'transparent',
              border: 'none',
            }}
          >
            Submit feedback
          </button>
        </div>
      )}

      {canShowGraph && graphOpen && (
        <MemoryRecordsBrowser
          open={graphOpen}
          onClose={() => setGraphOpen(false)}
          initialRunId={executionId}
          initialView="graph"
        />
      )}
    </div>
  );
};

export default CrewActionsBar;
