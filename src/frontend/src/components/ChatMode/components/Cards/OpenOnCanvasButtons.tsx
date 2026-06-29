import React, { useState } from 'react';
import { GenerationCompleteData } from '../../types/dispatcher';
import { buildCrewGraph, deriveCrewName, CrewNameConflictError } from '../../api/crews';
import { useUILayoutStore } from '../../../../store/uiLayout';

/**
 * Two actions that load a generated/saved crew straight onto a builder canvas:
 *   • Open in Agent Builder — synthesizes the SAME nodes/edges as "Save to
 *     catalog" (shared buildCrewGraph) and hands them to the WorkflowDesigner via
 *     the existing `catalogLoadCrew` event, then switches to crew mode.
 *   • Open in Flow Builder — a flow node references the crew by id, so the crew
 *     must exist in the catalog first; `ensureSaved` saves it (idempotent) and
 *     returns the id, then a crewNode is handed over via `catalogLoadFlow`.
 *
 * Shared by the post-generation actions row (research/deep crews) and the
 * answer-mode "Saved to catalog" card, so both expose the identical actions.
 */
interface OpenOnCanvasButtonsProps {
  data: GenerationCompleteData;
  /** Crew id if the crew is already in the catalog (skips the save for flow). */
  savedCrewId?: string;
  /** Display name to label the canvas crew / flow node. */
  savedName?: string;
  /** Save the crew (idempotent) and return its id — required for Flow Builder. */
  ensureSaved?: () => Promise<string>;
  /** Disable while a parent action (save/vote) is in flight. */
  disabled?: boolean;
}

const ICON_BTN =
  'flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-xs font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed';

const OpenOnCanvasButtons: React.FC<OpenOnCanvasButtonsProps> = ({
  data,
  savedCrewId,
  savedName,
  ensureSaved,
  disabled,
}) => {
  const setAppMode = useUILayoutStore((s) => s.setAppMode);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleOpenAgentBuilder = () => {
    setError(null);
    try {
      const { nodes, edges } = buildCrewGraph(data);
      window.dispatchEvent(
        new CustomEvent('catalogLoadCrew', {
          detail: { nodes, edges, name: savedName || deriveCrewName(data) },
        }),
      );
      setAppMode('crew');
    } catch {
      setError('Could not open this crew on the canvas');
    }
  };

  const handleOpenFlowBuilder = async () => {
    setBusy(true);
    setError(null);
    try {
      let crewId = savedCrewId;
      if (!crewId && ensureSaved) {
        crewId = await ensureSaved().catch(async (e) => {
          if (e instanceof CrewNameConflictError && ensureSaved) return ensureSaved();
          throw e;
        });
      }
      if (!crewId) throw new Error('no crew id');
      const crewName = savedName || deriveCrewName(data);
      const flowNode = {
        id: `crew-node-${crewId}`,
        type: 'crewNode',
        position: { x: 250, y: 150 },
        data: { id: String(crewId), label: crewName, crewName, crewId },
      };
      window.dispatchEvent(
        new CustomEvent('catalogLoadFlow', {
          detail: { nodes: [flowNode], edges: [], flowConfig: {} },
        }),
      );
      setAppMode('flow');
    } catch {
      setError('Could not open this crew on the flow canvas');
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <button
        type="button"
        onClick={handleOpenAgentBuilder}
        disabled={disabled || busy}
        title="Open this crew on the Agent Builder canvas"
        aria-label="Open in Agent Builder"
        className={ICON_BTN}
        style={{ color: 'var(--text-secondary)', backgroundColor: 'transparent', border: 'none' }}
      >
        <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <circle cx="6" cy="6" r="2.5" />
          <circle cx="18" cy="7" r="2.5" />
          <circle cx="12" cy="17" r="2.5" />
          <path strokeLinecap="round" d="M7.8 7.4l2.6 7.4M16.6 8.7l-3 6.4M8.3 6.4l7.2.4" />
        </svg>
        Open in Agent Builder
      </button>

      <button
        type="button"
        onClick={handleOpenFlowBuilder}
        disabled={disabled || busy}
        title="Open this crew on the Flow Builder canvas"
        aria-label="Open in Flow Builder"
        className={ICON_BTN}
        style={{ color: 'var(--text-secondary)', backgroundColor: 'transparent', border: 'none' }}
      >
        <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <rect x="3" y="4" width="6" height="5" rx="1" />
          <rect x="15" y="15" width="6" height="5" rx="1" />
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 6.5h4a2 2 0 012 2v9" />
        </svg>
        Open in Flow Builder
      </button>

      {error && (
        <span className="text-[11px]" style={{ color: '#ef4444' }}>{error}</span>
      )}
    </>
  );
};

export default OpenOnCanvasButtons;
