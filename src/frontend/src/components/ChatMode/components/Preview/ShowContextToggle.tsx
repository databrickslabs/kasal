import React from 'react';
import { useExecutionStore } from '../../store/executionStore';

/**
 * Checkbox in the preview-pane "working" header that toggles whether the live
 * retrieved-context / results feed is shown during a run. Reads/writes the
 * store directly so it works identically in the skeleton and the activity view.
 */
const ShowContextToggle: React.FC = () => {
  const show = useExecutionStore((s) => s.showRetrievedContext);
  const setShow = useExecutionStore((s) => s.setShowRetrievedContext);
  return (
    <label
      className="flex items-center gap-1.5 cursor-pointer select-none text-[11px]"
      style={{ color: 'var(--text-muted)' }}
      title="Show the retrieved context / live results while the agent runs"
    >
      <input
        type="checkbox"
        checked={show}
        onChange={(e) => setShow(e.target.checked)}
        style={{ accentColor: 'var(--accent)' }}
        aria-label="Show retrieved context"
      />
      Show context
    </label>
  );
};

export default ShowContextToggle;
