/**
 * Per-run memory graph — the run list's door into the Memory Browser.
 *
 * Chat already offers this on a finished run's action bar; the run table is the
 * other place a run is looked back at, so the same graph belongs here, scoped to
 * the row's run. Clicking opens the browser pinned to that run's `job_id` and
 * straight on the graph view — the concepts that run wrote to memory.
 *
 * The whole column stays quiet when the workspace has no memory at
 * all (no backend configured, or an empty store): a dead icon on every row is
 * worse than no column. Availability is probed ONCE per page load and shared by
 * every row, so a 50-row table costs one request, not fifty.
 */

import React, { useEffect, useState } from 'react';
import { IconButton, Tooltip } from '@mui/material';
// The brain reads as cognition, not hardware — Configuration's Memory section
// uses the chip icon for the STORE; this is what the run thought.
import PsychologyIcon from '@mui/icons-material/Psychology';

import { apiClient } from '../../config/api/ApiConfig';
import { MemoryRecordsBrowser } from '../MemoryBackend/MemoryRecordsBrowser';

interface ProbeResponse {
  records?: unknown[];
  count?: number;
  total?: number;
}

// Shared across every row and reused for the life of the page. `null` while
// nothing has asked yet; a single in-flight promise once the first row mounts.
let memoryProbe: Promise<boolean> | null = null;

const hasMemoryRecords = (): Promise<boolean> => {
  if (!memoryProbe) {
    memoryProbe = apiClient
      .get<ProbeResponse>('/memory-backend/records', { params: { limit: 1, offset: 0 } })
      .then((res) => (res.data?.total ?? res.data?.count ?? 0) > 0)
      // A backend that is unconfigured or unreachable has nothing to show, and
      // the run list must not surface an error for a secondary affordance.
      .catch(() => false);
  }
  return memoryProbe;
};

interface Props {
  /** Execution (job) id of the row's run — what the browser scopes records to. */
  jobId: string;
}

export const ExecutionMemoryButton: React.FC<Props> = ({ jobId }) => {
  const [available, setAvailable] = useState(false);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;
    hasMemoryRecords().then((ok) => {
      if (!cancelled) setAvailable(ok);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!available) return null;

  return (
    <>
      <Tooltip title="View this run's memory graph">
        <IconButton size="small" color="primary" onClick={() => setOpen(true)}>
          <PsychologyIcon fontSize="small" />
        </IconButton>
      </Tooltip>

      {open && (
        <MemoryRecordsBrowser
          open={open}
          onClose={() => setOpen(false)}
          initialRunId={jobId}
          initialView="graph"
        />
      )}
    </>
  );
};

export default ExecutionMemoryButton;
