import React, { useState } from 'react';
import { Box, IconButton, Tooltip } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import PictureAsPdfIcon from '@mui/icons-material/PictureAsPdf';
import PreviewIcon from '@mui/icons-material/Preview';
import TerminalIcon from '@mui/icons-material/Terminal';
import ScheduleIcon from '@mui/icons-material/Schedule';
import VisibilityIcon from '@mui/icons-material/Visibility';
import { Run } from '../../api/execution/ExecutionHistoryService';
import { generateRunPDF } from '../../utils/pdfGenerator';
import { useTranslation } from 'react-i18next';
import ReplayIcon from '@mui/icons-material/Replay';
import ExecutionStopButton from './ExecutionStopButton';
import CheckpointDialog from './CheckpointDialog';
import { useUserPreferencesStore } from '../../store/userPreferencesStore';

/**
 * Runs that ended badly — always worth offering a resume, since a checkpoint is
 * how you avoid redoing the work they got through.
 */
const FAILED_STATUSES = ['failed', 'stopped', 'cancelled'];

/**
 * A SUCCESSFUL run is resumable too, but only for flows.
 *
 * A flow keeps its checkpoint after completing, so it can be re-run from the
 * middle once a downstream crew changes — the upstream results are reused. A
 * crew discards its checkpoint on success (it is crash recovery, not a re-run
 * point), so offering the control on a completed crew run would be a dead
 * button on every successful row.
 *
 * The backend re-checks all of this; this only decides what to offer.
 */
const RESUMABLE_ON_SUCCESS_TYPES = ['flow'];

interface RunActionsProps {
  run: Run;
  onViewResult: (run: Run) => void;
  onShowTrace: (runId: string) => void;
  onShowLogs: (jobId: string) => void;
  onSchedule: (run: Run) => void;
  onDelete: (run: Run) => void;
  onStatusChange?: (runId: string, newStatus: string) => void;
}

const RunActions: React.FC<RunActionsProps> = ({
  run,
  onViewResult,
  onShowTrace,
  onShowLogs,
  onSchedule,
  onDelete,
  onStatusChange
}) => {
  const { t } = useTranslation();
  const { useNewExecutionUI } = useUserPreferencesStore();
  const [checkpointOpen, setCheckpointOpen] = useState(false);

  const status = run.status?.toLowerCase() || '';
  const isResumable =
    FAILED_STATUSES.includes(status) ||
    (status === 'completed' &&
      RESUMABLE_ON_SUCCESS_TYPES.includes(run.execution_type || ''));

  const resumeButton = isResumable ? (
    <Tooltip title="View checkpoint and resume">
      <IconButton
        size="small"
        onClick={() => setCheckpointOpen(true)}
        color="primary"
        aria-label="View checkpoint and resume"
      >
        <ReplayIcon fontSize="small" />
      </IconButton>
    </Tooltip>
  ) : null;

  const checkpointDialog = (
    <CheckpointDialog
      open={checkpointOpen}
      jobId={run.job_id}
      onClose={() => setCheckpointOpen(false)}
      onResumed={() => {
        // A resume creates a NEW run, so the list needs to refetch rather than
        // patch this row's status in place.
        onStatusChange?.(run.id, 'RUNNING');
      }}
    />
  );

  // If using new UI, only show Stop and Delete buttons (Result and Trace are in separate columns)
  if (useNewExecutionUI) {
    return (
      <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center' }}>
        {/* Stop button - only shows when execution is running */}
        <ExecutionStopButton
          executionId={run.job_id}
          status={run.status}
          variant="icon"
          size="small"
          onStatusChange={(newStatus) => {
            if (onStatusChange) {
              onStatusChange(run.id, newStatus);
            }
          }}
        />
        {resumeButton}
        <Tooltip title={t('runHistory.actions.deleteRun')}>
          <IconButton
            size="small"
            onClick={() => onDelete(run)}
            color="error"
          >
            <DeleteIcon fontSize="small" />
          </IconButton>
        </Tooltip>
        {checkpointDialog}
      </Box>
    );
  }

  // Traditional view with all buttons
  return (
    <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center' }}>
      {/* Stop button - only shows when execution is running */}
      <ExecutionStopButton
        executionId={run.job_id}
        status={run.status}
        variant="icon"
        size="small"
        onStatusChange={(newStatus) => {
          if (onStatusChange) {
            onStatusChange(run.id, newStatus);
          }
        }}
      />
      <Tooltip title={t('runHistory.actions.viewResult')}>
        <span>
          <IconButton
            size="small"
            onClick={() => onViewResult(run)}
            color="primary"
            disabled={['running', 'pending', 'queued', 'in_progress'].includes(run.status?.toLowerCase() || '')}
          >
            <PreviewIcon fontSize="small" />
          </IconButton>
        </span>
      </Tooltip>
      <Tooltip title={t('runHistory.actions.downloadPdf')}>
        <IconButton
          size="small"
          onClick={() => generateRunPDF(run)}
          color="primary"
        >
          <PictureAsPdfIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Tooltip title={t('runHistory.actions.viewTrace')}>
        <IconButton
          size="small"
          onClick={() => onShowTrace(run.id)}
          color="primary"
          aria-label="View execution trace"
        >
          <VisibilityIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Tooltip title={t('runHistory.actions.viewLogs')}>
        <span>
          <IconButton
            size="small"
            onClick={() => onShowLogs(run.job_id)}
            color="primary"
            disabled={['running', 'pending', 'queued', 'in_progress'].includes(run.status?.toLowerCase() || '')}
          >
            <TerminalIcon fontSize="small" />
          </IconButton>
        </span>
      </Tooltip>
      <Tooltip title={t('runHistory.actions.schedule')}>
        <IconButton
          size="small"
          onClick={() => onSchedule(run)}
          color="primary"
        >
          <ScheduleIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      {resumeButton}
      <Tooltip title={t('runHistory.actions.deleteRun')}>
        <IconButton
          size="small"
          onClick={() => onDelete(run)}
          color="error"
        >
          <DeleteIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      {checkpointDialog}
    </Box>
  );
};

export default RunActions; 