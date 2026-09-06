import React, { useState } from 'react';
import { Box, Button, IconButton, Tooltip, Menu, MenuItem, ListItemIcon, ListItemText } from '@mui/material';
import { MoreHorizontal, Info } from 'lucide-react';
import DeleteIcon from '@mui/icons-material/Delete';
import PictureAsPdfIcon from '@mui/icons-material/PictureAsPdf';
import PreviewIcon from '@mui/icons-material/Preview';
import TerminalIcon from '@mui/icons-material/Terminal';
import ScheduleIcon from '@mui/icons-material/Schedule';
import VisibilityIcon from '@mui/icons-material/Visibility';
import { Run } from '../../../api/execution/ExecutionHistoryService';
import { generateRunPDF } from '../../../utils/pdfGenerator';
import { useTranslation } from 'react-i18next';
import ReplayIcon from '@mui/icons-material/Replay';
import ExecutionStopButton from './ExecutionStopButton';
import CheckpointDialog from './CheckpointDialog';
import { useUserPreferencesStore } from '../../../store/userPreferencesStore';

/**
 * Runs that ended badly — always worth offering a resume, since a checkpoint is
 * how you avoid redoing the work they got through.
 */
const FAILED_STATUSES = ['failed', 'stopped', 'cancelled'];

/**
 * A SUCCESSFUL run is resumable too.
 *
 * Both crews and flows keep their checkpoint after completing, so either can be
 * re-run from the middle after an edit, reusing the parts that were already
 * good. The dialog says so plainly when a run turns out to have no checkpoint.
 *
 * The backend re-checks all of this; this only decides what to offer.
 */
const COMPLETED_STATUS = 'completed';

interface RunActionsProps {
  compact?: boolean;
  onShowDetails?: () => void;
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
  compact = false,
  onShowDetails,
  onViewResult,
  onShowTrace,
  onShowLogs,
  onSchedule,
  onDelete,
  onStatusChange
}) => {
  const { t } = useTranslation();
  const { useNewExecutionUI } = useUserPreferencesStore();
  const [menuAnchor, setMenuAnchor] = useState<HTMLElement | null>(null);
  const [checkpointOpen, setCheckpointOpen] = useState(false);

  const status = run.status?.toLowerCase() || '';
  const isResumable =
    FAILED_STATUSES.includes(status) || status === COMPLETED_STATUS;

  const resumeButton = isResumable ? (
    <Tooltip title="View checkpoint and resume">
      <IconButton
        size="small"
        onClick={() => setCheckpointOpen(true)}
        color="default"
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

  if (compact) {
    const isActive = ['running', 'pending', 'queued', 'in_progress', 'preparing', 'stopping', 'waiting_for_approval'].includes(status);
    const act = (callback: () => void) => { setMenuAnchor(null); callback(); };
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 0.5, whiteSpace: 'nowrap' }}>
        <ExecutionStopButton executionId={run.job_id} status={run.status} variant="icon" size="small"
          onStatusChange={newStatus => onStatusChange?.(run.id, newStatus)} />
        <Button size="small" color="inherit"
          onClick={() => isActive ? onShowTrace(run.id) : onViewResult(run)}
          sx={{ textTransform: 'none', fontSize: 11, fontWeight: 500, borderRadius: 2, px: 0.5, minWidth: 0 }}>
          {isActive ? 'Activity' : 'Result'}
        </Button>
        <IconButton size="small" aria-label={`Actions for ${run.run_name || run.job_id}`}
          aria-haspopup="menu" aria-expanded={Boolean(menuAnchor)} onClick={e => setMenuAnchor(e.currentTarget)}>
          <MoreHorizontal size={18} />
        </IconButton>
        <Menu anchorEl={menuAnchor} open={Boolean(menuAnchor)} onClose={() => setMenuAnchor(null)}
          PaperProps={{ sx: { borderRadius: 3, minWidth: 210, boxShadow: '0 8px 32px rgba(16,24,40,0.14)', '& .MuiMenuItem-root': { fontSize: 13, mx: 0.5, borderRadius: 1.5 } } }}>
          <MenuItem onClick={() => act(() => onShowTrace(run.id))}><ListItemIcon><VisibilityIcon fontSize="small" /></ListItemIcon><ListItemText>View trace</ListItemText></MenuItem>
          <MenuItem disabled={isActive} onClick={() => act(() => onShowLogs(run.job_id))}><ListItemIcon><TerminalIcon fontSize="small" /></ListItemIcon><ListItemText>View logs</ListItemText></MenuItem>
          <MenuItem onClick={() => act(() => onShowDetails?.())}><ListItemIcon><Info size={18} /></ListItemIcon><ListItemText>Run details</ListItemText></MenuItem>
          <MenuItem onClick={() => act(() => onSchedule(run))}><ListItemIcon><ScheduleIcon fontSize="small" /></ListItemIcon><ListItemText>Schedule execution</ListItemText></MenuItem>
          <MenuItem disabled={isActive} onClick={() => act(() => { void generateRunPDF(run); })}><ListItemIcon><PictureAsPdfIcon fontSize="small" /></ListItemIcon><ListItemText>Download PDF</ListItemText></MenuItem>
          {isResumable && <MenuItem onClick={() => act(() => setCheckpointOpen(true))}><ListItemIcon><ReplayIcon fontSize="small" /></ListItemIcon><ListItemText>Checkpoint and resume</ListItemText></MenuItem>}
          <MenuItem onClick={() => act(() => onDelete(run))} sx={{ color: 'error.main', mt: 0.5 }}><ListItemIcon sx={{ color: 'inherit' }}><DeleteIcon fontSize="small" /></ListItemIcon><ListItemText>Delete run</ListItemText></MenuItem>
        </Menu>
        {checkpointDialog}
      </Box>
    );
  }

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
            color="default"
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
          color="default"
        >
          <PictureAsPdfIcon fontSize="small" />
        </IconButton>
      </Tooltip>
      <Tooltip title={t('runHistory.actions.viewTrace')}>
        <IconButton
          size="small"
          onClick={() => onShowTrace(run.id)}
          color="default"
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
            color="default"
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
          color="default"
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