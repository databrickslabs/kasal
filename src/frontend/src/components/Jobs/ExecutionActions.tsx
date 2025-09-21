import React from 'react';
import { Box, IconButton, Tooltip } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import PictureAsPdfIcon from '@mui/icons-material/PictureAsPdf';
import PreviewIcon from '@mui/icons-material/Preview';
import TerminalIcon from '@mui/icons-material/Terminal';
import ScheduleIcon from '@mui/icons-material/Schedule';
import VisibilityIcon from '@mui/icons-material/Visibility';
import { Run } from '../../api/ExecutionHistoryService';
import { generateRunPDF } from '../../utils/pdfGenerator';
import { useTranslation } from 'react-i18next';
import ExecutionStopButton from '../ExecutionStopButton';
import { useUserPreferencesStore } from '../../store/userPreferencesStore';

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
        <Tooltip title={t('runHistory.actions.deleteRun')}>
          <IconButton
            size="small"
            onClick={() => onDelete(run)}
            color="error"
          >
            <DeleteIcon fontSize="small" />
          </IconButton>
        </Tooltip>
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
      <Tooltip title={t('runHistory.actions.deleteRun')}>
        <IconButton
          size="small"
          onClick={() => onDelete(run)}
          color="error"
        >
          <DeleteIcon fontSize="small" />
        </IconButton>
      </Tooltip>
    </Box>
  );
};

export default RunActions; 