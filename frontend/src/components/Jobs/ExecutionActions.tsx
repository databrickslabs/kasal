import React from 'react';
import { Box, IconButton, Tooltip } from '@mui/material';
import DeleteIcon from '@mui/icons-material/Delete';
import VisibilityIcon from '@mui/icons-material/Visibility';
import PictureAsPdfIcon from '@mui/icons-material/PictureAsPdf';
import PreviewIcon from '@mui/icons-material/Preview';
import TerminalIcon from '@mui/icons-material/Terminal';
import ScheduleIcon from '@mui/icons-material/Schedule';
import { Run } from '../../api/ExecutionHistoryService';
import { generateRunPDF } from '../../utils/pdfGenerator';
import { useTranslation } from 'react-i18next';

interface RunActionsProps {
  run: Run;
  onViewResult: (run: Run) => void;
  onShowTrace: (runId: string) => void;
  onShowLogs: (jobId: string) => void;
  onSchedule: (run: Run) => void;
  onDelete: (run: Run) => void;
}

const RunActions: React.FC<RunActionsProps> = ({
  run,
  onViewResult,
  onShowTrace,
  onShowLogs,
  onSchedule,
  onDelete
}) => {
  const { t } = useTranslation();

  return (
    <Box sx={{ display: 'flex', gap: 0.5 }}>
      <Tooltip title={t('runHistory.actions.viewResult')}>
        <IconButton
          size="small"
          onClick={() => onViewResult(run)}
          color="primary"
        >
          <PreviewIcon fontSize="small" />
        </IconButton>
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
        <IconButton
          size="small"
          onClick={() => onShowLogs(run.job_id)}
          color="primary"
        >
          <TerminalIcon fontSize="small" />
        </IconButton>
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