import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Tooltip,
  Typography,
} from '@mui/material';
import HistoryIcon from '@mui/icons-material/History';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import { useTranslation } from 'react-i18next';
import ExecutionCheckpointService, {
  ExecutionCheckpoint,
} from '../../api/execution/ExecutionCheckpointService';
import CheckpointUnitPicker from '../Checkpoints/CheckpointUnitPicker';

interface CheckpointDialogProps {
  open: boolean;
  jobId: string | null;
  onClose: () => void;
  /** Called with the NEW execution's job_id after a successful resume. */
  onResumed?: (newJobId: string) => void;
}

/**
 * The checkpoint view for a run — crew or flow, one component.
 *
 * Shows which units completed, whether any output was truncated, and lets an
 * operator resume from a chosen point. Crews had no checkpoint UI at all before
 * this (the resume endpoint existed but nothing called it); flows had one that
 * only worked for flows, hanging off flow-scoped endpoints.
 *
 * A unit is a task for a crew and a crew for a flow. That is the only
 * difference, and it is a label, so it is the only thing this branches on.
 */
const CheckpointDialog: React.FC<CheckpointDialogProps> = ({
  open,
  jobId,
  onClose,
  onResumed,
}) => {
  const { t } = useTranslation();
  const [checkpoint, setCheckpoint] = useState<ExecutionCheckpoint | null>(null);
  const [loading, setLoading] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fromUnit, setFromUnit] = useState<string>('');

  const load = useCallback(async () => {
    if (!jobId) return;
    setLoading(true);
    setError(null);
    try {
      setCheckpoint(await ExecutionCheckpointService.getCheckpoint(jobId));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load checkpoint');
    } finally {
      setLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    if (open) {
      setFromUnit('');
      void load();
    }
  }, [open, load]);

  const handleResume = async () => {
    if (!jobId) return;
    setResuming(true);
    setError(null);
    try {
      const result = await ExecutionCheckpointService.resume(
        jobId,
        fromUnit || undefined,
      );
      onResumed?.(result.execution_id);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to resume execution');
    } finally {
      setResuming(false);
    }
  };

  const handleExpire = async () => {
    if (!jobId) return;
    setError(null);
    try {
      await ExecutionCheckpointService.expire(jobId);
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to expire checkpoint');
    }
  };

  // A crew's units are tasks; a flow's are crews. Naming them correctly is the
  // whole of the path-specific behaviour here.
  const unitNoun = checkpoint?.kind === 'flow' ? 'crew' : 'task';

  const renderBody = () => {
    if (loading) {
      return (
        <Box display="flex" justifyContent="center" py={4}>
          <CircularProgress size={40} />
        </Box>
      );
    }

    if (!checkpoint) {
      return (
        <Box textAlign="center" py={4}>
          <Typography variant="body2" color="text.secondary">
            This run has no checkpoint. Nothing was recorded as complete, so a
            resume would start it over from the beginning.
          </Typography>
        </Box>
      );
    }

    return (
      <>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
          <Chip
            size="small"
            color="success"
            variant="outlined"
            label={`${checkpoint.completed_count}${
              checkpoint.unit_count ? ` / ${checkpoint.unit_count}` : ''
            } ${unitNoun}s complete`}
          />
          {checkpoint.status && (
            <Chip size="small" variant="outlined" label={checkpoint.status} />
          )}
          {checkpoint.truncated && (
            <Tooltip title="At least one stored output was capped at 500,000 characters. Resuming will restore the shortened text, not the original.">
              <Chip
                size="small"
                color="warning"
                variant="outlined"
                icon={<WarningAmberIcon />}
                label="Output truncated"
              />
            </Tooltip>
          )}
          {checkpoint.derived && (
            <Tooltip title="This run predates written checkpoints, so its units were reconstructed from an older payload. Fidelity is not guaranteed.">
              <Chip
                size="small"
                color="warning"
                variant="outlined"
                label="Legacy checkpoint"
              />
            </Tooltip>
          )}
        </Box>

        {!checkpoint.resumable && checkpoint.blocked_reason && (
          <Alert severity="info" sx={{ mb: 2 }}>
            {checkpoint.blocked_reason}
          </Alert>
        )}

        <Divider sx={{ mb: 2 }} />

        <Typography variant="body2" sx={{ mb: 1.5, fontWeight: 500 }}>
          Where should the run pick up?
        </Typography>

        <CheckpointUnitPicker
          units={checkpoint.units.map((unit) => ({
            key: unit.key,
            name: unit.name,
            outputPreview: unit.output_preview,
            truncated: unit.truncated,
          }))}
          value={fromUnit}
          onChange={setFromUnit}
          defaultOptionLabel={`Continue from the first incomplete ${unitNoun} (keep all ${checkpoint.completed_count} completed)`}
          renderUnitLabel={(unit) =>
            `Redo from "${unit.name || `${unitNoun} ${unit.key}`}"`
          }
        />

        <Alert severity="info" sx={{ mt: 2 }}>
          Resuming starts a <strong>new run</strong> linked to this one. This run
          stays as it is, so its history, traces and token costs are preserved
          separately.
        </Alert>
      </>
    );
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <HistoryIcon color="primary" />
        <Typography variant="h6" component="span">
          Checkpoint
          {checkpoint?.run_name ? ` — ${checkpoint.run_name}` : ''}
        </Typography>
      </DialogTitle>

      <DialogContent>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        {renderBody()}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2 }}>
        {checkpoint && (
          <Button onClick={handleExpire} color="error" size="small">
            Discard checkpoint
          </Button>
        )}
        <Box sx={{ flex: 1 }} />
        <Button onClick={onClose} disabled={resuming}>
          {t('common.cancel')}
        </Button>
        <Button
          onClick={handleResume}
          variant="contained"
          startIcon={
            resuming ? <CircularProgress size={16} color="inherit" /> : <PlayArrowIcon />
          }
          disabled={resuming || loading || !checkpoint?.resumable}
        >
          Resume
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default CheckpointDialog;
