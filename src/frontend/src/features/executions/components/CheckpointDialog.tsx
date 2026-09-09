import React, { useCallback, useEffect, useRef, useState } from 'react';
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
} from '../../../api/execution/ExecutionCheckpointService';
import CheckpointUnitPicker from '../checkpoints/components/CheckpointUnitPicker';
import { useThemeStore } from '../../../store/theme';
import { kasalStageSurface } from '../../../theme/kasalSurfaces';

interface CheckpointDialogProps {
  open: boolean;
  embedded?: boolean;
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
  embedded = false,
  jobId,
  onClose,
  onResumed,
}) => {
  const { t } = useTranslation();
  const dark = useThemeStore(s => s.isDarkMode);
  const [checkpoint, setCheckpoint] = useState<ExecutionCheckpoint | null>(null);
  const [loading, setLoading] = useState(false);
  const [resuming, setResuming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [fromUnit, setFromUnit] = useState<string>('');
  const requestId = useRef(0);

  const load = useCallback(async () => {
    if (!jobId) return;
    const request = ++requestId.current;
    setLoading(true);
    setCheckpoint(null);
    setError(null);
    try {
      const result = await ExecutionCheckpointService.getCheckpoint(jobId);
      if (request !== requestId.current) return;
      setCheckpoint(result);
      // A completed run has nothing left to continue: offer a real rerun.
      setFromUnit(result?.execution_status?.toLowerCase() === 'completed' ? result.units.at(-1)?.key || '' : '');
    } catch (err) {
      if (request === requestId.current) setError(err instanceof Error ? err.message : 'Failed to load checkpoint');
    } finally {
      if (request === requestId.current) setLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    if (open) {
      setFromUnit('');
      void load();
    }
    return () => { requestId.current += 1; };
  }, [open, load]);

  const handleResume = async () => {
    if (!jobId || resuming || !checkpoint?.resumable) return;
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

  // Where the run will actually pick up, given what has been edited since. The
  // backend computes this by comparing the checkpoint against the SAVED
  // definition; null means either nothing changed or there was nothing to
  // compare against, and the two are deliberately not distinguished here — in
  // both cases the picker below is the answer.
  const changedFrom = checkpoint?.changed_from_index ?? null;
  const changedName =
    changedFrom !== null ? checkpoint?.units[changedFrom]?.name ?? null : null;

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

        {changedFrom !== null && (
          <Alert severity="warning" sx={{ mb: 2 }}>
            <strong>
              {changedName
                ? `"${changedName}" has changed since this run.`
                : `${unitNoun} ${changedFrom + 1} has changed since this run.`}
            </strong>{' '}
            Resuming keeps the first {checkpoint.restorable_count} {unitNoun}
            {checkpoint.restorable_count === 1 ? '' : 's'} and re-runs from
            there. Everything after a change re-runs too — its input is what
            changed, even where its own wording did not.
          </Alert>
        )}

        <Typography variant="body2" sx={{ mb: 1.5, fontWeight: 500 }}>
          Where should the run pick up?
        </Typography>

        <CheckpointUnitPicker
          hideDefault={checkpoint.execution_status?.toLowerCase() === 'completed' && checkpoint.units.length > 0}
          units={checkpoint.units.map((unit) => ({
            key: unit.key,
            name: unit.name,
            outputPreview: unit.output_preview,
            truncated: unit.truncated,
            willRestore: unit.will_restore,
          }))}
          value={fromUnit}
          onChange={setFromUnit}
          defaultOptionLabel={
            changedFrom !== null
              ? `Resume as computed — keep ${checkpoint.restorable_count} of ` +
                `${checkpoint.completed_count} ${unitNoun}s, re-run the rest`
              : `Continue from the first incomplete ${unitNoun} (keep all ` +
                `${checkpoint.completed_count} completed)`
          }
          renderUnitLabel={(unit) =>
            `Redo from "${unit.name || `${unitNoun} ${unit.key}`}"`
          }
        />

        <Alert severity="info" sx={{ mt: 2 }}>
          Resuming starts a <strong>new run</strong> linked to this one. This run
          stays as it is, so its history, traces and token costs are preserved
          separately. It rebuilds from the saved {unitNoun === 'crew' ? 'flow' : 'crew'},
          so edits you have saved since are picked up — and a {unitNoun} whose
          model or tools changed is re-run at launch even where it reads as
          unchanged above.
        </Alert>
      </>
    );
  };

  const title = <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 3, pt: 3, pb: 2, flexShrink: 0 }}>
        <HistoryIcon sx={{ color: 'text.secondary' }} />
        <Typography variant="h6" component="span">
          {embedded ? 'Checkpoints' : 'Checkpoint'}
          {checkpoint?.run_name ? ` — ${checkpoint.run_name}` : ''}
        </Typography>
      </Box>;
  const content = <Box sx={{ px: 3, pb: 2, flex: 1, minHeight: 0, overflowY: 'auto' }}>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        {error && !checkpoint && <Button color="inherit" onClick={() => void load()}>Try again</Button>}
        {renderBody()}
      </Box>;
  const actions = <Box sx={{ px: 3, pr: embedded ? 7 : 3, pb: embedded ? { xs: 8, sm: 2 } : 2, pt: 1, flexWrap: 'wrap', display: 'flex', gap: 1, alignItems: 'center', flexShrink: 0 }}>
        {checkpoint && !embedded && (
          <Button onClick={handleExpire} color="error" size="small">
            Discard checkpoint
          </Button>
        )}
        <Box sx={{ flex: 1 }} />
        <Button color="inherit" onClick={onClose} disabled={resuming} sx={{ borderRadius: '12px', textTransform: 'none' }}>
          {t('common.cancel')}
        </Button>
        <Button
          onClick={handleResume}
          variant="contained"
          disableElevation
          sx={{ borderRadius: '12px', textTransform: 'none', bgcolor: 'text.primary', color: 'background.paper', '&:hover': { bgcolor: 'text.secondary' } }}
          startIcon={
            resuming ? <CircularProgress size={16} color="inherit" /> : <PlayArrowIcon />
          }
          disabled={resuming || loading || !checkpoint?.resumable}
        >
          {embedded ? 'Run from checkpoint' : 'Resume'}
        </Button>
      </Box>;
  if (embedded) return <Box role="region" aria-label="Execution checkpoints" sx={{ display: 'flex', flexDirection: 'column', width: '100%', height: '100%', minHeight: 0, ...kasalStageSurface(dark) }}>
    {title}{content}{actions}
  </Box>;
  return <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
    <DialogTitle sx={{ p: 0 }}>{title}</DialogTitle>
    <DialogContent sx={{ p: 0 }}>{content}</DialogContent>
    <DialogActions sx={{ p: 0 }}>{actions}</DialogActions>
  </Dialog>;
};

export default CheckpointDialog;
