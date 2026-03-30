import React, { useMemo, useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  Button,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  ListItemButton,
  IconButton,
  Typography,
  Box,
  Chip,
  CircularProgress,
  Alert,
  Divider,
  Tooltip,
  Collapse,
  Radio,
  RadioGroup,
  FormControlLabel
} from '@mui/material';
import {
  PlayArrow as PlayArrowIcon,
  Refresh as RefreshIcon,
  Delete as DeleteIcon,
  History as HistoryIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  CheckCircle as CheckCircleIcon
} from '@mui/icons-material';
import { useTranslation } from 'react-i18next';
import { FlowCheckpoint, CrewCheckpoint } from '../../api/FlowService';

interface CheckpointResumeDialogProps {
  open: boolean;
  onClose: () => void;
  checkpoints: FlowCheckpoint[];
  loading: boolean;
  error: string | null;
  flowName?: string;
  onStartFresh: () => void;
  onResumeFromCheckpoint: (checkpoint: FlowCheckpoint, selectedCrewSequence?: number) => void;
  onDeleteCheckpoint: (executionId: number) => void;
  onRefresh: () => void;
}

/**
 * Dialog for choosing to resume from a checkpoint or start fresh.
 * Shown when executing a flow that has available checkpoints.
 * Supports granular crew-level checkpoint selection.
 */
const CheckpointResumeDialog: React.FC<CheckpointResumeDialogProps> = ({
  open,
  onClose,
  checkpoints,
  loading,
  error,
  flowName,
  onStartFresh,
  onResumeFromCheckpoint,
  onDeleteCheckpoint,
  onRefresh
}) => {
  const { t } = useTranslation();
  const [expandedCheckpoint, setExpandedCheckpoint] = useState<number | null>(null);
  const [selectedCrewSequence, setSelectedCrewSequence] = useState<number | null>(null);

  // Format date for display
  const formatDate = (dateString: string): string => {
    try {
      const date = new Date(dateString);
      return date.toLocaleString();
    } catch {
      return dateString;
    }
  };

  // Calculate time ago
  const getTimeAgo = (dateString: string): string => {
    try {
      const date = new Date(dateString);
      const now = new Date();
      const diffMs = now.getTime() - date.getTime();
      const diffMins = Math.floor(diffMs / 60000);
      const diffHours = Math.floor(diffMins / 60);
      const diffDays = Math.floor(diffHours / 24);

      if (diffDays > 0) {
        return `${diffDays}d ago`;
      } else if (diffHours > 0) {
        return `${diffHours}h ago`;
      } else if (diffMins > 0) {
        return `${diffMins}m ago`;
      } else {
        return 'Just now';
      }
    } catch {
      return '';
    }
  };

  // Sort checkpoints by date (most recent first)
  const sortedCheckpoints = useMemo(() => {
    return [...checkpoints].sort((a, b) => {
      const dateA = new Date(a.created_at).getTime();
      const dateB = new Date(b.created_at).getTime();
      return dateB - dateA;
    });
  }, [checkpoints]);

  // Handle checkpoint expansion toggle
  const handleToggleExpand = (executionId: number) => {
    if (expandedCheckpoint === executionId) {
      setExpandedCheckpoint(null);
      setSelectedCrewSequence(null);
    } else {
      setExpandedCheckpoint(executionId);
      setSelectedCrewSequence(null);
    }
  };

  // Handle resume from checkpoint
  const handleResumeFromCheckpoint = (checkpoint: FlowCheckpoint) => {
    onResumeFromCheckpoint(checkpoint, selectedCrewSequence ?? undefined);
    setExpandedCheckpoint(null);
    setSelectedCrewSequence(null);
  };

  // Get the crew name for a given sequence in a checkpoint
  const getCrewNameBySequence = (checkpoint: FlowCheckpoint, sequence: number): string => {
    const crew = checkpoint.crew_checkpoints?.find(c => c.sequence === sequence);
    return crew?.crew_name || `Crew ${sequence}`;
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: { minHeight: 400 }
      }}
    >
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <HistoryIcon color="primary" />
        <Typography variant="h6" component="span">
          Resume or Start Fresh
        </Typography>
      </DialogTitle>

      <DialogContent>
        {flowName && (
          <DialogContentText sx={{ mb: 2 }}>
            Flow: <strong>{flowName}</strong>
          </DialogContentText>
        )}

        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}

        {loading ? (
          <Box display="flex" justifyContent="center" alignItems="center" py={4}>
            <CircularProgress size={40} />
          </Box>
        ) : checkpoints.length === 0 ? (
          <Box textAlign="center" py={4}>
            <Typography variant="body2" color="text.secondary">
              No checkpoints available. Click &quot;Start Fresh&quot; to begin a new execution.
            </Typography>
          </Box>
        ) : (
          <>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Found {checkpoints.length} checkpoint{checkpoints.length !== 1 ? 's' : ''} from previous executions.
              Click on a checkpoint to see completed crews and choose where to resume from.
            </Typography>

            <Divider sx={{ mb: 2 }} />

            <List sx={{ maxHeight: 400, overflow: 'auto' }}>
              {sortedCheckpoints.map((checkpoint, index) => {
                const hasCrewCheckpoints = checkpoint.crew_checkpoints && checkpoint.crew_checkpoints.length > 0;
                const isExpanded = expandedCheckpoint === checkpoint.execution_id;

                return (
                  <Box key={checkpoint.execution_id} sx={{ mb: 1 }}>
                    <ListItem
                      sx={{
                        border: '1px solid',
                        borderColor: isExpanded ? 'primary.main' : 'divider',
                        borderRadius: 1,
                        borderBottomLeftRadius: isExpanded ? 0 : 1,
                        borderBottomRightRadius: isExpanded ? 0 : 1,
                        '&:hover': {
                          backgroundColor: 'action.hover'
                        }
                      }}
                    >
                      <ListItemButton
                        onClick={() => hasCrewCheckpoints && handleToggleExpand(checkpoint.execution_id)}
                        disabled={!hasCrewCheckpoints}
                        sx={{ flexGrow: 1, py: 0 }}
                      >
                        <ListItemText
                          primary={
                            <Box display="flex" alignItems="center" gap={1}>
                              <Typography variant="subtitle2">
                                {checkpoint.run_name || `Execution #${checkpoint.execution_id}`}
                              </Typography>
                              {index === 0 && (
                                <Chip
                                  label="Latest"
                                  size="small"
                                  color="primary"
                                  variant="outlined"
                                />
                              )}
                              {hasCrewCheckpoints && checkpoint.crew_checkpoints && (
                                <Chip
                                  label={`${checkpoint.crew_checkpoints.length} crew${checkpoint.crew_checkpoints.length !== 1 ? 's' : ''}`}
                                  size="small"
                                  color="success"
                                  variant="outlined"
                                />
                              )}
                            </Box>
                          }
                          secondary={
                            <Box component="span" sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, mt: 0.5 }}>
                              <Typography variant="caption" color="text.secondary">
                                {formatDate(checkpoint.created_at)} ({getTimeAgo(checkpoint.created_at)})
                              </Typography>
                              {checkpoint.checkpoint_method && (
                                <Typography variant="caption" color="text.secondary">
                                  Last checkpoint: {checkpoint.checkpoint_method}
                                </Typography>
                              )}
                            </Box>
                          }
                        />
                        {hasCrewCheckpoints && (
                          isExpanded ? <ExpandLessIcon /> : <ExpandMoreIcon />
                        )}
                      </ListItemButton>
                      <ListItemSecondaryAction>
                        <Tooltip title={hasCrewCheckpoints && !isExpanded
                          ? "Expand to select crew checkpoint"
                          : "Resume from this checkpoint"}>
                          <IconButton
                            edge="end"
                            color="primary"
                            onClick={() => {
                              if (hasCrewCheckpoints && !isExpanded) {
                                handleToggleExpand(checkpoint.execution_id);
                              } else {
                                handleResumeFromCheckpoint(checkpoint);
                              }
                            }}
                            sx={{ mr: 1 }}
                          >
                            <PlayArrowIcon />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Delete checkpoint">
                          <IconButton
                            edge="end"
                            color="error"
                            onClick={() => onDeleteCheckpoint(checkpoint.execution_id)}
                            size="small"
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </ListItemSecondaryAction>
                    </ListItem>

                    {/* Expanded crew checkpoints section */}
                    <Collapse in={isExpanded}>
                      <Box
                        sx={{
                          border: '1px solid',
                          borderColor: 'primary.main',
                          borderTop: 'none',
                          borderBottomLeftRadius: 1,
                          borderBottomRightRadius: 1,
                          p: 2,
                          backgroundColor: 'background.paper'
                        }}
                      >
                        <Typography variant="body2" sx={{ mb: 1.5, fontWeight: 500 }}>
                          Select where to resume from:
                        </Typography>

                        <RadioGroup
                          value={selectedCrewSequence ?? ''}
                          onChange={(e) => setSelectedCrewSequence(e.target.value ? Number(e.target.value) : null)}
                        >
                          {/* Option to resume from the end (all crews completed) */}
                          <FormControlLabel
                            value=""
                            control={<Radio size="small" />}
                            label={
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                <Typography variant="body2">
                                  Resume from end (all {checkpoint.crew_checkpoints?.length} crews completed)
                                </Typography>
                              </Box>
                            }
                          />

                          {/* Individual crew checkpoints */}
                          {checkpoint.crew_checkpoints?.map((crew: CrewCheckpoint) => (
                            <FormControlLabel
                              key={crew.sequence}
                              value={crew.sequence}
                              control={<Radio size="small" />}
                              label={
                                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                                  <CheckCircleIcon
                                    fontSize="small"
                                    color="success"
                                    sx={{ fontSize: 16 }}
                                  />
                                  <Typography variant="body2">
                                    Resume after &quot;{crew.crew_name}&quot;
                                  </Typography>
                                  <Chip
                                    label={`#${crew.sequence}`}
                                    size="small"
                                    variant="outlined"
                                    sx={{ height: 20, fontSize: '0.7rem' }}
                                  />
                                  {crew.output_preview && (
                                    <Tooltip title={crew.output_preview}>
                                      <Typography
                                        variant="caption"
                                        color="text.secondary"
                                        sx={{
                                          maxWidth: 200,
                                          overflow: 'hidden',
                                          textOverflow: 'ellipsis',
                                          whiteSpace: 'nowrap'
                                        }}
                                      >
                                        {crew.output_preview.substring(0, 50)}...
                                      </Typography>
                                    </Tooltip>
                                  )}
                                </Box>
                              }
                            />
                          ))}
                        </RadioGroup>

                        <Box sx={{ mt: 2, display: 'flex', justifyContent: 'flex-end', gap: 1 }}>
                          <Button
                            size="small"
                            onClick={() => {
                              setExpandedCheckpoint(null);
                              setSelectedCrewSequence(null);
                            }}
                          >
                            Cancel
                          </Button>
                          <Button
                            variant="contained"
                            size="small"
                            startIcon={<PlayArrowIcon />}
                            onClick={() => handleResumeFromCheckpoint(checkpoint)}
                          >
                            {selectedCrewSequence
                              ? `Resume after "${getCrewNameBySequence(checkpoint, selectedCrewSequence)}"`
                              : 'Resume from End'
                            }
                          </Button>
                        </Box>
                      </Box>
                    </Collapse>
                  </Box>
                );
              })}
            </List>
          </>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button
          onClick={onRefresh}
          startIcon={<RefreshIcon />}
          disabled={loading}
          size="small"
        >
          Refresh
        </Button>
        <Box sx={{ flex: 1 }} />
        <Button onClick={onClose} disabled={loading}>
          {t('common.cancel')}
        </Button>
        <Button
          onClick={onStartFresh}
          variant="contained"
          color="primary"
          disabled={loading}
        >
          Start Fresh
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default CheckpointResumeDialog;
