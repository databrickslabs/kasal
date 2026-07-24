import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  DialogActions,
  FormControl,
  IconButton,
  InputLabel,
  ListItemText,
  Menu,
  MenuItem,
  Paper,
  Select,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import EditIcon from '@mui/icons-material/Edit';
import CloseIcon from '@mui/icons-material/Close';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import GavelIcon from '@mui/icons-material/Gavel';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { toast } from 'react-hot-toast';
import {
  CrewEval,
  LLMJudge,
  PromptOptimizationRun,
  PromptOptimizationService,
} from '../../api/PromptOptimizationService';
import { ModelService } from '../../api/ModelService';

interface CrewOptimizeDialogProps {
  open: boolean;
  crewId: string | null;
  crewName?: string;
  onClose: () => void;
}

const POLL_INTERVAL_MS = 10000;

const statusColor = (
  status: PromptOptimizationRun['status'],
): 'default' | 'info' | 'success' | 'error' | 'warning' => {
  switch (status) {
    case 'completed':
      return 'success';
    case 'failed':
      return 'error';
    case 'cancelled':
      return 'warning';
    case 'running':
      return 'info';
    default:
      return 'default';
  }
};

/** Human label for a flattened crew field key like "agent.<id>.role". */
const fieldLabel = (key: string): string => {
  const [kind, , field] = key.split('.', 3);
  return `${kind} ${field?.replace('_', ' ') ?? key}`;
};

const SectionHeader: React.FC<{ title: string; hint?: string }> = ({ title, hint }) => (
  <Box sx={{ mt: 3, mb: 1 }}>
    <Typography
      variant="overline"
      sx={{ letterSpacing: 1, color: 'text.secondary', lineHeight: 1.5 }}
    >
      {title}
    </Typography>
    {hint && (
      <Typography variant="caption" color="text.secondary" display="block">
        {hint}
      </Typography>
    )}
  </Box>
);

/**
 * GEPA optimization for a saved crew: every evaluation EXECUTES the crew for
 * real and judges score the final deliverable. Evaluation answers are graded
 * here (per judge) and proposals are applied back to the crew's records.
 */
const CrewOptimizeDialog: React.FC<CrewOptimizeDialogProps> = ({
  open,
  crewId,
  crewName,
  onClose,
}) => {
  const [models, setModels] = useState<string[]>([]);
  const [model, setModel] = useState('');
  const [budget, setBudget] = useState(10);
  const [guidance, setGuidance] = useState('');
  const [starting, setStarting] = useState(false);
  const [applying, setApplying] = useState<string | null>(null);
  const [runs, setRuns] = useState<PromptOptimizationRun[]>([]);
  const [expandedRun, setExpandedRun] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const pollRef = useRef<number | null>(null);

  // Evaluation answers (local MLflow traces) gradable in-app.
  const [evals, setEvals] = useState<CrewEval[]>([]);
  const [expandedEval, setExpandedEval] = useState<string | null>(null);
  const [evalGrade, setEvalGrade] = useState<Record<string, number>>({});
  const [evalComment, setEvalComment] = useState<Record<string, string>>({});
  const [evalExpectation, setEvalExpectation] = useState<Record<string, string>>({});
  const [evalJudge, setEvalJudge] = useState<Record<string, string>>({});
  const [submittingEval, setSubmittingEval] = useState<string | null>(null);

  // Custom LLM judges (registered scorers) — created here, used automatically.
  const [judges, setJudges] = useState<LLMJudge[]>([]);
  const [assignAnchor, setAssignAnchor] = useState<HTMLElement | null>(null);
  const [showJudgeForm, setShowJudgeForm] = useState(false);
  const [judgeName, setJudgeName] = useState('');
  const [judgeCriteria, setJudgeCriteria] = useState('');
  const [judgeModel, setJudgeModel] = useState('');
  const [savingJudge, setSavingJudge] = useState(false);
  const [editJudge, setEditJudge] = useState<LLMJudge | null>(null);
  const [editInstructions, setEditInstructions] = useState('');
  const [editModel, setEditModel] = useState('');
  const [savingEdit, setSavingEdit] = useState(false);
  const [deleteJudgeTarget, setDeleteJudgeTarget] = useState<LLMJudge | null>(null);
  const [deletingJudge, setDeletingJudge] = useState(false);

  const refreshRuns = useCallback(async () => {
    if (!crewId) return;
    try {
      const all = await PromptOptimizationService.listRuns();
      setRuns(all.filter((r) => r.kind === 'crew' && r.crew_id === crewId));
    } catch {
      /* transient */
    }
  }, [crewId]);

  const refreshEvals = useCallback(async () => {
    if (!crewId) return;
    try {
      setEvals(await PromptOptimizationService.listCrewEvals(crewId));
    } catch {
      setEvals([]);
    }
  }, [crewId]);

  const refreshJudges = useCallback(async () => {
    try {
      setJudges(await PromptOptimizationService.listJudges());
    } catch {
      setJudges([]);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    // Clear the previous crew's data FIRST — the dialog is a persistent
    // component, and stale runs/evals from the last-opened crew would render
    // until the (multi-second) MLflow fetch for this crew lands.
    setRuns([]);
    setEvals([]);
    setExpandedRun(null);
    setExpandedEval(null);
    setError(null);
    void refreshRuns();
    void refreshEvals();
    void refreshJudges();
    (async () => {
      try {
        const enabled = await ModelService.getInstance().getEnabledModels();
        setModels(Object.keys(enabled));
      } catch {
        setModels([]);
      }
    })();
  }, [open, refreshRuns, refreshEvals, refreshJudges]);

  const hasActiveRun = runs.some((r) => r.status === 'pending' || r.status === 'running');
  useEffect(() => {
    if (!open || !hasActiveRun) return;
    pollRef.current = window.setInterval(() => {
      void refreshRuns();
      void refreshEvals();
    }, POLL_INTERVAL_MS);
    return () => {
      if (pollRef.current) window.clearInterval(pollRef.current);
    };
  }, [open, hasActiveRun, refreshRuns, refreshEvals]);

  const handleStart = async () => {
    if (!crewId) return;
    setStarting(true);
    setError(null);
    try {
      const started = await PromptOptimizationService.startCrewOptimization({
        crew_id: crewId,
        model: model || undefined,
        guidance: guidance || undefined,
        max_metric_calls: budget,
      });
      setExpandedRun(started.run_id);
      await refreshRuns();
    } catch (e: unknown) {
      const detail =
        (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        'Failed to start crew optimization';
      setError(detail);
    } finally {
      setStarting(false);
    }
  };

  const [cancelling, setCancelling] = useState<string | null>(null);
  const handleCancel = async (runId: string) => {
    setCancelling(runId);
    try {
      await PromptOptimizationService.cancelRun(runId);
      toast.success('Stopping — the current execution finishes first');
      await refreshRuns();
    } catch {
      setError('Failed to request stop');
    } finally {
      setCancelling(null);
    }
  };

  const handleApply = async (runId: string) => {
    setApplying(runId);
    try {
      await PromptOptimizationService.applyRun(runId);
      toast.success('Optimized prompts applied to the crew');
      await refreshRuns();
    } catch {
      setError('Failed to apply the optimized prompts');
    } finally {
      setApplying(null);
    }
  };

  const handleEvalFeedback = async (traceId: string) => {
    const grade = evalGrade[traceId];
    const expectation = (evalExpectation[traceId] || '').trim();
    if (grade === undefined && !expectation) return;
    setSubmittingEval(traceId);
    try {
      // Judge attribution rides in the comment so the harvesting step can
      // tell WHICH judge's criteria this grade speaks to.
      const judge = evalJudge[traceId] || 'overall';
      const note = evalComment[traceId] || '';
      await PromptOptimizationService.addEvalFeedback(
        traceId,
        grade,
        judge === 'overall' ? note || undefined : `[judge: ${judge}] ${note}`.trim(),
        expectation || undefined,
      );
      toast.success('Assessment saved — the next run will use it');
      await refreshEvals();
      setExpandedEval(null);
    } catch {
      setError('Failed to save the assessment');
    } finally {
      setSubmittingEval(null);
    }
  };

  const handleCreateJudge = async () => {
    if (!crewId) return;
    setSavingJudge(true);
    try {
      // Created from a crew's dialog → assigned to THIS crew.
      await PromptOptimizationService.createJudge(
        judgeName,
        judgeCriteria,
        judgeModel || undefined,
        crewId,
      );
      toast.success('Judge created — it will grade the next optimization run');
      setJudgeName('');
      setJudgeCriteria('');
      setShowJudgeForm(false);
      await refreshJudges();
    } catch (e: unknown) {
      const detail =
        (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        'Failed to create judge';
      setError(detail);
    } finally {
      setSavingJudge(false);
    }
  };

  const handleUnassignJudge = async (fullName: string) => {
    try {
      // Deleting the crew-scoped copy unassigns; a library original remains.
      await PromptOptimizationService.deleteJudge(fullName);
      await refreshJudges();
    } catch {
      setError('Failed to unassign judge');
    }
  };

  const handleAssignJudge = async (name: string) => {
    if (!crewId) return;
    try {
      await PromptOptimizationService.assignJudge(name, crewId);
      toast.success(`Judge "${name}" assigned to this crew`);
      await refreshJudges();
    } catch {
      setError('Failed to assign judge');
    }
  };

  const openEditJudge = (judge: LLMJudge) => {
    setEditJudge(judge);
    setEditInstructions(judge.instructions || '');
    // Empty = keep the judge's current model; the stored value is an MLflow
    // model URI, not a Kasal model key, so there is no reliable reverse map.
    setEditModel('');
  };

  const handleUpdateJudge = async () => {
    if (!editJudge) return;
    setSavingEdit(true);
    try {
      await PromptOptimizationService.updateJudge(
        editJudge.full_name || editJudge.name,
        { instructions: editInstructions, model: editModel || undefined },
      );
      toast.success(
        editJudge.crew_id
          ? `Judge "${editJudge.name}" updated — the next run uses the new version`
          : `Library judge "${editJudge.name}" updated`,
      );
      setEditJudge(null);
      await refreshJudges();
    } catch (e: unknown) {
      const detail =
        (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        'Failed to update judge';
      setError(detail);
    } finally {
      setSavingEdit(false);
    }
  };

  const handleDeleteLibraryJudge = async () => {
    if (!deleteJudgeTarget) return;
    setDeletingJudge(true);
    try {
      await PromptOptimizationService.deleteJudge(
        deleteJudgeTarget.full_name || deleteJudgeTarget.name,
      );
      toast.success(`Judge "${deleteJudgeTarget.name}" deleted`);
      setDeleteJudgeTarget(null);
      await refreshJudges();
    } catch {
      setError('Failed to delete judge');
    } finally {
      setDeletingJudge(false);
    }
  };

  const crewPrefix = crewId ? crewId.replace(/-/g, '').slice(0, 12) : '';
  const assignedJudges = judges.filter((j) => j.crew_id === crewPrefix);
  const libraryJudges = judges.filter(
    (j) => !j.crew_id && !assignedJudges.some((a) => a.name === j.name),
  );

  const changedFields = (run: PromptOptimizationRun): string[] => {
    const baseline = run.baseline_fields || {};
    const optimized = run.optimized_fields || {};
    return Object.keys(optimized).filter(
      (k) => optimized[k] && optimized[k] !== baseline[k],
    );
  };

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1, pb: 1 }}>
        <AutoFixHighIcon fontSize="small" color="primary" />
        <Box>
          <Typography variant="h6" component="span">
            Optimize crew
          </Typography>
          {crewName && (
            <Typography variant="h6" component="span" color="text.secondary">
              {' '}
              — {crewName}
            </Typography>
          )}
        </Box>
        <Box sx={{ flexGrow: 1 }} />
        <IconButton onClick={onClose} size="small">
          <CloseIcon fontSize="small" />
        </IconButton>
      </DialogTitle>
      <DialogContent>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
          GEPA searches for better agent and task prompts. Every evaluation runs
          the crew for real and your judges score the final deliverable — the
          budget is the number of crew executions.
        </Typography>
        {error && (
          <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
            {error}
          </Alert>
        )}

        {/* Run configuration */}
        <Paper variant="outlined" sx={{ p: 2 }}>
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center' }}>
            <FormControl size="small" sx={{ minWidth: 230 }}>
              <InputLabel>Model</InputLabel>
              <Select value={model} label="Model" onChange={(e) => setModel(e.target.value)}>
                <MenuItem value="">
                  <em>Default</em>
                </MenuItem>
                {models.map((key) => (
                  <MenuItem key={key} value={key}>
                    {key}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Tooltip title="HARD CAP on crew executions — the run never exceeds this number. The baseline costs 1 execution; each further execution evaluates one NEW candidate prompt set (re-evaluations are cached and free). Executions have real side effects (tools, emails, database writes).">
              <TextField
                size="small"
                type="number"
                label="Max crew executions"
                value={budget}
                onChange={(e) =>
                  setBudget(Math.max(4, Math.min(40, Number(e.target.value) || 10)))
                }
                sx={{ width: 170 }}
              />
            </Tooltip>
            <TextField
              size="small"
              label="Judging guidance (optional)"
              placeholder="what does a good deliverable look like?"
              value={guidance}
              onChange={(e) => setGuidance(e.target.value)}
              sx={{ flex: 1, minWidth: 220 }}
            />
            <Button
              variant="contained"
              onClick={handleStart}
              disabled={starting || hasActiveRun || !crewId}
              startIcon={starting ? <CircularProgress size={16} /> : <AutoFixHighIcon />}
            >
              {starting ? 'Starting…' : hasActiveRun ? 'Run in progress…' : 'Start GEPA'}
            </Button>
          </Box>

          {/* Judges */}
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              flexWrap: 'wrap',
              mt: 2,
              pt: 1.5,
              borderTop: '1px dashed',
              borderColor: 'divider',
            }}
          >
            <GavelIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
            <Typography variant="caption" color="text.secondary" sx={{ mr: 0.5 }}>
              Judges
            </Typography>
            <Chip
              size="small"
              variant="outlined"
              label="Quality (built-in)"
              title="Grades every deliverable 0-10 on completeness, specificity, and fidelity to the expected outputs"
            />
            {assignedJudges.map((j) => (
              <Chip
                key={j.full_name || j.name}
                size="small"
                color="primary"
                variant="outlined"
                clickable
                label={j.name}
                title={`${j.instructions || ''}\n\nClick to edit this judge.`}
                onClick={() => openEditJudge(j)}
                onDelete={() => handleUnassignJudge(j.full_name || j.name)}
              />
            ))}
            {libraryJudges.length > 0 && (
              <>
                <Chip
                  size="small"
                  clickable
                  variant="outlined"
                  icon={<AddIcon sx={{ fontSize: 16 }} />}
                  label="Assign"
                  onClick={(e) => setAssignAnchor(e.currentTarget)}
                />
                <Menu
                  anchorEl={assignAnchor}
                  open={Boolean(assignAnchor)}
                  onClose={() => setAssignAnchor(null)}
                >
                  {libraryJudges.map((j) => (
                    <MenuItem
                      key={j.full_name || j.name}
                      onClick={() => {
                        setAssignAnchor(null);
                        void handleAssignJudge(j.name);
                      }}
                    >
                      <ListItemText
                        primary={<Typography variant="body2">{j.name}</Typography>}
                        secondary={
                          j.instructions ? (
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              sx={{
                                display: 'block',
                                maxWidth: 320,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                              }}
                            >
                              {j.instructions}
                            </Typography>
                          ) : undefined
                        }
                      />
                      <Tooltip title="Edit judge">
                        <IconButton
                          size="small"
                          edge="end"
                          sx={{ ml: 1 }}
                          onClick={(e) => {
                            e.stopPropagation();
                            setAssignAnchor(null);
                            openEditJudge(j);
                          }}
                        >
                          <EditIcon sx={{ fontSize: 16 }} />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete from library">
                        <IconButton
                          size="small"
                          edge="end"
                          onClick={(e) => {
                            e.stopPropagation();
                            setAssignAnchor(null);
                            setDeleteJudgeTarget(j);
                          }}
                        >
                          <DeleteOutlineIcon sx={{ fontSize: 16 }} />
                        </IconButton>
                      </Tooltip>
                    </MenuItem>
                  ))}
                </Menu>
              </>
            )}
            <Button size="small" onClick={() => setShowJudgeForm((v) => !v)}>
              {showJudgeForm ? 'Cancel' : '+ Create judge'}
            </Button>
          </Box>
          {showJudgeForm && (
            <Box sx={{ mt: 1.5 }}>
              <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', mb: 1 }}>
                <TextField
                  size="small"
                  label="Judge name"
                  value={judgeName}
                  onChange={(e) => setJudgeName(e.target.value)}
                  sx={{ width: 200 }}
                />
                <FormControl size="small" sx={{ minWidth: 220 }}>
                  <InputLabel>Judge model</InputLabel>
                  <Select
                    value={judgeModel}
                    label="Judge model"
                    onChange={(e) => setJudgeModel(e.target.value)}
                  >
                    <MenuItem value="">
                      <em>Default</em>
                    </MenuItem>
                    {models.map((key) => (
                      <MenuItem key={key} value={key}>
                        {key}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Box>
              <TextField
                fullWidth
                multiline
                minRows={2}
                size="small"
                label="Evaluation criteria (plain language)"
                placeholder="e.g. Score 0-10: the answer must cite at least three named sources and include a structured summary table."
                value={judgeCriteria}
                onChange={(e) => setJudgeCriteria(e.target.value)}
              />
              <Box sx={{ mt: 1, textAlign: 'right' }}>
                <Button
                  size="small"
                  variant="contained"
                  disabled={savingJudge || !judgeName.trim() || !judgeCriteria.trim()}
                  onClick={handleCreateJudge}
                >
                  {savingJudge ? 'Creating…' : 'Create judge'}
                </Button>
              </Box>
            </Box>
          )}
        </Paper>

        {/* Runs */}
        <SectionHeader title="Optimization runs" />
        {runs.length === 0 && (
          <Typography variant="body2" color="text.secondary">
            No optimization runs for this crew yet.
          </Typography>
        )}
        {runs.map((run) => {
          const changed = changedFields(run);
          return (
            <Paper key={run.run_id} variant="outlined" sx={{ p: 1.5, mb: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, flexWrap: 'wrap' }}>
                <Chip size="small" label={run.status} color={statusColor(run.status)} />
                {run.status === 'running' && <CircularProgress size={14} />}
                {typeof run.executions_used === 'number' &&
                  typeof run.execution_cap === 'number' && (
                    <Chip
                      size="small"
                      variant="outlined"
                      label={`${run.executions_used}/${run.execution_cap} executions`}
                    />
                  )}
                {typeof run.candidates_tried === 'number' && run.candidates_tried > 1 && (
                  <Tooltip title="Distinct prompt variants executed (baseline + GEPA candidates). Re-evaluations of an already-executed variant are cached and free.">
                    <Chip
                      size="small"
                      variant="outlined"
                      label={`${run.candidates_tried} variants tried`}
                    />
                  </Tooltip>
                )}
                {typeof run.human_feedback_count === 'number' &&
                  run.human_feedback_count > 0 && (
                    <Tooltip title="Your grades, comments and expectations on past evaluation answers — folded into this run's judge rubric AND into what the GEPA reflection model sees.">
                      <Chip
                        size="small"
                        variant="outlined"
                        color="secondary"
                        label={`guided by ${run.human_feedback_count} human notes`}
                      />
                    </Tooltip>
                  )}
                {typeof run.initial_score === 'number' &&
                  typeof run.final_score === 'number' && (
                    <Chip
                      size="small"
                      variant="outlined"
                      label={`score ${run.initial_score.toFixed(2)} → ${run.final_score.toFixed(2)}`}
                    />
                  )}
                {run.applied && (
                  <Chip size="small" color="success" variant="outlined" label="applied" />
                )}
                <Typography variant="caption" color="text.secondary">
                  {run.created_at ? new Date(run.created_at).toLocaleString() : ''}
                </Typography>
                <Box sx={{ flexGrow: 1 }} />
                {(run.status === 'running' || run.status === 'pending') && (
                  <Button
                    size="small"
                    color="error"
                    variant="outlined"
                    disabled={cancelling === run.run_id}
                    onClick={() => handleCancel(run.run_id)}
                  >
                    {cancelling === run.run_id ? 'Stopping…' : 'Stop'}
                  </Button>
                )}
                {run.status === 'completed' && changed.length > 0 && (
                  <Button
                    size="small"
                    variant="outlined"
                    disabled={run.applied || applying === run.run_id}
                    onClick={() => handleApply(run.run_id)}
                  >
                    {applying === run.run_id
                      ? 'Applying…'
                      : run.applied
                        ? 'Applied'
                        : `Apply ${changed.length} changes`}
                  </Button>
                )}
                <IconButton
                  size="small"
                  onClick={() =>
                    setExpandedRun(expandedRun === run.run_id ? null : run.run_id)
                  }
                >
                  {expandedRun === run.run_id ? (
                    <ExpandLessIcon fontSize="small" />
                  ) : (
                    <ExpandMoreIcon fontSize="small" />
                  )}
                </IconButton>
              </Box>
              {expandedRun === run.run_id && (
                <Box sx={{ mt: 1.5 }}>
                  {run.error && (
                    <Alert severity="error" sx={{ mb: 1.5, whiteSpace: 'pre-wrap' }}>
                      {run.error}
                    </Alert>
                  )}
                  {run.status === 'completed' && changed.length === 0 && (
                    <Alert severity="info">
                      No field changes proposed — the baseline prompts won this
                      run&apos;s search.
                    </Alert>
                  )}
                  {changed.map((key) => (
                    <Box key={key} sx={{ mb: 2 }}>
                      <Chip
                        size="small"
                        variant="outlined"
                        label={fieldLabel(key)}
                        sx={{ mb: 0.5 }}
                      />
                      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                        <TextField
                          fullWidth
                          multiline
                          maxRows={6}
                          size="small"
                          label="Current"
                          value={run.baseline_fields?.[key] || ''}
                          InputProps={{ readOnly: true, sx: { fontSize: 13 } }}
                          sx={{ flex: 1, minWidth: 260 }}
                        />
                        <TextField
                          fullWidth
                          multiline
                          maxRows={6}
                          size="small"
                          label="Proposed"
                          value={run.optimized_fields?.[key] || ''}
                          InputProps={{ readOnly: true, sx: { fontSize: 13 } }}
                          sx={{ flex: 1, minWidth: 260 }}
                        />
                      </Box>
                    </Box>
                  ))}
                </Box>
              )}
            </Paper>
          );
        })}

        {/* Evaluation answers */}
        {evals.length > 0 && (
          <>
            <SectionHeader
              title={`Evaluation answers — ${evals.filter((e) => e.assessment_count === 0).length} to grade, ${evals.filter((e) => e.assessment_count > 0).length} graded`}
              hint="Grade answers against a judge's criteria (or overall) — the next run folds your grades into that judge's guidance."
            />
            {[...evals]
              .sort((a, b) =>
                a.assessment_count === b.assessment_count
                  ? (b.timestamp_ms || 0) - (a.timestamp_ms || 0)
                  : a.assessment_count - b.assessment_count,
              )
              .map((ev) => (
              <Paper key={ev.trace_id} variant="outlined" sx={{ p: 1.5, mb: 1 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                  <Typography variant="body2" sx={{ flex: 1 }} noWrap>
                    {ev.deliverable.slice(0, 110) || '(empty deliverable)'}
                  </Typography>
                  {ev.assessment_count > 0 && (
                    <Chip
                      size="small"
                      color="success"
                      variant="outlined"
                      label={`${ev.assessment_count} graded`}
                    />
                  )}
                  <Button
                    size="small"
                    onClick={() =>
                      setExpandedEval(expandedEval === ev.trace_id ? null : ev.trace_id)
                    }
                  >
                    {expandedEval === ev.trace_id ? 'Hide' : 'Grade'}
                  </Button>
                </Box>
                {expandedEval === ev.trace_id && (
                  <Box sx={{ mt: 1.5 }}>
                    <Typography
                      variant="overline"
                      sx={{ color: 'text.secondary', letterSpacing: 0.6 }}
                    >
                      Answer
                    </Typography>
                    {/* Rendered markdown, not raw text — crew deliverables are
                        mostly GFM tables, unreadable as pipe soup. */}
                    <Box
                      sx={{
                        border: 1,
                        borderColor: 'divider',
                        borderRadius: 1,
                        p: 1.5,
                        maxHeight: 340,
                        overflow: 'auto',
                        fontSize: 13,
                        lineHeight: 1.55,
                        '& p': { m: 0, mb: 1 },
                        '& h1, & h2, & h3, & h4': { m: 0, mb: 1, fontSize: 14, fontWeight: 600 },
                        '& table': {
                          borderCollapse: 'collapse',
                          width: 'max-content',
                          maxWidth: '100%',
                          mb: 1,
                        },
                        '& th, & td': {
                          border: 1,
                          borderColor: 'divider',
                          px: 1,
                          py: 0.5,
                          textAlign: 'left',
                          verticalAlign: 'top',
                        },
                        '& th': { bgcolor: 'action.hover', fontWeight: 600 },
                        '& code': {
                          bgcolor: 'action.hover',
                          px: 0.5,
                          borderRadius: 0.5,
                          fontSize: 12,
                        },
                        '& pre': { overflow: 'auto', m: 0, mb: 1 },
                        '& ul, & ol': { m: 0, mb: 1, pl: 2.5 },
                        '& a': { wordBreak: 'break-all' },
                      }}
                    >
                      {ev.deliverable ? (
                        <ReactMarkdown
                          remarkPlugins={[remarkGfm]}
                          components={{
                            a: ({ node: _node, ...props }) => (
                              <a {...props} target="_blank" rel="noreferrer" />
                            ),
                          }}
                        >
                          {ev.deliverable}
                        </ReactMarkdown>
                      ) : (
                        <Typography variant="body2" color="text.secondary">
                          (empty deliverable)
                        </Typography>
                      )}
                    </Box>
                    <Box
                      sx={{
                        display: 'flex',
                        gap: 2,
                        mt: 1.5,
                        alignItems: 'center',
                        flexWrap: 'wrap',
                      }}
                    >
                      <FormControl size="small" sx={{ minWidth: 170 }}>
                        <InputLabel>Grading for</InputLabel>
                        <Select
                          value={evalJudge[ev.trace_id] ?? 'overall'}
                          label="Grading for"
                          onChange={(e) =>
                            setEvalJudge((j) => ({ ...j, [ev.trace_id]: e.target.value }))
                          }
                        >
                          <MenuItem value="overall">Overall quality</MenuItem>
                          {assignedJudges.map((j) => (
                            <MenuItem key={j.full_name || j.name} value={j.name}>
                              {j.name}
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                      <TextField
                        size="small"
                        type="number"
                        label="Grade (0-10)"
                        value={evalGrade[ev.trace_id] ?? ''}
                        onChange={(e) =>
                          setEvalGrade((g) => ({
                            ...g,
                            [ev.trace_id]: Math.max(
                              0,
                              Math.min(10, Number(e.target.value) || 0),
                            ),
                          }))
                        }
                        sx={{ width: 120 }}
                      />
                      <TextField
                        size="small"
                        label="What's wrong / right? (optional)"
                        value={evalComment[ev.trace_id] ?? ''}
                        onChange={(e) =>
                          setEvalComment((c) => ({ ...c, [ev.trace_id]: e.target.value }))
                        }
                        sx={{ flex: 1, minWidth: 200 }}
                      />
                      <Button
                        size="small"
                        variant="contained"
                        disabled={
                          (evalGrade[ev.trace_id] === undefined &&
                            !(evalExpectation[ev.trace_id] || '').trim()) ||
                          submittingEval === ev.trace_id
                        }
                        onClick={() => handleEvalFeedback(ev.trace_id)}
                      >
                        {submittingEval === ev.trace_id ? 'Saving…' : 'Save'}
                      </Button>
                    </Box>
                    <TextField
                      fullWidth
                      multiline
                      minRows={2}
                      size="small"
                      label="Expectation — what SHOULD this answer contain? (optional)"
                      placeholder="e.g. Must list the 4-3-3 formation with player roles, include FIFA 2026 defensive tricks, and cite at least two sources. No implementation action plan."
                      value={evalExpectation[ev.trace_id] ?? ''}
                      onChange={(e) =>
                        setEvalExpectation((x) => ({
                          ...x,
                          [ev.trace_id]: e.target.value,
                        }))
                      }
                      sx={{ mt: 1.5 }}
                    />
                  </Box>
                )}
              </Paper>
            ))}
          </>
        )}
      </DialogContent>

      {/* Edit judge — instructions and/or model; saving registers a new
          version under the same registry name (latest version wins). */}
      <Dialog
        open={Boolean(editJudge)}
        onClose={() => setEditJudge(null)}
        fullWidth
        maxWidth="sm"
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <GavelIcon fontSize="small" color="primary" />
          Edit judge — {editJudge?.name}
        </DialogTitle>
        <DialogContent>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 2 }}>
            {editJudge?.crew_id
              ? 'This judge is assigned to this crew — changes apply to the next optimization run.'
              : 'This is a shared library judge — copies already assigned to crews keep their current version until re-assigned.'}
          </Typography>
          <FormControl size="small" fullWidth sx={{ mb: 2 }}>
            <InputLabel>Judge model</InputLabel>
            <Select
              value={editModel}
              label="Judge model"
              onChange={(e) => setEditModel(e.target.value)}
            >
              <MenuItem value="">
                <em>Keep current{editJudge?.model ? ` (${editJudge.model})` : ''}</em>
              </MenuItem>
              {models.map((key) => (
                <MenuItem key={key} value={key}>
                  {key}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            fullWidth
            multiline
            minRows={4}
            maxRows={14}
            size="small"
            label="Evaluation criteria (plain language)"
            value={editInstructions}
            onChange={(e) => setEditInstructions(e.target.value)}
            helperText="Reference the answer as {{ outputs }} — added automatically when missing."
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setEditJudge(null)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={savingEdit || (!editInstructions.trim() && !editModel)}
            onClick={() => void handleUpdateJudge()}
          >
            {savingEdit ? 'Saving…' : 'Save'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete library judge — confirm first; crew-assigned copies survive. */}
      <Dialog open={Boolean(deleteJudgeTarget)} onClose={() => setDeleteJudgeTarget(null)}>
        <DialogTitle>Delete judge “{deleteJudgeTarget?.name}”?</DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary">
            The judge is removed from the shared library. Crews that already have
            it assigned keep their own copy until it is unassigned there.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteJudgeTarget(null)}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            disabled={deletingJudge}
            onClick={() => void handleDeleteLibraryJudge()}
          >
            {deletingJudge ? 'Deleting…' : 'Delete'}
          </Button>
        </DialogActions>
      </Dialog>
    </Dialog>
  );
};

export default CrewOptimizeDialog;
