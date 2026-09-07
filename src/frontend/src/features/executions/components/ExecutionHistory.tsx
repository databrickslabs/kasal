import React, { useState, useEffect, forwardRef, useRef } from 'react';
import {
  Box,
  Card,
  CardContent,
  Alert,
  IconButton,
  TextField,
  Pagination,
  CircularProgress,
  Typography,
  Button,
  Menu,
  MenuItem,
} from '@mui/material';
import { History, X, ArrowDownWideNarrow } from 'lucide-react';
import { useThemeStore } from '../../../store/theme';
import DeleteIcon from '@mui/icons-material/Delete';
import SearchIcon from '@mui/icons-material/Search';
import InsightsIcon from '@mui/icons-material/Insights';
import RecipeCurationButton from './RecipeCurationButton';
import ExecutionMemoryButton from './ExecutionMemoryButton';
import ExecutionHistorySkeleton from './ExecutionHistorySkeleton';
import { refreshRecipeIndexIfStale } from './recipeIndexCache';
import RecipeEffectivenessDialog from './RecipeEffectivenessDialog';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import { Run, calculateDurationFromTraces } from '../../../api/execution/ExecutionHistoryService';
import { ScheduleService } from '../../../api/execution/ScheduleService';
import ShowTraceTimeline from './ShowTraceTimeline';
import ShowResult from './ShowResult';
import { ResultValue } from '../../../types/execution/result';
import ShowLogs from './ShowLogs';
import { executionLogService } from '../../../api/execution/ExecutionLogs';
import type { LogMessage, LogEntry } from '../../../api/execution/ExecutionLogs';
import { useTranslation } from 'react-i18next';
import { toast } from 'react-hot-toast';
import { useRunResult } from '../../../hooks/global/useExecutionResult';
import { useRunHistory } from '../../../hooks/global/useExecutionHistory';
import { useRunStatusStore } from '../../../store/runStatus';
import RunActions from './ExecutionActions';
import RunDialogs from './RunDialogs';
import { AgentYaml, TaskYaml } from '../../../types/workflow/crewPayload';
import { useTaskExecutionStore } from '../../../store/taskExecutionStore';
import { usePermissions } from '../../../hooks/usePermissions';
import ExecutionStatusBadge from './ExecutionStatusBadge';

export interface RunHistoryRef {
  refreshRuns: () => Promise<void>;
}

// Component to handle async duration loading
const DurationCell: React.FC<{ run: Run }> = ({ run }) => {
  const [duration, setDuration] = useState<string>('-');
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    let mounted = true;
    
    const loadDuration = async () => {
      try {
        const calculatedDuration = await calculateDurationFromTraces(run);
        if (mounted) {
          setDuration(calculatedDuration);
          setLoading(false);
        }
      } catch (error) {
        if (mounted) {
          setDuration('-');
          setLoading(false);
        }
      }
    };

    // Only calculate for completed jobs
    const status = (run.status || '').toUpperCase();
    if (status === 'COMPLETED' || status === 'FAILED' || status === 'CANCELLED') {
      loadDuration();
    } else {
      setDuration('-');
      setLoading(false);
    }

    return () => {
      mounted = false;
    };
  }, [run]);

  if (loading) return <CircularProgress size={12} color="inherit" />;
  return <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5, color: 'text.secondary', fontSize: 12 }}>
    {duration !== '-' && <AccessTimeIcon sx={{ fontSize: 13 }} />}{duration}
  </Box>;
};

interface ScheduleCreateData {
  name: string;
  cron_expression: string;
  execution_type?: 'crew' | 'flow';
  // Crew fields
  agents_yaml?: Record<string, AgentYaml>;
  tasks_yaml?: Record<string, TaskYaml>;
  // Flow fields
  flow_id?: string;
  nodes?: Array<{ id: string; type: string; position: { x: number; y: number }; data: Record<string, unknown> }>;
  edges?: Array<{ id: string; source: string; target: string; sourceHandle?: string; targetHandle?: string }>;
  flow_config?: Record<string, unknown>;
  // Common fields
  inputs?: Record<string, unknown>;
  is_active?: boolean;
  model?: string;
}

interface RunHistoryProps {
  jobIds?: string[];
  title?: string;
  embedded?: boolean;
  onClose?: () => void;
  executionHistoryHeight?: number;
  onExecutionCountChange?: (count: number) => void;
}

const RunHistory = forwardRef<RunHistoryRef, RunHistoryProps>(({ onClose, onExecutionCountChange, jobIds, title = 'Activity', embedded = false }, ref) => {
  const { t } = useTranslation();
  const { showRunResult, selectedRun, isOpen, closeRunResult } = useRunResult();
  const { userRole } = usePermissions();
  const dark = useThemeStore(state => state.isDarkMode);
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);
  const {
    runs,
    searchQuery,
    loading: _loading,
    showSkeleton,
    error,
    page: _page,
    totalPages: _totalPages,
    totalRuns: _totalRuns,
    jobsPerPage: _jobsPerPage,
    sortField,
    sortOrder,
    fetchRuns,
    handlePageChange: _handlePageChange,
    handleSearchChange,
    handleDeleteAllRuns,
    handleDeleteRun,
    getCurrentPageJobs: _getCurrentPageJobs,
    handleSort,
  } = useRunHistory(jobIds);

  // SSE handles all updates automatically - no polling needed

  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedRunForTrace, setSelectedRunForTrace] = useState<Run | null>(null);
  const [showTraceOpen, setShowTraceOpen] = useState<boolean>(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [recipesDialogOpen, setRecipesDialogOpen] = useState(false);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [showLogsDialog, setShowLogsDialog] = useState(false);
  const [selectedJobLogs, setSelectedJobLogs] = useState<LogEntry[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [isConnecting, setIsConnecting] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [scheduleDialogOpen, setScheduleDialogOpen] = useState(false);
  const [selectedRunForSchedule, setSelectedRunForSchedule] = useState<Run | null>(null);
  const [scheduleName, setScheduleName] = useState('');
  const [cronExpression, setCronExpression] = useState('0 0 * * *');
  const [anchorEl, setAnchorEl] = useState<HTMLButtonElement | null>(null);
  const scheduleNameInputRef = useRef<HTMLInputElement>(null);
  const [deleteRunDialogOpen, setDeleteRunDialogOpen] = useState(false);
  const [runToDelete, setRunToDelete] = useState<Run | null>(null);
  const [localPage, setLocalPage] = useState(1);

  // Initialize static refs outside of useEffect  
  const isInitializedRef = useRef<boolean>(false);
  const previousTraceOpenRef = useRef<boolean>(false);
  const previousLogsDialogRef = useRef<boolean>(false);
  const userActivityTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  const itemsPerPage = 20;
  const startIndex = (localPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const displayedRuns = runs.slice(startIndex, endIndex);
  const totalLocalPages = Math.ceil(runs.length / itemsPerPage);
  
  // Memoize the result for ShowResult to prevent unnecessary re-renders
  const memoizedResult = React.useMemo(() => {
    return (selectedRun?.result as Record<string, ResultValue>) || {};
  }, [selectedRun?.result]);

  // Effect for initializing ref values
  useEffect(() => {
    previousTraceOpenRef.current = showTraceOpen;
    previousLogsDialogRef.current = showLogsDialog;
  }, [showTraceOpen, showLogsDialog]);
  
  // Reset local page when runs change or search query changes
  useEffect(() => {
    setLocalPage(1);
  }, [runs.length, searchQuery, itemsPerPage]);

  // Notify parent of execution count changes
  useEffect(() => {
    if (onExecutionCountChange) {
      onExecutionCountChange(runs.length);
    }
  }, [runs.length, onExecutionCountChange]);

  // A row is on screen before its recipe is mined, so re-read the job→recipe
  // index as the list changes (self-throttled by a TTL) — otherwise the Reusable
  // control never appears for a just-finished run until a full page reload. The
  // cache also re-reads on job completion, which is when mining actually runs.
  useEffect(() => {
    refreshRecipeIndexIfStale();
  }, [runs]);
  
  // Effect for periodic job status check
  useEffect(() => {
    // Prevent duplicate initialization
    if (isInitializedRef.current) {
      return;
    }
    
    console.log('=== DEBUG: RunHistory useEffect - initializing ===');
    isInitializedRef.current = true;
    
    // Initial fetch and setup function
    const initializeAndSetup = async () => {
      try {
        await fetchRuns();
      } catch (err) {
        console.error('[RunHistory] Error in initial fetch:', err);
      }

      // SSE handles all updates automatically - no polling or user activity tracking needed
      // Just return empty cleanup function
      return () => {
        // Cleanup handled by SSE connection manager
      };
    };

    // Store cleanup function
    const cleanup = initializeAndSetup();

    // Return cleanup function
    return () => {
      console.log('=== DEBUG: RunHistory useEffect cleanup running ===');
      cleanup.then(cleanupFn => cleanupFn());
    };
  }, [fetchRuns]);

  // Effect for handling dialog state changes
  useEffect(() => {
    if (!isInitializedRef.current) {
      return;
    }

    // Only re-load data if we're closing dialogs (potentially stale data)
    const isClosingTrace = previousTraceOpenRef.current && !showTraceOpen;
    const isClosingLogs = previousLogsDialogRef.current && !showLogsDialog;
    
    if (isClosingTrace || isClosingLogs) {
      console.log('[RunHistory] Dialog closed, refreshing data');
      fetchRuns().catch(err => console.error('[RunHistory] Error refreshing after dialog close:', err));
    }
  }, [showTraceOpen, showLogsDialog, fetchRuns]);

  // Effect for immediate refresh on execution creation or update
  useEffect(() => {
    // Create an event listener for the refreshRunHistory event
    const handleRefreshRunHistory = () => {
      console.log('[RunHistory] Received refreshRunHistory event, fetching latest runs');
      fetchRuns().catch(err => console.error('[RunHistory] Error refreshing on event:', err));
    };

    // Add event listener
    window.addEventListener('refreshRunHistory', handleRefreshRunHistory);

    // Clean up listener on component unmount
    return () => {
      window.removeEventListener('refreshRunHistory', handleRefreshRunHistory);
    };
  }, [fetchRuns]);

  const handleShowTrace = (runId: string) => {
    console.log(`[RunHistory] Showing trace for run ID: ${runId}`);
    setSelectedRunId(runId);
    // Find the run data
    const run = runs.find(r => r.id === runId);
    setSelectedRunForTrace(run || null);
    setShowTraceOpen(true);
  };

  const handleCloseTrace = () => {
    console.log('[RunHistory] Closing trace dialog');
    setShowTraceOpen(false);
    setSelectedRunId(null);
    setSelectedRunForTrace(null);
    fetchRuns().catch(err => console.error('Error refreshing after closing trace:', err));
  };

  const handleShowResult = (run: Run) => {
    showRunResult(run);
  };


  const handleDeleteAllRunsClick = async () => {
    try {
      setDeleteLoading(true);
      await handleDeleteAllRuns();
      setDeleteDialogOpen(false);
    } finally {
      setDeleteLoading(false);
    }
  };

  const handleDeleteRunConfirm = async () => {
    if (runToDelete) {
      try {
        setDeleteLoading(true);
        await handleDeleteRun(runToDelete.id);
        setDeleteRunDialogOpen(false);
        setRunToDelete(null);
      } catch (err) {
        console.error('Error deleting run:', err);
        toast.error(t('runHistory.deleteRunError'));
      } finally {
        setDeleteLoading(false);
      }
    }
  };

  const openDeleteRunDialog = (run: Run) => {
    setRunToDelete(run);
    setDeleteRunDialogOpen(true);
  };

  const handleShowLogs = async (jobId: string) => {
    try {
      // Dispatch event to track this job as viewed
      window.dispatchEvent(new CustomEvent('jobViewed', { detail: { jobId } }));
      
      setIsConnecting(true);
      setConnectionError(null);
      setSelectedJobId(jobId);
      setShowLogsDialog(true);
      
      // Fetch historical logs via REST
      const historicalLogs = await executionLogService.getHistoricalLogs(jobId);
      setSelectedJobLogs(historicalLogs.map(({ job_id, execution_id, ...rest }: LogMessage) => ({
        ...rest,
        output: rest.output || rest.content,
        id: rest.id || Date.now()
      })));

      // Load task states for this execution
      const { loadTaskStates } = useTaskExecutionStore.getState();
      await loadTaskStates(jobId);

      setIsConnecting(false);
    } catch (error) {
      console.error('Error fetching job logs:', error);
      setConnectionError('Failed to fetch logs');
      setIsConnecting(false);
    }
  };

  const handleCloseLogs = () => {
    if (selectedJobId) {
      setSelectedJobId(null);
    }
    // Clear task states when closing dialog
    const { clearTaskStates } = useTaskExecutionStore.getState();
    clearTaskStates();
    setShowLogsDialog(false);
    setSelectedJobLogs([]);
    fetchRuns().catch(err => console.error('Error refreshing after closing logs:', err));
  };

  // No WebSocket cleanup needed — logs are fetched via REST only


  const handleScheduleJob = async () => {
    if (!selectedRunForSchedule || !scheduleName || !cronExpression) {
      toast.error('Please fill in all required fields');
      return;
    }

    // Determine execution type from the run
    const executionType = selectedRunForSchedule.execution_type ||
                         selectedRunForSchedule.inputs?.execution_type ||
                         'crew';

    // Check if this is a flow execution
    const isFlowExecution = executionType === 'flow';

    if (isFlowExecution) {
      // Handle flow execution scheduling
      const flow_id = selectedRunForSchedule.flow_id || selectedRunForSchedule.inputs?.flow_id;
      const nodes = selectedRunForSchedule.inputs?.nodes;
      const edges = selectedRunForSchedule.inputs?.edges;
      const flow_config = selectedRunForSchedule.inputs?.flow_config;

      // Validate flow configuration
      if (!flow_id && !(nodes && edges && nodes.length > 0 && edges.length >= 0)) {
        console.error('CRITICAL: Flow execution missing configuration', {
          executionId: selectedRunForSchedule.id,
          jobId: selectedRunForSchedule.job_id,
          runName: selectedRunForSchedule.run_name,
          hasFlowId: !!flow_id,
          hasNodes: !!nodes,
          hasEdges: !!edges,
          nodesCount: nodes?.length || 0,
          edgesCount: edges?.length || 0
        });

        toast.error('❌ Cannot schedule: This flow execution is missing its flow configuration. Please create a new execution with proper configuration instead.', {
          duration: 10000,
        });
        return;
      }

      try {
        const scheduleData: ScheduleCreateData = {
          name: scheduleName,
          cron_expression: cronExpression,
          execution_type: 'flow',
          flow_id: flow_id,
          nodes: nodes,
          edges: edges,
          flow_config: flow_config || {},
          inputs: selectedRunForSchedule.inputs?.inputs || {},
          is_active: true,
          model: selectedRunForSchedule.inputs?.model,
        };

        await ScheduleService.createSchedule(scheduleData);
        setScheduleDialogOpen(false);
        setSelectedRunForSchedule(null);
        setScheduleName('');
        setCronExpression('0 0 * * *');
        toast.success('Flow schedule created successfully');
      } catch (error) {
        console.error('Error scheduling flow job:', error);
        toast.error('Failed to schedule flow job');
      }
    } else {
      // Handle crew execution scheduling (existing logic)
      let agents_yaml = null;
      let tasks_yaml = null;

      // First try to get from the inputs object (this is where the complete config is stored)
      if (selectedRunForSchedule.inputs?.agents_yaml) {
        agents_yaml = selectedRunForSchedule.inputs.agents_yaml;
      }
      if (selectedRunForSchedule.inputs?.tasks_yaml) {
        tasks_yaml = selectedRunForSchedule.inputs.tasks_yaml;
      }

      // Fallback to direct properties (now properly populated from backend)
      if (!agents_yaml && selectedRunForSchedule.agents_yaml) {
        try {
          agents_yaml = typeof selectedRunForSchedule.agents_yaml === 'string'
            ? JSON.parse(selectedRunForSchedule.agents_yaml)
            : selectedRunForSchedule.agents_yaml;
        } catch (e) {
          console.warn('Failed to parse agents_yaml string:', e);
        }
      }
      if (!tasks_yaml && selectedRunForSchedule.tasks_yaml) {
        try {
          tasks_yaml = typeof selectedRunForSchedule.tasks_yaml === 'string'
            ? JSON.parse(selectedRunForSchedule.tasks_yaml)
            : selectedRunForSchedule.tasks_yaml;
        } catch (e) {
          console.warn('Failed to parse tasks_yaml string:', e);
        }
      }

      // Validate crew configuration
      if (!agents_yaml || !tasks_yaml || Object.keys(agents_yaml).length === 0 || Object.keys(tasks_yaml).length === 0) {
        console.error('CRITICAL: Crew execution missing configuration', {
          executionId: selectedRunForSchedule.id,
          jobId: selectedRunForSchedule.job_id,
          runName: selectedRunForSchedule.run_name,
          hasInputs: !!selectedRunForSchedule.inputs,
          hasAgentsYaml: !!agents_yaml,
          hasTasksYaml: !!tasks_yaml,
          agentsYamlKeys: agents_yaml ? Object.keys(agents_yaml) : [],
          tasksYamlKeys: tasks_yaml ? Object.keys(tasks_yaml) : []
        });

        toast.error('❌ Cannot schedule: This execution is missing its agent and task configuration. Please create a new execution with proper configuration instead.', {
          duration: 10000,
        });
        return;
      }

      try {
        const scheduleData: ScheduleCreateData = {
          name: scheduleName,
          cron_expression: cronExpression,
          execution_type: 'crew',
          agents_yaml: agents_yaml,
          tasks_yaml: tasks_yaml,
          inputs: selectedRunForSchedule.inputs?.inputs || {},
          is_active: true,
          model: selectedRunForSchedule.inputs?.model,
        };

        await ScheduleService.createSchedule(scheduleData);
        setScheduleDialogOpen(false);
        setSelectedRunForSchedule(null);
        setScheduleName('');
        setCronExpression('0 0 * * *');
        toast.success('Crew schedule created successfully');
      } catch (error) {
        console.error('Error scheduling crew job:', error);
        toast.error('Failed to schedule crew job');
      }
    }
  };

  const handleOpenScheduleDialog = (run: Run) => {
    // Reset state before opening
    setCronExpression('0 0 * * *');
    setSelectedRunForSchedule(run);
    setScheduleName(`${
      run.run_name?.startsWith('"') && run.run_name?.endsWith('"') 
        ? run.run_name.slice(1, -1) 
        : run.run_name
    } Schedule`);
    setScheduleDialogOpen(true);
    setTimeout(() => {
      if (scheduleNameInputRef.current) {
        scheduleNameInputRef.current.focus();
      }
    }, 150);
  };

  // Expose refreshRuns method to parent components via ref
  React.useImperativeHandle(ref, () => ({
    refreshRuns: async () => {
      // Wrapper function that maintains Promise<void> return type
      await fetchRuns();
      return;
    }
  }));

  if (showSkeleton) {
    return <ExecutionHistorySkeleton />;
  }


  return (
    <>
      <Card component="section" aria-label={title} sx={{
        boxShadow: 'none', height: '100%', borderRadius: 0, backgroundImage: 'none',
        bgcolor: 'transparent', color: dark ? '#E8ECEF' : '#20262D',
      }}>
        <CardContent sx={{ p: 0, height: '100%', '&:last-child': { pb: 0 }, display: 'flex', flexDirection: 'column' }}>
          {error && <Alert severity="warning">{error}<Button color="inherit" onClick={() => void fetchRuns()}>Retry</Button></Alert>}
          <Box sx={{ px: 1.5, pt: 2, pb: 1, flexShrink: 0 }}>
            {!embedded && <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
              <History size={20} strokeWidth={1.7} />
              <Typography component="h2" sx={{ fontSize: 15, fontWeight: 600, letterSpacing: '-0.025em', flex: 1 }}>{title}</Typography>
              {onClose && <IconButton size="small" aria-label={`Close ${title.toLowerCase()}`} onClick={onClose}><X size={18} /></IconButton>}
            </Box>}
            <Typography sx={{ fontSize: 12, color: 'text.secondary', mb: 2 }}>{jobIds ? 'Executions from this session' : 'All teamspace executions, including scheduled and API runs'}</Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flex: 1, minWidth: 0, bgcolor: dark ? '#292F36' : '#F0F2F5', borderRadius: 2.5, px: 1.25, py: 0.5, '&:focus-within': { boxShadow: dark ? '0 0 0 2px #65717E' : '0 0 0 2px #CDD2D8' } }}>
                <SearchIcon sx={{ fontSize: 18, color: 'text.secondary' }} />
                <TextField fullWidth size="small" variant="standard" placeholder="Search runs" value={searchQuery} onChange={handleSearchChange} onKeyDown={e => e.stopPropagation()}
                  inputProps={{ 'aria-label': 'Search runs' }} InputProps={{ disableUnderline: true }} sx={{ '& .MuiInputBase-root': { fontSize: 13 } }} />
              </Box>
              <IconButton size="small" aria-label="History options" aria-haspopup="menu" aria-expanded={Boolean(anchorEl)} onClick={e => setAnchorEl(e.currentTarget)}><ArrowDownWideNarrow size={19} /></IconButton>
              <Menu anchorEl={anchorEl} open={Boolean(anchorEl)} onClose={() => setAnchorEl(null)} PaperProps={{ sx: { borderRadius: 3, boxShadow: '0 8px 32px rgba(16,24,40,0.14)' } }}>
                <MenuItem selected={sortField === 'created_at'} onClick={() => { handleSort('created_at'); setAnchorEl(null); }} sx={{ fontSize: 13 }}>Sort by date {sortField === 'created_at' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</MenuItem>
                <MenuItem selected={sortField === 'status'} onClick={() => { handleSort('status'); setAnchorEl(null); }} sx={{ fontSize: 13 }}>Sort by status {sortField === 'status' ? (sortOrder === 'desc' ? '↓' : '↑') : ''}</MenuItem>
                <MenuItem onClick={() => { setAnchorEl(null); setRecipesDialogOpen(true); }} sx={{ fontSize: 13, gap: 1 }}><InsightsIcon fontSize="small" />Reuse insights</MenuItem>
                {jobIds === undefined && userRole !== 'operator' && <MenuItem disabled={!runs.length} onClick={() => { setAnchorEl(null); setDeleteDialogOpen(true); }} sx={{ fontSize: 13, gap: 1, color: 'error.main' }}><DeleteIcon fontSize="small" />{t('runHistory.deleteAllRuns')}</MenuItem>}
              </Menu>
            </Box>
          </Box>
          <Box sx={{ px: 1.5, pb: 1, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography sx={{ fontSize: 11, fontWeight: 500, color: 'text.secondary' }}>{runs.length} {runs.length === 1 ? 'run' : 'runs'}</Typography>
            <Typography sx={{ fontSize: 11, color: 'text.secondary' }}>{sortField === 'status' ? 'By status' : sortOrder === 'desc' ? 'Newest first' : 'Oldest first'}</Typography>
          </Box>
          <Box sx={{ flex: '1 1 auto', minHeight: 0, overflowY: 'auto', px: 0.75, pb: 1.5 }}>
            {displayedRuns.length === 0 ? <Box sx={{ px: 2.5, py: 6, textAlign: 'center' }}>
              <Box sx={{ display: 'inline-flex', p: 2, borderRadius: 4, bgcolor: 'action.hover', mb: 2 }}><History size={28} strokeWidth={1.3} /></Box>
              <Typography sx={{ fontSize: 14, fontWeight: 500, mb: 1 }}>{searchQuery ? t('runHistory.noSearchResults') : 'Your work, all in one place'}</Typography>
              <Typography sx={{ fontSize: 13, color: 'text.secondary', lineHeight: 1.6 }}>{searchQuery ? 'Try a different run name.' : 'Run your agents or flow to follow progress and return to the results here.'}</Typography>
            </Box> : <Box component="ul" aria-label="Job runs" sx={{ listStyle: 'none', p: 0, m: 0 }}>
              {displayedRuns.map(run => {
                const name = run.run_name?.replace(/^"|"$/g, '') || run.job_id;
                const expanded = expandedRunId === run.id;
                const toggleDetails = () => setExpandedRunId(expanded ? null : run.id);
                const count = (value: unknown) => { try { return Object.keys(typeof value === 'string' ? JSON.parse(value) : value || {}).length; } catch { return 0; } };
                const agents = count(run.inputs?.agents_yaml || run.agents_yaml);
                const tasks = count(run.inputs?.tasks_yaml || run.tasks_yaml);
                return <Box component="li" key={run.id} sx={{ px: 1, py: 1, mb: 0.5, borderRadius: 2, bgcolor: expanded ? (dark ? '#232930' : '#F0F2F5') : 'transparent', '&:hover': { bgcolor: dark ? '#2A3139' : '#EDF0F4' }, transition: 'background-color 150ms' }}>
                  <Button color="inherit" onClick={toggleDetails} aria-expanded={expanded} aria-label={`Details for ${name}`} title={name}
                    sx={{ p: 0, mb: 0.5, minWidth: 0, width: '100%', display: 'block', textAlign: 'left', fontSize: 12, fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{name}</Button>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 0.25 }}>
                    <ExecutionStatusBadge appearance="soft" showIcon={false} status={run.status} size="small" executionId={run.job_id} onApprovalComplete={() => { fetchRuns(); }} />
                    <RunActions compact run={run} onShowDetails={toggleDetails} onViewResult={handleShowResult} onShowTrace={handleShowTrace} onShowLogs={handleShowLogs} onSchedule={handleOpenScheduleDialog} onDelete={openDeleteRunDialog} onStatusChange={() => { fetchRuns(); }} />
                  </Box>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 0.25, gap: 1 }}>
                    <Typography title={new Date(run.created_at).toLocaleString()} sx={{ fontSize: 10, color: 'text.secondary' }}>{new Date(run.created_at).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })} · {new Date(run.created_at).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })}</Typography>
                    <DurationCell run={run} />
                  </Box>
                  {expanded && <Box role="region" aria-label={`Details for ${name}`} sx={{ pt: 1.5, overflowWrap: 'anywhere' }}>
                    <Typography sx={{ fontSize: 11, color: 'text.secondary', mb: 1 }}>{run.harness === 'crewai' ? 'CrewAI' : 'Kasal'} · {agents} agents · {tasks} tasks</Typography><Typography sx={{ fontSize: 11, color: 'text.secondary' }}>Submitter</Typography><Typography sx={{ fontSize: 12, mb: 1 }}>{run.group_email || '—'}</Typography>
                    <Typography sx={{ fontSize: 11, color: 'text.secondary' }}>Execution ID</Typography><Typography sx={{ fontSize: 12, mb: 1 }}>{run.job_id}</Typography>
                    <ExecutionMemoryButton jobId={run.job_id} /><RecipeCurationButton jobId={run.job_id} />
                  </Box>}
                </Box>;
              })}
            </Box>}
          </Box>
          {totalLocalPages > 1 && <Box sx={{ display: 'flex', justifyContent: 'center', px: 1, py: 1, flexShrink: 0 }}>
            <Pagination count={totalLocalPages} page={localPage} onChange={(_, value) => setLocalPage(value)} color="standard" size="small" />
          </Box>}

          {selectedRunId && (
            <ShowTraceTimeline
              open={showTraceOpen}
              onClose={handleCloseTrace}
              runId={selectedRunId}
              run={selectedRunForTrace || undefined}
              onViewResult={handleShowResult}
              onShowLogs={handleShowLogs}
            />
          )}


          {showLogsDialog && selectedJobId && (
            <ShowLogs
              open={showLogsDialog}
              onClose={handleCloseLogs}
              logs={selectedJobLogs}
              jobId={selectedJobId}
              isConnecting={isConnecting}
              connectionError={connectionError}
            />
          )}

          <RunDialogs
            deleteDialogOpen={deleteDialogOpen}
            deleteLoading={deleteLoading}
            scheduleDialogOpen={scheduleDialogOpen}
            scheduleName={scheduleName}
            cronExpression={cronExpression}
            scheduleNameInputRef={scheduleNameInputRef}
            deleteRunDialogOpen={deleteRunDialogOpen}
            onCloseDeleteDialog={() => setDeleteDialogOpen(false)}
            onCloseScheduleDialog={() => {
              setScheduleDialogOpen(false);
              setScheduleName('');
              setCronExpression('0 0 * * *');
              setSelectedRunForSchedule(null);
            }}
            onCloseDeleteRunDialog={() => setDeleteRunDialogOpen(false)}
            onDeleteAllRuns={handleDeleteAllRunsClick}
            onDeleteRun={handleDeleteRunConfirm}
            onScheduleJob={handleScheduleJob}
            onScheduleNameChange={(e) => setScheduleName(e.target.value)}
            onCronExpressionChange={(e) => setCronExpression(e.target.value)}
          />

          <ShowResult
            open={isOpen && !!selectedRun}
            onClose={closeRunResult}
            result={memoizedResult}
            run={selectedRun || undefined}
          />

          <RecipeEffectivenessDialog
            open={recipesDialogOpen}
            onClose={() => setRecipesDialogOpen(false)}
          />
        </CardContent>
      </Card>
    </>
  );
});

RunHistory.displayName = 'History';

export default RunHistory;