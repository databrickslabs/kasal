import React, { useState, useEffect, forwardRef, useRef } from 'react';
import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  Card,
  CardContent,
  Alert,
  IconButton,
  Tooltip,
  TextField,
  InputAdornment,
  Pagination,
  Popover,
} from '@mui/material';
import { Theme } from '@mui/material/styles';
import DeleteIcon from '@mui/icons-material/Delete';
import SearchIcon from '@mui/icons-material/Search';
import FilterListIcon from '@mui/icons-material/FilterList';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import { Run } from '../../api/ExecutionHistoryService';
import { ScheduleService } from '../../api/ScheduleService';
import ShowTrace from './ShowTrace';
import ShowResult from './ShowResult';
import { ResultValue } from '../../types/result';
import ShowLogs from './ShowLogs';
import { executionLogService } from '../../api/ExecutionLogs';
import type { LogMessage, LogEntry } from '../../api/ExecutionLogs';
import { useTranslation } from 'react-i18next';
import { toast } from 'react-hot-toast';
import { useRunResult } from '../../hooks/global/useExecutionResult';
import { useRunHistory } from '../../hooks/global/useExecutionHistory';
import { useRunStatusStore } from '../../store/runStatus';
import RunActions from './ExecutionActions';
import RunDialogs from './RunDialogs';
import { AgentYaml, TaskYaml } from '../../types/crew';

export interface RunHistoryRef {
  refreshRuns: () => Promise<void>;
}

interface ScheduleCreateData {
  name: string;
  cron_expression: string;
  agents_yaml: Record<string, AgentYaml>;
  tasks_yaml: Record<string, TaskYaml>;
  inputs: Record<string, unknown>;
  is_active: boolean;
  planning: boolean;
}

interface RunHistoryProps {
  executionHistoryHeight?: number;
  onExecutionCountChange?: (count: number) => void;
}

const RunHistory = forwardRef<RunHistoryRef, RunHistoryProps>(({ executionHistoryHeight = 200, onExecutionCountChange }, ref) => {
  const { t } = useTranslation();
  const { showRunResult, selectedRun, isOpen, closeRunResult } = useRunResult();
  const {
    runs,
    searchQuery,
    loading,
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
  } = useRunHistory();

  const {
    startPolling,
    stopPolling,
    setUserActive,
    cleanup: cleanupStore
  } = useRunStatusStore();

  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [showTraceOpen, setShowTraceOpen] = useState<boolean>(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
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
  const searchInputRef = useRef<HTMLInputElement>(null);
  const scheduleNameInputRef = useRef<HTMLInputElement>(null);
  const [deleteRunDialogOpen, setDeleteRunDialogOpen] = useState(false);
  const [runToDelete, setRunToDelete] = useState<Run | null>(null);
  const [localPage, setLocalPage] = useState(1);

  // Initialize static refs outside of useEffect  
  const isInitializedRef = useRef<boolean>(false);
  const previousTraceOpenRef = useRef<boolean>(false);
  const previousLogsDialogRef = useRef<boolean>(false);
  const userActivityTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Calculate items per page based on execution history height
  // Each row is approximately 32px, header is ~40px, pagination is ~40px
  const itemsPerPage = React.useMemo(() => {
    const availableHeight = executionHistoryHeight - 80; // Subtract header and pagination
    return Math.max(6, Math.floor(availableHeight / 32)); // At least 6 items
  }, [executionHistoryHeight]);
  const startIndex = (localPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const displayedRuns = runs.slice(startIndex, endIndex);
  const totalLocalPages = Math.ceil(runs.length / itemsPerPage);

  // Effect for initializing ref values
  useEffect(() => {
    previousTraceOpenRef.current = showTraceOpen;
    previousLogsDialogRef.current = showLogsDialog;
  }, [showTraceOpen, showLogsDialog]);
  
  // Reset local page when runs change or search query changes
  useEffect(() => {
    setLocalPage(1);
  }, [runs.length, searchQuery]);

  // Notify parent of execution count changes
  useEffect(() => {
    if (onExecutionCountChange) {
      onExecutionCountChange(runs.length);
    }
  }, [runs.length, onExecutionCountChange]);
  
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

      // Track user activity
      const handleUserActivity = () => {
        setUserActive(true);
        if (userActivityTimeoutRef.current) {
          clearTimeout(userActivityTimeoutRef.current);
        }
        
        // Set user as inactive after 5 minutes of no activity
        userActivityTimeoutRef.current = setTimeout(() => {
          setUserActive(false);
        }, 5 * 60 * 1000);
      };
      
      // Set up event listeners for user activity
      window.addEventListener('mousemove', handleUserActivity);
      window.addEventListener('keydown', handleUserActivity);
      window.addEventListener('click', handleUserActivity);
      
      // Initialize the activity timeout
      handleUserActivity();
      
      // Start polling
      startPolling();

      // Return cleanup function
      return () => {
        if (userActivityTimeoutRef.current) {
          clearTimeout(userActivityTimeoutRef.current);
        }
        
        window.removeEventListener('mousemove', handleUserActivity);
        window.removeEventListener('keydown', handleUserActivity);
        window.removeEventListener('click', handleUserActivity);
        
        stopPolling();
        cleanupStore();
      };
    };

    // Store cleanup function
    const cleanup = initializeAndSetup();

    // Return cleanup function
    return () => {
      console.log('=== DEBUG: RunHistory useEffect cleanup running ===');
      cleanup.then(cleanupFn => cleanupFn());
    };
  }, [fetchRuns, startPolling, stopPolling, setUserActive, cleanupStore]);

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
    setShowTraceOpen(true);
  };

  const handleCloseTrace = () => {
    console.log('[RunHistory] Closing trace dialog');
    setShowTraceOpen(false);
    setSelectedRunId(null);
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
      setIsConnecting(true);
      setConnectionError(null);
      setSelectedJobId(jobId);
      setShowLogsDialog(true);
      
      // Fetch historical logs and connect to WebSocket
      const historicalLogs = await executionLogService.getHistoricalLogs(jobId);
      setSelectedJobLogs(historicalLogs.map(({ job_id, execution_id, ...rest }: LogMessage) => ({
        ...rest,
        // Ensure we handle both content and output fields
        output: rest.output || rest.content,
        id: rest.id || Date.now()
      })));
      
      executionLogService.connectToJobLogs(jobId);
      
      const unsubscribeConnect = executionLogService.onConnected(jobId, () => {
        setIsConnecting(false);
        console.log('Connected to WebSocket for job logs:', jobId);
      });
      
      const unsubscribeLogs = executionLogService.onJobLogs(jobId, (logMessage: LogMessage) => {
        setSelectedJobLogs(prevLogs => [...prevLogs, {
          id: logMessage.id || Date.now(),
          output: logMessage.output || logMessage.content,
          timestamp: logMessage.timestamp
        }]);
      });
      
      const unsubscribeError = executionLogService.onError(jobId, (error: Event | Error) => {
        console.error('WebSocket error:', error);
        setConnectionError('Failed to connect to log stream');
        setIsConnecting(false);
      });
      
      const unsubscribeClose = executionLogService.onClose(jobId, (event: CloseEvent) => {
        console.log('WebSocket closed:', event);
        setIsConnecting(false);
      });
      
      // Store the unsubscribe functions to be called on cleanup
      return () => {
        unsubscribeConnect();
        unsubscribeLogs();
        unsubscribeError();
        unsubscribeClose();
        executionLogService.disconnectFromJobLogs(jobId);
      };
    } catch (error) {
      console.error('Error setting up job logs:', error);
      setConnectionError('Failed to fetch logs');
      setIsConnecting(false);
      return () => {
        console.log('Log connection cleanup with no active subscriptions');
      };
    }
  };

  const handleCloseLogs = () => {
    if (selectedJobId) {
      executionLogService.disconnectFromJobLogs(selectedJobId);
      setSelectedJobId(null);
    }
    setShowLogsDialog(false);
    setSelectedJobLogs([]);
    fetchRuns().catch(err => console.error('Error refreshing after closing logs:', err));
  };

  useEffect(() => {
    return () => {
      executionLogService.cleanup();
    };
  }, []);

  const handleScheduleJob = async () => {
    if (!selectedRunForSchedule || !scheduleName || !cronExpression) {
      toast.error('Please fill in all required fields');
      return;
    }

    // Extract complete YAML configuration data from execution inputs
    let agents_yaml = null;
    let tasks_yaml = null;
    
    // First try to get from the inputs object (this is where the complete config is stored)
    if (selectedRunForSchedule.inputs?.agents_yaml) {
      agents_yaml = selectedRunForSchedule.inputs.agents_yaml;
    }
    if (selectedRunForSchedule.inputs?.tasks_yaml) {
      tasks_yaml = selectedRunForSchedule.inputs.tasks_yaml;
    }
    
    // Fallback to direct properties (though these are usually strings or empty)
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

    let finalAgentsYaml = '';
    let finalTasksYaml = '';
    
    if (agents_yaml && tasks_yaml && Object.keys(agents_yaml).length > 0 && Object.keys(tasks_yaml).length > 0) {
      // We have complete configuration from the execution
      finalAgentsYaml = JSON.stringify(agents_yaml);
      finalTasksYaml = JSON.stringify(tasks_yaml);
      toast.success('Using complete agent and task configuration from execution');
    } else {
      // Create a minimal dummy configuration as fallback
      const executionName = selectedRunForSchedule.run_name || 'Scheduled Task';
      
      finalAgentsYaml = JSON.stringify({
        "research_agent": {
          "role": "Research Specialist",
          "goal": `Execute ${executionName}`,
          "backstory": `You are a specialist responsible for executing the scheduled task: ${executionName}`,
          "tools": [],
          "llm": "gpt-4o-mini"
        }
      });
      
      finalTasksYaml = JSON.stringify({
        "main_task": {
          "description": `Execute the scheduled task: ${executionName}`,
          "agent": "research_agent",
          "expected_output": "Task execution completed successfully"
        }
      });
      
      toast('⚠️ Using dummy configuration. The selected execution does not contain complete agent/task configuration.', {
        duration: 5000,
        style: {
          background: '#f59e0b',
          color: 'white',
        },
      });
    }

    try {
      const scheduleData: ScheduleCreateData = {
        name: scheduleName,
        cron_expression: cronExpression,
        agents_yaml: JSON.parse(finalAgentsYaml || '{}'),
        tasks_yaml: JSON.parse(finalTasksYaml || '{}'),
        inputs: {},
        is_active: true,
        planning: false,
      };

      await ScheduleService.createSchedule(scheduleData);
      toast.success('Job scheduled successfully');
      setScheduleDialogOpen(false);
      setSelectedRunForSchedule(null);
      setScheduleName('');
      setCronExpression('0 0 * * *');
    } catch (error) {
      console.error('Error scheduling job:', error);
      toast.error('Failed to schedule job');
    }
  };

  const handleOpenScheduleDialog = (run: Run) => {
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

  const handleFilterClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
    setTimeout(() => {
      if (searchInputRef.current) {
        searchInputRef.current.focus();
      }
    }, 150);
  };
  
  const handleFilterClose = () => {
    setAnchorEl(null);
  };
  
  const open = Boolean(anchorEl);
  const filterId = open ? 'filter-popover' : undefined;


  const renderSortIcon = (field: 'status' | 'created_at') => {
    if (sortField !== field) return null;
    return sortOrder === 'asc' ? <ArrowUpwardIcon fontSize="small" /> : <ArrowDownwardIcon fontSize="small" />;
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
    return (
      <Card sx={{ boxShadow: 'none', height: '100%' }}>
        <CardContent sx={{ p: 0, height: '100%', '&:last-child': { pb: 0 }, display: 'flex', flexDirection: 'column' }}>
          <TableContainer sx={{ flex: '1 1 auto', overflow: 'auto' }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', backgroundColor: theme => theme.palette.background.paper }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {t('jobs.runName')}
                    </Box>
                  </TableCell>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', backgroundColor: theme => theme.palette.background.paper }}>
                    {t('jobs.status')}
                  </TableCell>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', backgroundColor: theme => theme.palette.background.paper }}>
                    {t('jobs.duration')}
                  </TableCell>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', backgroundColor: theme => theme.palette.background.paper }}>
                    {t('jobs.date')}
                  </TableCell>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', backgroundColor: theme => theme.palette.background.paper, textAlign: 'center' }}>
                    {t('jobs.actions')}
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {/* Skeleton loading rows */}
                {Array.from({ length: 3 }, (_, index) => (
                  <TableRow key={`skeleton-${index}`}>
                    <TableCell sx={{ py: 0.25, fontSize: '0.75rem' }}>
                      <Box 
                        sx={{ 
                          height: '1rem', 
                          backgroundColor: theme => theme.palette.action.hover,
                          borderRadius: '4px',
                          animation: 'pulse 1.5s ease-in-out infinite',
                          '@keyframes pulse': {
                            '0%': { opacity: 1 },
                            '50%': { opacity: 0.4 },
                            '100%': { opacity: 1 }
                          }
                        }} 
                      />
                    </TableCell>
                    <TableCell sx={{ py: 0.25, fontSize: '0.75rem' }}>
                      <Box 
                        sx={{ 
                          height: '1.5rem', 
                          width: '60px',
                          backgroundColor: theme => theme.palette.action.hover,
                          borderRadius: '12px',
                          animation: 'pulse 1.5s ease-in-out infinite',
                          '@keyframes pulse': {
                            '0%': { opacity: 1 },
                            '50%': { opacity: 0.4 },
                            '100%': { opacity: 1 }
                          }
                        }} 
                      />
                    </TableCell>
                    <TableCell sx={{ py: 0.25, fontSize: '0.75rem' }}>
                      <Box 
                        sx={{ 
                          height: '1rem', 
                          width: '40px',
                          backgroundColor: theme => theme.palette.action.hover,
                          borderRadius: '4px',
                          animation: 'pulse 1.5s ease-in-out infinite',
                          '@keyframes pulse': {
                            '0%': { opacity: 1 },
                            '50%': { opacity: 0.4 },
                            '100%': { opacity: 1 }
                          }
                        }} 
                      />
                    </TableCell>
                    <TableCell sx={{ py: 0.25, fontSize: '0.75rem' }}>
                      <Box 
                        sx={{ 
                          height: '1rem', 
                          width: '80px',
                          backgroundColor: theme => theme.palette.action.hover,
                          borderRadius: '4px',
                          animation: 'pulse 1.5s ease-in-out infinite',
                          '@keyframes pulse': {
                            '0%': { opacity: 1 },
                            '50%': { opacity: 0.4 },
                            '100%': { opacity: 1 }
                          }
                        }} 
                      />
                    </TableCell>
                    <TableCell sx={{ py: 0.25, fontSize: '0.75rem', textAlign: 'center' }}>
                      <Box 
                        sx={{ 
                          height: '1.5rem', 
                          width: '60px',
                          backgroundColor: theme => theme.palette.action.hover,
                          borderRadius: '4px',
                          animation: 'pulse 1.5s ease-in-out infinite',
                          '@keyframes pulse': {
                            '0%': { opacity: 1 },
                            '50%': { opacity: 0.4 },
                            '100%': { opacity: 1 }
                          },
                          margin: '0 auto'
                        }} 
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Alert severity="error" sx={{ mb: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <>
      <Card sx={{ boxShadow: 'none', height: '100%' }}>
        <CardContent sx={{ p: 0, height: '100%', '&:last-child': { pb: 0 }, display: 'flex', flexDirection: 'column' }}>
          <TableContainer sx={{ flex: '1 1 auto', overflow: 'auto' }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', backgroundColor: theme => theme.palette.background.paper }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {t('runHistory.columns.jobId')}
                      <Tooltip title={t('runHistory.filter')}>
                        <IconButton 
                          size="small" 
                          onClick={handleFilterClick}
                          sx={{ 
                            p: 0.25,
                            color: searchQuery ? (theme: Theme) => theme.palette.primary.main : 'inherit'
                          }}
                          aria-describedby={filterId}
                        >
                          <FilterListIcon sx={{ fontSize: '1rem' }} />
                        </IconButton>
                      </Tooltip>
                      <Popover
                        id={filterId}
                        open={open}
                        anchorEl={anchorEl}
                        onClose={handleFilterClose}
                        anchorOrigin={{
                          vertical: 'bottom',
                          horizontal: 'left',
                        }}
                      >
                        <Box sx={{ p: 1.5 }}>
                          <TextField
                            inputRef={searchInputRef}
                            size="small"
                            placeholder={t('runHistory.search')}
                            variant="outlined"
                            value={searchQuery}
                            onChange={handleSearchChange}
                            InputProps={{
                              startAdornment: (
                                <InputAdornment position="start">
                                  <SearchIcon fontSize="small" />
                                </InputAdornment>
                              ),
                            }}
                            sx={{ width: '200px' }}
                          />
                        </Box>
                      </Popover>
                    </Box>
                  </TableCell>
                  <TableCell 
                    sx={{ py: 0.25, fontSize: '0.8125rem', cursor: 'pointer', backgroundColor: theme => theme.palette.background.paper }}
                    onClick={() => handleSort('status')}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      {t('runHistory.columns.status')}
                      {renderSortIcon('status')}
                    </Box>
                  </TableCell>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', backgroundColor: theme => theme.palette.background.paper }}>
                    Submitter
                  </TableCell>
                  <TableCell 
                    sx={{ py: 0.25, fontSize: '0.8125rem', cursor: 'pointer', backgroundColor: theme => theme.palette.background.paper }}
                    onClick={() => handleSort('created_at')}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      {t('runHistory.columns.startTime')}
                      {renderSortIcon('created_at')}
                    </Box>
                  </TableCell>
                  <TableCell sx={{ py: 0.25, fontSize: '0.8125rem', width: '240px', backgroundColor: theme => theme.palette.background.paper }}>
                    <Box sx={{ 
                      display: 'flex', 
                      justifyContent: 'space-between', 
                      alignItems: 'center',
                      position: 'relative',
                      '&:hover .delete-all-button': {
                        opacity: 1,
                        visibility: 'visible'
                      }
                    }}>
                      <Box>{t('runHistory.columns.actions')}</Box>
                      <Tooltip title={t('runHistory.deleteAllRuns')}>
                        <IconButton
                          className="delete-all-button"
                          size="small"
                          color="error"
                          onClick={() => setDeleteDialogOpen(true)}
                          disabled={runs.length === 0}
                          sx={{ 
                            height: '20px', 
                            width: '20px',
                            p: 0.25,
                            opacity: 0,
                            visibility: 'hidden',
                            transition: 'opacity 0.2s ease-in-out, visibility 0.2s ease-in-out',
                            '&.Mui-disabled': {
                              opacity: 0,
                              visibility: 'hidden'
                            }
                          }}
                        >
                          <DeleteIcon sx={{ fontSize: '0.875rem' }} />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {displayedRuns.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} align="center" sx={{ py: 1, fontSize: '0.8125rem' }}>
                      {searchQuery ? t('runHistory.noSearchResults') : t('runHistory.noRuns')}
                    </TableCell>
                  </TableRow>
                ) : (
                  displayedRuns.map((run) => (
                    <TableRow 
                      key={`${run.id}-${run.status}`} 
                      sx={{ 
                        transition: 'all 0.2s ease-in-out',
                        '&:hover': {
                          backgroundColor: (theme) => theme.palette.action.hover
                        },
                        '& td': { py: 0.25, fontSize: '0.8125rem' }
                      }}
                    >
                      <TableCell>{
                        run.run_name?.startsWith('"') && run.run_name?.endsWith('"') 
                          ? run.run_name.slice(1, -1) 
                          : run.run_name
                      }</TableCell>
                      <TableCell>
                        <Chip
                          label={t(`runHistory.status.${run.status.toLowerCase()}`)}
                          color={
                            run.status === 'completed'
                              ? 'success'
                              : run.status === 'failed'
                                ? 'error'
                                : run.status === 'running'
                                  ? 'primary'
                                  : run.status === 'queued' || run.status === 'pending'
                                    ? 'warning'
                                    : 'default'
                          }
                          size="small"
                          sx={{ 
                            transition: 'all 0.2s ease-in-out',
                            opacity: loading ? 0.7 : 1,
                            animation: (run.status === 'running' || run.status === 'pending' || run.status === 'queued') 
                              ? 'pulse 2s infinite' : 'none',
                            '@keyframes pulse': {
                              '0%': { opacity: 1 },
                              '50%': { opacity: 0.6 },
                              '100%': { opacity: 1 }
                            },
                            height: '18px',
                            '& .MuiChip-label': { px: 0.75, fontSize: '0.7rem' }
                          }}
                        />
                      </TableCell>
                      <TableCell>{run.group_email || '-'}</TableCell>
                      <TableCell>{new Date(run.created_at).toLocaleString()}</TableCell>
                      <TableCell>
                        <RunActions
                          run={run}
                          onViewResult={handleShowResult}
                          onShowTrace={handleShowTrace}
                          onShowLogs={handleShowLogs}
                          onSchedule={handleOpenScheduleDialog}
                          onDelete={openDeleteRunDialog}
                        />
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>

          {totalLocalPages > 1 && (
            <Box sx={{ 
              display: 'flex', 
              justifyContent: 'center', 
              py: 0.25, 
              borderTop: 1, 
              borderColor: 'divider',
              flex: '0 0 auto'
            }}>
              <Pagination
                count={totalLocalPages}
                page={localPage}
                onChange={(_, value) => setLocalPage(value)}
                color="primary"
                size="small"
                sx={{ '& .MuiPaginationItem-root': { minWidth: '20px', height: '20px', fontSize: '0.7rem' } }}
              />
            </Box>
          )}

          {selectedRunId && (
            <ShowTrace
              open={showTraceOpen}
              onClose={handleCloseTrace}
              runId={selectedRunId}
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
            onCloseScheduleDialog={() => setScheduleDialogOpen(false)}
            onCloseDeleteRunDialog={() => setDeleteRunDialogOpen(false)}
            onDeleteAllRuns={handleDeleteAllRunsClick}
            onDeleteRun={handleDeleteRunConfirm}
            onScheduleJob={handleScheduleJob}
            onScheduleNameChange={(e) => setScheduleName(e.target.value)}
            onCronExpressionChange={(e) => setCronExpression(e.target.value)}
          />

          {isOpen && selectedRun && (
            <ShowResult
              open={isOpen}
              onClose={closeRunResult}
              result={selectedRun.result as Record<string, ResultValue>}
            />
          )}
        </CardContent>
      </Card>
    </>
  );
});

RunHistory.displayName = 'History';

export default RunHistory;