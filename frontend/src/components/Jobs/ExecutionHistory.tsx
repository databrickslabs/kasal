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
  CircularProgress,
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
import { Run, RunService } from '../../api/ExecutionHistoryService';
import { ScheduleService } from '../../api/ScheduleService';
import ShowTrace from './ShowTrace';
import ShowResult from './ShowResult';
import { ResultValue } from '../../types/result';
import ShowLogs from './ShowLogs';
import { executionLogService } from '../../api/ExecutionLogs';
import type { LogMessage, LogEntry } from '../../api/ExecutionLogs';
import { useTranslation } from 'react-i18next';
import { toast } from 'react-hot-toast';
import LoadCrew from '../Crew/LoadCrew';
import { Node, Edge } from 'reactflow';
import { useRunResult } from '../../hooks/global/useExecutionResult';
import { useRunHistory } from '../../hooks/global/useExecutionHistory';
import { useRunStatusStore } from '../../store/runStatus';
import RunActions from './ExecutionActions';
import RunDialogs from './RunDialogs';
import { AgentYaml, TaskYaml } from '../../types/crew';

export interface RunHistoryRef {
  refreshRuns: () => Promise<void>;
}

interface RunHistoryProps {
  onCrewLoad?: (nodes: Node[], edges: Edge[]) => void;
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

const RunHistory = forwardRef<RunHistoryRef, RunHistoryProps>((props, ref) => {
  const { t } = useTranslation();
  const { showRunResult, selectedRun, isOpen, closeRunResult } = useRunResult();
  const {
    runs,
    searchQuery,
    loading,
    error,
    page,
    totalPages,
    totalRuns,
    jobsPerPage,
    sortField,
    sortOrder,
    fetchRuns,
    handlePageChange,
    handleSearchChange,
    handleDeleteAllRuns,
    handleDeleteRun,
    getCurrentPageJobs,
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
  const [selectedRunForLoad, setSelectedRunForLoad] = useState<Run | null>(null);
  const [loadCrewOpen, setLoadCrewOpen] = useState(false);
  const [deleteRunDialogOpen, setDeleteRunDialogOpen] = useState(false);
  const [runToDelete, setRunToDelete] = useState<Run | null>(null);

  // Initialize static refs outside of useEffect  
  const isInitializedRef = useRef<boolean>(false);
  const previousTraceOpenRef = useRef<boolean>(false);
  const previousLogsDialogRef = useRef<boolean>(false);
  const userActivityTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Effect for initializing ref values
  useEffect(() => {
    previousTraceOpenRef.current = showTraceOpen;
    previousLogsDialogRef.current = showLogsDialog;
  }, [showTraceOpen, showLogsDialog]);
  
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

  const handleLoadCrew = (run: Run) => {
    console.log('Loading crew from run:', run);
    console.log('Run inputs:', run.inputs);
    
    // Check if we have valid YAML data
    const hasAgentsYaml = !!(run.agents_yaml && run.agents_yaml.trim());
    const hasTasksYaml = !!(run.tasks_yaml && run.tasks_yaml.trim());
    
    console.log('YAML data check:', {
      agents_yaml_present: hasAgentsYaml,
      tasks_yaml_present: hasTasksYaml,
      agents_yaml_length: run.agents_yaml?.length || 0,
      tasks_yaml_length: run.tasks_yaml?.length || 0
    });
    
    // If we don't have YAML data, try to refresh the run data
    if (!hasAgentsYaml || !hasTasksYaml) {
      console.log('Missing YAML data, attempting to refresh run data');
      
      // Fetch the run data again to make sure we have the latest
      RunService.getInstance().getRunById(run.id)
        .then(refreshedRun => {
          if (refreshedRun) {
            console.log('Refreshed run data:', {
              agents_yaml_present: !!refreshedRun.agents_yaml,
              tasks_yaml_present: !!refreshedRun.tasks_yaml
            });
            
            // Now we can use the refreshed data
            setSelectedRunForLoad(refreshedRun);
            setLoadCrewOpen(true);
          } else {
            console.error('Failed to refresh run data');
            toast.error('Failed to load crew configuration');
          }
        })
        .catch(err => {
          console.error('Error refreshing run data:', err);
          toast.error('Failed to load crew configuration');
        });
    } else {
      // We already have the YAML data, proceed normally
      setSelectedRunForLoad(run);
      setLoadCrewOpen(true);
    }
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

    if (!selectedRunForSchedule.inputs?.agents_yaml || !selectedRunForSchedule.inputs?.tasks_yaml) {
      toast.error('Invalid job configuration');
      return;
    }

    try {
      const scheduleData: ScheduleCreateData = {
        name: scheduleName,
        cron_expression: cronExpression,
        agents_yaml: JSON.parse(selectedRunForSchedule.inputs?.agents_yaml || '{}'),
        tasks_yaml: JSON.parse(selectedRunForSchedule.inputs?.tasks_yaml || '{}'),
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

  const handleCrewLoaded = (nodes: Node[], edges: Edge[]) => {
    if (props.onCrewLoad) {
      props.onCrewLoad(nodes, edges);
    }
    setLoadCrewOpen(false);
    setSelectedRunForLoad(null);
  };

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

  if (loading && !runs.length) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
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
        <CardContent sx={{ p: 0, height: '100%', '&:last-child': { pb: 0 } }}>
          <TableContainer sx={{ maxHeight: '130px' }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell sx={{ py: 0.5 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {t('runHistory.columns.jobId')}
                      <Tooltip title={t('runHistory.filter')}>
                        <IconButton 
                          size="small" 
                          onClick={handleFilterClick}
                          sx={{ 
                            p: 0.5,
                            color: searchQuery ? (theme: Theme) => theme.palette.primary.main : 'inherit'
                          }}
                          aria-describedby={filterId}
                        >
                          <FilterListIcon fontSize="small" />
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
                    sx={{ py: 0.5, cursor: 'pointer' }}
                    onClick={() => handleSort('status')}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      {t('runHistory.columns.status')}
                      {renderSortIcon('status')}
                    </Box>
                  </TableCell>
                  <TableCell 
                    sx={{ py: 0.5, cursor: 'pointer' }}
                    onClick={() => handleSort('created_at')}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      {t('runHistory.columns.startTime')}
                      {renderSortIcon('created_at')}
                    </Box>
                  </TableCell>
                  <TableCell sx={{ py: 0.5, width: '240px' }}>
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
                            height: '28px', 
                            width: '28px',
                            opacity: 0,
                            visibility: 'hidden',
                            transition: 'opacity 0.2s ease-in-out, visibility 0.2s ease-in-out',
                            '&.Mui-disabled': {
                              opacity: 0,
                              visibility: 'hidden'
                            }
                          }}
                        >
                          <DeleteIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {getCurrentPageJobs().length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={4} align="center" sx={{ py: 0.5 }}>
                      {searchQuery ? t('runHistory.noSearchResults') : t('runHistory.noRuns')}
                    </TableCell>
                  </TableRow>
                ) : (
                  getCurrentPageJobs().map((run) => (
                    <TableRow 
                      key={`${run.id}-${run.status}`} 
                      sx={{ 
                        transition: 'all 0.2s ease-in-out',
                        '&:hover': {
                          backgroundColor: (theme) => theme.palette.action.hover
                        }
                      }}
                    >
                      <TableCell sx={{ py: 0.5 }}>{
                        run.run_name?.startsWith('"') && run.run_name?.endsWith('"') 
                          ? run.run_name.slice(1, -1) 
                          : run.run_name
                      }</TableCell>
                      <TableCell sx={{ py: 0.5 }}>
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
                            }
                          }}
                        />
                      </TableCell>
                      <TableCell sx={{ py: 0.5 }}>{new Date(run.created_at).toLocaleString()}</TableCell>
                      <TableCell sx={{ py: 0.5 }}>
                        <RunActions
                          run={run}
                          onViewResult={handleShowResult}
                          onLoadCrew={handleLoadCrew}
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

          {totalRuns > jobsPerPage && (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 0.5 }}>
              <Pagination
                count={totalPages}
                page={page}
                onChange={handlePageChange}
                color="primary"
                size="small"
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

          {selectedRunForLoad && selectedRunForLoad.inputs && (() => {
            console.log('About to render LoadCrew with selectedRunForLoad:', selectedRunForLoad);
            console.log('YAML inputs present:', {
              agents_yaml: !!selectedRunForLoad.inputs.agents_yaml,
              tasks_yaml: !!selectedRunForLoad.inputs.tasks_yaml,
            });
            return (
              <LoadCrew
                open={loadCrewOpen}
                onClose={() => {
                  setLoadCrewOpen(false);
                  setSelectedRunForLoad(null);
                }}
                onCrewLoad={handleCrewLoaded}
                inputs={{
                  agents_yaml: typeof selectedRunForLoad.inputs.agents_yaml === 'string'
                    ? selectedRunForLoad.inputs.agents_yaml
                    : JSON.stringify(selectedRunForLoad.inputs.agents_yaml, null, 2),
                  tasks_yaml: typeof selectedRunForLoad.inputs.tasks_yaml === 'string'
                    ? selectedRunForLoad.inputs.tasks_yaml
                    : JSON.stringify(selectedRunForLoad.inputs.tasks_yaml, null, 2)
                }}
                runName={
                  selectedRunForLoad.run_name?.startsWith('"') && selectedRunForLoad.run_name?.endsWith('"') 
                    ? selectedRunForLoad.run_name.slice(1, -1) 
                    : selectedRunForLoad.run_name
                }
              />
            );
          })()}

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