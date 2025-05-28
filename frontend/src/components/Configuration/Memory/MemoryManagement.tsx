import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Paper,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Button,
  TextField,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  Chip,
  CircularProgress,
  Alert,
  Grid,
  Tooltip,
} from '@mui/material';
import {
  Delete as DeleteIcon,
  Search as SearchIcon,
  Refresh as RefreshIcon,
  Storage as StorageIcon,
  MemoryRounded as MemoryIcon,
  BarChart as StatsIcon,
  CleaningServices as CleanupIcon,
  Info as InfoIcon,
} from '@mui/icons-material';
import { useTranslation } from 'react-i18next';
import { MemoryService } from '../../../api/MemoryService';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

interface MemoryDetails {
  memory_path: string;
  size_bytes: number;
  creation_date: string;
  last_modified: string;
  long_term_memory?: {
    path: string;
    size_bytes: number;
    tables?: string[];
    records?: Array<{
      timestamp: string;
      content: string;
    }>;
  };
  short_term_memory?: {
    messages?: Array<{
      role: string;
      content: string;
    }>;
  };
}

interface MemoryStats {
  total_crews: number;
  total_size: number;
  avg_size: number;
  oldest_memory: string | {crew: string; timestamp: string};
  newest_memory?: string;
  crew_details?: Record<string, CrewDetail>;
}

interface CrewDetail {
  size: number;
  last_modified: string | { timestamp: string };
  messages_count?: number;
}

interface SearchResult {
  crew_name: string;
  snippet: string;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`memory-tabpanel-${index}`}
      aria-labelledby={`memory-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

function a11yProps(index: number) {
  return {
    id: `memory-tab-${index}`,
    'aria-controls': `memory-tabpanel-${index}`,
  };
}

const MemoryManagement: React.FC = () => {
  const { t } = useTranslation();
  const [tabValue, setTabValue] = useState(0);
  const [memories, setMemories] = useState<string[]>([]);
  const [selectedMemory, setSelectedMemory] = useState<string | null>(null);
  const [memoryDetails, setMemoryDetails] = useState<MemoryDetails | null>(null);
  const [memoryStats, setMemoryStats] = useState<MemoryStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirmDialogOpen, setConfirmDialogOpen] = useState(false);
  const [confirmAction, setConfirmAction] = useState<'reset' | 'resetAll' | 'delete' | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [cleanupDays, setCleanupDays] = useState(30);
  const [customPath, setCustomPath] = useState<string>('');
  const [pathDialogOpen, setPathDialogOpen] = useState(false);
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [memorySizes, setMemorySizes] = useState<Record<string, number>>({});

  const memoryService = MemoryService.getInstance();

  useEffect(() => {
    // Initialize customPath from MemoryService
    const savedPath = memoryService.getMemoryPath();
    if (savedPath) {
      setCustomPath(savedPath);
    }
  }, [memoryService]);

  const handlePathChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setCustomPath(event.target.value);
  };

  const handleSavePath = () => {
    memoryService.setMemoryPath(customPath.trim() || null);
    setPathDialogOpen(false);
    fetchMemories(); // Reload memories with new path
  };

  const openPathDialog = () => {
    setPathDialogOpen(true);
  };

  const handleClosePathDialog = () => {
    // Reset to current path if canceled
    setCustomPath(memoryService.getMemoryPath() || '');
    setPathDialogOpen(false);
  };

  const fetchMemories = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await memoryService.listMemories();
      
      // Ensure that data is always an array
      let data: string[] = [];
      if (Array.isArray(response)) {
        data = response;
      } else if (response && typeof response === 'object') {
        // Try to find an array in the response
        const responseObj = response as Record<string, unknown>;
        if ('data' in responseObj && Array.isArray(responseObj.data)) {
          data = responseObj.data as string[];
        }
      }
      
      setMemories(data);
      if (data.length > 0) {
        setSelectedMemory(prev => prev || data[0]);
        
        // First try to get sizes from memory stats
        try {
          const stats = await memoryService.getMemoryStats(true);
          if (stats && stats.crew_details) {
            const sizes: Record<string, number> = {};
            Object.entries(stats.crew_details).forEach(([crew, details]) => {
              sizes[crew] = details.size * 1024 || 0; // Convert KB to bytes
            });
            setMemorySizes(sizes);
            
            // If we have missing sizes, fetch individual details for each memory
            const missingSizes = data.filter(memory => !sizes[memory] || sizes[memory] === 0);
            if (missingSizes.length > 0) {
              // Fetch details for memories with missing sizes
              await Promise.all(missingSizes.map(async (memory) => {
                try {
                  const details = await memoryService.getMemoryDetails(memory);
                  if (details && details.size_bytes) {
                    setMemorySizes(prev => ({
                      ...prev,
                      [memory]: details.size_bytes
                    }));
                  }
                } catch (error) {
                  console.error(`Error fetching details for memory ${memory}:`, error);
                }
              }));
            }
          } else {
            // If stats don't have crew details, fetch details for each memory
            await Promise.all(data.map(async (memory) => {
              try {
                const details = await memoryService.getMemoryDetails(memory);
                if (details && details.size_bytes) {
                  setMemorySizes(prev => ({
                    ...prev,
                    [memory]: details.size_bytes
                  }));
                }
              } catch (error) {
                console.error(`Error fetching details for memory ${memory}:`, error);
              }
            }));
          }
        } catch (statsError) {
          console.error("Error fetching memory stats:", statsError);
          
          // Fallback: fetch details for each memory
          await Promise.all(data.map(async (memory) => {
            try {
              const details = await memoryService.getMemoryDetails(memory);
              if (details && details.size_bytes) {
                setMemorySizes(prev => ({
                  ...prev,
                  [memory]: details.size_bytes
                }));
              }
            } catch (error) {
              console.error(`Error fetching details for memory ${memory}:`, error);
            }
          }));
        }
      }
    } catch (err) {
      setError('Failed to fetch memories');
      console.error(err);
      setMemories([]); // Set to empty array in case of error
    } finally {
      setLoading(false);
    }
  }, [memoryService]);

  const fetchMemoryDetails = useCallback(async (crewName: string) => {
    setLoading(true);
    setError(null);
    try {
      const data = await memoryService.getMemoryDetails(crewName);
      setMemoryDetails(data);
      
      // Update the size in memorySizes state to match the detailed view
      if (data && data.size_bytes) {
        setMemorySizes(prev => ({
          ...prev,
          [crewName]: data.size_bytes
        }));
      }
    } catch (err) {
      setError(`Failed to fetch details for memory "${crewName}"`);
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [memoryService]);

  const fetchMemoryStats = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await memoryService.getMemoryStats(true);
      setMemoryStats(data);
    } catch (err) {
      setError('Failed to fetch memory statistics');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [memoryService]);

  // Load memories when component mounts
  useEffect(() => {
    fetchMemories();
  }, [fetchMemories]);

  // Fetch memory data when tab changes or selection changes
  useEffect(() => {
    if (detailsDialogOpen && selectedMemory) {
      fetchMemoryDetails(selectedMemory);
    } else if (tabValue === 1) {
      fetchMemoryStats();
    }
  }, [tabValue, selectedMemory, detailsDialogOpen, fetchMemoryDetails, fetchMemoryStats]);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const handleMemorySelect = (crewName: string) => {
    setSelectedMemory(crewName);
  };

  const handleResetMemory = async () => {
    if (!selectedMemory) return;

    setLoading(true);
    setError(null);
    try {
      await memoryService.resetMemory(selectedMemory);
      setConfirmDialogOpen(false);
      fetchMemories();
    } catch (err) {
      setError(`Failed to reset memory "${selectedMemory}"`);
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteMemory = async () => {
    if (!selectedMemory) return;

    setLoading(true);
    setError(null);
    try {
      await memoryService.deleteMemory(selectedMemory);
      setConfirmDialogOpen(false);
      setSelectedMemory(null);
      fetchMemories();
    } catch (err) {
      setError(`Failed to delete memory "${selectedMemory}"`);
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleResetAllMemories = async () => {
    setLoading(true);
    setError(null);
    try {
      await memoryService.resetAllMemories();
      setConfirmDialogOpen(false);
      fetchMemories();
    } catch (err) {
      setError('Failed to reset all memories');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const openConfirmDialog = (action: 'reset' | 'resetAll' | 'delete') => {
    setConfirmAction(action);
    setConfirmDialogOpen(true);
  };

  const handleCleanupMemories = async () => {
    setLoading(true);
    setError(null);
    try {
      await memoryService.cleanupOldMemories(cleanupDays);
      fetchMemories();
      fetchMemoryStats();
    } catch (err) {
      setError('Failed to cleanup memories');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;

    setLoading(true);
    setError(null);
    try {
      const results = await memoryService.searchMemories(searchQuery);
      setSearchResults(results);
    } catch (err) {
      setError(`Failed to search memories for "${searchQuery}"`);
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (dateString: string | { timestamp: string } | undefined | null) => {
    if (!dateString) return t('memory.stats.formatting.noValue');
    
    // If it's an object with timestamp property, use that property
    if (typeof dateString === 'object' && dateString.timestamp) {
      return new Date(dateString.timestamp).toLocaleString();
    }
    
    // If it's a string, format it directly
    if (typeof dateString === 'string') {
      return new Date(dateString).toLocaleString();
    }
    
    return t('memory.stats.formatting.noValue');
  };

  const formatSize = (bytes: number) => {
    if (bytes === undefined || bytes === null) return t('memory.stats.formatting.noValue');
    if (bytes < 1024) return t('memory.stats.formatting.sizeBytes', { size: bytes });
    if (bytes < 1024 * 1024) return t('memory.stats.formatting.sizeKB', { size: (bytes / 1024).toFixed(2) });
    return t('memory.stats.formatting.sizeMB', { size: (bytes / (1024 * 1024)).toFixed(2) });
  };

  // Handle memory object safely
  const safeMemoryString = (memory: string | { crew: string } | Record<string, unknown> | undefined | null): string => {
    if (!memory) return t('memory.stats.formatting.noValue');
    
    if (typeof memory === 'string') {
      return memory;
    }
    
    // If it's an object with crew property, use that
    if (typeof memory === 'object') {
      if ('crew' in memory && typeof memory.crew === 'string') {
        return memory.crew;
      }
      
      // Try to stringify the object safely
      try {
        return JSON.stringify(memory);
      } catch (e) {
        return t('memory.stats.formatting.noValue');
      }
    }
    
    return t('memory.stats.formatting.noValue');
  };

  return (
    <Box sx={{ maxWidth: '100%', overflowX: 'hidden' }}>
      {/* Header with path info */}
      <Box sx={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center', 
        mb: 2,
        borderBottom: 1,
        borderColor: 'divider',
        pb: 1
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <MemoryIcon sx={{ mr: 1, color: 'primary.main' }} />
          <Typography variant="h5" component="h1">
            {t('memory.title')}
          </Typography>
          <Typography variant="caption" sx={{ ml: 2, color: 'text.secondary' }}>
            {t('memory.currentPath')}: <Typography component="span" variant="caption" fontWeight="medium">{memoryService.getMemoryPath() || t('memory.defaultPath')}</Typography>
          </Typography>
        </Box>
        <Button 
          variant="outlined" 
          size="small"
          startIcon={<StorageIcon />}
          onClick={openPathDialog}
        >
          {t('memory.changePath')}
        </Button>
      </Box>

      {/* Error message */}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {/* Tab navigation */}
      <Tabs
        value={tabValue}
        onChange={handleTabChange}
        variant="scrollable"
        scrollButtons="auto"
        sx={{ 
          borderBottom: 1, 
          borderColor: 'divider',
          mb: 2
        }}
      >
        <Tab
          label={t('memory.overview')}
          icon={<StorageIcon fontSize="small" />}
          iconPosition="start"
          {...a11yProps(0)}
          sx={{ minHeight: 48 }}
        />
        <Tab
          label={t('memory.stats.title')}
          icon={<StatsIcon fontSize="small" />}
          iconPosition="start"
          {...a11yProps(1)}
          sx={{ minHeight: 48 }}
        />
      </Tabs>

      {/* Loading indicator */}
      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', my: 1 }}>
          <CircularProgress size={24} />
        </Box>
      )}

      {/* Overview Tab */}
      <TabPanel value={tabValue} index={0}>
        <Grid container spacing={2}>
          {/* Top controls row */}
          <Grid item xs={12}>
            <Box sx={{ 
              display: 'flex',
              flexWrap: 'wrap',
              gap: 2,
              mb: 2,
              alignItems: 'center'
            }}>
              {/* Search */}
              <TextField 
                label={t('memory.search.query')}
                variant="outlined"
                size="small"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                InputProps={{
                  endAdornment: (
                    <Button 
                      size="small" 
                      onClick={handleSearch}
                      disabled={!searchQuery.trim()}
                      sx={{ minWidth: 0, px: 1 }}
                    >
                      <SearchIcon fontSize="small" />
                    </Button>
                  )
                }}
                sx={{ flexGrow: 1, maxWidth: 400 }}
              />
              
              {/* Cleanup */}
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <TextField 
                  label={t('memory.cleanup.days')}
                  type="number"
                  variant="outlined"
                  size="small"
                  value={cleanupDays}
                  onChange={(e) => setCleanupDays(parseInt(e.target.value) || 30)}
                  sx={{ width: 80 }}
                />
                <Button 
                  variant="outlined" 
                  color="warning"
                  size="small"
                  startIcon={<CleanupIcon />}
                  onClick={handleCleanupMemories}
                >
                  {t('memory.cleanup.button')}
                </Button>
              </Box>
              
              {/* Refresh/Reset */}
              <Box sx={{ display: 'flex', gap: 1, ml: 'auto' }}>
                <Button 
                  variant="outlined" 
                  color="primary" 
                  size="small"
                  startIcon={<RefreshIcon />}
                  onClick={fetchMemories}
                >
                  {t('memory.refresh')}
                </Button>
                <Button 
                  variant="outlined" 
                  color="error" 
                  size="small"
                  startIcon={<DeleteIcon />}
                  onClick={() => openConfirmDialog('resetAll')}
                  disabled={memories.length === 0}
                >
                  {t('memory.resetAll')}
                </Button>
              </Box>
            </Box>
          </Grid>
          
          {/* Search results */}
          {searchResults.length > 0 && (
            <Grid item xs={12}>
              <Paper variant="outlined" sx={{ mb: 2 }}>
                <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider', bgcolor: 'background.subtle' }}>
                  <Typography variant="subtitle2" sx={{ display: 'flex', alignItems: 'center' }}>
                    <SearchIcon fontSize="small" sx={{ mr: 1 }} />
                    {t('memory.search.title')} ({searchResults.length})
                  </Typography>
                </Box>
                <TableContainer sx={{ maxHeight: 200 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell width="30%">{t('memory.search.crew')}</TableCell>
                        <TableCell>{t('memory.search.snippet')}</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {searchResults.map((result, idx) => (
                        <TableRow key={idx} hover>
                          <TableCell sx={{ py: 1 }}>{result.crew_name}</TableCell>
                          <TableCell sx={{ py: 1, wordBreak: 'break-word' }}>{result.snippet}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Paper>
            </Grid>
          )}
          
          {/* Memory list */}
          <Grid item xs={12}>
            <Paper variant="outlined">
              <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider', bgcolor: 'background.subtle' }}>
                <Typography variant="subtitle2" sx={{ display: 'flex', alignItems: 'center' }}>
                  <MemoryIcon fontSize="small" sx={{ mr: 1 }} />
                  {t('memory.availableMemories')} ({memories.length})
                </Typography>
              </Box>
              
              {memories.length === 0 ? (
                <Box sx={{ p: 2, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <Typography color="text.secondary" variant="body2">
                    {t('memory.noMemories')}
                  </Typography>
                </Box>
              ) : (
                <TableContainer sx={{ maxHeight: 400 }}>
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ width: '60%' }}>{t('memory.crewName')}</TableCell>
                        <TableCell align="right" sx={{ width: '20%' }}>{t('memory.detailFields.size')}</TableCell>
                        <TableCell align="right">{t('memory.actions')}</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {memories.map((memory) => (
                        <TableRow 
                          key={memory}
                          selected={selectedMemory === memory}
                          hover
                          onClick={() => handleMemorySelect(memory)}
                          sx={{ cursor: 'pointer' }}
                        >
                          <TableCell sx={{ py: 1 }}>{memory}</TableCell>
                          <TableCell align="right" sx={{ py: 1 }}>
                            {formatSize(memorySizes[memory] || 0)}
                          </TableCell>
                          <TableCell align="right" sx={{ py: 1 }}>
                            <Box sx={{ display: 'flex', justifyContent: 'flex-end' }}>
                              <Tooltip title={t('memory.reset')}>
                                <Button
                                  size="small"
                                  color="warning"
                                  variant="text"
                                  sx={{ minWidth: 'auto', mx: 0.5 }}
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    setSelectedMemory(memory);
                                    openConfirmDialog('reset');
                                  }}
                                >
                                  <RefreshIcon fontSize="small" />
                                </Button>
                              </Tooltip>
                              <Tooltip title={t('memory.deleteMemory')}>
                                <Button
                                  size="small"
                                  color="error"
                                  variant="text"
                                  sx={{ minWidth: 'auto', mx: 0.5 }}
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    setSelectedMemory(memory);
                                    openConfirmDialog('delete');
                                  }}
                                >
                                  <DeleteIcon fontSize="small" />
                                </Button>
                              </Tooltip>
                              <Tooltip title={t('memory.details')}>
                                <Button
                                  size="small"
                                  color="primary"
                                  variant="text"
                                  sx={{ minWidth: 'auto', mx: 0.5 }}
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    setSelectedMemory(memory);
                                    fetchMemoryDetails(memory);
                                    setDetailsDialogOpen(true);
                                  }}
                                >
                                  <InfoIcon fontSize="small" />
                                </Button>
                              </Tooltip>
                            </Box>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Paper>
          </Grid>
        </Grid>
      </TabPanel>

      {/* Stats Tab */}
      <TabPanel value={tabValue} index={1}>
        {!memoryStats ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 200 }}>
            <Typography color="text.secondary">
              {t('memory.noStats')}
            </Typography>
          </Box>
        ) : (
          <Grid container spacing={2}>
            {/* Summary stats */}
            <Grid item xs={12}>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
                <Paper variant="outlined" sx={{ p: 2, minWidth: 140, flex: 1 }}>
                  <Typography variant="caption" color="text.secondary" display="block">{t('memory.stats.totalCrews')}</Typography>
                  <Typography variant="h4" color="primary.main">{memoryStats.total_crews || 0}</Typography>
                </Paper>
                <Paper variant="outlined" sx={{ p: 2, minWidth: 140, flex: 1 }}>
                  <Typography variant="caption" color="text.secondary" display="block">{t('memory.stats.totalSize')}</Typography>
                  <Typography variant="h4" color="secondary.main">{formatSize(memoryStats.total_size * 1024 || 0)}</Typography>
                </Paper>
                <Paper variant="outlined" sx={{ p: 2, minWidth: 140, flex: 1 }}>
                  <Typography variant="caption" color="text.secondary" display="block">{t('memory.stats.avgSize')}</Typography>
                  <Typography variant="h4" color="info.main">{formatSize(memoryStats.avg_size * 1024 || 0)}</Typography>
                </Paper>
                <Paper variant="outlined" sx={{ p: 2, minWidth: 140, flex: 1 }}>
                  <Typography variant="caption" color="text.secondary" display="block">{t('memory.stats.oldestMemory')}</Typography>
                  <Typography variant="body2" sx={{ wordBreak: 'break-word' }}>
                    {safeMemoryString(memoryStats.oldest_memory)}
                  </Typography>
                </Paper>
              </Box>
            </Grid>

            {/* Detailed stats */}
            {memoryStats.crew_details && typeof memoryStats.crew_details === 'object' && (
              <Grid item xs={12}>
                <Paper variant="outlined" sx={{ mt: 2 }}>
                  <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider', bgcolor: 'background.subtle' }}>
                    <Typography variant="subtitle2" sx={{ display: 'flex', alignItems: 'center' }}>
                      <StatsIcon fontSize="small" sx={{ mr: 1 }} />
                      {t('memory.stats.detailedStats')}
                    </Typography>
                  </Box>
                  <TableContainer sx={{ maxHeight: 400 }}>
                    <Table size="small" stickyHeader>
                      <TableHead>
                        <TableRow>
                          <TableCell sx={{ py: 1 }}>{t('memory.stats.crew')}</TableCell>
                          <TableCell align="right" sx={{ py: 1 }}>{t('memory.stats.size')}</TableCell>
                          <TableCell sx={{ py: 1 }}>{t('memory.stats.lastModified')}</TableCell>
                          <TableCell align="right" sx={{ py: 1 }}>{t('memory.stats.messagesCount')}</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {Object.entries(memoryStats.crew_details).map(([crew, details]) => (
                          <TableRow key={crew} hover>
                            <TableCell sx={{ py: 0.75, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                              {crew}
                            </TableCell>
                            <TableCell align="right" sx={{ py: 0.75 }}>
                              {formatSize(details?.size * 1024 || 0)}
                            </TableCell>
                            <TableCell sx={{ py: 0.75 }}>
                              {formatDate(details?.last_modified)}
                            </TableCell>
                            <TableCell align="right" sx={{ py: 0.75 }}>
                              {details?.messages_count ?? t('memory.stats.formatting.noValue')}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                </Paper>
              </Grid>
            )}
          </Grid>
        )}
      </TabPanel>

      {/* Path Change Dialog */}
      <Dialog open={pathDialogOpen} onClose={handleClosePathDialog} maxWidth="sm" fullWidth>
        <DialogTitle>{t('memory.changePath')}</DialogTitle>
        <DialogContent>
          <DialogContentText>
            {t('memory.changePathDescription')}
          </DialogContentText>
          <TextField
            autoFocus
            margin="dense"
            label={t('memory.path')}
            fullWidth
            variant="outlined"
            value={customPath}
            onChange={handlePathChange}
            placeholder="/path/to/memory/directory"
            helperText={t('memory.pathHelp')}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClosePathDialog}>{t('common.cancel')}</Button>
          <Button onClick={handleSavePath} variant="contained">{t('common.save')}</Button>
        </DialogActions>
      </Dialog>

      {/* Confirmation Dialog */}
      <Dialog
        open={confirmDialogOpen}
        onClose={() => setConfirmDialogOpen(false)}
      >
        <DialogTitle>
          {confirmAction === 'reset' 
            ? t('memory.confirmReset.title')
            : confirmAction === 'delete'
              ? t('memory.confirmDelete.title')
              : t('memory.confirmResetAll.title')}
        </DialogTitle>
        <DialogContent>
          <DialogContentText>
            {confirmAction === 'reset' 
              ? t('memory.confirmReset.message', { name: selectedMemory })
              : confirmAction === 'delete'
                ? t('memory.confirmDelete.message', { name: selectedMemory })
                : t('memory.confirmResetAll.message')}
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmDialogOpen(false)}>
            {t('common.cancel')}
          </Button>
          {confirmAction === 'reset' && (
            <Button 
              onClick={handleResetMemory} 
              color="error"
              autoFocus
            >
              {t('common.confirm')}
            </Button>
          )}
          {confirmAction === 'delete' && (
            <Button 
              onClick={handleDeleteMemory} 
              color="error"
              autoFocus
            >
              {t('common.confirm')}
            </Button>
          )}
          {confirmAction === 'resetAll' && (
            <Button 
              onClick={handleResetAllMemories} 
              color="error"
              autoFocus
            >
              {t('common.confirm')}
            </Button>
          )}
        </DialogActions>
      </Dialog>

      {/* Memory Details Dialog */}
      <Dialog
        open={detailsDialogOpen}
        onClose={() => setDetailsDialogOpen(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center' }}>
          <MemoryIcon fontSize="small" sx={{ mr: 1 }} />
          {selectedMemory ? t('memory.detailsFor', { name: selectedMemory }) : t('memory.details')}
        </DialogTitle>
        <DialogContent>
          {!selectedMemory ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 200 }}>
              <Typography color="text.secondary">
                {t('memory.selectMemory')}
              </Typography>
            </Box>
          ) : !memoryDetails ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 200 }}>
              <Typography color="text.secondary">
                {t('memory.noDetails')}
              </Typography>
            </Box>
          ) : (
            <Grid container spacing={2}>
              {/* Basic info */}
              <Grid item xs={12}>
                <Paper variant="outlined">
                  <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider', bgcolor: 'background.subtle' }}>
                    <Typography variant="subtitle2" sx={{ display: 'flex', alignItems: 'center' }}>
                      <MemoryIcon fontSize="small" sx={{ mr: 1 }} />
                      {t('memory.detailsFor', { name: selectedMemory })}
                    </Typography>
                  </Box>
                  <Box sx={{ p: 2 }}>
                    <Grid container spacing={2}>
                      <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="caption" color="text.secondary">{t('memory.detailFields.path')}</Typography>
                        <Typography variant="body2" sx={{ wordBreak: 'break-all' }}>{memoryDetails.memory_path}</Typography>
                      </Grid>
                      <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="caption" color="text.secondary">{t('memory.detailFields.size')}</Typography>
                        <Typography variant="body2">{formatSize(memoryDetails.size_bytes)}</Typography>
                      </Grid>
                      <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="caption" color="text.secondary">{t('memory.detailFields.created')}</Typography>
                        <Typography variant="body2">{formatDate(memoryDetails.creation_date)}</Typography>
                      </Grid>
                      <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="caption" color="text.secondary">{t('memory.detailFields.modified')}</Typography>
                        <Typography variant="body2">{formatDate(memoryDetails.last_modified)}</Typography>
                      </Grid>
                    </Grid>
                  </Box>
                </Paper>
              </Grid>

              {/* Long-Term Memory */}
              {memoryDetails.long_term_memory && (
                <Grid item xs={12} md={6}>
                  <Paper variant="outlined" sx={{ height: '100%' }}>
                    <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider', bgcolor: 'primary.50' }}>
                      <Typography variant="subtitle2" color="primary.main">
                        {t('memory.longTerm.title')}
                      </Typography>
                    </Box>
                    
                    <Box sx={{ px: 2, py: 1 }}>
                      <Grid container spacing={1}>
                        <Grid item xs={12}>
                          <Typography variant="caption" color="text.secondary">{t('memory.detailFields.path')}</Typography>
                          <Typography variant="body2" sx={{ fontSize: '0.8rem', wordBreak: 'break-all' }}>
                            {memoryDetails.long_term_memory.path}
                          </Typography>
                        </Grid>
                        <Grid item xs={12}>
                          <Typography variant="caption" color="text.secondary">{t('memory.detailFields.size')}</Typography>
                          <Typography variant="body2">{formatSize(memoryDetails.long_term_memory.size_bytes || 0)}</Typography>
                        </Grid>
                      </Grid>

                      {memoryDetails.long_term_memory.tables && memoryDetails.long_term_memory.tables.length > 0 && (
                        <Box sx={{ mt: 1 }}>
                          <Typography variant="caption" color="text.secondary">{t('memory.detailFields.tables')}</Typography>
                          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.5 }}>
                            {memoryDetails.long_term_memory.tables.map((table: string) => (
                              <Chip key={table} label={table} size="small" variant="outlined" sx={{ height: 20, '& .MuiChip-label': { px: 1, fontSize: '0.7rem' } }} />
                            ))}
                          </Box>
                        </Box>
                      )}

                      {memoryDetails.long_term_memory.records && memoryDetails.long_term_memory.records.length > 0 && (
                        <Box sx={{ mt: 1 }}>
                          <Typography variant="caption" color="text.secondary">{t('memory.detailFields.records')}</Typography>
                          <TableContainer sx={{ maxHeight: 300, mt: 0.5, border: 1, borderColor: 'divider', borderRadius: 1 }}>
                            <Table size="small" stickyHeader>
                              <TableHead>
                                <TableRow>
                                  <TableCell sx={{ py: 0.5 }}>{t('memory.detailFields.timestamp')}</TableCell>
                                  <TableCell sx={{ py: 0.5 }}>{t('memory.detailFields.content')}</TableCell>
                                </TableRow>
                              </TableHead>
                              <TableBody>
                                {memoryDetails.long_term_memory.records.map((record, idx) => (
                                  <TableRow key={idx} hover>
                                    <TableCell sx={{ py: 0.5 }}>{record?.timestamp ? formatDate(record.timestamp) : 'N/A'}</TableCell>
                                    <TableCell sx={{ py: 0.5, wordBreak: 'break-word' }}>{record?.content || 'N/A'}</TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </TableContainer>
                        </Box>
                      )}
                    </Box>
                  </Paper>
                </Grid>
              )}

              {/* Short-Term Memory */}
              {memoryDetails.short_term_memory && (
                <Grid item xs={12} md={6}>
                  <Paper variant="outlined" sx={{ height: '100%' }}>
                    <Box sx={{ px: 2, py: 1, borderBottom: 1, borderColor: 'divider', bgcolor: 'secondary.50' }}>
                      <Typography variant="subtitle2" color="secondary.main">
                        {t('memory.shortTerm.title')}
                      </Typography>
                    </Box>
                    
                    {memoryDetails.short_term_memory?.messages && memoryDetails.short_term_memory.messages.length > 0 ? (
                      <TableContainer sx={{ maxHeight: 400 }}>
                        <Table size="small" stickyHeader>
                          <TableHead>
                            <TableRow>
                              <TableCell sx={{ py: 0.5, width: '15%' }}>{t('memory.detailFields.role')}</TableCell>
                              <TableCell sx={{ py: 0.5 }}>{t('memory.detailFields.content')}</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {memoryDetails.short_term_memory.messages.map((msg, idx) => (
                              <TableRow key={idx} hover>
                                <TableCell sx={{ py: 0.5 }}>
                                  <Chip 
                                    label={msg?.role || t('memory.stats.formatting.noValue')} 
                                    size="small" 
                                    variant="outlined"
                                    color={msg?.role === 'assistant' ? 'secondary' : 'default'}
                                    sx={{ height: 20, '& .MuiChip-label': { px: 1, fontSize: '0.7rem' } }}
                                  />
                                </TableCell>
                                <TableCell sx={{ py: 0.5, wordBreak: 'break-word' }}>{msg?.content || t('memory.stats.formatting.noValue')}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </TableContainer>
                    ) : (
                      <Box sx={{ p: 2, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                        <Typography color="text.secondary" variant="body2">
                          {t('memory.noMessages')}
                        </Typography>
                      </Box>
                    )}
                  </Paper>
                </Grid>
              )}
            </Grid>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDetailsDialogOpen(false)}>{t('common.close')}</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default MemoryManagement; 