/**
 * Converter Dashboard
 * Displays conversion history, jobs, and saved configurations
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  IconButton,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  CircularProgress,
  Grid,
  Card,
  CardContent,
  LinearProgress,
} from '@mui/material';
import {
  Refresh as RefreshIcon,
  Delete as DeleteIcon,
  Visibility as ViewIcon,
  Cancel as CancelIcon,
  PlayArrow as UseIcon,
} from '@mui/icons-material';
import { ConverterService } from '../../api/ConverterService';
import type {
  ConversionHistory,
  ConversionJob,
  SavedConverterConfiguration,
  ConversionStatistics,
} from '../../types/converter';
import toast from 'react-hot-toast';
import { format } from 'date-fns';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`converter-tabpanel-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

export const ConverterDashboard: React.FC = () => {
  const [tabValue, setTabValue] = useState(0);
  const [isLoading, setIsLoading] = useState(false);

  // History state
  const [history, setHistory] = useState<ConversionHistory[]>([]);
  const [statistics, setStatistics] = useState<ConversionStatistics | null>(null);

  // Jobs state
  const [jobs, setJobs] = useState<ConversionJob[]>([]);

  // Saved configs state
  const [configurations, setConfigurations] = useState<SavedConverterConfiguration[]>([]);

  // Dialog state
  const [detailsDialogOpen, setDetailsDialogOpen] = useState(false);
  const [detailsContent, setDetailsContent] = useState<any>(null);

  useEffect(() => {
    loadData();
  }, [tabValue]);

  const loadData = async () => {
    setIsLoading(true);
    try {
      if (tabValue === 0) {
        // Load history and statistics
        const [historyData, statsData] = await Promise.all([
          ConverterService.listHistory({ limit: 100 }),
          ConverterService.getStatistics(30),
        ]);
        setHistory(historyData.history);
        setStatistics(statsData);
      } else if (tabValue === 1) {
        // Load jobs
        const jobsData = await ConverterService.listJobs(undefined, 100);
        setJobs(jobsData.jobs);
      } else if (tabValue === 2) {
        // Load saved configurations
        const configsData = await ConverterService.listConfigurations({ limit: 100 });
        setConfigurations(configsData.configurations);
      }
    } catch (error: any) {
      toast.error(`Failed to load data: ${error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleViewDetails = (item: any) => {
    setDetailsContent(item);
    setDetailsDialogOpen(true);
  };

  const handleCancelJob = async (jobId: string) => {
    try {
      await ConverterService.cancelJob(jobId);
      toast.success('Job cancelled successfully');
      loadData();
    } catch (error: any) {
      toast.error(`Failed to cancel job: ${error.message}`);
    }
  };

  const handleDeleteConfig = async (configId: number) => {
    if (!confirm('Are you sure you want to delete this configuration?')) return;

    try {
      await ConverterService.deleteConfiguration(configId);
      toast.success('Configuration deleted successfully');
      loadData();
    } catch (error: any) {
      toast.error(`Failed to delete configuration: ${error.message}`);
    }
  };

  const handleUseConfig = async (config: SavedConverterConfiguration) => {
    try {
      await ConverterService.trackConfigurationUsage(config.id);
      toast.success('Configuration loaded');
      // Emit event to load config in main form
      window.dispatchEvent(
        new CustomEvent('loadConverterConfig', { detail: config.configuration })
      );
    } catch (error: any) {
      toast.error(`Failed to load configuration: ${error.message}`);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'success':
      case 'completed':
        return 'success';
      case 'failed':
        return 'error';
      case 'running':
        return 'info';
      case 'pending':
        return 'warning';
      case 'cancelled':
        return 'default';
      default:
        return 'default';
    }
  };

  return (
    <Paper sx={{ width: '100%' }}>
      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs value={tabValue} onChange={(_, newValue) => setTabValue(newValue)}>
          <Tab label="Conversion History" />
          <Tab label="Active Jobs" />
          <Tab label="Saved Configurations" />
        </Tabs>
      </Box>

      {/* History Tab */}
      <TabPanel value={tabValue} index={0}>
        <Box sx={{ mb: 3 }}>
          <Button
            startIcon={isLoading ? <CircularProgress size={20} /> : <RefreshIcon />}
            onClick={loadData}
            disabled={isLoading}
          >
            Refresh
          </Button>
        </Box>

        {/* Statistics Cards */}
        {statistics && (
          <Grid container spacing={2} sx={{ mb: 3 }}>
            <Grid item xs={12} sm={6} md={3}>
              <Card>
                <CardContent>
                  <Typography color="text.secondary" gutterBottom>
                    Total Conversions
                  </Typography>
                  <Typography variant="h4">{statistics.total_conversions}</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Card>
                <CardContent>
                  <Typography color="text.secondary" gutterBottom>
                    Success Rate
                  </Typography>
                  <Typography variant="h4">{statistics.success_rate.toFixed(1)}%</Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Card>
                <CardContent>
                  <Typography color="text.secondary" gutterBottom>
                    Avg. Execution Time
                  </Typography>
                  <Typography variant="h4">
                    {statistics.average_execution_time_ms.toFixed(0)}ms
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} sm={6} md={3}>
              <Card>
                <CardContent>
                  <Typography color="text.secondary" gutterBottom>
                    Failed
                  </Typography>
                  <Typography variant="h4" color="error">
                    {statistics.failed}
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          </Grid>
        )}

        {/* History Table */}
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>ID</TableCell>
                <TableCell>Source → Target</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Measures</TableCell>
                <TableCell>Execution Time</TableCell>
                <TableCell>Created</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {history.map((entry) => (
                <TableRow key={entry.id}>
                  <TableCell>{entry.id}</TableCell>
                  <TableCell>
                    <Chip label={entry.source_format} size="small" sx={{ mr: 1 }} />
                    →
                    <Chip label={entry.target_format} size="small" sx={{ ml: 1 }} />
                  </TableCell>
                  <TableCell>
                    <Chip label={entry.status} color={getStatusColor(entry.status)} size="small" />
                  </TableCell>
                  <TableCell>{entry.measure_count || '-'}</TableCell>
                  <TableCell>{entry.execution_time_ms ? `${entry.execution_time_ms}ms` : '-'}</TableCell>
                  <TableCell>{format(new Date(entry.created_at), 'MMM dd, HH:mm')}</TableCell>
                  <TableCell>
                    <IconButton size="small" onClick={() => handleViewDetails(entry)}>
                      <ViewIcon />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>

      {/* Jobs Tab */}
      <TabPanel value={tabValue} index={1}>
        <Box sx={{ mb: 3 }}>
          <Button
            startIcon={isLoading ? <CircularProgress size={20} /> : <RefreshIcon />}
            onClick={loadData}
            disabled={isLoading}
          >
            Refresh
          </Button>
        </Box>

        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Job ID</TableCell>
                <TableCell>Name</TableCell>
                <TableCell>Source → Target</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Progress</TableCell>
                <TableCell>Created</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {jobs.map((job) => (
                <TableRow key={job.id}>
                  <TableCell>{job.id.substring(0, 8)}...</TableCell>
                  <TableCell>{job.name || '-'}</TableCell>
                  <TableCell>
                    <Chip label={job.source_format} size="small" sx={{ mr: 1 }} />
                    →
                    <Chip label={job.target_format} size="small" sx={{ ml: 1 }} />
                  </TableCell>
                  <TableCell>
                    <Chip label={job.status} color={getStatusColor(job.status)} size="small" />
                  </TableCell>
                  <TableCell>
                    {job.progress !== undefined ? (
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <LinearProgress
                          variant="determinate"
                          value={job.progress * 100}
                          sx={{ flexGrow: 1 }}
                        />
                        <Typography variant="body2">{(job.progress * 100).toFixed(0)}%</Typography>
                      </Box>
                    ) : (
                      '-'
                    )}
                  </TableCell>
                  <TableCell>{format(new Date(job.created_at), 'MMM dd, HH:mm')}</TableCell>
                  <TableCell>
                    <IconButton size="small" onClick={() => handleViewDetails(job)}>
                      <ViewIcon />
                    </IconButton>
                    {(job.status === 'pending' || job.status === 'running') && (
                      <IconButton size="small" onClick={() => handleCancelJob(job.id)}>
                        <CancelIcon />
                      </IconButton>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>

      {/* Saved Configurations Tab */}
      <TabPanel value={tabValue} index={2}>
        <Box sx={{ mb: 3 }}>
          <Button
            startIcon={isLoading ? <CircularProgress size={20} /> : <RefreshIcon />}
            onClick={loadData}
            disabled={isLoading}
          >
            Refresh
          </Button>
        </Box>

        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Source → Target</TableCell>
                <TableCell>Public</TableCell>
                <TableCell>Use Count</TableCell>
                <TableCell>Last Used</TableCell>
                <TableCell>Created</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {configurations.map((config) => (
                <TableRow key={config.id}>
                  <TableCell>{config.name}</TableCell>
                  <TableCell>
                    <Chip label={config.source_format} size="small" sx={{ mr: 1 }} />
                    →
                    <Chip label={config.target_format} size="small" sx={{ ml: 1 }} />
                  </TableCell>
                  <TableCell>
                    {config.is_public ? (
                      <Chip label="Public" color="primary" size="small" />
                    ) : (
                      <Chip label="Private" size="small" />
                    )}
                  </TableCell>
                  <TableCell>{config.use_count}</TableCell>
                  <TableCell>
                    {config.last_used_at
                      ? format(new Date(config.last_used_at), 'MMM dd, HH:mm')
                      : 'Never'}
                  </TableCell>
                  <TableCell>{format(new Date(config.created_at), 'MMM dd, HH:mm')}</TableCell>
                  <TableCell>
                    <IconButton size="small" onClick={() => handleUseConfig(config)}>
                      <UseIcon />
                    </IconButton>
                    <IconButton size="small" onClick={() => handleViewDetails(config)}>
                      <ViewIcon />
                    </IconButton>
                    <IconButton size="small" onClick={() => handleDeleteConfig(config.id)}>
                      <DeleteIcon />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </TabPanel>

      {/* Details Dialog */}
      <Dialog open={detailsDialogOpen} onClose={() => setDetailsDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>Details</DialogTitle>
        <DialogContent>
          <pre>{JSON.stringify(detailsContent, null, 2)}</pre>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDetailsDialogOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
};

export default ConverterDashboard;
