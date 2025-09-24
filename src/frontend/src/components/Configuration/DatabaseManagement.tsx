import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Typography,
  Alert,
  TextField,
  CircularProgress,
  Paper,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Chip,
  Grid,
  Card,
  CardContent,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  FormHelperText,
  Tabs,
  Tab,
  Divider,
  FormControlLabel,
  RadioGroup,
  Radio,
  FormLabel,
  Collapse,
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  CloudDownload as DownloadIcon,
  Refresh as RefreshIcon,
  Storage as StorageIcon,
  OpenInNew as OpenInNewIcon,
  Add as AddIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
  Info as InfoIcon,
  CloudQueue as CloudIcon,
  Save as SaveIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
  DataObject as DataObjectIcon,
} from '@mui/icons-material';
import { apiClient } from '../../config/api/ApiConfig';
import { DatabricksService } from '../../api/DatabricksService';

interface DatabaseInfo {
  success: boolean;
  database_path?: string;
  database_type?: string;
  size_mb?: number;
  created_at?: string;
  modified_at?: string;
  tables?: Record<string, number>;
  total_tables?: number;
  error?: string;
}

interface BackupFile {
  filename: string;
  size_mb: number;
  created_at: string;
}

interface ExportResult {
  success: boolean;
  backup_path?: string;
  backup_filename?: string;
  volume_path?: string;
  volume_browse_url?: string;
  export_files?: BackupFile[];
  size_mb?: number;
  original_size_mb?: number;
  timestamp?: string;
  catalog?: string;
  schema?: string;
  volume?: string;
  error?: string;
}

interface ImportResult {
  success: boolean;
  imported_from?: string;
  backup_filename?: string;
  volume_path?: string;
  size_mb?: number;
  tables?: string[];
  table_counts?: Record<string, number>;
  timestamp?: string;
  error?: string;
}

interface BackupList {
  success: boolean;
  backups?: Array<{
    filename: string;
    size_mb: number;
    created_at: string;
    databricks_url: string;
  }>;
  volume_path?: string;
  total_backups?: number;
  error?: string;
}

interface LakebaseConfig {
  enabled: boolean;
  instance_name: string;
  capacity: string;
  retention_days: number;
  node_count: number;
  instance_status?: 'NOT_CREATED' | 'CREATING' | 'READY' | 'STOPPED' | 'ERROR' | 'NOT_FOUND';
  endpoint?: string;
  created_at?: string;
  migration_status?: 'pending' | 'in_progress' | 'completed' | 'failed';
  migration_completed?: boolean;
  migration_result?: {
    total_tables: number;
    total_rows: number;
    migrated_tables?: Array<{table: string; rows: number}>;
  };
  migration_error?: string;
}

interface LakebaseInstance {
  name: string;
  state: string;
  capacity: string;
  read_write_dns: string;
  created_at: string;
  node_count?: number;
}

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
      id={`database-tabpanel-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ py: 2 }}>{children}</Box>}
    </div>
  );
}

const DatabaseManagement: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [databaseInfo, setDatabaseInfo] = useState<DatabaseInfo | null>(null);
  const [backups, setBackups] = useState<BackupList | null>(null);
  const [exportDialog, setExportDialog] = useState(false);
  const [importDialog, setImportDialog] = useState(false);
  const [selectedBackup, setSelectedBackup] = useState<string | null>(null);
  const [exportResult, setExportResult] = useState<ExportResult | null>(null);
  const [tabValue, setTabValue] = useState(0);

  // Lakebase configuration state
  const [lakebaseBackend, setLakebaseBackend] = useState<'disabled' | 'lakebase'>('disabled');
  const [lakebaseMode, setLakebaseMode] = useState<'create' | 'connect'>('connect'); // Default to connect mode
  const [lakebaseConfig, setLakebaseConfig] = useState<LakebaseConfig>({
    enabled: false,
    instance_name: 'kasal-lakebase',
    capacity: 'CU_1',
    retention_days: 14,
    node_count: 1,
    instance_status: 'NOT_CREATED'
  });
  const [creatingInstance, setCreatingInstance] = useState(false);
  const [checkingInstance, setCheckingInstance] = useState(false);
  const [manualMode] = useState(false);
  const [expandedSections, setExpandedSections] = useState({
    lakebaseConfig: false,
    migration: false,
  });
  const [disableConfirmDialog, setDisableConfirmDialog] = useState(false);

  // Export/Import form state
  const [catalog, setCatalog] = useState('users');
  const [schema, setSchema] = useState('default');
  const [volumeName, setVolumeName] = useState('kasal_backups');
  const [exportFormat, setExportFormat] = useState('sql');

  // Load database info on mount
  useEffect(() => {
    loadDatabaseInfo();
    loadLakebaseConfig();
  }, []);

  const loadDatabaseInfo = async () => {
    try {
      setLoading(true);
      const response = await apiClient.get<DatabaseInfo>('/database-management/info');
      setDatabaseInfo(response.data);
    } catch (err) {
      setError('Failed to load database information');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const loadLakebaseConfig = async () => {
    try {
      const response = await apiClient.get<LakebaseConfig>('/database-management/lakebase/config');
      setLakebaseConfig(response.data);
      setLakebaseBackend(response.data.enabled ? 'lakebase' : 'disabled');

      // Always check instance status if we have an instance name
      // This ensures we get the current status from Databricks, not cached status
      if (response.data.instance_name) {
        await checkLakebaseInstance(response.data.instance_name);
      }
    } catch (err) {
      console.error('Failed to load Lakebase configuration:', err);
    }
  };

  const checkLakebaseInstance = async (instanceName: string) => {
    try {
      setCheckingInstance(true);
      const response = await apiClient.get<LakebaseInstance>(`/database-management/lakebase/instance/${instanceName}`);

      // Map Databricks instance states to our status types
      let status: LakebaseConfig['instance_status'];
      const state = response.data.state?.toUpperCase();

      if (state === 'READY' || state === 'AVAILABLE' || state === 'RUNNING') {
        status = 'READY';
      } else if (state === 'STOPPED' || state === 'STOPPING') {
        status = 'STOPPED';
      } else if (state === 'CREATING' || state === 'STARTING') {
        status = 'CREATING';
      } else if (state === 'ERROR' || state === 'FAILED') {
        status = 'ERROR';
      } else if (state === 'NOT_FOUND') {
        status = 'NOT_FOUND';
      } else {
        // Default to READY if it has an endpoint and unknown state
        status = response.data.read_write_dns ? 'READY' : 'STOPPED';
      }

      setLakebaseConfig(prev => ({
        ...prev,
        instance_status: status,
        endpoint: response.data.read_write_dns,
        created_at: response.data.created_at
      }));
    } catch (err: any) {
      if (err.response?.status === 404) {
        setLakebaseConfig(prev => ({ ...prev, instance_status: 'NOT_CREATED' }));
      } else {
        console.error('Failed to check Lakebase instance:', err);
      }
    } finally {
      setCheckingInstance(false);
    }
  };

  const createLakebaseInstance = async () => {
    try {
      setCreatingInstance(true);
      setError(null);

      const response = await apiClient.post<LakebaseInstance>('/database-management/lakebase/create', {
        instance_name: lakebaseConfig.instance_name,
        capacity: lakebaseConfig.capacity,
        retention_days: lakebaseConfig.retention_days,
        node_count: lakebaseConfig.node_count
      });

      setLakebaseConfig(prev => ({
        ...prev,
        enabled: true,
        instance_status: 'READY',
        endpoint: response.data.read_write_dns,
        created_at: response.data.created_at
      }));

      setSuccess(`Lakebase instance "${response.data.name}" created successfully!`);

      // Save configuration
      await saveLakebaseConfig();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to create Lakebase instance');
    } finally {
      setCreatingInstance(false);
    }
  };

  const saveLakebaseConfig = async () => {
    try {
      setLoading(true);
      setError(null);

      const configToSave = {
        ...lakebaseConfig,
        enabled: lakebaseBackend === 'lakebase'
      };

      const response = await apiClient.post<LakebaseConfig>('/database-management/lakebase/config', configToSave);
      setLakebaseConfig(response.data);
      setSuccess('Lakebase configuration saved successfully!');
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to save Lakebase configuration');
    } finally {
      setLoading(false);
    }
  };

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleString();
  };

  const formatSize = (mb: number) => {
    if (mb < 1) return `${(mb * 1024).toFixed(2)} KB`;
    if (mb > 1024) return `${(mb / 1024).toFixed(2)} GB`;
    return `${mb.toFixed(2)} MB`;
  };

  const getStatusIcon = (status?: string) => {
    switch (status) {
      case 'READY':
        return <CheckIcon color="success" />;
      case 'CREATING':
        return <CircularProgress size={16} />;
      case 'STOPPED':
        return <WarningIcon color="warning" />;
      case 'ERROR':
        return <ErrorIcon color="error" />;
      default:
        return <InfoIcon color="info" />;
    }
  };

  const loadBackups = async () => {
    try {
      setLoading(true);
      const response = await apiClient.post<BackupList>('/database-management/list-backups', {
        catalog,
        schema,
        volume_name: volumeName
      });
      setBackups(response.data);
    } catch (err: any) {
      setError(err.response?.data?.error || 'Failed to load backups');
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await apiClient.post<ExportResult>('/database-management/export', {
        catalog,
        schema,
        volume_name: volumeName,
        export_format: exportFormat
      });
      setExportResult(response.data);
      if (response.data.success) {
        setSuccess('Database exported successfully!');
        setExportDialog(false);
        loadBackups();
      } else {
        setError(response.data.error || 'Export failed');
      }
    } catch (err: any) {
      setError(err.response?.data?.error || 'Failed to export database');
    } finally {
      setLoading(false);
    }
  };

  const handleImport = async () => {
    if (!selectedBackup) {
      setError('Please select a backup file to import');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      const response = await apiClient.post<ImportResult>('/database-management/import', {
        catalog,
        schema,
        volume_name: volumeName,
        backup_filename: selectedBackup
      });

      if (response.data.success) {
        setSuccess(`Database imported successfully from ${response.data.backup_filename}!`);
        setImportDialog(false);
        loadDatabaseInfo();
      } else {
        setError(response.data.error || 'Import failed');
      }
    } catch (err: any) {
      setError(err.response?.data?.error || 'Failed to import database');
    } finally {
      setLoading(false);
    }
  };

  const toggleSection = (section: keyof typeof expandedSections) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section]
    }));
  };

  const handleConfirmDisableLakebase = async () => {
    setDisableConfirmDialog(false);

    // Proceed with disabling Lakebase
    setLakebaseBackend('disabled');
    setLakebaseConfig(prev => ({ ...prev, enabled: false }));

    try {
      setLoading(true);
      setError(null);

      const configToSave = {
        ...lakebaseConfig,
        enabled: false
      };

      await apiClient.post<LakebaseConfig>('/database-management/lakebase/config', configToSave);
      setSuccess('Lakebase disabled and configuration deleted. System now using SQLite (or PostgreSQL in local development).');
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to disable Lakebase');
      // Revert the UI state on error
      setLakebaseBackend('lakebase');
      setLakebaseConfig(prev => ({ ...prev, enabled: true }));
    } finally {
      setLoading(false);
    }
  };

  const getViewInDatabricksUrl = async () => {
    try {
      // First try to get from Databricks configuration
      const databricksConfig = await DatabricksService.getConfiguration();
      let workspaceUrl = databricksConfig?.workspace_url || '';

      // If workspace URL is empty or just 'compute', try to get from environment
      if (!workspaceUrl || workspaceUrl === 'compute' || !workspaceUrl.includes('.')) {
        // Try to get from current window location if running in Databricks
        if (window.location.hostname.includes('databricks')) {
          workspaceUrl = `https://${window.location.hostname}`;
        } else {
          // Fallback - try to get from API endpoint
          try {
            const response = await apiClient.get('/databricks/workspace-url');
            workspaceUrl = response.data.workspace_url || '';
          } catch {
            // If all else fails, prompt user to configure it
            console.warn('Unable to determine Databricks workspace URL');
          }
        }
      }

      // Clean up the URL
      if (workspaceUrl) {
        // Remove trailing slash if present
        if (workspaceUrl.endsWith('/')) {
          workspaceUrl = workspaceUrl.slice(0, -1);
        }

        // Ensure it has a protocol
        if (!workspaceUrl.startsWith('http://') && !workspaceUrl.startsWith('https://')) {
          // If it's just a domain, add https
          workspaceUrl = `https://${workspaceUrl}`;
        }

        // Construct the URL for database instances
        return `${workspaceUrl}/compute/database-instances/${lakebaseConfig.instance_name}`;
      }

      // If we still don't have a URL, return a placeholder
      return '#';
    } catch (err) {
      console.error('Failed to get Databricks workspace URL:', err);
      return '#';
    }
  };

  return (
    <Box>
      <Typography variant="h6" sx={{ mb: 3 }}>
        Database Management
      </Typography>

      <Paper sx={{ mb: 3 }}>
        <Tabs value={tabValue} onChange={handleTabChange}>
          <Tab label="General" icon={<StorageIcon />} />
          <Tab label="Databricks Import/Export" icon={<CloudIcon />} />
          <Tab label="Lakebase" icon={<DataObjectIcon />} />
        </Tabs>
      </Paper>

      {/* General Tab */}
      <TabPanel value={tabValue} index={0}>
        {databaseInfo && (
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Database Information
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" color="text.secondary">Type</Typography>
                  <Typography variant="body1">{databaseInfo.database_type}</Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" color="text.secondary">Size</Typography>
                  <Typography variant="body1">{formatSize(databaseInfo.size_mb || 0)}</Typography>
                </Grid>
                <Grid item xs={12}>
                  <Typography variant="body2" color="text.secondary">Path</Typography>
                  <Typography variant="body1" sx={{ wordBreak: 'break-all' }}>
                    {databaseInfo.database_path}
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" color="text.secondary">Created</Typography>
                  <Typography variant="body1">{formatDate(databaseInfo.created_at || '')}</Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" color="text.secondary">Modified</Typography>
                  <Typography variant="body1">{formatDate(databaseInfo.modified_at || '')}</Typography>
                </Grid>
                {databaseInfo.tables && (
                  <Grid item xs={12}>
                    <Typography variant="body2" color="text.secondary">
                      Tables ({databaseInfo.total_tables})
                    </Typography>
                    <Box sx={{ mt: 1 }}>
                      {Object.entries(databaseInfo.tables).map(([table, count]) => (
                        <Chip
                          key={table}
                          label={`${table} (${count} rows)`}
                          size="small"
                          sx={{ m: 0.5 }}
                        />
                      ))}
                    </Box>
                  </Grid>
                )}
              </Grid>
            </CardContent>
          </Card>
        )}
      </TabPanel>

      {/* Databricks Import/Export Tab */}
      <TabPanel value={tabValue} index={1}>
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Databricks Volume Settings
            </Typography>
            <Grid container spacing={2} sx={{ mb: 3 }}>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Catalog"
                  value={catalog}
                  onChange={(e) => setCatalog(e.target.value)}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Schema"
                  value={schema}
                  onChange={(e) => setSchema(e.target.value)}
                />
              </Grid>
              <Grid item xs={12} md={4}>
                <TextField
                  fullWidth
                  label="Volume Name"
                  value={volumeName}
                  onChange={(e) => setVolumeName(e.target.value)}
                />
              </Grid>
            </Grid>

            <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
              <Button
                variant="contained"
                startIcon={<UploadIcon />}
                onClick={() => setExportDialog(true)}
              >
                Export to Volume
              </Button>
              <Button
                variant="outlined"
                startIcon={<DownloadIcon />}
                onClick={() => {
                  loadBackups();
                  setImportDialog(true);
                }}
              >
                Import from Volume
              </Button>
              <Button
                variant="outlined"
                startIcon={<RefreshIcon />}
                onClick={loadBackups}
              >
                List Backups
              </Button>
            </Box>

            {backups && backups.backups && backups.backups.length > 0 && (
              <Box>
                <Typography variant="subtitle1" gutterBottom>
                  Available Backups ({backups.total_backups})
                </Typography>
                {backups.volume_path && (
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                    Volume Path: {backups.volume_path}
                  </Typography>
                )}
                <List>
                  {backups.backups.map((backup) => (
                    <ListItem key={backup.filename} divider>
                      <ListItemText
                        primary={backup.filename}
                        secondary={`${formatSize(backup.size_mb)} • ${formatDate(backup.created_at)}`}
                      />
                      <ListItemSecondaryAction>
                        {backup.databricks_url && (
                          <IconButton
                            edge="end"
                            onClick={() => window.open(backup.databricks_url, '_blank')}
                            title="View in Databricks"
                          >
                            <OpenInNewIcon />
                          </IconButton>
                        )}
                      </ListItemSecondaryAction>
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}
          </CardContent>
        </Card>
      </TabPanel>

      {/* Lakebase Tab - Memory Backend Style */}
      <TabPanel value={tabValue} index={2}>
        <Paper sx={{ p: 3 }}>
          {/* Radio Button Selection - Same style as Memory Backend */}
          <FormControl component="fieldset" sx={{ mb: 3 }}>
            <FormLabel component="legend">
              <Typography variant="h6" sx={{ mb: 2 }}>
                Database Backend Configuration
              </Typography>
            </FormLabel>
            <RadioGroup
              value={lakebaseBackend}
              onChange={async (e) => {
                const newValue = e.target.value as 'disabled' | 'lakebase';

                // If switching to disabled, show confirmation dialog
                if (newValue === 'disabled' && lakebaseBackend === 'lakebase') {
                  setDisableConfirmDialog(true);
                  return;
                }

                // Otherwise, proceed with enabling Lakebase
                setLakebaseBackend(newValue);
                setLakebaseConfig(prev => ({ ...prev, enabled: newValue === 'lakebase' }));

                // Immediately save the configuration to apply changes
                try {
                  setLoading(true);
                  setError(null);

                  const configToSave = {
                    ...lakebaseConfig,
                    enabled: newValue === 'lakebase'
                  };

                  await apiClient.post<LakebaseConfig>('/database-management/lakebase/config', configToSave);

                  if (newValue === 'disabled') {
                    setSuccess('Lakebase disabled and configuration deleted. System now using PostgreSQL/SQLite.');
                  } else {
                    setSuccess('Lakebase enabled. System will use Lakebase when available.');
                  }
                } catch (err: any) {
                  setError(err.response?.data?.detail || 'Failed to update Lakebase configuration');
                  // Revert the UI state on error
                  setLakebaseBackend(newValue === 'lakebase' ? 'disabled' : 'lakebase');
                  setLakebaseConfig(prev => ({ ...prev, enabled: newValue !== 'lakebase' }));
                } finally {
                  setLoading(false);
                }
              }}
            >
              <FormControlLabel
                value="disabled"
                control={<Radio />}
                label={
                  <Box>
                    <Typography variant="body1">Disabled</Typography>
                    <Typography variant="caption" color="text.secondary">
                      Use default SQLite/PostgreSQL database
                    </Typography>
                  </Box>
                }
              />
              <FormControlLabel
                value="lakebase"
                control={<Radio />}
                label={
                  <Box>
                    <Typography variant="body1">Databricks Lakebase</Typography>
                    <Typography variant="caption" color="text.secondary">
                      Fully-managed PostgreSQL OLTP engine within Databricks
                    </Typography>
                  </Box>
                }
              />
            </RadioGroup>
          </FormControl>

          {/* Lakebase Configuration - Only show when selected */}
          {lakebaseBackend === 'lakebase' && (
            <>
              <Divider sx={{ mb: 3 }} />

              {/* Current Status Section */}
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle1" sx={{ mb: 2, display: 'flex', alignItems: 'center' }}>
                  <StorageIcon sx={{ mr: 1 }} />
                  Current Status
                </Typography>

                <Grid container spacing={2} alignItems="center">
                  <Grid item>
                    <Chip
                      icon={getStatusIcon(lakebaseConfig.instance_status)}
                      label={lakebaseConfig.instance_status || 'NOT_CONFIGURED'}
                      color={lakebaseConfig.instance_status === 'READY' ? 'success' : 'default'}
                    />
                  </Grid>
                  {lakebaseConfig.instance_name && lakebaseConfig.instance_status === 'READY' && (
                    <Grid item>
                      <Typography variant="body2" color="text.secondary">
                        Instance: {lakebaseConfig.instance_name}
                      </Typography>
                    </Grid>
                  )}
                  {lakebaseConfig.endpoint && lakebaseConfig.instance_status === 'READY' && (
                    <Grid item xs={12}>
                      <Typography variant="body2" color="text.secondary">
                        Endpoint: {lakebaseConfig.endpoint}
                      </Typography>
                    </Grid>
                  )}
                </Grid>

                {/* View in Databricks Button */}
                {lakebaseConfig.instance_status === 'READY' && (
                  <Box sx={{ mt: 2 }}>
                    <Button
                      variant="outlined"
                      size="small"
                      startIcon={<OpenInNewIcon />}
                      onClick={async () => {
                        const url = await getViewInDatabricksUrl();
                        window.open(url, '_blank');
                      }}
                    >
                      View in Databricks
                    </Button>
                    <Button
                      variant="outlined"
                      size="small"
                      startIcon={<RefreshIcon />}
                      onClick={() => checkLakebaseInstance(lakebaseConfig.instance_name)}
                      disabled={checkingInstance}
                      sx={{ ml: 1 }}
                    >
                      Refresh Status
                    </Button>
                  </Box>
                )}
              </Box>

              <Divider sx={{ mb: 3 }} />

              {/* Connection Mode Selection - Clear toggle between Create and Connect */}
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle1" sx={{ mb: 2 }}>
                  How would you like to set up Lakebase?
                </Typography>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <Card
                      variant={lakebaseMode === 'connect' ? 'outlined' : 'elevation'}
                      sx={{
                        border: lakebaseMode === 'connect' ? '2px solid' : 'none',
                        borderColor: lakebaseMode === 'connect' ? 'primary.main' : 'transparent',
                        cursor: 'pointer',
                        height: '100%'
                      }}
                      onClick={() => setLakebaseMode('connect')}
                    >
                      <CardContent>
                        <Typography variant="h6" gutterBottom color={lakebaseMode === 'connect' ? 'primary' : 'inherit'}>
                          Connect to Existing Instance
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          I already have a Lakebase instance and want to connect to it
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <Card
                      variant={lakebaseMode === 'create' ? 'outlined' : 'elevation'}
                      sx={{
                        border: lakebaseMode === 'create' ? '2px solid' : 'none',
                        borderColor: lakebaseMode === 'create' ? 'primary.main' : 'transparent',
                        cursor: 'pointer',
                        height: '100%'
                      }}
                      onClick={() => setLakebaseMode('create')}
                    >
                      <CardContent>
                        <Typography variant="h6" gutterBottom color={lakebaseMode === 'create' ? 'primary' : 'inherit'}>
                          Create New Instance
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Create a new Lakebase instance in my Databricks workspace
                        </Typography>
                      </CardContent>
                    </Card>
                  </Grid>
                </Grid>
              </Box>

              {/* Connect to Existing Instance Form */}
              {lakebaseMode === 'connect' && (
                <Box sx={{ mb: 3 }}>
                  <Paper sx={{ p: 2, backgroundColor: 'background.default' }}>
                    <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 'bold' }}>
                      Connect to Existing Lakebase Instance
                    </Typography>
                    <Grid container spacing={2}>
                      <Grid item xs={12}>
                        <TextField
                          fullWidth
                          label="Instance Name"
                          value={lakebaseConfig.instance_name}
                          onChange={(e) => setLakebaseConfig({ ...lakebaseConfig, instance_name: e.target.value })}
                          helperText="Name of your existing Lakebase instance"
                          placeholder="kasal-lakebase"
                          required
                        />
                      </Grid>
                      <Grid item xs={12}>
                        <TextField
                          fullWidth
                          label="Endpoint (Optional)"
                          value={lakebaseConfig.endpoint || ''}
                          onChange={(e) => setLakebaseConfig({
                            ...lakebaseConfig,
                            endpoint: e.target.value,
                            instance_status: e.target.value ? 'READY' : 'NOT_CREATED'
                          })}
                          helperText="Leave empty to auto-detect, or enter manually (e.g., instance-xxxx.database.cloud.databricks.com)"
                          placeholder="instance-xxxx.database.cloud.databricks.com"
                        />
                      </Grid>
                      <Grid item xs={12}>
                        <Button
                          variant="contained"
                          onClick={async () => {
                            try {
                              setLoading(true);
                              // First check if the instance exists
                              await checkLakebaseInstance(lakebaseConfig.instance_name);
                              // Then save the configuration
                              await saveLakebaseConfig();
                              setSuccess('Successfully connected to existing Lakebase instance');
                            } catch (err) {
                              setError('Failed to connect to instance. Please verify the instance name.');
                            } finally {
                              setLoading(false);
                            }
                          }}
                          disabled={!lakebaseConfig.instance_name || loading}
                        >
                          Connect to Instance
                        </Button>
                      </Grid>
                    </Grid>
                  </Paper>
                </Box>
              )}

              {/* Create New Instance Form */}
              {lakebaseMode === 'create' && (
                <Box sx={{ mb: 3 }}>
                  <Paper sx={{ p: 2, backgroundColor: 'background.default' }}>
                    <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 'bold' }}>
                      Create New Lakebase Instance
                    </Typography>
                    <Grid container spacing={2}>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          label="Instance Name"
                          value={lakebaseConfig.instance_name}
                          onChange={(e) => setLakebaseConfig({ ...lakebaseConfig, instance_name: e.target.value })}
                          helperText="1-63 characters, letters and hyphens only"
                          placeholder="kasal-lakebase"
                          required
                        />
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <FormControl fullWidth>
                          <InputLabel>Capacity</InputLabel>
                          <Select
                            value={lakebaseConfig.capacity}
                            onChange={(e) => setLakebaseConfig({ ...lakebaseConfig, capacity: e.target.value })}
                            label="Capacity"
                          >
                            <MenuItem value="CU_1">CU_1 (Small - 1 compute unit)</MenuItem>
                            <MenuItem value="CU_2">CU_2 (Medium - 2 compute units)</MenuItem>
                            <MenuItem value="CU_4">CU_4 (Large - 4 compute units)</MenuItem>
                          </Select>
                          <FormHelperText>Select compute capacity based on your workload</FormHelperText>
                        </FormControl>
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          type="number"
                          label="Backup Retention (Days)"
                          value={lakebaseConfig.retention_days}
                          onChange={(e) => setLakebaseConfig({ ...lakebaseConfig, retention_days: parseInt(e.target.value) })}
                          inputProps={{ min: 2, max: 35 }}
                          helperText="How long to keep backups (2-35 days)"
                        />
                      </Grid>
                      <Grid item xs={12} md={6}>
                        <TextField
                          fullWidth
                          type="number"
                          label="High Availability Nodes"
                          value={lakebaseConfig.node_count}
                          onChange={(e) => setLakebaseConfig({ ...lakebaseConfig, node_count: parseInt(e.target.value) })}
                          inputProps={{ min: 1, max: 3 }}
                          helperText="1 = Basic, 2+ = High Availability"
                        />
                      </Grid>
                      <Grid item xs={12}>
                        <Alert severity="info" sx={{ mb: 2 }}>
                          Creating a new instance will provision resources in your Databricks workspace.
                          This may take 3-5 minutes.
                        </Alert>
                        <Button
                          variant="contained"
                          onClick={createLakebaseInstance}
                          disabled={!lakebaseConfig.instance_name || creatingInstance || lakebaseConfig.instance_status === 'READY'}
                        >
                          {creatingInstance ? 'Creating Instance...' : 'Create Instance'}
                        </Button>
                      </Grid>
                    </Grid>
                  </Paper>
                </Box>
              )}

              {/* Migration Section - Collapsible */}
              {lakebaseConfig.endpoint && (
                <>
                  <Box sx={{ mb: 3 }}>
                    <Button
                      onClick={() => toggleSection('migration')}
                      endIcon={expandedSections.migration ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                      sx={{ mb: 2 }}
                    >
                      Migration Status
                    </Button>

                    <Collapse in={expandedSections.migration}>
                      {lakebaseConfig.migration_status ? (
                        <Alert
                          severity={
                            lakebaseConfig.migration_status === 'completed' ? 'success' :
                            lakebaseConfig.migration_status === 'failed' ? 'error' : 'info'
                          }
                        >
                          <Typography variant="body2">
                            Migration Status: <strong>{lakebaseConfig.migration_status}</strong>
                            {lakebaseConfig.migration_result && (
                              <span> ({lakebaseConfig.migration_result.total_tables} tables, {lakebaseConfig.migration_result.total_rows} rows)</span>
                            )}
                          </Typography>
                          {lakebaseConfig.migration_error && (
                            <Typography variant="caption" color="error.main" sx={{ display: 'block', mt: 0.5 }}>
                              Error: {lakebaseConfig.migration_error}
                            </Typography>
                          )}
                        </Alert>
                      ) : (
                        <Alert severity="warning">
                          No migration has been performed yet. Click &apos;Migrate Data&apos; to start migration.
                        </Alert>
                      )}

                      {!lakebaseConfig.migration_completed && (
                        <Button
                          variant="contained"
                          color="warning"
                          onClick={async () => {
                            try {
                              setLoading(true);
                              setError(null);
                              await apiClient.post('/database-management/lakebase/migrate', {
                                instance_name: lakebaseConfig.instance_name,
                                endpoint: lakebaseConfig.endpoint
                              });
                              setSuccess('Migration started successfully');
                              // Refresh config to get migration status
                              await loadLakebaseConfig();
                            } catch (err: any) {
                              setError(err.response?.data?.detail || 'Failed to start migration');
                            } finally {
                              setLoading(false);
                            }
                          }}
                          disabled={loading}
                          sx={{ mt: 2 }}
                        >
                          Migrate Data to Lakebase
                        </Button>
                      )}
                    </Collapse>
                  </Box>
                </>
              )}

              {/* Action Buttons */}
              <Box sx={{ mt: 3, display: 'flex', gap: 2 }}>
                {!manualMode && lakebaseConfig.instance_status === 'NOT_CREATED' && (
                  <Button
                    variant="contained"
                    startIcon={creatingInstance ? <CircularProgress size={20} /> : <AddIcon />}
                    onClick={createLakebaseInstance}
                    disabled={creatingInstance || !lakebaseConfig.instance_name}
                  >
                    Create Lakebase Instance
                  </Button>
                )}

                {(manualMode || lakebaseConfig.instance_status !== 'NOT_CREATED') && (
                  <Button
                    variant="contained"
                    startIcon={<SaveIcon />}
                    onClick={saveLakebaseConfig}
                    disabled={loading || (!manualMode && lakebaseConfig.instance_status === 'NOT_CREATED')}
                  >
                    Save Configuration
                  </Button>
                )}
              </Box>

              {/* Info Alert */}
              <Alert severity="info" sx={{ mt: 3 }}>
                <Typography variant="body2">
                  <strong>Lakebase</strong> is a fully-managed PostgreSQL OLTP engine within Databricks that provides:
                </Typography>
                <ul style={{ margin: '8px 0 0 0', paddingLeft: '20px' }}>
                  <li>High availability with automatic failover</li>
                  <li>Point-in-time restore and automated backups</li>
                  <li>Built-in monitoring and alerting</li>
                  <li>Seamless integration with Databricks ecosystem</li>
                </ul>
              </Alert>
            </>
          )}
        </Paper>
      </TabPanel>

      {/* Export Dialog */}
      <Dialog open={exportDialog} onClose={() => setExportDialog(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Export Database to Databricks Volume</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2 }}>
            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel>Export Format</InputLabel>
              <Select
                value={exportFormat}
                onChange={(e) => setExportFormat(e.target.value)}
                label="Export Format"
              >
                <MenuItem value="sql">SQL</MenuItem>
                <MenuItem value="json">JSON</MenuItem>
                <MenuItem value="csv">CSV</MenuItem>
              </Select>
            </FormControl>
            <Alert severity="info">
              Database will be exported to: /Volumes/{catalog}/{schema}/{volumeName}/
            </Alert>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setExportDialog(false)}>Cancel</Button>
          <Button onClick={handleExport} variant="contained" disabled={loading}>
            {loading ? <CircularProgress size={20} /> : 'Export'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Import Dialog */}
      <Dialog open={importDialog} onClose={() => setImportDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Import Database from Databricks Volume</DialogTitle>
        <DialogContent>
          {backups && backups.backups && backups.backups.length > 0 ? (
            <Box sx={{ pt: 2 }}>
              <Typography variant="body2" sx={{ mb: 2 }}>
                Select a backup to import:
              </Typography>
              <List>
                {backups.backups.map((backup) => (
                  <ListItem
                    key={backup.filename}
                    button
                    selected={selectedBackup === backup.filename}
                    onClick={() => setSelectedBackup(backup.filename)}
                  >
                    <ListItemText
                      primary={backup.filename}
                      secondary={`${formatSize(backup.size_mb)} • ${formatDate(backup.created_at)}`}
                    />
                  </ListItem>
                ))}
              </List>
              <Alert severity="warning" sx={{ mt: 2 }}>
                Warning: This will replace the current database with the selected backup.
              </Alert>
            </Box>
          ) : (
            <Box sx={{ pt: 2 }}>
              <Alert severity="info">No backups found in the specified volume.</Alert>
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setImportDialog(false)}>Cancel</Button>
          <Button
            onClick={handleImport}
            variant="contained"
            color="warning"
            disabled={loading || !selectedBackup}
          >
            {loading ? <CircularProgress size={20} /> : 'Import'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Export Result Dialog */}
      {exportResult && (
        <Dialog open={true} onClose={() => setExportResult(null)} maxWidth="md" fullWidth>
          <DialogTitle>Export Complete</DialogTitle>
          <DialogContent>
            <Box sx={{ pt: 2 }}>
              <Alert severity="success" sx={{ mb: 2 }}>
                Database exported successfully!
              </Alert>
              <Typography variant="body2" sx={{ mb: 1 }}>
                <strong>Backup Path:</strong> {exportResult.volume_path}
              </Typography>
              {exportResult.backup_filename && (
                <Typography variant="body2" sx={{ mb: 1 }}>
                  <strong>Filename:</strong> {exportResult.backup_filename}
                </Typography>
              )}
              {exportResult.size_mb && (
                <Typography variant="body2" sx={{ mb: 1 }}>
                  <strong>Size:</strong> {formatSize(exportResult.size_mb)}
                </Typography>
              )}
              {exportResult.volume_browse_url && (
                <Button
                  variant="outlined"
                  startIcon={<OpenInNewIcon />}
                  onClick={() => window.open(exportResult.volume_browse_url, '_blank')}
                  sx={{ mt: 2 }}
                >
                  View in Databricks
                </Button>
              )}
            </Box>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setExportResult(null)}>Close</Button>
          </DialogActions>
        </Dialog>
      )}

      {/* Success/Error Messages */}
      {success && (
        <Alert severity="success" onClose={() => setSuccess(null)} sx={{ mt: 2 }}>
          {success}
        </Alert>
      )}
      {error && (
        <Alert severity="error" onClose={() => setError(null)} sx={{ mt: 2 }}>
          {error}
        </Alert>
      )}

      {/* Confirmation Dialog for Disabling Lakebase */}
      <Dialog
        open={disableConfirmDialog}
        onClose={() => setDisableConfirmDialog(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <WarningIcon color="warning" />
            Disable Databricks Lakebase
          </Box>
        </DialogTitle>
        <DialogContent>
          <Typography variant="body1" sx={{ mb: 2 }}>
            Are you sure you want to disable Databricks Lakebase?
          </Typography>
          <Alert severity="info" sx={{ mb: 2 }}>
            <Typography variant="body2" component="div">
              This action will:
              <ul style={{ margin: '8px 0', paddingLeft: '20px' }}>
                <li>Delete the Lakebase configuration from your database</li>
                <li>Revert all database operations to <strong>SQLite</strong> (in Databricks Apps) or <strong>PostgreSQL</strong> (in local development)</li>
                <li>Keep your Lakebase instance running (if created) - you can re-enable it later</li>
                <li>Preserve any data already migrated to Lakebase</li>
              </ul>
            </Typography>
          </Alert>
          <Typography variant="body2" color="text.secondary">
            Note: You can re-enable Lakebase at any time by selecting it again.
          </Typography>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDisableConfirmDialog(false)}>
            Cancel
          </Button>
          <Button
            onClick={handleConfirmDisableLakebase}
            color="warning"
            variant="contained"
            startIcon={<StorageIcon />}
          >
            Disable and Use SQLite/PostgreSQL
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default DatabaseManagement;