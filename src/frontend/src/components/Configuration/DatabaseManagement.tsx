import React, { useState, useEffect, useCallback, useRef } from 'react';
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
  DialogContentText,
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
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  CloudDownload as DownloadIcon,
  Refresh as RefreshIcon,
  Storage as StorageIcon,
  OpenInNew as OpenInNewIcon,
  CheckCircle as CheckIcon,
  Error as ErrorIcon,
  Warning as WarningIcon,
  Info as InfoIcon,
  CloudQueue as CloudIcon,
  DataObject as DataObjectIcon,
  DeleteSweep as DeleteSweepIcon,
} from '@mui/icons-material';
import { apiClient, config } from '../../config/api/ApiConfig';
import { useDatabaseStore } from '../../store/databaseStore';
import { APIKeysService } from '../../api/APIKeysService';

// Type guard for Axios errors with response data
interface ErrorWithResponse {
  response?: {
    status?: number;
    data?: {
      detail?: string;
      error?: string;
    };
  };
  message?: string;
}

function isErrorWithResponse(error: unknown): error is ErrorWithResponse {
  return typeof error === 'object' && error !== null && 'response' in error;
}

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
  lakebase_enabled?: boolean;
  lakebase_instance?: string;
  lakebase_endpoint?: string;
  connection_error?: string;
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

interface LakebaseInstanceOption {
  name: string;
  state: string | null;
  capacity: string | null;
  read_write_dns: string | null;
  node_count: number | null;
}

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

interface FailedTableDetail {
  table: string;
  error_type: string;
  error_message: string;
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
  // Zustand store
  const {
    databaseInfo,
    setDatabaseInfo,
    lakebaseConfig,
    setLakebaseConfig,
    lakebaseMode,
    setLakebaseMode,
    schemaExists,
    setSchemaExists,
    showMigrationDialog,
    setShowMigrationDialog,
    migrationOption,
    setMigrationOption,
    loading,
    setLoading,
    checkingInstance,
    setCheckingInstance,
    creatingInstance,
    setCreatingInstance,
    error,
    setError,
    success,
    setSuccess,
  } = useDatabaseStore();

  // Local state for non-Lakebase features
  const [lakebaseBackend, setLakebaseBackend] = useState<'disabled' | 'lakebase'>('disabled');
  const [disableConfirmDialog, setDisableConfirmDialog] = useState(false);
  const [backups, setBackups] = useState<BackupList | null>(null);
  const [exportDialog, setExportDialog] = useState(false);
  const exportFormat = 'sqlite'; // Fixed format for database export
  const [importDialog, setImportDialog] = useState(false);
  const [selectedBackup, setSelectedBackup] = useState<string | null>(null);
  const [exportResult, setExportResult] = useState<ExportResult | null>(null);
  const [tabValue, setTabValue] = useState(0);

  // Migration logs dialog state
  const [migrationLogsDialog, setMigrationLogsDialog] = useState(false);
  const [migrationLogs, setMigrationLogs] = useState<string[]>([]);
  const migrationLogsEndRef = useRef<HTMLDivElement>(null);

  // Export/Import form state
  const [catalog, setCatalog] = useState('users');
  const [schema, setSchema] = useState('default');
  const [volumeName, setVolumeName] = useState('kasal_backups');
  const [hasDatabricksApiKey, setHasDatabricksApiKey] = useState(false);

  // Lakebase instance dropdown state
  const [lakebaseInstances, setLakebaseInstances] = useState<LakebaseInstanceOption[]>([]);
  const [loadingInstances, setLoadingInstances] = useState(false);
  const [instanceLoadError, setInstanceLoadError] = useState(false);

  // Housekeeping state
  const [housekeepingDate, setHousekeepingDate] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return d.toISOString().split('T')[0];
  });
  const [housekeepingLoading, setHousekeepingLoading] = useState(false);
  const [housekeepingResult, setHousekeepingResult] = useState<{
    success: boolean;
    deleted?: Record<string, number>;
    total_deleted?: number;
    cutoff_date?: string;
    error?: string;
  } | null>(null);
  const [showHousekeepingConfirm, setShowHousekeepingConfirm] = useState(false);

  // Feature flag - set to true to enable Lakebase feature
  const showLakebase = true;

  // Auto-scroll migration logs to bottom when new lines are added
  useEffect(() => {
    migrationLogsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [migrationLogs]);

  // Load database info on mount
  useEffect(() => {
    loadDatabaseInfo();
    loadLakebaseConfig();
    checkDatabricksApiKey();
  // eslint-disable-next-line react-hooks/exhaustive-deps
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

  const checkDatabricksApiKey = async () => {
    try {
      const apiKeysService = APIKeysService.getInstance();
      const keys = await apiKeysService.getAPIKeys();
      const databricksKey = keys.find(k => k.name === 'DATABRICKS_API_KEY');
      setHasDatabricksApiKey(!!databricksKey && !!databricksKey.value);
    } catch (error) {
      console.error('Failed to check DATABRICKS_API_KEY:', error);
      setHasDatabricksApiKey(false);
    }
  };

  const loadLakebaseInstances = useCallback(async () => {
    try {
      setLoadingInstances(true);
      setInstanceLoadError(false);
      const response = await apiClient.get<LakebaseInstanceOption[]>('/database-management/lakebase/instances');
      setLakebaseInstances(response.data || []);
    } catch (err) {
      console.error('Failed to load Lakebase instances:', err);
      setInstanceLoadError(true);
      setLakebaseInstances([]);
    } finally {
      setLoadingInstances(false);
    }
  }, []);

  const checkLakebaseInstance = useCallback(async (instanceName: string) => {
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

      setLakebaseConfig({
        instance_status: status,
        endpoint: response.data.read_write_dns,
        created_at: response.data.created_at
      });
    } catch (err: unknown) {
      if (isErrorWithResponse(err) && err.response?.status === 404) {
        setLakebaseConfig({ instance_status: 'NOT_CREATED' });
      } else {
        console.error('Failed to check Lakebase instance:', err);
      }
    } finally {
      setCheckingInstance(false);
    }
  }, [setCheckingInstance, setLakebaseConfig]);

  const loadLakebaseConfig = useCallback(async () => {
    try {
      setCheckingInstance(true);
      const response = await apiClient.get<LakebaseConfig>('/database-management/lakebase/config');

      if (response.data) {
        setLakebaseConfig(response.data);
        setLakebaseBackend(response.data.enabled ? 'lakebase' : 'disabled');

        // If config has instance name and is enabled, check instance status
        if (response.data.enabled && response.data.instance_name) {
          await checkLakebaseInstance(response.data.instance_name);
        }
      }
    } catch (err: unknown) {
      // Config not found is OK - it means Lakebase hasn't been set up yet
      if (!isErrorWithResponse(err) || err.response?.status !== 404) {
        console.error('Failed to load Lakebase configuration:', err);
      }
    } finally {
      setCheckingInstance(false);
    }
  }, [setLakebaseConfig, setCheckingInstance, checkLakebaseInstance]);

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

      setLakebaseConfig({
        enabled: true,
        instance_status: 'READY',
        endpoint: response.data.read_write_dns,
        created_at: response.data.created_at
      });

      setSuccess(`Lakebase instance "${response.data.name}" created successfully!`);

      // New instance has no kasal schema — show migration dialog
      setSchemaExists(false);
      setMigrationOption('recreate');
      setShowMigrationDialog(true);
    } catch (err: unknown) {
      const errorMessage = isErrorWithResponse(err)
        ? err.response?.data?.detail || 'Failed to create Lakebase instance'
        : 'Failed to create Lakebase instance';
      setError(errorMessage);
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
    } catch (err: unknown) {
      const errorMessage = isErrorWithResponse(err)
        ? err.response?.data?.detail || 'Failed to save Lakebase configuration'
        : 'Failed to save Lakebase configuration';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
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
    } catch (err: unknown) {
      const errorMessage = isErrorWithResponse(err)
        ? err.response?.data?.error || 'Failed to load backups'
        : 'Failed to load backups';
      setError(errorMessage);
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
    } catch (err: unknown) {
      const errorMessage = isErrorWithResponse(err)
        ? err.response?.data?.error || 'Failed to export database'
        : 'Failed to export database';
      setError(errorMessage);
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
    } catch (err: unknown) {
      const errorMessage = isErrorWithResponse(err)
        ? err.response?.data?.error || 'Failed to import database'
        : 'Failed to import database';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const migrateLakebase = async (recreateSchema = false, migrateData = true) => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(''); // Clear previous success messages
      setMigrationLogs([]); // Clear previous logs
      setMigrationLogsDialog(true); // Open logs dialog

      // Build headers from apiClient
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      };

      // Add auth token if available
      const token = localStorage.getItem('token');
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }

      // Add group context if available
      const selectedGroupId = localStorage.getItem('selectedGroupId');
      if (selectedGroupId) {
        headers['group_id'] = selectedGroupId;
      }

      // Use streaming endpoint with backend URL from config
      const response = await fetch(`${config.apiUrl}/database-management/lakebase/migrate/stream`, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({
          instance_name: lakebaseConfig.instance_name,
          endpoint: lakebaseConfig.endpoint,
          recreate_schema: recreateSchema,
          migrate_data: migrateData
        })
      });

      if (!response.ok) {
        throw new Error(`Migration failed: ${response.statusText}`);
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      const allLogs: string[] = [];

      while (reader) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const event = JSON.parse(line.substring(6));
              let logMessage = '';

              // Handle different event types — all progress goes to the logs dialog only.
              // The success banner gets a single final message when migration completes.
              switch (event.type) {
                case 'start':
                case 'info':
                case 'progress':
                case 'success':
                case 'warning':
                  logMessage = event.message;
                  allLogs.push(logMessage);
                  setMigrationLogs([...allLogs]);
                  break;

                case 'table_start':
                  logMessage = `[${event.progress}/${event.total}] ${event.message}`;
                  allLogs.push(logMessage);
                  setMigrationLogs([...allLogs]);
                  break;

                case 'table_complete':
                  logMessage = `[${event.progress}/${event.total}] ${event.message}`;
                  allLogs.push(logMessage);
                  setMigrationLogs([...allLogs]);
                  break;

                case 'table_error':
                  logMessage = `[${event.progress || '?'}/${event.total || '?'}] ${event.message}`;
                  allLogs.push(logMessage);
                  setMigrationLogs([...allLogs]);
                  break;

                case 'complete':
                  allLogs.push('');
                  allLogs.push(event.message);
                  setMigrationLogs([...allLogs]);
                  break;

                case 'result': {
                  // Final result with full details
                  allLogs.push('');
                  allLogs.push('📊 Migration Summary:');
                  allLogs.push(`  • Tables migrated: ${event.total_tables}`);
                  allLogs.push(`  • Total rows: ${event.total_rows.toLocaleString()}`);
                  allLogs.push(`  • Duration: ${event.duration.toFixed(2)} seconds`);

                  if (!event.success && event.failed_tables_details?.length > 0) {
                    allLogs.push('');
                    allLogs.push('⚠️ Failed tables:');
                    event.failed_tables_details.forEach((failed: FailedTableDetail) => {
                      allLogs.push(`  • ${failed.table}: ${failed.error_type} - ${failed.error_message}`);
                    });
                  }

                  setMigrationLogs([...allLogs]);

                  // Show final summary in the appropriate banner
                  if (event.success) {
                    setSuccess(`Migration complete — ${event.total_tables} tables, ${event.total_rows.toLocaleString()} rows in ${event.duration.toFixed(1)}s`);
                  } else {
                    setError(`Migration failed — ${event.failed_tables_details?.length || 0} table(s) failed. Lakebase disabled, using SQLite.`);
                  }
                  break;
                }

                case 'error':
                  setError(event.message);
                  setLoading(false);
                  return;
              }
            } catch (parseError) {
              console.error('Failed to parse SSE event:', line, parseError);
            }
          }
        }
      }

      // Refresh config and database info to reflect the backend switch
      await loadLakebaseConfig();
      await loadDatabaseInfo();
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to start migration';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const handleConfirmDisableLakebase = async () => {
    setDisableConfirmDialog(false);

    // Proceed with disabling Lakebase
    setLakebaseBackend('disabled');
    setLakebaseConfig({ enabled: false });

    try {
      setLoading(true);
      setError(null);

      const configToSave = {
        ...lakebaseConfig,
        enabled: false
      };

      await apiClient.post<LakebaseConfig>('/database-management/lakebase/config', configToSave);
      setSuccess('Lakebase disabled. All sessions switched back to SQLite/PostgreSQL.');
      // Reload database info to reflect the backend change in the General tab
      await loadDatabaseInfo();
    } catch (err: unknown) {
      const errorMessage = isErrorWithResponse(err)
        ? err.response?.data?.detail || 'Failed to disable Lakebase'
        : 'Failed to disable Lakebase';
      setError(errorMessage);
      // Revert the UI state on error
      setLakebaseBackend('lakebase');
      setLakebaseConfig({ enabled: true });
    } finally {
      setLoading(false);
    }
  };

  const getViewInDatabricksUrl = async () => {
    try {
      // Get workspace info from the new endpoint
      const response = await apiClient.get('/database-management/lakebase/workspace-info');

      if (response.data.success) {
        const { workspace_url, organization_id } = response.data;

        // Construct the URL for database instances with organization ID
        // Format: https://{workspace}/compute/database-instances/{instance_name}?o={org_id}
        return `${workspace_url}/compute/database-instances/${lakebaseConfig.instance_name}?o=${organization_id}`;
      }

      // Fallback: try to get from window location if running in Databricks
      if (window.location.hostname.includes('databricks')) {
        const workspaceUrl = `https://${window.location.hostname}`;
        // Without org ID, the URL might not work, but it's better than nothing
        return `${workspaceUrl}/compute/database-instances/${lakebaseConfig.instance_name}`;
      }

      return '#';
    } catch (err) {
      console.error('Failed to get Databricks workspace URL:', err);

      // Fallback: try to get from window location if running in Databricks
      if (window.location.hostname.includes('databricks')) {
        const workspaceUrl = `https://${window.location.hostname}`;
        return `${workspaceUrl}/compute/database-instances/${lakebaseConfig.instance_name}`;
      }

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
          {showLakebase && <Tab label="Lakebase" icon={<DataObjectIcon />} />}
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
                <Grid item xs={12}>
                  <Alert severity={
                    databaseInfo.database_type === 'lakebase'
                      ? (databaseInfo.connection_error ? 'warning' : 'success')
                      : 'info'
                  }>
                    <Typography variant="body2" fontWeight="bold">
                      Current Database Backend: {databaseInfo.database_type?.toUpperCase()}
                    </Typography>
                    {databaseInfo.database_type === 'lakebase' && databaseInfo.lakebase_instance && (
                      <Typography variant="caption" display="block">
                        Connected to Lakebase instance: {databaseInfo.lakebase_instance}
                      </Typography>
                    )}
                    {databaseInfo.connection_error && (
                      <Typography variant="caption" display="block" color="warning.dark" sx={{ mt: 0.5 }}>
                        {databaseInfo.connection_error}
                      </Typography>
                    )}
                  </Alert>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Typography variant="body2" color="text.secondary">Type</Typography>
                  <Typography variant="body1">{databaseInfo.database_type}</Typography>
                </Grid>

                {/* Lakebase-specific information */}
                {databaseInfo.database_type === 'lakebase' && databaseInfo.lakebase_endpoint && (
                  <Grid item xs={12}>
                    <Typography variant="body2" color="text.secondary">Endpoint</Typography>
                    <Typography variant="body1" sx={{ wordBreak: 'break-all' }}>
                      {databaseInfo.lakebase_endpoint}
                    </Typography>
                  </Grid>
                )}

                {/* SQLite/PostgreSQL-specific information */}
                {databaseInfo.database_type !== 'lakebase' && (
                  <>
                    <Grid item xs={12} md={6}>
                      <Typography variant="body2" color="text.secondary">Size</Typography>
                      <Typography variant="body1">{formatSize(databaseInfo.size_mb || 0)}</Typography>
                    </Grid>
                    {databaseInfo.database_path && (
                      <Grid item xs={12}>
                        <Typography variant="body2" color="text.secondary">Path</Typography>
                        <Typography variant="body1" sx={{ wordBreak: 'break-all' }}>
                          {databaseInfo.database_path}
                        </Typography>
                      </Grid>
                    )}
                    <Grid item xs={12} md={6}>
                      <Typography variant="body2" color="text.secondary">Created</Typography>
                      <Typography variant="body1">{formatDate(databaseInfo.created_at || '')}</Typography>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Typography variant="body2" color="text.secondary">Modified</Typography>
                      <Typography variant="body1">{formatDate(databaseInfo.modified_at || '')}</Typography>
                    </Grid>
                  </>
                )}
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
        {/* Data Housekeeping Card */}
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Data Housekeeping
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Delete execution history, traces, logs, and LLM logs older than a specified date.
              This can reduce database size and speed up migrations.
            </Typography>
            <Box sx={{ display: 'flex', gap: 2, alignItems: 'flex-start', flexWrap: 'wrap' }}>
              <TextField
                type="date"
                label="Cutoff Date"
                value={housekeepingDate}
                onChange={(e) => {
                  setHousekeepingDate(e.target.value);
                  setHousekeepingResult(null);
                }}
                InputLabelProps={{ shrink: true }}
                helperText="Records older than this date will be deleted"
                sx={{ minWidth: 200 }}
              />
              <Button
                variant="outlined"
                color="warning"
                startIcon={housekeepingLoading ? <CircularProgress size={18} /> : <DeleteSweepIcon />}
                onClick={() => setShowHousekeepingConfirm(true)}
                disabled={housekeepingLoading || !housekeepingDate}
                sx={{ mt: 1 }}
              >
                {housekeepingLoading ? 'Running...' : 'Run Housekeeping'}
              </Button>
            </Box>
            {housekeepingResult && housekeepingResult.success && (
              <Alert severity="success" sx={{ mt: 2 }}>
                <Typography variant="body2" fontWeight="bold">
                  Housekeeping completed — {housekeepingResult.total_deleted} records deleted
                </Typography>
                <Box sx={{ mt: 1 }}>
                  {housekeepingResult.deleted && Object.entries(housekeepingResult.deleted).map(([table, count]) => (
                    <Chip
                      key={table}
                      label={`${table}: ${count}`}
                      size="small"
                      sx={{ m: 0.5 }}
                      color={count > 0 ? 'primary' : 'default'}
                      variant={count > 0 ? 'filled' : 'outlined'}
                    />
                  ))}
                </Box>
              </Alert>
            )}
            {housekeepingResult && !housekeepingResult.success && (
              <Alert severity="error" sx={{ mt: 2 }}>
                {housekeepingResult.error || 'Housekeeping failed'}
              </Alert>
            )}
          </CardContent>
        </Card>

        {/* Housekeeping Confirmation Dialog */}
        <Dialog open={showHousekeepingConfirm} onClose={() => setShowHousekeepingConfirm(false)}>
          <DialogTitle>Confirm Data Housekeeping</DialogTitle>
          <DialogContent>
            <DialogContentText>
              This will permanently delete all records older than <strong>{housekeepingDate}</strong> from the following tables:
            </DialogContentText>
            <Box sx={{ mt: 1, ml: 2 }}>
              <Typography variant="body2">• executionhistory (+ taskstatus, errortrace)</Typography>
              <Typography variant="body2">• execution_trace</Typography>
              <Typography variant="body2">• execution_logs</Typography>
              <Typography variant="body2">• llmlog</Typography>
            </Box>
            <DialogContentText sx={{ mt: 2 }}>
              This action cannot be undone. Are you sure?
            </DialogContentText>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setShowHousekeepingConfirm(false)}>Cancel</Button>
            <Button
              color="warning"
              variant="contained"
              onClick={async () => {
                setShowHousekeepingConfirm(false);
                setHousekeepingLoading(true);
                setHousekeepingResult(null);
                try {
                  const response = await apiClient.post('/database-management/housekeeping', {
                    cutoff_date: housekeepingDate,
                  });
                  setHousekeepingResult(response.data);
                  // Refresh database info to show updated row counts
                  loadDatabaseInfo();
                } catch (err) {
                  if (isErrorWithResponse(err)) {
                    setHousekeepingResult({
                      success: false,
                      error: err.response?.data?.detail || err.response?.data?.error || err.message || 'Housekeeping failed',
                    });
                  } else {
                    setHousekeepingResult({ success: false, error: 'Housekeeping failed' });
                  }
                } finally {
                  setHousekeepingLoading(false);
                }
              }}
            >
              Delete Old Data
            </Button>
          </DialogActions>
        </Dialog>
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

            <Box sx={{ display: 'flex', gap: 2, mb: 3, flexDirection: 'row', alignItems: 'flex-start' }}>
              <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                <Button
                  variant="contained"
                  startIcon={<UploadIcon />}
                  onClick={() => setExportDialog(true)}
                  disabled={!hasDatabricksApiKey}
                >
                  Export to Volume
                </Button>
                {!hasDatabricksApiKey && (
                  <FormHelperText error sx={{ ml: 1, mt: 0.5 }}>
                    Please set DATABRICKS_API_KEY in API Keys before exporting
                  </FormHelperText>
                )}
              </Box>
              <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                <Button
                  variant="outlined"
                  startIcon={<DownloadIcon />}
                  onClick={() => {
                    loadBackups();
                    setImportDialog(true);
                  }}
                  disabled={!hasDatabricksApiKey}
                >
                  Import from Volume
                </Button>
                {!hasDatabricksApiKey && (
                  <FormHelperText error sx={{ ml: 1, mt: 0.5 }}>
                    Please set DATABRICKS_API_KEY in API Keys before importing
                  </FormHelperText>
                )}
              </Box>
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
      {showLakebase && <TabPanel value={tabValue} index={2}>
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
              onChange={(e) => {
                const newValue = e.target.value as 'disabled' | 'lakebase';

                // If switching to disabled, show confirmation dialog
                if (newValue === 'disabled' && lakebaseBackend === 'lakebase') {
                  setDisableConfirmDialog(true);
                  return;
                }

                // Selecting "Databricks Lakebase" just expands the configuration form.
                // Actual enabling happens when the user connects to or creates an instance.
                setLakebaseBackend(newValue);
                setError(null);
                setSuccess(null);
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
                    {migrationLogs.length > 0 && (
                      <Button
                        variant="outlined"
                        size="small"
                        startIcon={<DataObjectIcon />}
                        onClick={() => setMigrationLogsDialog(true)}
                        sx={{ ml: 1 }}
                      >
                        Migration Logs
                      </Button>
                    )}
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
                          Connect to Existing Kasal
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Connect to an existing Lakebase instance
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
                        {instanceLoadError ? (
                          /* Fallback: show text field if API call fails */
                          <TextField
                            fullWidth
                            label="Instance Name"
                            value={lakebaseConfig.instance_name}
                            onChange={(e) => setLakebaseConfig({ ...lakebaseConfig, instance_name: e.target.value })}
                            helperText={
                              <Box component="span" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                                Could not load instances. Enter the name manually.
                                <Button size="small" onClick={loadLakebaseInstances} sx={{ minWidth: 'auto', p: 0, textTransform: 'none' }}>
                                  Retry
                                </Button>
                              </Box>
                            }
                            placeholder="kasal-lakebase"
                            required
                          />
                        ) : (
                          <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
                            <FormControl fullWidth required>
                              <InputLabel>Instance Name</InputLabel>
                              <Select
                                value={lakebaseConfig.instance_name || ''}
                                onChange={(e) => {
                                  const selectedName = e.target.value;
                                  const selectedInstance = lakebaseInstances.find(i => i.name === selectedName);
                                  setLakebaseConfig({
                                    ...lakebaseConfig,
                                    instance_name: selectedName,
                                    endpoint: selectedInstance?.read_write_dns || '',
                                    instance_status: selectedInstance?.read_write_dns ? 'READY' : 'NOT_CREATED'
                                  });
                                }}
                                label="Instance Name"
                                onOpen={() => {
                                  if (lakebaseInstances.length === 0 && !loadingInstances) {
                                    loadLakebaseInstances();
                                  }
                                }}
                                disabled={loadingInstances}
                                startAdornment={loadingInstances ? <CircularProgress size={18} sx={{ mr: 1 }} /> : undefined}
                              >
                                {lakebaseInstances.length === 0 && !loadingInstances && (
                                  <MenuItem disabled value="">
                                    <em>No instances found</em>
                                  </MenuItem>
                                )}
                                {lakebaseInstances.map((inst) => (
                                  <MenuItem key={inst.name} value={inst.name}>
                                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                                      <Typography>{inst.name}</Typography>
                                      <Chip
                                        label={inst.state || 'UNKNOWN'}
                                        size="small"
                                        color={
                                          inst.state === 'READY' || inst.state === 'AVAILABLE' || inst.state === 'RUNNING'
                                            ? 'success'
                                            : inst.state === 'STOPPED' || inst.state === 'STOPPING'
                                            ? 'warning'
                                            : inst.state === 'ERROR' || inst.state === 'FAILED'
                                            ? 'error'
                                            : 'default'
                                        }
                                        sx={{ ml: 'auto' }}
                                      />
                                    </Box>
                                  </MenuItem>
                                ))}
                              </Select>
                              <FormHelperText>Select an existing Lakebase instance from your workspace</FormHelperText>
                            </FormControl>
                            <IconButton
                              onClick={loadLakebaseInstances}
                              disabled={loadingInstances}
                              title="Refresh instances"
                              sx={{ mt: 1 }}
                            >
                              <RefreshIcon />
                            </IconButton>
                          </Box>
                        )}
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
                          helperText="Auto-populated from selected instance, or enter manually"
                          placeholder="instance-xxxx.database.cloud.databricks.com"
                        />
                      </Grid>

                      {/* Setup Option */}
                      <Grid item xs={12}>
                        <FormControl component="fieldset">
                          <FormLabel component="legend" sx={{ mb: 1 }}>
                            Setup Option
                          </FormLabel>
                          <RadioGroup
                            value={migrationOption}
                            onChange={(e) => setMigrationOption(e.target.value as 'recreate' | 'use' | 'schema_only')}
                          >
                            <FormControlLabel
                              value="recreate"
                              control={<Radio />}
                              label={
                                <Box>
                                  <Typography variant="body2" fontWeight="bold">Migrate Schema & Data</Typography>
                                  <Typography variant="caption" color="text.secondary">
                                    Create schema and copy all data from current database
                                  </Typography>
                                </Box>
                              }
                            />
                            <FormControlLabel
                              value="schema_only"
                              control={<Radio />}
                              label={
                                <Box>
                                  <Typography variant="body2" fontWeight="bold">Schema Only</Typography>
                                  <Typography variant="caption" color="text.secondary">
                                    Create empty tables without migrating data
                                  </Typography>
                                </Box>
                              }
                            />
                            <FormControlLabel
                              value="use"
                              control={<Radio />}
                              label={
                                <Box>
                                  <Typography variant="body2" fontWeight="bold">Use Existing Data</Typography>
                                  <Typography variant="caption" color="text.secondary">
                                    Instance already has Kasal schema and data — just connect
                                  </Typography>
                                </Box>
                              }
                            />
                          </RadioGroup>
                        </FormControl>
                      </Grid>

                      <Grid item xs={12}>
                        <Button
                          variant="contained"
                          onClick={async () => {
                            try {
                              setLoading(true);
                              setError(null);

                              // First check if the instance exists
                              await checkLakebaseInstance(lakebaseConfig.instance_name);

                              // Test connection and get schema/migration status
                              const testResponse = await apiClient.get(`/database-management/lakebase/test/${lakebaseConfig.instance_name}`);

                              // Check if the connection test itself failed
                              if (!testResponse.data.success) {
                                setError(`Connection test failed: ${testResponse.data.error || 'Unknown error'}. Verify the instance is running and accessible.`);
                                return;
                              }

                              // Save the configuration
                              await saveLakebaseConfig();

                              const hasSchema = testResponse.data.has_kasal_schema;
                              setSchemaExists(hasSchema);

                              if (migrationOption === 'use') {
                                // Use existing — verify schema exists, then enable directly
                                if (!hasSchema) {
                                  setError('No kasal schema found on this instance. Please use "Migrate Schema & Data" or "Schema Only" instead.');
                                  return;
                                }
                                const response = await apiClient.post('/database-management/lakebase/enable', {
                                  instance_name: lakebaseConfig.instance_name,
                                  endpoint: lakebaseConfig.endpoint
                                });
                                if (response.data.success) {
                                  setSuccess(`Connected to Lakebase. Using existing schema with ${testResponse.data.table_count} table(s).`);
                                  // Refresh config and info in background — don't block the button
                                  loadLakebaseConfig().catch(() => {});
                                  loadDatabaseInfo().catch(() => {});
                                }
                              } else {
                                // Migrate schema+data or schema only
                                const migrateData = migrationOption === 'recreate';
                                setSuccess(`Connected successfully! Starting ${migrateData ? 'schema & data migration' : 'schema creation'}...`);
                                await migrateLakebase(true, migrateData);
                              }
                            } catch (err: unknown) {
                              const errorMessage = isErrorWithResponse(err)
                                ? err.response?.data?.detail || 'Failed to connect to instance. Please verify the instance name and ensure it is running.'
                                : 'Failed to connect to instance. Please verify the instance name and ensure it is running.';
                              setError(errorMessage);
                            } finally {
                              setLoading(false);
                            }
                          }}
                          disabled={!lakebaseConfig.instance_name || loading}
                        >
                          {loading ? <CircularProgress size={20} sx={{ mr: 1 }} /> : null}
                          {migrationOption === 'use' ? 'Connect' : migrationOption === 'schema_only' ? 'Connect & Create Schema' : 'Connect & Migrate'}
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

              {/* Info Alert - Only show when creating a new instance */}
              {lakebaseMode === 'create' && (
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
              )}
            </>
          )}
        </Paper>
      </TabPanel>}

      {/* Export Dialog */}
      <Dialog open={exportDialog} onClose={() => setExportDialog(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Export Database to Databricks Volume</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2 }}>
            <Alert severity="info">
              Database will be exported as a .db file to: /Volumes/{catalog}/{schema}/{volumeName}/
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
                    sx={{
                      cursor: 'pointer',
                      backgroundColor: selectedBackup === backup.filename ? 'action.selected' : 'transparent',
                      '&:hover': {
                        backgroundColor: 'action.hover'
                      }
                    }}
                    onClick={() => setSelectedBackup(backup.filename)}
                  >
                    <ListItemText
                      primary={backup.filename}
                      secondary={`${formatSize(backup.size_mb)} • ${formatDate(backup.created_at)}`}
                    />
                  </ListItem>
                ))}
              </List>
              <Alert severity="info" sx={{ mt: 2 }}>
                The selected backup will be added to the current database (data will be merged, not replaced).
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
      {/* Migration Options Dialog */}
      <Dialog
        open={showMigrationDialog}
        onClose={() => setShowMigrationDialog(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <DataObjectIcon color="primary" />
            Setup Lakebase
          </Box>
        </DialogTitle>
        <DialogContent>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            {schemaExists
              ? 'A kasal schema already exists in Lakebase. Choose how to proceed.'
              : 'Choose how to set up your Lakebase instance.'}
          </Typography>

          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            {/* Primary action: Migrate Data */}
            <Button
              variant="contained"
              color="primary"
              fullWidth
              disabled={loading}
              startIcon={<UploadIcon />}
              onClick={async () => {
                setShowMigrationDialog(false);
                await migrateLakebase(true, true);
              }}
              sx={{ justifyContent: 'flex-start', py: 1.5, px: 2, textTransform: 'none' }}
            >
              <Box sx={{ textAlign: 'left' }}>
                <Typography variant="body1" fontWeight="bold">
                  {schemaExists ? 'Recreate and Migrate Data' : 'Migrate Data'}
                </Typography>
                <Typography variant="caption" sx={{ opacity: 0.85 }}>
                  {schemaExists
                    ? 'Reset schema and copy all data from current database'
                    : 'Create schema and copy all data from current database'}
                </Typography>
              </Box>
            </Button>

            {/* Secondary action: Schema Only */}
            <Button
              variant="outlined"
              fullWidth
              disabled={loading}
              startIcon={<StorageIcon />}
              onClick={async () => {
                setShowMigrationDialog(false);
                await migrateLakebase(true, false);
              }}
              sx={{ justifyContent: 'flex-start', py: 1.5, px: 2, textTransform: 'none' }}
            >
              <Box sx={{ textAlign: 'left' }}>
                <Typography variant="body1" fontWeight="bold">
                  {schemaExists ? 'Recreate Schema Only' : 'Schema Only'}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  Create empty tables without migrating data
                </Typography>
              </Box>
            </Button>

            {/* Use Existing / Connect without migration */}
            {schemaExists ? (
              <Button
                variant="outlined"
                fullWidth
                startIcon={<CheckIcon />}
                onClick={async () => {
                  setShowMigrationDialog(false);
                  try {
                    setLoading(true);
                    const response = await apiClient.post('/database-management/lakebase/enable', {
                      instance_name: lakebaseConfig.instance_name,
                      endpoint: lakebaseConfig.endpoint
                    });
                    if (response.data.success) {
                      setSuccess('Connected to Lakebase. Using existing schema.');
                      await loadLakebaseConfig();
                      await loadDatabaseInfo();
                    }
                  } catch (err: unknown) {
                    const errorMessage = isErrorWithResponse(err)
                      ? err.response?.data?.detail || 'Failed to enable Lakebase'
                      : 'Failed to enable Lakebase';
                    setError(errorMessage);
                  } finally {
                    setLoading(false);
                  }
                }}
                sx={{ justifyContent: 'flex-start', py: 1.5, px: 2, textTransform: 'none' }}
              >
                <Box sx={{ textAlign: 'left' }}>
                  <Typography variant="body1" fontWeight="bold">
                    Use Existing Schema
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Connect to the existing kasal schema as-is
                  </Typography>
                </Box>
              </Button>
            ) : null}
          </Box>

          {schemaExists && (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Recreate options will delete existing data in the kasal schema.
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowMigrationDialog(false)}>
            Cancel
          </Button>
        </DialogActions>
      </Dialog>

      {/* Disable Lakebase Confirmation Dialog */}
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

      {/* Migration Logs Dialog */}
      <Dialog
        open={migrationLogsDialog}
        onClose={() => setMigrationLogsDialog(false)}
        maxWidth="lg"
        fullWidth
        PaperProps={{
          sx: { height: '80vh' }
        }}
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <DataObjectIcon />
            Migration Logs
            {loading && <CircularProgress size={20} sx={{ ml: 1 }} />}
          </Box>
        </DialogTitle>
        <DialogContent dividers>
          <Box
            sx={{
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              whiteSpace: 'pre-wrap',
              bgcolor: 'grey.900',
              color: 'grey.100',
              p: 2,
              borderRadius: 1,
              height: '100%',
              overflow: 'auto',
              '&::-webkit-scrollbar': {
                width: '8px',
              },
              '&::-webkit-scrollbar-track': {
                bgcolor: 'grey.800',
              },
              '&::-webkit-scrollbar-thumb': {
                bgcolor: 'grey.600',
                borderRadius: '4px',
                '&:hover': {
                  bgcolor: 'grey.500',
                },
              },
            }}
          >
            {migrationLogs.length === 0 ? (
              <Typography color="grey.400">
                Waiting for migration to start...
              </Typography>
            ) : (
              migrationLogs.map((log, index) => (
                <Box key={index} sx={{ mb: 0.5 }}>
                  {log}
                </Box>
              ))
            )}
            <div ref={migrationLogsEndRef} />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => {
              const logText = migrationLogs.join('\n');
              const blob = new Blob([logText], { type: 'text/plain' });
              const url = URL.createObjectURL(blob);
              const a = document.createElement('a');
              a.href = url;
              a.download = `migration-logs-${new Date().toISOString()}.txt`;
              a.click();
              URL.revokeObjectURL(url);
            }}
            disabled={migrationLogs.length === 0}
          >
            Download Logs
          </Button>
          <Button onClick={() => setMigrationLogsDialog(false)}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default DatabaseManagement;