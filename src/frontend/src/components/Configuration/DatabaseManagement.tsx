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
} from '@mui/material';
import {
  CloudUpload as UploadIcon,
  CloudDownload as DownloadIcon,
  Refresh as RefreshIcon,
  Storage as StorageIcon,
  OpenInNew as OpenInNewIcon,
  Close as CloseIcon,
} from '@mui/icons-material';
import { apiClient } from '../../config/api/ApiConfig';

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
  const [showExportFiles, setShowExportFiles] = useState(false);
  
  // Export/Import form state
  const [catalog, setCatalog] = useState('users');
  const [schema, setSchema] = useState('default');
  const [volumeName, setVolumeName] = useState('kasal_backups');
  const [exportFormat, setExportFormat] = useState('sql');
  
  // Load database info on mount
  useEffect(() => {
    loadDatabaseInfo();
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

  const loadBackups = async () => {
    try {
      setLoading(true);
      const response = await apiClient.post<BackupList>('/database-management/list-backups', {
        catalog,
        schema,
        volume_name: volumeName
      });
      setBackups(response.data);
    } catch (err) {
      setError('Failed to load backups');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async () => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const response = await apiClient.post<ExportResult>('/database-management/export', {
        catalog,
        schema,
        volume_name: volumeName,
        export_format: exportFormat
      });
      
      if (response.data.success) {
        setSuccess(`Database exported successfully to ${response.data.volume_path}`);
        setExportDialog(false);
        setExportResult(response.data);
        setShowExportFiles(true);
        
        // Reload backups
        await loadBackups();
      } else {
        setError(response.data.error || 'Export failed');
      }
    } catch (err) {
      setError('Failed to export database');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleImport = async () => {
    if (!selectedBackup) {
      setError('Please select a backup to import');
      return;
    }
    
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const response = await apiClient.post<ImportResult>('/database-management/import', {
        catalog,
        schema,
        volume_name: volumeName,
        backup_filename: selectedBackup
      });
      
      if (response.data.success) {
        setSuccess(`Database imported successfully from ${selectedBackup}`);
        setImportDialog(false);
        setSelectedBackup(null);
        
        // Reload database info
        await loadDatabaseInfo();
      } else {
        setError(response.data.error || 'Import failed');
      }
    } catch (err) {
      setError('Failed to import database');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const formatSize = (sizeInMB: number | undefined) => {
    if (!sizeInMB) return 'Unknown';
    if (sizeInMB < 1) {
      return `${(sizeInMB * 1024).toFixed(2)} KB`;
    }
    return `${sizeInMB.toFixed(2)} MB`;
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleString();
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
        <StorageIcon sx={{ mr: 1.5, color: 'primary.main' }} />
        <Typography variant="h6">Database Management</Typography>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}
      
      {success && (
        <Alert severity="success" sx={{ mb: 2 }} onClose={() => setSuccess(null)}>
          {success}
        </Alert>
      )}

      {/* Database Info Card */}
      {databaseInfo?.success && (
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
              <Typography variant="subtitle1" fontWeight="medium">
                Current Database Information
              </Typography>
              <IconButton size="small" onClick={loadDatabaseInfo}>
                <RefreshIcon />
              </IconButton>
            </Box>
            
            <Grid container spacing={2}>
              <Grid item xs={12} sm={4}>
                <Typography variant="body2" color="text.secondary">Database Size</Typography>
                <Typography variant="body1">
                  {databaseInfo.size_mb ? formatSize(databaseInfo.size_mb) : 'Calculating...'}
                </Typography>
              </Grid>
              
              <Grid item xs={12} sm={4}>
                <Typography variant="body2" color="text.secondary">Total Tables</Typography>
                <Typography variant="body1">
                  {databaseInfo.total_tables || 0} tables
                </Typography>
              </Grid>
              
              <Grid item xs={12} sm={4}>
                <Typography variant="body2" color="text.secondary">Database Type</Typography>
                <Typography variant="body1">
                  {databaseInfo.database_type || 'SQLite'}
                </Typography>
              </Grid>
            </Grid>
            
            {databaseInfo.tables && Object.keys(databaseInfo.tables).length > 0 && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                  Table Record Counts
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                  {Object.entries(databaseInfo.tables).map(([table, count]) => (
                    <Chip
                      key={table}
                      label={`${table}: ${count}`}
                      size="small"
                      variant="outlined"
                    />
                  ))}
                </Box>
              </Box>
            )}
          </CardContent>
        </Card>
      )}

      {/* Action Buttons */}
      <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
        <Button
          variant="contained"
          startIcon={<UploadIcon />}
          onClick={() => setExportDialog(true)}
          disabled={loading}
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
          disabled={loading}
        >
          Import from Volume
        </Button>
      </Box>

      {/* Export Dialog */}
      <Dialog open={exportDialog} onClose={() => setExportDialog(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Export Database to Databricks Volume</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <TextField
              fullWidth
              label="Catalog"
              value={catalog}
              onChange={(e) => setCatalog(e.target.value)}
              sx={{ mb: 2 }}
              helperText="Databricks catalog name (e.g., 'users')"
            />
            
            <TextField
              fullWidth
              label="Schema"
              value={schema}
              onChange={(e) => setSchema(e.target.value)}
              sx={{ mb: 2 }}
              helperText="Databricks schema name (e.g., 'default')"
            />
            
            <TextField
              fullWidth
              label="Volume Name"
              value={volumeName}
              onChange={(e) => setVolumeName(e.target.value)}
              sx={{ mb: 2 }}
              helperText="Volume name for storing backups"
            />
            
            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel>Export Format</InputLabel>
              <Select
                value={exportFormat}
                onChange={(e) => setExportFormat(e.target.value)}
                label="Export Format"
              >
                <MenuItem value="sql">SQL Dump (.sql)</MenuItem>
                <MenuItem value="sqlite">SQLite Database (.db)</MenuItem>
              </Select>
              <FormHelperText>
                {exportFormat === 'sql' && 'Creates a SQL script with INSERT statements that can be executed to restore data'}
                {exportFormat === 'sqlite' && 'Converts to SQLite database format - a portable single-file database'}
              </FormHelperText>
            </FormControl>
            
            <Alert severity="info" sx={{ mt: 2 }}>
              The database will be exported to: /Volumes/{catalog}/{schema}/{volumeName}/
            </Alert>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setExportDialog(false)}>Cancel</Button>
          <Button
            onClick={handleExport}
            variant="contained"
            disabled={loading}
            startIcon={loading ? <CircularProgress size={16} /> : <UploadIcon />}
          >
            Export
          </Button>
        </DialogActions>
      </Dialog>

      {/* Import Dialog */}
      <Dialog open={importDialog} onClose={() => setImportDialog(false)} maxWidth="md" fullWidth>
        <DialogTitle>Import Database from Databricks Volume</DialogTitle>
        <DialogContent>
          <Box sx={{ mt: 2 }}>
            <Grid container spacing={2} sx={{ mb: 2 }}>
              <Grid item xs={4}>
                <TextField
                  fullWidth
                  label="Catalog"
                  value={catalog}
                  onChange={(e) => setCatalog(e.target.value)}
                  size="small"
                />
              </Grid>
              <Grid item xs={4}>
                <TextField
                  fullWidth
                  label="Schema"
                  value={schema}
                  onChange={(e) => setSchema(e.target.value)}
                  size="small"
                />
              </Grid>
              <Grid item xs={4}>
                <TextField
                  fullWidth
                  label="Volume Name"
                  value={volumeName}
                  onChange={(e) => setVolumeName(e.target.value)}
                  size="small"
                />
              </Grid>
            </Grid>
            
            <Button
              onClick={loadBackups}
              startIcon={<RefreshIcon />}
              disabled={loading}
              sx={{ mb: 2 }}
            >
              Load Backups
            </Button>
            
            {backups?.success && backups.backups && backups.backups.length > 0 ? (
              <Paper variant="outlined">
                <List>
                  {backups.backups.map((backup) => (
                    <ListItem
                      key={backup.filename}
                      onClick={() => setSelectedBackup(backup.filename)}
                      sx={{
                        cursor: 'pointer',
                        backgroundColor: selectedBackup === backup.filename ? 'action.selected' : 'transparent',
                        '&:hover': {
                          backgroundColor: 'action.hover'
                        }
                      }}
                    >
                      <ListItemText
                        primary={backup.filename}
                        secondary={`${formatSize(backup.size_mb)} • ${formatDate(backup.created_at)}`}
                      />
                      <ListItemSecondaryAction>
                        <IconButton
                          edge="end"
                          onClick={() => window.open(backup.databricks_url, '_blank')}
                          title="Open in Databricks"
                        >
                          <OpenInNewIcon />
                        </IconButton>
                      </ListItemSecondaryAction>
                    </ListItem>
                  ))}
                </List>
              </Paper>
            ) : (
              <Alert severity="info">
                No backups found in {catalog}.{schema}.{volumeName}
              </Alert>
            )}
            
            {selectedBackup && (
              <Alert severity="warning" sx={{ mt: 2 }}>
                Warning: Importing will replace the current database with the selected backup.
              </Alert>
            )}
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setImportDialog(false)}>Cancel</Button>
          <Button
            onClick={handleImport}
            variant="contained"
            color="warning"
            disabled={loading || !selectedBackup}
            startIcon={loading ? <CircularProgress size={16} /> : <DownloadIcon />}
          >
            Import Selected
          </Button>
        </DialogActions>
      </Dialog>

      {/* Export Files Dialog */}
      <Dialog
        open={showExportFiles}
        onClose={() => setShowExportFiles(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Typography variant="h6">Export Completed Successfully</Typography>
            <IconButton
              edge="end"
              onClick={() => setShowExportFiles(false)}
              aria-label="close"
            >
              <CloseIcon />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          {exportResult && exportResult.success && (
            <Box>
              <Alert severity="success" sx={{ mb: 2 }}>
                Database exported to volume: {exportResult.volume_path}
              </Alert>
              
              <Typography variant="subtitle1" sx={{ mb: 1, fontWeight: 'medium' }}>
                Export Details:
              </Typography>
              
              <Box sx={{ mb: 3 }}>
                <Grid container spacing={2}>
                  <Grid item xs={12} sm={6}>
                    <Typography variant="body2" color="text.secondary">Backup Filename</Typography>
                    <Typography variant="body1">{exportResult.backup_filename}</Typography>
                  </Grid>
                  <Grid item xs={12} sm={6}>
                    <Typography variant="body2" color="text.secondary">Size</Typography>
                    <Typography variant="body1">{formatSize(exportResult.size_mb || 0)}</Typography>
                  </Grid>
                </Grid>
              </Box>

              <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 'medium' }}>
                Browse Exported Files in Databricks:
              </Typography>
              
              {exportResult.volume_browse_url && (
                <Paper elevation={0} sx={{ p: 2, backgroundColor: 'grey.50', mb: 3 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Box>
                      <Typography variant="body1" fontWeight="medium">
                        Databricks Volume
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        Browse all backups in {exportResult.volume_path}
                      </Typography>
                    </Box>
                    <Button
                      endIcon={<OpenInNewIcon />}
                      onClick={() => window.open(exportResult.volume_browse_url, '_blank')}
                      variant="contained"
                      size="small"
                    >
                      Open Volume
                    </Button>
                  </Box>
                </Paper>
              )}

              {exportResult.export_files && exportResult.export_files.length > 0 && (
                <Box sx={{ mt: 3 }}>
                  <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 'medium' }}>
                    All Available Backups in Volume:
                  </Typography>
                  <Paper elevation={0} sx={{ p: 1, backgroundColor: 'grey.50' }}>
                    <List dense>
                      {exportResult.export_files.map((file) => (
                        <ListItem key={file.filename}>
                          <ListItemText
                            primary={file.filename}
                            secondary={`${formatSize(file.size_mb)} • ${formatDate(file.created_at)}`}
                          />
                        </ListItem>
                      ))}
                    </List>
                  </Paper>
                </Box>
              )}
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowExportFiles(false)} variant="contained">
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default DatabaseManagement;