import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Alert,
  AlertTitle,
  CircularProgress,
  LinearProgress,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Snackbar,
} from '@mui/material';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import CloudDownloadIcon from '@mui/icons-material/CloudDownload';
import DatabaseIcon from '@mui/icons-material/Storage';
import InfoIcon from '@mui/icons-material/Info';
import WarningIcon from '@mui/icons-material/Warning';
import { useTranslation } from 'react-i18next';
import { DatabaseService, DatabaseStatus } from '../../../api/DatabaseService';

const DatabaseConfiguration: React.FC = () => {
  const { t } = useTranslation();
  const [status, setStatus] = useState<DatabaseStatus | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [exportLoading, setExportLoading] = useState<boolean>(false);
  const [importLoading, setImportLoading] = useState<boolean>(false);
  const [notification, setNotification] = useState({
    open: false,
    message: '',
    severity: 'success' as 'success' | 'error',
  });
  const fileInputRef = useRef<HTMLInputElement>(null);
  const databaseService = DatabaseService.getInstance();

  const loadDatabaseStatus = useCallback(async () => {
    try {
      setLoading(true);
      const databaseStatus = await databaseService.getDatabaseStatus();
      setStatus(databaseStatus);
    } catch (error) {
      console.error('Error fetching database status:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to load database status',
        severity: 'error',
      });
    } finally {
      setLoading(false);
    }
  }, [databaseService]);

  // Load database status on component mount
  useEffect(() => {
    loadDatabaseStatus();
  }, [loadDatabaseStatus]);

  const handleExportDatabase = async () => {
    try {
      setExportLoading(true);
      await databaseService.exportDatabase();
      setNotification({
        open: true,
        message: t('database.exportSuccess', { defaultValue: 'Database exported successfully' }),
        severity: 'success',
      });
    } catch (error) {
      console.error('Error exporting database:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to export database',
        severity: 'error',
      });
    } finally {
      setExportLoading(false);
    }
  };

  const handleImportButtonClick = () => {
    if (fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files || files.length === 0) return;

    const file = files[0];
    
    try {
      setImportLoading(true);
      await databaseService.importDatabase(file);
      setNotification({
        open: true,
        message: t('database.importSuccess', { defaultValue: 'Database imported successfully. The application will reload.' }),
        severity: 'success',
      });
      
      // Clear file input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
      
      // Reload status
      await loadDatabaseStatus();
      
      // Reload page after a short delay to ensure the database is loaded properly
      setTimeout(() => {
        window.location.reload();
      }, 3000);
    } catch (error) {
      console.error('Error importing database:', error);
      setNotification({
        open: true,
        message: error instanceof Error ? error.message : 'Failed to import database',
        severity: 'error',
      });
    } finally {
      setImportLoading(false);
    }
  };

  const handleCloseNotification = () => {
    setNotification({
      ...notification,
      open: false,
    });
  };

  if (loading) {
    return (
      <Box sx={{ p: 2 }}>
        <LinearProgress />
        <Typography variant="body1" sx={{ mt: 2 }}>
          {t('database.loadingStatus', { defaultValue: 'Loading database status...' })}
        </Typography>
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h6" sx={{ mb: 2 }}>
        {t('database.title', { defaultValue: 'Database Management' })}
      </Typography>
      
      <Alert severity="info" sx={{ mb: 3 }}>
        <AlertTitle>{t('database.backupInfo', { defaultValue: 'Database Backup & Restore' })}</AlertTitle>
        {t('database.description', { 
          defaultValue: 'Export your database to create a backup of all your configurations, agents, tasks, and other settings. Import a database file to restore your system to a previous state.' 
        })}
      </Alert>

      {/* Database Status */}
      <Paper sx={{ p: 2, mb: 3 }} elevation={1}>
        <Typography variant="subtitle1" sx={{ mb: 1, fontWeight: 'medium', display: 'flex', alignItems: 'center' }}>
          <DatabaseIcon sx={{ mr: 1, fontSize: '1.2rem' }} />
          {t('database.status', { defaultValue: 'Database Status' })}
        </Typography>
        
        {status ? (
          <List dense>
            <ListItem>
              <ListItemIcon sx={{ minWidth: 36 }}>
                <InfoIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText 
                primary={t('database.size', { defaultValue: 'Size' })}
                secondary={status.size_human}
              />
            </ListItem>
            <ListItem>
              <ListItemIcon sx={{ minWidth: 36 }}>
                <InfoIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText 
                primary={t('database.lastModified', { defaultValue: 'Last Modified' })}
                secondary={status.last_modified ? new Date(status.last_modified).toLocaleString() : 'N/A'}
              />
            </ListItem>
            <ListItem>
              <ListItemIcon sx={{ minWidth: 36 }}>
                <InfoIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText 
                primary={t('database.path', { defaultValue: 'Path' })}
                secondary={status.path}
              />
            </ListItem>
          </List>
        ) : (
          <Typography variant="body2" color="text.secondary">
            {t('database.noStatus', { defaultValue: 'Database status information is not available.' })}
          </Typography>
        )}
      </Paper>

      {/* Import/Export Buttons */}
      <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
        <Button
          variant="contained"
          startIcon={exportLoading ? <CircularProgress size={20} color="inherit" /> : <CloudDownloadIcon />}
          onClick={handleExportDatabase}
          disabled={exportLoading || importLoading}
        >
          {t('database.export', { defaultValue: 'Export Database' })}
        </Button>
        
        <Button
          variant="outlined"
          color="primary"
          startIcon={importLoading ? <CircularProgress size={20} color="inherit" /> : <CloudUploadIcon />}
          onClick={handleImportButtonClick}
          disabled={exportLoading || importLoading}
        >
          {t('database.import', { defaultValue: 'Import Database' })}
        </Button>
        
        {/* Hidden file input for database import */}
        <input
          type="file"
          accept=".db"
          ref={fileInputRef}
          style={{ display: 'none' }}
          onChange={handleFileSelect}
        />
      </Box>

      <Box sx={{ mt: 3 }}>
        <Alert severity="warning">
          <AlertTitle>
            <Box sx={{ display: 'flex', alignItems: 'center' }}>
              <WarningIcon sx={{ mr: 1 }} />
              {t('database.warning', { defaultValue: 'Warning' })}
            </Box>
          </AlertTitle>
          <Typography variant="body2">
            {t('database.importWarning', { 
              defaultValue: 'Importing a database will replace all current data. Make sure to export your current database first if you want to keep it as a backup.' 
            })}
          </Typography>
        </Alert>
      </Box>

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={handleCloseNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert
          onClose={handleCloseNotification}
          severity={notification.severity}
          sx={{ width: '100%' }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export default DatabaseConfiguration; 