import React, { useState, useCallback, useEffect, useMemo } from 'react';
import {
  Card,
  CardContent,
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  IconButton,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Snackbar,
  Alert,
  Tooltip,
  AlertColor,
  CircularProgress,
  Tabs,
  Tab,
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import KeyIcon from '@mui/icons-material/Key';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import StorageIcon from '@mui/icons-material/Storage';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff';
import { 
  APIKeysService, 
  ApiKey, 
  ApiKeyCreate, 
  ApiKeyUpdate,
  DatabricksSecret, 
  DatabricksSecretCreate,
  DatabricksSecretUpdate 
} from '../../../api';

// Extended interfaces for editing with masked values
interface ApiKeyWithMasked extends ApiKey {
  maskedValue?: string;
}

interface DatabricksSecretWithMasked extends DatabricksSecret {
  maskedValue?: string;
}
import { useAPIKeys } from '../../../hooks/global/useAPIKeys';
import { useAPIKeysStore } from '../../../store/apiKeys';
import { NotificationState } from '../../../types/common';

function APIKeys(): JSX.Element {
  const { secrets: apiKeys, loading, error, updateSecrets: updateApiKeys } = useAPIKeys();
  const { editDialogOpen, providerToEdit, closeApiKeyEditor } = useAPIKeysStore();
  const [editDialog, setEditDialog] = useState<boolean>(false);
  const [editingApiKey, setEditingApiKey] = useState<ApiKeyWithMasked | null>(null);
  const [editingDatabricksSecret, setEditingDatabricksSecret] = useState<DatabricksSecretWithMasked | null>(null);
  const [notification, setNotification] = useState<NotificationState>({ 
    open: false, 
    message: '', 
    severity: 'success' 
  });
  const [createDialog, setCreateDialog] = useState<boolean>(false);
  const [newApiKey, setNewApiKey] = useState<ApiKeyCreate>({ 
    name: '', 
    value: '', 
    description: '' 
  });
  const [databricksEnabled, setDatabricksEnabled] = useState<boolean>(false);
  const [activeTab, setActiveTab] = useState<number>(0);
  const [databricksSecrets, setDatabricksSecrets] = useState<DatabricksSecret[]>([]);
  const [loadingDatabricksSecrets, setLoadingDatabricksSecrets] = useState<boolean>(false);

  // Add predefined model API keys
  const modelApiKeys = [
    'OPENAI_API_KEY',
    'DATABRICKS_API_KEY',
    'ANTHROPIC_API_KEY',
    'QWEN_API_KEY',
    'DEEPSEEK_API_KEY',
    'GROK_API_KEY',
    'GEMINI_API_KEY'
  ];

  // Map provider names to API key names with proper typing
  const providerToKeyName = useMemo(() => {
    const mapping: Record<string, string> = {
      'openai': 'OPENAI_API_KEY',
      'anthropic': 'ANTHROPIC_API_KEY',
      'databricks': 'DATABRICKS_API_KEY',
      'qwen': 'QWEN_API_KEY',
      'deepseek': 'DEEPSEEK_API_KEY',
      'grok': 'GROK_API_KEY',
      'gemini': 'GEMINI_API_KEY'
    };
    return mapping;
  }, []);

  // Handle store-triggered edit dialog
  useEffect(() => {
    if (editDialogOpen && providerToEdit && !loading && apiKeys.length > 0) {
      // Map provider name to API key name
      const keyName = providerToKeyName[providerToEdit.toLowerCase()];
      
      if (keyName) {
        // Find the API key
        const apiKey = apiKeys.find(key => key.name === keyName);
        
        if (apiKey) {
          // Auto-open the edit dialog for this key
          setEditingApiKey(apiKey);
          setEditDialog(true);
          
          // Set active tab to model API keys (tab 0)
          setActiveTab(0);
          
          // Reset the store state
          closeApiKeyEditor();
        }
      }
    }
  }, [editDialogOpen, providerToEdit, apiKeys, loading, closeApiKeyEditor, providerToKeyName]);

  const showNotification = useCallback((message: string, severity: AlertColor = 'success') => {
    setNotification({
      open: true,
      message,
      severity,
    });
  }, []);

  const fetchApiKeys = useCallback(async () => {
    try {
      const apiKeysService = APIKeysService.getInstance();
      const apiKeysData = await apiKeysService.getAPIKeys();
      updateApiKeys(apiKeysData);
    } catch (error) {
      showNotification(error instanceof Error ? error.message : 'Error fetching API keys', 'error');
    }
  }, [showNotification, updateApiKeys]);

  const fetchDatabricksSecrets = useCallback(async () => {
    if (!databricksEnabled) return;
    
    try {
      setLoadingDatabricksSecrets(true);
      const apiKeysService = APIKeysService.getInstance();
      const secrets = await apiKeysService.getDatabricksSecrets();
      setDatabricksSecrets(secrets);
    } catch (error) {
      showNotification(error instanceof Error ? error.message : 'Error fetching Databricks secrets', 'error');
    } finally {
      setLoadingDatabricksSecrets(false);
    }
  }, [databricksEnabled, showNotification]);

  useEffect(() => {
    const checkDatabricksEnabled = async () => {
      try {
        const apiKeysService = APIKeysService.getInstance();
        const enabled = await apiKeysService.isDatabricksEnabled();
        setDatabricksEnabled(enabled);
      } catch (error) {
        console.error('Error checking Databricks enabled state:', error);
        setDatabricksEnabled(false);
      }
    };

    checkDatabricksEnabled();
  }, []);

  useEffect(() => {
    if (databricksEnabled && activeTab === 2) {
      fetchDatabricksSecrets();
    }
  }, [databricksEnabled, activeTab, fetchDatabricksSecrets]);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  // Filter out model API keys from local keys
  const localApiKeys = apiKeys.filter(key => !modelApiKeys.includes(key.name));

  if (loading && activeTab !== 2) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 3 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Card sx={{ mt: 8 }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <KeyIcon sx={{ mr: 1, color: 'error.main' }} />
            <Typography variant="h5">API Keys & Secrets</Typography>
          </Box>
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        </CardContent>
      </Card>
    );
  }

  const handleEditApiKey = (apiKey: ApiKey) => {
    // Create a copy for editing - show key status as placeholder
    const editingCopy = {
      ...apiKey,
      value: '', // Start with empty value for user input
      maskedValue: apiKey.value === 'Set' ? '•••••••••••••••• (hidden for security)' : 'Not configured' // Show status based on API response
    };
    setEditingApiKey(editingCopy);
    setEditingDatabricksSecret(null);
    setEditDialog(true);
  };

  const handleEditDatabricksSecret = (secret: DatabricksSecret) => {
    // Create a copy for editing - show key status as placeholder
    const editingCopy = {
      ...secret,
      value: '', // Start with empty value for user input
      maskedValue: secret.value === 'Set' ? '•••••••••••••••• (hidden for security)' : 'Not configured' // Show status based on API response
    };
    setEditingDatabricksSecret(editingCopy);
    setEditingApiKey(null);
    setEditDialog(true);
  };

  const handleSave = async () => {
    try {
      const apiKeysService = APIKeysService.getInstance();
      
      if (editingApiKey) {
        if (!editingApiKey.value) {
          showNotification('Value is required', 'error');
          return;
        }

        const updateData: ApiKeyUpdate = {
          value: editingApiKey.value,
          description: editingApiKey.description || ''
        };
        
        const result = await apiKeysService.updateAPIKey(editingApiKey.name, updateData);
        await fetchApiKeys();
        showNotification(result.message);
      } else if (editingDatabricksSecret) {
        if (!editingDatabricksSecret.value) {
          showNotification('Value is required', 'error');
          return;
        }

        const updateData: DatabricksSecretUpdate = {
          value: editingDatabricksSecret.value,
          description: editingDatabricksSecret.description || ''
        };
        
        const result = await apiKeysService.updateDatabricksSecret(editingDatabricksSecret.name, updateData);
        await fetchDatabricksSecrets();
        showNotification(result.message);
      }
      
      setEditDialog(false);
    } catch (error) {
      showNotification(error instanceof Error ? error.message : 'Error updating key/secret', 'error');
    }
  };


  const formatSecretValue = (value: string) => {
    const isSet = value === "Set";
    const isNotSet = value === "Not set" || !value || value.trim() === '';
    
    if (isSet) {
      return (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <CheckCircleIcon sx={{ color: 'success.main', fontSize: 20 }} />
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <VisibilityOffIcon sx={{ color: 'text.secondary', fontSize: 16 }} />
            <Typography variant="body2" sx={{ color: 'text.secondary', fontStyle: 'italic' }}>
              Hidden
            </Typography>
          </Box>
        </Box>
      );
    }
    
    if (isNotSet) {
      return (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <ErrorIcon sx={{ color: 'warning.main', fontSize: 20 }} />
          <Typography variant="body2" sx={{ color: 'text.secondary' }}>
            Not configured
          </Typography>
        </Box>
      );
    }
    
    // Fallback for any other values
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <CheckCircleIcon sx={{ color: 'success.main', fontSize: 20 }} />
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Configured
        </Typography>
      </Box>
    );
  };

  const handleCreate = async () => {
    try {
      if (!newApiKey.name || !newApiKey.value) {
        showNotification('Name and value are required', 'error');
        return;
      }

      const apiKeysService = APIKeysService.getInstance();
      const result = await apiKeysService.createAPIKey({
        name: newApiKey.name.trim(),
        value: newApiKey.value,
        description: newApiKey.description || ''
      });
      
      setCreateDialog(false);
      setNewApiKey({ name: '', value: '', description: '' });
      await fetchApiKeys();
      showNotification(result.message);
    } catch (error) {
      showNotification(error instanceof Error ? error.message : 'Error creating API key', 'error');
    }
  };

  const handleCreateDatabricksSecret = async () => {
    try {
      if (!newApiKey.name || !newApiKey.value) {
        showNotification('Name and value are required', 'error');
        return;
      }

      const apiKeysService = APIKeysService.getInstance();
      const secretData: DatabricksSecretCreate = {
        name: newApiKey.name.trim(),
        value: newApiKey.value,
        description: newApiKey.description || ''
      };
      const result = await apiKeysService.createDatabricksSecret(secretData);
      
      setCreateDialog(false);
      setNewApiKey({ name: '', value: '', description: '' });
      await fetchDatabricksSecrets();
      showNotification(result.message);
    } catch (error) {
      showNotification(error instanceof Error ? error.message : 'Error creating Databricks secret', 'error');
    }
  };

  const handleDeleteApiKey = async (apiKeyName: string) => {
    if (window.confirm(`Are you sure you want to delete the key "${apiKeyName}"?`)) {
      try {
        const apiKeysService = APIKeysService.getInstance();
        const result = await apiKeysService.deleteAPIKey(apiKeyName);
        
        await fetchApiKeys();
        showNotification(result.message);
      } catch (error) {
        showNotification(error instanceof Error ? error.message : 'Error deleting API key', 'error');
      }
    }
  };

  const handleDeleteDatabricksSecret = async (secretName: string) => {
    if (window.confirm(`Are you sure you want to delete the Databricks secret "${secretName}"?`)) {
      try {
        const apiKeysService = APIKeysService.getInstance();
        const result = await apiKeysService.deleteDatabricksSecret(secretName);
        
        await fetchDatabricksSecrets();
        showNotification(result.message);
      } catch (error) {
        showNotification(error instanceof Error ? error.message : 'Error deleting Databricks secret', 'error');
      }
    }
  };

  const renderApiKeysTable = (apiKeysList: ApiKey[]) => {
    return (
      <TableContainer component={Paper} sx={{ mt: 2 }}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Key Name</TableCell>
              <TableCell>Value</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {apiKeysList.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} align="center">
                  No API keys found
                </TableCell>
              </TableRow>
            ) : (
              apiKeysList.map((apiKey) => (
                <TableRow key={apiKey.id}>
                  <TableCell>{apiKey.name}</TableCell>
                  <TableCell>
                    {formatSecretValue(apiKey.value)}
                  </TableCell>
                  <TableCell>{apiKey.description}</TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      <Tooltip title="Edit">
                        <IconButton onClick={() => handleEditApiKey(apiKey)}>
                          <EditIcon />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Delete">
                        <IconButton onClick={() => handleDeleteApiKey(apiKey.name)} color="error">
                          <DeleteIcon />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </TableContainer>
    );
  };

  const renderDatabricksSecretsTable = (secretsList: DatabricksSecret[]) => {
    return (
      <Box sx={{ opacity: !databricksEnabled ? 0.6 : 1, pointerEvents: !databricksEnabled ? 'none' : 'auto' }}>
        <TableContainer component={Paper} sx={{ mt: 2 }}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Key Name</TableCell>
                <TableCell>Value</TableCell>
                <TableCell>Description</TableCell>
                <TableCell>Scope</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {loadingDatabricksSecrets ? (
                <TableRow>
                  <TableCell colSpan={5} align="center">
                    <CircularProgress size={24} />
                  </TableCell>
                </TableRow>
              ) : secretsList.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} align="center">
                    No Databricks secrets found
                  </TableCell>
                </TableRow>
              ) : (
                secretsList.map((secret) => (
                  <TableRow key={secret.id}>
                    <TableCell>{secret.name}</TableCell>
                    <TableCell>
                      {formatSecretValue(secret.value)}
                    </TableCell>
                    <TableCell>{secret.description}</TableCell>
                    <TableCell>{secret.scope}</TableCell>
                    <TableCell>
                      <Box sx={{ display: 'flex', gap: 1 }}>
                        <Tooltip title="Edit">
                          <IconButton onClick={() => handleEditDatabricksSecret(secret)}>
                            <EditIcon />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Delete">
                          <IconButton onClick={() => handleDeleteDatabricksSecret(secret.name)} color="error">
                            <DeleteIcon />
                          </IconButton>
                        </Tooltip>
                      </Box>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
    );
  };

  return (
    <Card sx={{ mt: 8 }}>
      <CardContent>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
          <KeyIcon sx={{ mr: 1 }} />
          <Typography variant="h5">API Keys & Secrets</Typography>
        </Box>

        <Tabs value={activeTab} onChange={handleTabChange} sx={{ mb: 2 }}>
          <Tab label="Model API Keys" />
          <Tab label="Local Keystore" />
          {databricksEnabled && <Tab label="Databricks Secrets Store" />}
        </Tabs>

        {activeTab === 0 && (
          <>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <KeyIcon sx={{ mr: 1 }} />
              <Typography variant="subtitle1">Model API Keys</Typography>
            </Box>
            <TableContainer component={Paper}>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Key Name</TableCell>
                    <TableCell>Value</TableCell>
                    <TableCell>Description</TableCell>
                    <TableCell>Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {modelApiKeys.map((keyName) => {
                    const apiKey = apiKeys.find(k => k.name === keyName);
                    return (
                      <TableRow key={keyName}>
                        <TableCell>{keyName}</TableCell>
                        <TableCell>
                          {apiKey ? (
                            formatSecretValue(apiKey.value)
                          ) : (
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <ErrorIcon sx={{ color: 'warning.main', fontSize: 20 }} />
                              <Typography variant="body2" sx={{ color: 'text.secondary' }}>
                                Not configured
                              </Typography>
                            </Box>
                          )}
                        </TableCell>
                        <TableCell>{apiKey?.description || ''}</TableCell>
                        <TableCell>
                          <Tooltip title={apiKey ? 'Edit' : 'Set Key'}>
                            <IconButton
                              onClick={() => {
                                if (apiKey) {
                                  handleEditApiKey(apiKey);
                                } else {
                                  setNewApiKey({
                                    name: keyName,
                                    value: '',
                                    description: `API Key for ${keyName}`
                                  });
                                  setCreateDialog(true);
                                }
                              }}
                            >
                              <EditIcon />
                            </IconButton>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          </>
        )}

        {activeTab === 1 && (
          <>
            <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 2 }}>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={() => {
                  setNewApiKey({ name: '', value: '', description: '' });
                  setCreateDialog(true);
                }}
              >
                Add New Key
              </Button>
            </Box>
            {renderApiKeysTable(localApiKeys)}
          </>
        )}

        {activeTab === 2 && databricksEnabled && (
          <>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <StorageIcon sx={{ mr: 1 }} />
                <Typography variant="subtitle1">Databricks Secrets Store</Typography>
              </Box>
              <Button
                variant="contained"
                startIcon={<AddIcon />}
                onClick={() => {
                  setNewApiKey({ name: '', value: '', description: '' });
                  setCreateDialog(true);
                }}
              >
                Add New Secret
              </Button>
            </Box>
            {renderDatabricksSecretsTable(databricksSecrets)}
          </>
        )}

        {/* Create Dialog */}
        <Dialog open={createDialog} onClose={() => setCreateDialog(false)} maxWidth="sm" fullWidth>
          <DialogTitle>
            {modelApiKeys.includes(newApiKey.name) 
              ? 'Set Model API Key' 
              : activeTab === 2 
                ? 'Create New Databricks Secret'
                : 'Create New API Key'
            }
          </DialogTitle>
          <DialogContent>
            <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
              <TextField
                label="Name"
                value={newApiKey.name}
                onChange={(e) => setNewApiKey({ ...newApiKey, name: e.target.value })}
                disabled={modelApiKeys.includes(newApiKey.name)}
                fullWidth
              />
              <TextField
                label="Value"
                value={newApiKey.value}
                onChange={(e) => setNewApiKey({ ...newApiKey, value: e.target.value })}
                fullWidth
              />
              <TextField
                label="Description"
                value={newApiKey.description}
                onChange={(e) => setNewApiKey({ ...newApiKey, description: e.target.value })}
                fullWidth
                multiline
                rows={2}
              />
            </Box>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setCreateDialog(false)}>Cancel</Button>
            <Button 
              onClick={activeTab === 2 ? handleCreateDatabricksSecret : handleCreate} 
              variant="contained"
            >
              {modelApiKeys.includes(newApiKey.name) ? 'Set Key' : 'Create'}
            </Button>
          </DialogActions>
        </Dialog>

        {/* Edit Dialog */}
        <Dialog open={editDialog} onClose={() => setEditDialog(false)}>
          <DialogTitle>
            {editingDatabricksSecret ? 'Edit Databricks Secret' : 'Edit API Key'}
          </DialogTitle>
          <DialogContent>
            <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
              <TextField
                label="Name"
                value={(editingApiKey || editingDatabricksSecret)?.name || ''}
                disabled
                fullWidth
              />
              <TextField
                label="Value"
                placeholder={
                  (editingApiKey?.maskedValue || editingDatabricksSecret?.maskedValue) 
                    ? `Current: ${editingApiKey?.maskedValue || editingDatabricksSecret?.maskedValue}` 
                    : "Enter new value"
                }
                value={(editingApiKey || editingDatabricksSecret)?.value || ''}
                onChange={(e) => {
                  if (editingApiKey) {
                    setEditingApiKey({...editingApiKey, value: e.target.value});
                  } else if (editingDatabricksSecret) {
                    setEditingDatabricksSecret({...editingDatabricksSecret, value: e.target.value});
                  }
                }}
                fullWidth
                required
                helperText="Current value is masked for security. Enter a new value to update."
              />
              <TextField
                label="Description"
                value={(editingApiKey || editingDatabricksSecret)?.description || ''}
                onChange={(e) => {
                  if (editingApiKey) {
                    setEditingApiKey({...editingApiKey, description: e.target.value});
                  } else if (editingDatabricksSecret) {
                    setEditingDatabricksSecret({...editingDatabricksSecret, description: e.target.value});
                  }
                }}
                fullWidth
              />
              {editingDatabricksSecret && (
                <TextField
                  label="Scope"
                  value={editingDatabricksSecret.scope}
                  disabled
                  fullWidth
                />
              )}
            </Box>
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setEditDialog(false)}>Cancel</Button>
            <Button onClick={handleSave} variant="contained">Save</Button>
          </DialogActions>
        </Dialog>

        <Snackbar
          open={notification.open}
          autoHideDuration={6000}
          onClose={() => setNotification((prev: NotificationState) => ({ ...prev, open: false }))}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
        >
          <Alert
            onClose={() => setNotification((prev: NotificationState) => ({ ...prev, open: false }))}
            severity={notification.severity}
            sx={{ width: '100%' }}
          >
            {notification.message}
          </Alert>
        </Snackbar>
      </CardContent>
    </Card>
  );
}

export default APIKeys; 