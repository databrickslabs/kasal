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

import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import VisibilityOffIcon from '@mui/icons-material/VisibilityOff';
import { 
  APIKeysService, 
  ApiKey, 
  ApiKeyCreate, 
  ApiKeyUpdate,
 
} from '../../../api';

// Extended interfaces for editing with masked values
interface ApiKeyWithMasked extends ApiKey {
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
  const [activeTab, setActiveTab] = useState<number>(0);

  // Deep-link listener: allow other components to switch tab/open a specific key
  useEffect(() => {
    const setTabHandler = (evt: Event) => {
      try {
        const custom = evt as CustomEvent<{ tab?: string }>;
        if (custom.detail?.tab === 'local') setActiveTab(1);
        if (custom.detail?.tab === 'models') setActiveTab(0);
      } catch (e) { /* no-op */ }
    };
    const focusKeyHandler = (evt: Event) => {
      try {
        const custom = evt as CustomEvent<{ name?: string }>;
        const name = custom.detail?.name;
        if (!name) return;
        // Decide which tab based on predefined model keys vs local keystore
        const isModel = modelApiKeys.includes(name);
        setActiveTab(isModel ? 0 : 1);
        // Prefill create dialog for convenience
        setNewApiKey({ name, value: '', description: `API Key for ${name}` });
        setCreateDialog(true);
      } catch (e) { /* no-op */ }
    };
    window.addEventListener('kasal:api-keys:set-tab', setTabHandler as EventListener);
    window.addEventListener('kasal:api-keys:focus-key', focusKeyHandler as EventListener);
    return () => {
      window.removeEventListener('kasal:api-keys:set-tab', setTabHandler as EventListener);
      window.removeEventListener('kasal:api-keys:focus-key', focusKeyHandler as EventListener);
    };
  }, []);

  // Add predefined model API keys
  const modelApiKeys = [
    'OPENAI_API_KEY',
    'DATABRICKS_API_KEY',
    'ANTHROPIC_API_KEY',
    'QWEN_API_KEY',
    'DEEPSEEK_API_KEY',
    'GROK_API_KEY',
    'GEMINI_API_KEY',
    'POWERBI_USERNAME',
    'POWERBI_PASSWORD',
    'POWERBI_CLIENT_SECRET'
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







  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  // Filter out model API keys from local keys
  const localApiKeys = apiKeys.filter(key => !modelApiKeys.includes(key.name));

  if (loading) {
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
    // Check if this is a placeholder key (negative ID)
    if (apiKey.id < 0) {
      // Open create dialog for placeholder keys
      setNewApiKey({
        name: apiKey.name,
        value: '',
        description: apiKey.description || ''
      });
      setCreateDialog(true);
      return;
    }
    
    // Create a copy for editing - show key status as placeholder
    const editingCopy = {
      ...apiKey,
      value: '', // Start with empty value for user input
      maskedValue: apiKey.value === 'Set' ? '•••••••••••••••• (hidden for security)' : 'Not configured' // Show status based on API response
    };
    setEditingApiKey(editingCopy);
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
                      {apiKey.id >= 0 && (
                        <Tooltip title="Delete">
                          <IconButton onClick={() => handleDeleteApiKey(apiKey.name)} color="error">
                            <DeleteIcon />
                          </IconButton>
                        </Tooltip>
                      )}
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
            {(() => {
              // Add placeholder entries for specific keys if they don't exist
              const placeholderKeys = ['SERPER_API_KEY', 'PERPLEXITY_API_KEY', 'FIRECRAWL_API_KEY', 'EXA_API_KEY', 'LINKUP_API_KEY', 'COMPOSIO_API_KEY'];
              const existingKeyNames = localApiKeys.map(k => k.name);

              const placeholderApiKeys: ApiKey[] = placeholderKeys
                .filter(keyName => !existingKeyNames.includes(keyName))
                .map((keyName, index) => ({
                  id: -1000 - index, // Use negative IDs for placeholders
                  name: keyName,
                  value: 'Not set',
                  description: `API Key for ${keyName.replace(/_/g, ' ').toLowerCase()}`,
                  created_at: '',
                  updated_at: ''
                }));

              const combinedKeys = [...localApiKeys, ...placeholderApiKeys];
              return renderApiKeysTable(combinedKeys);
            })()}
          </>
        )}



        {/* Create Dialog */}
        <Dialog open={createDialog} onClose={() => setCreateDialog(false)} maxWidth="sm" fullWidth>
          <DialogTitle>
            {modelApiKeys.includes(newApiKey.name) 
              ? 'Set Model API Key' 
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
              onClick={handleCreate} 
              variant="contained"
            >
              {modelApiKeys.includes(newApiKey.name) ? 'Set Key' : 'Create'}
            </Button>
          </DialogActions>
        </Dialog>

        {/* Edit Dialog */}
        <Dialog open={editDialog} onClose={() => setEditDialog(false)}>
          <DialogTitle>
            Edit API Key
          </DialogTitle>
          <DialogContent>
            <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
              <TextField
                label="Name"
                value={editingApiKey?.name || ''}
                disabled
                fullWidth
              />
              <TextField
                label="Value"
                placeholder={
                  editingApiKey?.maskedValue 
                    ? `Current: ${editingApiKey?.maskedValue}` 
                    : "Enter new value"
                }
                value={editingApiKey?.value || ''}
                onChange={(e) => {
                  if (editingApiKey) {
                    setEditingApiKey({...editingApiKey, value: e.target.value});
                  }
                }}
                fullWidth
                required
                helperText="Current value is masked for security. Enter a new value to update."
              />
              <TextField
                label="Description"
                value={editingApiKey?.description || ''}
                onChange={(e) => {
                  if (editingApiKey) {
                    setEditingApiKey({...editingApiKey, description: e.target.value});
                  }
                }}
                fullWidth
              />

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