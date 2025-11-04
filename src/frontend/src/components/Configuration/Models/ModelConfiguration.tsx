import React, { useEffect } from 'react';
import {
  Typography,
  Box,
  Button,
  CircularProgress,
  TextField,
  InputAdornment,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  IconButton,
  Tooltip,
  Stack,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
  Divider,
  Alert,
  TableContainer,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  Switch,
  FormControlLabel,
  ButtonGroup,
  DialogContentText
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import ModelIcon from '@mui/icons-material/Psychology';
import SaveIcon from '@mui/icons-material/Save';
import EditIcon from '@mui/icons-material/Edit';
import AddIcon from '@mui/icons-material/Add';
import CloseIcon from '@mui/icons-material/Close';
import PowerSettingsNewIcon from '@mui/icons-material/PowerSettingsNew';
import PowerOffIcon from '@mui/icons-material/PowerOff';
import { useTranslation } from 'react-i18next';
import { ModelService } from '../../../api/ModelService';
import { ModelConfig, Models } from '../../../types/models';
import { useModelConfig } from '../../../hooks/global/useModelConfig';
import { useSnackbar } from 'notistack';

interface ModelEditDialogProps {
  open: boolean;
  onClose: () => void;
  model: ModelConfig & { key: string } | null;
  onSave: (key: string, model: ModelConfig) => void;
  isNew?: boolean;
}

const ModelEditDialog: React.FC<ModelEditDialogProps> = ({
  open,
  onClose,
  model,
  onSave,
  isNew = false
}) => {
  const { t } = useTranslation();
  const [editedModel, setEditedModel] = React.useState<ModelConfig & { key: string } | null>(model);
  const [errors, setErrors] = React.useState<Record<string, string>>({});

  useEffect(() => {
    if (isNew && !model) {
      // Initialize with empty model for new model creation
      setEditedModel({
        key: '',
        name: '',
        provider: '',
        temperature: 0.7,
        context_window: 4096,
        max_output_tokens: 1024,
        extended_thinking: false,
        enabled: true
      });
    } else {
      setEditedModel(model);
    }
    setErrors({});
  }, [model, isNew]);

  if (!editedModel) return null;

  const handleTextChange = (field: keyof (ModelConfig & { key: string })) => (
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    let value: string | number | boolean = e.target.value;

    // Convert numeric fields
    if (field === 'temperature' && value !== '') {
      value = parseFloat(value as string);
      if (isNaN(value) || value < 0 || value > 2) {
        setErrors(prev => ({ ...prev, temperature: 'Must be between 0 and 2' }));
      } else {
        setErrors(prev => {
          const { temperature, ...rest } = prev;
          return rest;
        });
      }
    } else if ((field === 'context_window' || field === 'max_output_tokens') && value !== '') {
      value = parseInt(value as string, 10);
      if (isNaN(value) || value <= 0) {
        setErrors(prev => ({ ...prev, [field]: 'Must be a positive number' }));
      } else {
        setErrors(prev => {
          const newErrors = { ...prev };
          delete newErrors[field];
          return newErrors;
        });
      }
    } else if (field === 'key' || field === 'name') {
      if (!value) {
        setErrors(prev => ({ ...prev, [field]: 'This field is required' }));
      } else {
        setErrors(prev => {
          const newErrors = { ...prev };
          delete newErrors[field];
          return newErrors;
        });
      }
    }

    setEditedModel(prev => prev ? { ...prev, [field]: value } : null);
  };

  const handleSelectChange = (field: keyof (ModelConfig & { key: string })) => (
    event: SelectChangeEvent<string>
  ) => {
    const value = event.target.value;
    setEditedModel(prev => prev ? { ...prev, [field]: value } : null);
  };

  const handleBooleanChange = (field: 'extended_thinking') => (
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    setEditedModel(prev => prev ? { ...prev, [field]: e.target.checked } : null);
  };

  const handleSave = () => {
    // Validate required fields
    const newErrors: Record<string, string> = {};
    if (!editedModel.key) newErrors.key = 'Key is required';
    if (!editedModel.name) newErrors.name = 'Name is required';

    if (Object.keys(newErrors).length > 0 || Object.keys(errors).length > 0) {
      setErrors({ ...errors, ...newErrors });
      return;
    }

    const { key, ...modelConfig } = editedModel;
    onSave(key, modelConfig);
    onClose();
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: {
          overflowY: 'visible',
          borderRadius: 2,
          boxShadow: '0 8px 32px rgba(0,0,0,0.12)'
        }
      }}
    >
      <DialogTitle sx={{
        borderBottom: '1px solid',
        borderColor: 'divider',
        p: 3
      }}>
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <ModelIcon sx={{ mr: 1.5, color: 'primary.main', fontSize: '1.5rem' }} />
          {isNew
            ? <Typography variant="h5">{t('configuration.models.add', { defaultValue: 'Add New Model' })}</Typography>
            : <Typography variant="h5">{t('configuration.models.edit', { defaultValue: 'Edit Model' })}</Typography>}
        </Box>
        <IconButton
          onClick={onClose}
          sx={{
            position: 'absolute',
            right: 16,
            top: 16,
            color: 'text.secondary',
            '&:hover': {
              bgcolor: 'action.hover',
              color: 'primary.main'
            }
          }}
        >
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent sx={{ p: 3 }}>
        <Stack spacing={3} sx={{ mt: 1 }}>
          <TextField
            label={t('configuration.models.key', { defaultValue: 'Model Key' })}
            value={editedModel.key || ''}
            onChange={handleTextChange('key')}
            fullWidth
            required
            disabled={!isNew}
            error={!!errors.key}
            size="medium"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: 1.5,
              }
            }}
            helperText={errors.key || t('configuration.models.keyHelp', { defaultValue: 'Unique identifier for this model' })}
          />

          <TextField
            label={t('configuration.models.name', { defaultValue: 'Model Name' })}
            value={editedModel.name || ''}
            onChange={handleTextChange('name')}
            fullWidth
            required
            error={!!errors.name}
            size="medium"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: 1.5,
              }
            }}
            helperText={errors.name || t('configuration.models.nameHelp', { defaultValue: 'Full name of the model (e.g., "gpt-4-turbo-preview")' })}
          />

          <FormControl fullWidth size="medium" sx={{
            '& .MuiOutlinedInput-root': {
              borderRadius: 1.5,
            }
          }}>
            <InputLabel>
              {t('configuration.models.provider', { defaultValue: 'Provider' })}
            </InputLabel>
            <Select
              value={editedModel.provider || ''}
              onChange={handleSelectChange('provider')}
              label={t('configuration.models.provider', { defaultValue: 'Provider' })}
            >
              <MenuItem value="">
                <em>{t('common.none', { defaultValue: 'None' })}</em>
              </MenuItem>
              <MenuItem value="openai">OpenAI</MenuItem>
              <MenuItem value="anthropic">Anthropic</MenuItem>
              <MenuItem value="gemini">Gemini</MenuItem>
              <MenuItem value="mistral">Mistral</MenuItem>
              <MenuItem value="ollama">Ollama</MenuItem>
              <MenuItem value="cohere">Cohere</MenuItem>
              <MenuItem value="databricks">Databricks</MenuItem>
              <MenuItem value="local">Local</MenuItem>
            </Select>
          </FormControl>

          <Divider sx={{ my: 1.5 }} />
          <Typography variant="subtitle1" color="text.primary" fontWeight="medium" sx={{ pt: 1.5 }}>
            {t('configuration.models.parameters', { defaultValue: 'Model Parameters' })}
          </Typography>

          <TextField
            label={t('configuration.models.temperature', { defaultValue: 'Temperature' })}
            value={editedModel.temperature === undefined ? '' : editedModel.temperature}
            onChange={handleTextChange('temperature')}
            type="number"
            fullWidth
            inputProps={{ step: 0.1, min: 0, max: 2 }}
            error={!!errors.temperature}
            size="medium"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: 1.5,
              }
            }}
            helperText={errors.temperature || t('configuration.models.temperatureHelp', { defaultValue: 'Controls randomness (0.0 to 2.0)' })}
          />

          <TextField
            label={t('configuration.models.contextWindow', { defaultValue: 'Context Window' })}
            value={editedModel.context_window === undefined ? '' : editedModel.context_window}
            onChange={handleTextChange('context_window')}
            type="number"
            fullWidth
            inputProps={{ min: 1 }}
            error={!!errors.context_window}
            size="medium"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: 1.5,
              }
            }}
            helperText={errors.context_window || t('configuration.models.contextWindowHelp', { defaultValue: 'Maximum token context length' })}
          />

          <TextField
            label={t('configuration.models.maxOutputTokens', { defaultValue: 'Max Output Tokens' })}
            value={editedModel.max_output_tokens === undefined ? '' : editedModel.max_output_tokens}
            onChange={handleTextChange('max_output_tokens')}
            type="number"
            fullWidth
            inputProps={{ min: 1 }}
            error={!!errors.max_output_tokens}
            size="medium"
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: 1.5,
              }
            }}
            helperText={errors.max_output_tokens || t('configuration.models.maxOutputTokensHelp', { defaultValue: 'Maximum tokens to generate' })}
          />

          <FormControlLabel
            sx={{ mt: 1 }}
            control={
              <Switch
                checked={!!editedModel.extended_thinking}
                onChange={handleBooleanChange('extended_thinking')}
                color="primary"
              />
            }
            label={t('configuration.models.extendedThinking', { defaultValue: 'Extended Thinking' })}
          />
        </Stack>
      </DialogContent>
      <DialogActions sx={{ px: 3, py: 2.5, borderTop: '1px solid', borderColor: 'divider' }}>
        <Button onClick={onClose}>
          {t('common.cancel', { defaultValue: 'Cancel' })}
        </Button>
        <Button
          onClick={handleSave}
          variant="contained"
          startIcon={<SaveIcon />}
          disabled={Object.keys(errors).length > 0}
          sx={{ px: 3 }}
        >
          {t('common.save', { defaultValue: 'Save' })}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const ConfirmDeleteDialog: React.FC<{
  open: boolean;
  modelKey: string;
  onClose: () => void;
  onConfirm: () => void;
}> = ({ open, modelKey, onClose, onConfirm }) => {
  const { t } = useTranslation();

  return (
    <Dialog
      open={open}
      onClose={onClose}
      aria-labelledby="alert-dialog-title"
      aria-describedby="alert-dialog-description"
    >
      <DialogTitle id="alert-dialog-title">
        {t('configuration.models.confirmDeleteTitle', { defaultValue: 'Delete Model?' })}
      </DialogTitle>
      <DialogContent>
        <DialogContentText id="alert-dialog-description">
          {t('configuration.models.confirmDeleteMessage', {
            defaultValue: 'Are you sure you want to delete the model {{modelKey}}? This action cannot be undone.',
            modelKey
          })}
        </DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} color="primary">
          {t('common.cancel', { defaultValue: 'Cancel' })}
        </Button>
        <Button onClick={onConfirm} color="error" variant="contained" autoFocus>
          {t('common.delete', { defaultValue: 'Delete' })}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const ModelConfiguration: React.FC<{ mode?: 'system' | 'workspace' | 'auto' }> = ({ mode = 'auto' }) => {
  const { t } = useTranslation();
  const isSystemView = mode === 'system';
  const {
    models,
    currentEditModel,
    editDialogOpen,
    isNewModel,
    loading,
    saving,
    searchTerm,
    error,
    setModels,
    setCurrentEditModel,
    setEditDialogOpen,
    setIsNewModel,
    setLoading,
    setSaving,
    setSearchTerm,
    setError,
    incrementRefreshKey,
  } = useModelConfig();
  const { enqueueSnackbar } = useSnackbar();
  const [modelToDelete, setModelToDelete] = React.useState<string | null>(null);

  const [providerFilter, setProviderFilter] = React.useState<string>('all');
  const uniqueProviders = React.useMemo(() => {
    const set = new Set<string>();
    Object.values(models).forEach(m => { if (m.provider) set.add(String(m.provider)); });
    return Array.from(set).sort();
  }, [models]);

  // Persist provider filter selection for Global view
  useEffect(() => {
    if (isSystemView) {
      const saved = localStorage.getItem('models_global_providerFilter');
      if (saved) setProviderFilter(saved);
    }
  }, [isSystemView]);

  useEffect(() => {
    if (isSystemView) {
      localStorage.setItem('models_global_providerFilter', providerFilter);
    }
  }, [isSystemView, providerFilter]);

  useEffect(() => {
    const fetchModels = async () => {
      try {
        setLoading(true);
        const modelService = ModelService.getInstance();
        if (isSystemView) {
          const fetchedModels = await modelService.getGlobalModels();
          setModels(fetchedModels);
        } else {
          // Workspace view: only show models that are enabled globally
          const [workspaceModels, globalModels] = await Promise.all([
            modelService.getModels(true),
            modelService.getGlobalModels(),
          ]);

          const allowedKeys = new Set(
            Object.entries(globalModels)
              .filter(([_, m]) => m.enabled)
              .map(([k]) => k)
          );

          const filtered: Models = {};
          for (const [k, m] of Object.entries(workspaceModels)) {
            if (allowedKeys.has(k)) filtered[k] = m;
          }
          setModels(filtered);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch models');
      } finally {
        setLoading(false);
      }
    };

    fetchModels();
  }, [setLoading, setModels, setError, isSystemView]);

  const handleSaveModel = async (key: string, model: ModelConfig) => {
    try {
      setSaving(true);
      const modelService = ModelService.getInstance();

      if (isNewModel) {
        // For new models, use POST directly via the service's createModel method
        await modelService.createModel(key, model);
      } else {
        // For existing models, use the existing saveModels method
        await modelService.saveModels({ [key]: model });
      }

      const updatedModels = isSystemView
        ? await modelService.getGlobalModels()
        : await modelService.getModels(true);
      setModels(updatedModels);
      setCurrentEditModel(null);
      setEditDialogOpen(false);

      // Notify other components that models have changed
      incrementRefreshKey();
    } catch (err) {
      console.error('Error saving model:', err);
      setError(err instanceof Error ? err.message : 'Failed to save model');
    } finally {
      setSaving(false);
    }
  };

  const handleAddModel = () => {
    setIsNewModel(true);
    setCurrentEditModel(null);
    setEditDialogOpen(true);
  };

  const handleEditModel = (model: ModelConfig & { key: string }) => {
    setIsNewModel(false);
    setCurrentEditModel(model);
    setEditDialogOpen(true);
  };

  const handleDeleteModel = async (key: string) => {
    try {
      setSaving(true);

      // Use ModelService as the proper service layer
      const modelService = ModelService.getInstance();
      await modelService.deleteModel(key);

      // Refresh from global models after deletion (system view only)
      const refreshed = await modelService.getGlobalModels();
      setModels(refreshed);

      // Show success message
      enqueueSnackbar(
        t('configuration.models.modelDeleted', { defaultValue: 'Model deleted successfully' }),
        { variant: 'success' }
      );

      // Notify other components that models have changed
      incrementRefreshKey();

      // Schedule a background refresh after a delay to ensure consistency
      setTimeout(() => {
        console.log('[ModelConfiguration] Running background refresh after deletion');
        modelService.getGlobalModels()
          .then(refreshedModels => {
            console.log('[ModelConfiguration] Background refresh complete, updating models');
            setModels(refreshedModels);
            incrementRefreshKey();
          })
          .catch(err => {
            console.warn('[ModelConfiguration] Background refresh failed:', err);
          });
      }, 2000); // 2 second delay
    } catch (err) {
      console.error('Error deleting model:', err);
      setError(err instanceof Error ? err.message : 'Failed to delete model');

      enqueueSnackbar(
        t('configuration.models.deleteFailed', { defaultValue: 'Failed to delete model' }),
        { variant: 'error' }
      );
    } finally {
      setSaving(false);
    }
  };

  const handleToggleModel = async (key: string, enabled: boolean) => {
    try {
      setSaving(true);
      const modelService = ModelService.getInstance();

      if (isSystemView) {
        await modelService.enableGlobalModel(key, enabled);
        const updatedModels = await modelService.getGlobalModels();
        setModels(updatedModels);
      } else {
        await modelService.enableModel(key, enabled);
        const [updatedModels, globalModels] = await Promise.all([
          modelService.getModels(true),
          modelService.getGlobalModels(),
        ]);
        const allowedKeys = new Set(
          Object.entries(globalModels)
            .filter(([_, m]) => m.enabled)
            .map(([k]) => k)
        );
        const filtered: Models = {};
        for (const [k, m] of Object.entries(updatedModels)) {
          if (allowedKeys.has(k)) filtered[k] = m;
        }
        setModels(filtered);
      }

      enqueueSnackbar(
        enabled
          ? t('configuration.models.modelEnabled', { defaultValue: 'Model enabled successfully' })
          : t('configuration.models.modelDisabled', { defaultValue: 'Model disabled successfully' }),
        { variant: 'success' }
      );

      // Notify other components that models have changed
      incrementRefreshKey();
    } catch (err) {
      console.error(`Error toggling model ${key}:`, err);
      setError(err instanceof Error ? err.message : 'Failed to update model status');
      enqueueSnackbar(
        t('configuration.models.toggleFailed', { defaultValue: 'Failed to toggle model status' }),
        { variant: 'error' }
      );
    } finally {
      setSaving(false);
    }
  };

  const handleEnableAllModels = async () => {
    try {
      setSaving(true);
      const modelService = ModelService.getInstance();
      const updatedModels = await modelService.enableAllModels();
      setModels(updatedModels);
      setError(null);

      // Show success feedback to the user
      enqueueSnackbar(
        t('configuration.models.allModelsEnabled', { defaultValue: 'All models have been enabled' }),
        { variant: 'success' }
      );

      // Notify other components that models have changed
      incrementRefreshKey();
    } catch (err) {
      console.error('Error enabling all models:', err);
      setError(err instanceof Error ? err.message : 'Failed to enable all models');

      // Show error feedback
      enqueueSnackbar(
        t('configuration.models.enableAllFailed', { defaultValue: 'Failed to enable all models' }),
        { variant: 'error' }
      );
    } finally {
      setSaving(false);
    }
  };

  const handleDisableAllModels = async () => {
    try {
      setSaving(true);
      const modelService = ModelService.getInstance();
      const updatedModels = await modelService.disableAllModels();
      setModels(updatedModels);
      setError(null);

      // Show success feedback to the user
      enqueueSnackbar(
        t('configuration.models.allModelsDisabled', { defaultValue: 'All models have been disabled' }),
        { variant: 'success' }
      );

      // Notify other components that models have changed
      incrementRefreshKey();
    } catch (err) {
      console.error('Error disabling all models:', err);
      setError(err instanceof Error ? err.message : 'Failed to disable all models');

      // Show error feedback
      enqueueSnackbar(
        t('configuration.models.disableAllFailed', { defaultValue: 'Failed to disable all models' }),
        { variant: 'error' }
      );
    } finally {
      setSaving(false);
    }
  };

  const handleConfirmDeleteOpen = (key: string) => {
    setModelToDelete(key);
  };

  const handleConfirmDeleteClose = () => {
    setModelToDelete(null);
  };

  const filteredModels = Object.entries(models)
    .filter(([key, model]) =>
      key.toLowerCase().includes(searchTerm.toLowerCase()) ||
      model.name.toLowerCase().includes(searchTerm.toLowerCase())
    )
    .filter(([_, model]) =>
      !isSystemView || providerFilter === 'all' || (model.provider || '') === providerFilter
    )
    .sort((a, b) => a[1].name.localeCompare(b[1].name)); // Sort alphabetically by model name

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ p: 3 }}>
      <Stack spacing={3}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h4" component="h1">
            {isSystemView
              ? t('configuration.models.globalTitle', { defaultValue: 'Global Models (System Admin)' })
              : t('configuration.models.workspaceTitle', { defaultValue: 'Workspace Models' })}
          </Typography>
          {isSystemView && (
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              onClick={handleAddModel}
              disabled={saving}
            >
              {t('configuration.models.add', { defaultValue: 'Add Model' })}
            </Button>
          )}
        </Box>

        {error && (
          <Alert severity="error" onClose={() => setError(null)}>
            {error}
          </Alert>
        )}

        <Paper sx={{ p: 2 }}>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} alignItems={{ xs: 'stretch', sm: 'center' }}>
            <TextField
              fullWidth
              variant="outlined"
              placeholder={t('configuration.models.search', { defaultValue: 'Search models...' })}
              value={searchTerm}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon />
                  </InputAdornment>
                ),
              }}
            />

            {isSystemView && (
              <>
                <FormControl sx={{ minWidth: 220 }} size="small">
                  <InputLabel>{t('configuration.models.provider', { defaultValue: 'Provider' })}</InputLabel>
                  <Select
                    label={t('configuration.models.provider', { defaultValue: 'Provider' })}
                    value={providerFilter}
                    onChange={(e: SelectChangeEvent<string>) => setProviderFilter(e.target.value)}
                  >
                    <MenuItem value="all">{t('common.all', { defaultValue: 'All' })}</MenuItem>
                    {uniqueProviders.map((p) => (
                      <MenuItem key={p} value={p}>{p}</MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <Button
                  variant="text"
                  size="small"
                  onClick={() => {
                    setSearchTerm('');
                    setProviderFilter('all');
                  }}
                  sx={{ whiteSpace: 'nowrap' }}
                >
                  {t('common.clearFilters', { defaultValue: 'Clear filters' })}
                </Button>
              </>
            )}
          </Stack>
        </Paper>

        {isSystemView && (
          <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 2 }}>
            <ButtonGroup variant="outlined" size="small">
              <Button
                startIcon={<PowerSettingsNewIcon />}
                onClick={handleEnableAllModels}
                disabled={saving}
                color="success"
              >
                {t('configuration.models.enableAll')}
              </Button>
              <Button
                startIcon={<PowerOffIcon />}
                onClick={handleDisableAllModels}
                disabled={saving}
                color="error"
              >
                {t('configuration.models.disableAll')}
              </Button>
            </ButtonGroup>
          </Box>
        )}

        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>{t('configuration.models.key', { defaultValue: 'Key' })}</TableCell>
                <TableCell>{t('configuration.models.name', { defaultValue: 'Name' })}</TableCell>
                <TableCell>{t('configuration.models.provider', { defaultValue: 'Provider' })}</TableCell>
                <TableCell>{t('configuration.models.status', { defaultValue: 'Status' })}</TableCell>
                <TableCell>{t('configuration.models.actions', { defaultValue: 'Actions' })}</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredModels.map(([key, model]) => (
                <TableRow key={key}>
                  <TableCell>{key}</TableCell>
                  <TableCell>{model.name}</TableCell>
                  <TableCell>{model.provider}</TableCell>
                  <TableCell>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={model.enabled}
                          onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleToggleModel(key, e.target.checked)}
                          disabled={saving}
                        />
                      }
                      label={model.enabled ? t('configuration.models.enabled', { defaultValue: 'Enabled' }) : t('configuration.models.disabled', { defaultValue: 'Disabled' })}
                    />
                  </TableCell>
                  <TableCell>
                    {isSystemView ? (
                      <Stack direction="row" spacing={1}>
                        <Tooltip title={t('common.edit', { defaultValue: 'Edit' })}>
                          <IconButton
                            onClick={() => handleEditModel({ ...model, key })}
                            disabled={saving}
                          >
                            <EditIcon />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title={t('common.delete', { defaultValue: 'Delete' })}>
                          <IconButton
                            onClick={() => handleConfirmDeleteOpen(key)}
                            disabled={saving}
                          >
                            <CloseIcon />
                          </IconButton>
                        </Tooltip>
                      </Stack>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        {t('configuration.models.workspaceActionsHelp', { defaultValue: 'Enable/disable only'})}
                      </Typography>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Stack>

      <ModelEditDialog
        open={editDialogOpen}
        onClose={() => {
          setEditDialogOpen(false);
          setCurrentEditModel(null);
        }}
        model={currentEditModel}
        onSave={handleSaveModel}
        isNew={isNewModel}
      />

      <ConfirmDeleteDialog
        open={modelToDelete !== null}
        modelKey={modelToDelete || ''}
        onClose={handleConfirmDeleteClose}
        onConfirm={() => {
          handleDeleteModel(modelToDelete || '');
          setModelToDelete(null);
        }}
      />
    </Box>
  );
};

export default ModelConfiguration;