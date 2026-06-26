import React, { useState, useEffect, useRef } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  FormControl,
  FormLabel,
  Checkbox,
  FormControlLabel,
  TextField,
  Box,
  Alert,
  CircularProgress,
  Typography,
  IconButton,
  ToggleButtonGroup,
  ToggleButton,
  LinearProgress,
  Link,
  InputLabel,
  Select,
  MenuItem,
  SelectChangeEvent,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import DownloadIcon from '@mui/icons-material/Download';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import { CrewExportService } from '../../api/CrewExportService';
import { ModelService } from '../../api/ModelService';
import { Models } from '../../types/models';
import {
  ExportFormat,
  ExportOptions,
  AppDeploymentStatusResponse,
  LakebaseInstance,
} from '../../types/crewExport';

interface ExportCrewDialogProps {
  open: boolean;
  onClose: () => void;
  crewId: string;
  crewName: string;
}

const FORMAT_LABELS: Record<string, { label: string; description: string }> = {
  [ExportFormat.DATABRICKS_NOTEBOOK]: {
    label: 'Notebook',
    description: 'Databricks Notebook (.ipynb) - ready for Databricks import',
  },
  [ExportFormat.DATABRICKS_APP]: {
    label: 'App',
    description:
      'Databricks App (.zip) - FastAPI project deployable via databricks apps deploy',
  },
};

const ExportCrewDialog: React.FC<ExportCrewDialogProps> = ({
  open,
  onClose,
  crewId,
  crewName
}) => {
  const [exportFormat, setExportFormat] = useState<ExportFormat>(
    ExportFormat.DATABRICKS_APP
  );
  const [options, setOptions] = useState<ExportOptions>({
    include_custom_tools: true,
    include_comments: true,
    include_tracing: true,
    include_evaluation: true,
    include_deployment: true,
    include_static_frontend: true,
    include_obo_auth: true,
    model_override: ''
  });
  const [isExporting, setIsExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  // Databricks Apps deployment state
  const defaultAppName = crewName
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 30) || 'agent-crew';
  const [appName, setAppName] = useState<string>(defaultAppName);
  const [deployCatalog, setDeployCatalog] = useState<string>('');
  const [deploySchema, setDeploySchema] = useState<string>('');
  const [deployExperiment, setDeployExperiment] = useState<string>('');
  const [deployWarehouse, setDeployWarehouse] = useState<string>('');
  // Lakebase: '' = none, '__create__' = create a new instance, else an existing
  // instance name to attach.
  const [lakebaseChoice, setLakebaseChoice] = useState<string>('');
  const [newLakebaseName, setNewLakebaseName] = useState<string>('');
  const [lakebaseInstances, setLakebaseInstances] = useState<LakebaseInstance[]>(
    []
  );
  const [loadingLakebase, setLoadingLakebase] = useState(false);
  const [isDeploying, setIsDeploying] = useState(false);
  const [deployStatus, setDeployStatus] =
    useState<AppDeploymentStatusResponse | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Available LLM models (same enabled list the rest of the UI uses).
  const [availableModels, setAvailableModels] = useState<Models>({});
  const [loadingModels, setLoadingModels] = useState(false);

  useEffect(() => {
    if (!open) return;
    setLoadingModels(true);
    ModelService.getInstance()
      .getActiveModels()
      .then((m) => setAvailableModels(m))
      .catch((e) => console.error('Failed to load models:', e))
      .finally(() => setLoadingModels(false));
  }, [open]);

  // Load the workspace's Lakebase instances for the deploy screen dropdown.
  useEffect(() => {
    if (!open) return;
    setLoadingLakebase(true);
    CrewExportService.listLakebaseInstances()
      .then((list) => setLakebaseInstances(list))
      .catch((e) => console.error('Failed to load Lakebase instances:', e))
      .finally(() => setLoadingLakebase(false));
  }, [open]);

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  };

  // Clear any in-flight polling when the dialog unmounts.
  useEffect(() => stopPolling, []);

  const handleDeployApp = async () => {
    setIsDeploying(true);
    setError(null);
    setSuccess(null);
    setDeployStatus(null);
    stopPolling();

    // Resolve the Lakebase selection: attach an existing instance, create a new
    // one, or none.
    const creatingLakebase = lakebaseChoice === '__create__';
    const lakebaseInstance = creatingLakebase
      ? newLakebaseName.trim() || undefined
      : lakebaseChoice || undefined;

    try {
      const started = await CrewExportService.deployApp(crewId, {
        config: {
          app_name: appName || undefined,
          options,
          model: options.model_override || undefined,
          catalog: deployCatalog || undefined,
          schema_name: deploySchema || undefined,
          experiment_name: deployExperiment || undefined,
          lakebase_instance: lakebaseInstance,
          create_lakebase: creatingLakebase,
          warehouse_id: deployWarehouse || undefined,
        },
      });
      setDeployStatus({ ...started, step: 'QUEUED' });

      // Poll until the deploy reaches a terminal state.
      pollRef.current = setInterval(async () => {
        try {
          const status = await CrewExportService.getAppDeploymentStatus(
            crewId,
            started.deployment_id
          );
          setDeployStatus(status);
          if (status.status === 'SUCCEEDED' || status.status === 'FAILED') {
            stopPolling();
            setIsDeploying(false);
            if (status.status === 'SUCCEEDED') {
              setSuccess(`App "${status.app_name}" deployed successfully.`);
            } else {
              setError(status.error || 'Deployment failed');
            }
          }
        } catch (pollErr) {
          console.error('Poll error:', pollErr);
        }
      }, 5000);
    } catch (err) {
      console.error('Deploy error:', err);
      const message =
        (err as { response?: { data?: { detail?: string } } })?.response?.data
          ?.detail ||
        (err instanceof Error ? err.message : 'Failed to start deployment');
      setError(message);
      setIsDeploying(false);
    }
  };

  const handleFormatChange = (
    _event: React.MouseEvent<HTMLElement>,
    newFormat: ExportFormat | null
  ) => {
    if (newFormat) {
      setExportFormat(newFormat);
    }
  };

  const handleOptionChange = (option: keyof ExportOptions) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setOptions(prev => ({
      ...prev,
      [option]: event.target.checked
    }));
  };

  const handleModelOverrideChange = (event: SelectChangeEvent<string>) => {
    setOptions(prev => ({
      ...prev,
      model_override: event.target.value || undefined
    }));
  };

  const handleExport = async () => {
    setIsExporting(true);
    setError(null);
    setSuccess(null);

    try {
      // First, generate the export
      const exportResponse = await CrewExportService.exportCrew(crewId, {
        export_format: exportFormat,
        options
      });

      // Then, download the file with the same options
      const blob = await CrewExportService.downloadExport(crewId, exportFormat, options);

      // Determine filename based on format
      const sanitizedName = exportResponse.metadata.sanitized_name;
      const filename =
        exportFormat === ExportFormat.DATABRICKS_APP
          ? `${sanitizedName}_app.zip`
          : `${sanitizedName}.ipynb`;

      // Trigger download
      CrewExportService.triggerDownload(blob, filename);

      const formatLabel =
        exportFormat === ExportFormat.DATABRICKS_APP
          ? 'Databricks App'
          : 'Databricks Notebook';
      setSuccess(`Successfully exported crew as ${formatLabel}`);

      // Close dialog after successful export
      setTimeout(() => {
        onClose();
      }, 1500);
    } catch (err) {
      console.error('Export error:', err);
      setError(err instanceof Error ? err.message : 'Failed to export crew');
    } finally {
      setIsExporting(false);
    }
  };

  const handleClose = () => {
    if (!isExporting && !isDeploying) {
      stopPolling();
      setError(null);
      setSuccess(null);
      setDeployStatus(null);
      onClose();
    }
  };

  const isNotebook = exportFormat === ExportFormat.DATABRICKS_NOTEBOOK;
  const isApp = exportFormat === ExportFormat.DATABRICKS_APP;

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle>
        Export Crew: {crewName}
        <IconButton
          aria-label="close"
          onClick={handleClose}
          disabled={isExporting}
          sx={{
            position: 'absolute',
            right: 8,
            top: 8,
          }}
        >
          <CloseIcon />
        </IconButton>
      </DialogTitle>

      <DialogContent dividers>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {/* Format Selector */}
          <FormControl component="fieldset">
            <FormLabel component="legend">Export Format</FormLabel>
            <ToggleButtonGroup
              value={exportFormat}
              exclusive
              onChange={handleFormatChange}
              size="small"
              sx={{ mt: 1 }}
            >
              {/* Notebook export temporarily hidden from the UI — re-enable by
                  uncommenting this ToggleButton. */}
              {/* <ToggleButton value={ExportFormat.DATABRICKS_NOTEBOOK}>
                Databricks Notebook
              </ToggleButton> */}
              <ToggleButton value={ExportFormat.DATABRICKS_APP}>
                Databricks App
              </ToggleButton>
            </ToggleButtonGroup>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
              {FORMAT_LABELS[exportFormat]?.description}
            </Typography>
          </FormControl>

          {/* Custom tool implementations, comments, static UI and OBO auth are
              always included for app exports — no toggles needed. */}

          {/* Notebook-specific Features */}
          {isNotebook && (
            <FormControl component="fieldset">
              <FormLabel component="legend">Notebook Features</FormLabel>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, mt: 1 }}>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={options.include_tracing ?? true}
                      onChange={handleOptionChange('include_tracing')}
                    />
                  }
                  label="Include MLflow tracing/autolog"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={options.include_evaluation ?? true}
                      onChange={handleOptionChange('include_evaluation')}
                    />
                  }
                  label="Include evaluation metrics"
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={options.include_deployment ?? true}
                      onChange={handleOptionChange('include_deployment')}
                    />
                  }
                  label="Include deployment code"
                />
              </Box>
            </FormControl>
          )}

          {/* App deployment configuration */}
          {isApp && (
            <FormControl component="fieldset">
              <FormLabel component="legend">Databricks App</FormLabel>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, mt: 1 }}>
                <TextField
                  label="App name (for direct deploy)"
                  value={appName}
                  onChange={(e) =>
                    setAppName(
                      e.target.value
                        .toLowerCase()
                        .replace(/[^a-z0-9-]/g, '-')
                        .slice(0, 30)
                    )
                  }
                  helperText="Lowercase letters, numbers and hyphens. Used when deploying to Databricks Apps."
                  fullWidth
                  size="small"
                  sx={{ mt: 1 }}
                  disabled={isDeploying}
                />
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <TextField
                    label="Catalog"
                    value={deployCatalog}
                    onChange={(e) => setDeployCatalog(e.target.value)}
                    helperText="UC catalog for tools/memory"
                    size="small"
                    fullWidth
                    disabled={isDeploying}
                  />
                  <TextField
                    label="Schema"
                    value={deploySchema}
                    onChange={(e) => setDeploySchema(e.target.value)}
                    helperText="UC schema for tools/memory"
                    size="small"
                    fullWidth
                    disabled={isDeploying}
                  />
                </Box>
                <TextField
                  label="MLflow experiment (optional)"
                  value={deployExperiment}
                  onChange={(e) => setDeployExperiment(e.target.value)}
                  helperText="Name or workspace path; created/reused and linked for tracing."
                  fullWidth
                  size="small"
                  disabled={isDeploying}
                />
                <TextField
                  label="SQL Warehouse ID (optional)"
                  value={deployWarehouse}
                  onChange={(e) => setDeployWarehouse(e.target.value)}
                  helperText="Used to store MLflow traces in Unity Catalog. Defaults to the workspace's configured warehouse."
                  fullWidth
                  size="small"
                  disabled={isDeploying}
                />
                <FormControl fullWidth size="small">
                  <InputLabel id="lakebase-label">
                    Lakebase (persistent memory)
                  </InputLabel>
                  <Select
                    labelId="lakebase-label"
                    label="Lakebase (persistent memory)"
                    value={lakebaseChoice}
                    onChange={(e) => setLakebaseChoice(e.target.value)}
                    disabled={isDeploying || loadingLakebase}
                  >
                    <MenuItem value="">
                      <em>None (no persistent memory)</em>
                    </MenuItem>
                    {loadingLakebase ? (
                      <MenuItem disabled value="">
                        <CircularProgress size={18} sx={{ mr: 1 }} /> Loading
                        instances...
                      </MenuItem>
                    ) : (
                      lakebaseInstances.map((lb) => (
                        <MenuItem key={lb.name} value={lb.name}>
                          {lb.name}
                          {lb.state ? ` (${lb.state})` : ''}
                        </MenuItem>
                      ))
                    )}
                    <MenuItem value="__create__">
                      Create new Lakebase instance…
                    </MenuItem>
                  </Select>
                </FormControl>
                {lakebaseChoice === '__create__' && (
                  <TextField
                    label="New Lakebase instance name"
                    value={newLakebaseName}
                    onChange={(e) =>
                      setNewLakebaseName(
                        e.target.value
                          .toLowerCase()
                          .replace(/[^a-z0-9-]/g, '-')
                          .slice(0, 63)
                      )
                    }
                    helperText="A new Lakebase instance is created and attached to the app."
                    fullWidth
                    size="small"
                    disabled={isDeploying}
                  />
                )}
              </Box>
            </FormControl>
          )}

          {/* App deployment progress */}
          {isApp && deployStatus && (
            <Box>
              <Typography variant="body2" sx={{ mb: 0.5 }}>
                {isDeploying ? 'Deploying to Databricks Apps…' : 'Deployment status'}
                {deployStatus.step ? ` — ${deployStatus.step}` : ''}
              </Typography>
              {isDeploying && <LinearProgress sx={{ mb: 1 }} />}
              {deployStatus.message && (
                <Typography variant="caption" color="text.secondary">
                  {deployStatus.message}
                </Typography>
              )}
              {deployStatus.status === 'SUCCEEDED' && deployStatus.app_url && (
                <Typography variant="body2" sx={{ mt: 1 }}>
                  App URL:{' '}
                  <Link
                    href={deployStatus.app_url}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {deployStatus.app_url}
                  </Link>
                </Typography>
              )}
            </Box>
          )}

          {/* Model selection (uses the same enabled model list as the rest of the UI) */}
          <FormControl fullWidth>
            <InputLabel id="model-override-label">Model (optional)</InputLabel>
            <Select
              labelId="model-override-label"
              label="Model (optional)"
              value={options.model_override || ''}
              onChange={handleModelOverrideChange}
              disabled={loadingModels}
            >
              <MenuItem value="">
                <em>Keep each agent&apos;s configured model</em>
              </MenuItem>
              {loadingModels ? (
                <MenuItem disabled value="">
                  <CircularProgress size={18} sx={{ mr: 1 }} /> Loading models...
                </MenuItem>
              ) : (
                Object.entries(availableModels)
                  .filter(([, model]) => model?.enabled === true)
                  .map(([key, model]) => (
                    <MenuItem key={key} value={key}>
                      {model?.name || key}
                      {model?.provider ? ` (${model.provider})` : ''}
                    </MenuItem>
                  ))
              )}
            </Select>
          </FormControl>

          {/* Error/Success Messages */}
          {error && (
            <Alert severity="error" onClose={() => setError(null)}>
              {error}
            </Alert>
          )}
          {success && (
            <Alert severity="success" onClose={() => setSuccess(null)}>
              {success}
            </Alert>
          )}
        </Box>
      </DialogContent>

      <DialogActions>
        <Button onClick={handleClose} disabled={isExporting || isDeploying}>
          Cancel
        </Button>
        <Button
          onClick={handleExport}
          variant="outlined"
          disabled={isExporting || isDeploying}
          startIcon={isExporting ? <CircularProgress size={20} /> : <DownloadIcon />}
        >
          {isExporting ? 'Exporting...' : 'Export & Download'}
        </Button>
        {isApp && (
          <Button
            onClick={handleDeployApp}
            variant="contained"
            disabled={isExporting || isDeploying}
            startIcon={
              isDeploying ? <CircularProgress size={20} /> : <RocketLaunchIcon />
            }
          >
            {isDeploying ? 'Deploying...' : 'Deploy to Databricks Apps'}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default ExportCrewDialog;
