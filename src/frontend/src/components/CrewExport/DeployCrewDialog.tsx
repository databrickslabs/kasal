import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Checkbox,
  Box,
  Alert,
  CircularProgress,
  Typography,
  IconButton,
  Divider,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import RocketLaunchIcon from '@mui/icons-material/RocketLaunch';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { CrewExportService } from '../../api/CrewExportService';
import { ModelServingConfig, DeploymentResponse } from '../../types/crewExport';

interface DeployCrewDialogProps {
  open: boolean;
  onClose: () => void;
  crewId: string;
  crewName: string;
}

const DeployCrewDialog: React.FC<DeployCrewDialogProps> = ({
  open,
  onClose,
  crewId,
  crewName
}) => {
  const [config, setConfig] = useState<ModelServingConfig>({
    model_name: '',
    endpoint_name: '',
    workload_size: 'Small',
    scale_to_zero_enabled: true,
    unity_catalog_model: true,
    catalog_name: '',
    schema_name: ''
  });
  const [isDeploying, setIsDeploying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [deploymentResult, setDeploymentResult] = useState<DeploymentResponse | null>(null);

  const handleInputChange = (field: keyof ModelServingConfig) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setConfig(prev => ({
      ...prev,
      [field]: event.target.value
    }));
  };

  const handleCheckboxChange = (field: keyof ModelServingConfig) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setConfig(prev => ({
      ...prev,
      [field]: event.target.checked
    }));
  };

  const handleWorkloadSizeChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setConfig(prev => ({
      ...prev,
      workload_size: event.target.value as 'Small' | 'Medium' | 'Large'
    }));
  };

  const handleDeploy = async () => {
    // Validation
    if (!config.model_name) {
      setError('Model name is required');
      return;
    }

    if (config.unity_catalog_model && (!config.catalog_name || !config.schema_name)) {
      setError('Catalog name and schema name are required when using Unity Catalog');
      return;
    }

    setIsDeploying(true);
    setError(null);
    setDeploymentResult(null);

    try {
      const result = await CrewExportService.deployCrew(crewId, { config });
      setDeploymentResult(result);
    } catch (err) {
      console.error('Deployment error:', err);
      setError(err instanceof Error ? err.message : 'Failed to deploy crew');
    } finally {
      setIsDeploying(false);
    }
  };

  const handleClose = () => {
    if (!isDeploying) {
      setError(null);
      setDeploymentResult(null);
      onClose();
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="md"
      fullWidth
    >
      <DialogTitle>
        Deploy Crew to Databricks Model Serving: {crewName}
        <IconButton
          aria-label="close"
          onClick={handleClose}
          disabled={isDeploying}
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
        {!deploymentResult ? (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            {/* Basic Configuration */}
            <Box>
              <Typography variant="h6" gutterBottom>
                Basic Configuration
              </Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <TextField
                  label="Model Name *"
                  value={config.model_name}
                  onChange={handleInputChange('model_name')}
                  fullWidth
                  required
                  helperText="Name for the registered model"
                />
                <TextField
                  label="Endpoint Name"
                  value={config.endpoint_name}
                  onChange={handleInputChange('endpoint_name')}
                  fullWidth
                  helperText="Name for serving endpoint (defaults to model name)"
                />
              </Box>
            </Box>

            <Divider />

            {/* Serving Configuration */}
            <Box>
              <Typography variant="h6" gutterBottom>
                Serving Configuration
              </Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <FormControl component="fieldset">
                  <FormLabel component="legend">Workload Size</FormLabel>
                  <RadioGroup
                    value={config.workload_size}
                    onChange={handleWorkloadSizeChange}
                    row
                  >
                    <FormControlLabel value="Small" control={<Radio />} label="Small" />
                    <FormControlLabel value="Medium" control={<Radio />} label="Medium" />
                    <FormControlLabel value="Large" control={<Radio />} label="Large" />
                  </RadioGroup>
                </FormControl>

                <FormControlLabel
                  control={
                    <Checkbox
                      checked={config.scale_to_zero_enabled}
                      onChange={handleCheckboxChange('scale_to_zero_enabled')}
                    />
                  }
                  label="Enable scale to zero"
                />
              </Box>
            </Box>

            <Divider />

            {/* Unity Catalog Configuration */}
            <Box>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={config.unity_catalog_model}
                    onChange={handleCheckboxChange('unity_catalog_model')}
                  />
                }
                label="Register in Unity Catalog"
              />

              {config.unity_catalog_model && (
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 2 }}>
                  <TextField
                    label="Catalog Name *"
                    value={config.catalog_name}
                    onChange={handleInputChange('catalog_name')}
                    fullWidth
                    required
                  />
                  <TextField
                    label="Schema Name *"
                    value={config.schema_name}
                    onChange={handleInputChange('schema_name')}
                    fullWidth
                    required
                  />
                </Box>
              )}
            </Box>

            {/* Error Message */}
            {error && (
              <Alert severity="error" onClose={() => setError(null)}>
                {error}
              </Alert>
            )}
          </Box>
        ) : (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
            <Alert severity="success">
              Crew deployed successfully!
            </Alert>

            <Box>
              <Typography variant="h6" gutterBottom>
                Deployment Details
              </Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                <Typography variant="body2">
                  <strong>Model:</strong> {deploymentResult.model_name} (v{deploymentResult.model_version})
                </Typography>
                <Typography variant="body2">
                  <strong>Endpoint:</strong> {deploymentResult.endpoint_name}
                </Typography>
                <Typography variant="body2">
                  <strong>Status:</strong> {deploymentResult.endpoint_status}
                </Typography>
                <Typography variant="body2">
                  <strong>Deployed At:</strong> {new Date(deploymentResult.deployed_at).toLocaleString()}
                </Typography>
              </Box>
            </Box>

            <Box>
              <TextField
                label="Endpoint URL"
                value={deploymentResult.endpoint_url}
                fullWidth
                InputProps={{
                  readOnly: true,
                }}
                onClick={() => copyToClipboard(deploymentResult.endpoint_url)}
              />
            </Box>

            <Accordion>
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Typography>Usage Example</Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Box
                  component="pre"
                  sx={{
                    backgroundColor: 'grey.100',
                    p: 2,
                    borderRadius: 1,
                    overflow: 'auto',
                    fontSize: '0.875rem',
                    cursor: 'pointer'
                  }}
                  onClick={() => copyToClipboard(deploymentResult.usage_example)}
                >
                  {deploymentResult.usage_example}
                </Box>
              </AccordionDetails>
            </Accordion>
          </Box>
        )}
      </DialogContent>

      <DialogActions>
        {!deploymentResult ? (
          <>
            <Button onClick={handleClose} disabled={isDeploying}>
              Cancel
            </Button>
            <Button
              onClick={handleDeploy}
              variant="contained"
              disabled={isDeploying}
              startIcon={isDeploying ? <CircularProgress size={20} /> : <RocketLaunchIcon />}
            >
              {isDeploying ? 'Deploying...' : 'Deploy to Model Serving'}
            </Button>
          </>
        ) : (
          <Button onClick={handleClose} variant="contained">
            Close
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default DeployCrewDialog;
