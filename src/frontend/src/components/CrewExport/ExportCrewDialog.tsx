import React, { useState } from 'react';
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
  IconButton
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import DownloadIcon from '@mui/icons-material/Download';
import { CrewExportService } from '../../api/CrewExportService';
import { ExportFormat, ExportOptions } from '../../types/crewExport';

interface ExportCrewDialogProps {
  open: boolean;
  onClose: () => void;
  crewId: string;
  crewName: string;
}

const ExportCrewDialog: React.FC<ExportCrewDialogProps> = ({
  open,
  onClose,
  crewId,
  crewName
}) => {
  const exportFormat = ExportFormat.DATABRICKS_NOTEBOOK; // Always export as notebook
  const [options, setOptions] = useState<ExportOptions>({
    include_custom_tools: true,
    include_comments: true,
    include_tracing: true,
    include_evaluation: true,
    include_deployment: true,
    model_override: ''
  });
  const [isExporting, setIsExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const handleOptionChange = (option: keyof ExportOptions) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setOptions(prev => ({
      ...prev,
      [option]: event.target.checked
    }));
  };

  const handleModelOverrideChange = (event: React.ChangeEvent<HTMLInputElement>) => {
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

      // Determine filename
      const filename = `${exportResponse.metadata.sanitized_name}.ipynb`;

      // Trigger download
      CrewExportService.triggerDownload(blob, filename);

      setSuccess('Successfully exported crew as Databricks Notebook');

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
    if (!isExporting) {
      setError(null);
      setSuccess(null);
      onClose();
    }
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle>
        Export To Notebook: {crewName}
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
          {/* Export Format Info */}
          <Box>
            <Typography variant="body2" color="text.secondary">
              Exporting as Databricks Notebook (.ipynb) - ready for Databricks import
            </Typography>
          </Box>

          {/* Export Options */}
          <FormControl component="fieldset">
            <FormLabel component="legend">Export Options</FormLabel>
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, mt: 1 }}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={options.include_custom_tools}
                    onChange={handleOptionChange('include_custom_tools')}
                  />
                }
                label="Include custom tool implementations"
              />
              <FormControlLabel
                control={
                  <Checkbox
                    checked={options.include_comments}
                    onChange={handleOptionChange('include_comments')}
                  />
                }
                label="Add explanatory comments"
              />
            </Box>
          </FormControl>

          {/* Databricks Notebook Features */}
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

          {/* Model Override */}
          <TextField
            label="Model Override (optional)"
            value={options.model_override || ''}
            onChange={handleModelOverrideChange}
            helperText="Override the LLM model for all agents"
            fullWidth
            placeholder="e.g., databricks-llama-4-maverick"
          />

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
        <Button onClick={handleClose} disabled={isExporting}>
          Cancel
        </Button>
        <Button
          onClick={handleExport}
          variant="contained"
          disabled={isExporting}
          startIcon={isExporting ? <CircularProgress size={20} /> : <DownloadIcon />}
        >
          {isExporting ? 'Exporting...' : 'Export & Download'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ExportCrewDialog;
