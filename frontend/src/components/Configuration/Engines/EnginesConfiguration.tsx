import React, { useEffect, useState } from 'react';
import {
  Box,
  Typography,
  Switch,
  FormControlLabel,
  Paper,
  Divider,
  Alert,
  Stack,
  CircularProgress
} from '@mui/material';
import EngineeringIcon from '@mui/icons-material/Engineering';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import { useFlowConfigStore } from '../../../store/flowConfig';
import { EngineConfigService } from '../../../api/EngineConfigService';

const EnginesConfiguration: React.FC = () => {
  const { crewAIFlowEnabled, setCrewAIFlowEnabled } = useFlowConfigStore();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);

  // Load initial state from backend
  useEffect(() => {
    const loadFlowConfig = async () => {
      try {
        setLoading(true);
        const response = await EngineConfigService.getCrewAIFlowEnabled();
        setCrewAIFlowEnabled(response.flow_enabled);
      } catch (err) {
        console.error('Failed to load flow configuration:', err);
        setError('Failed to load configuration from server');
      } finally {
        setLoading(false);
      }
    };

    loadFlowConfig();
  }, [setCrewAIFlowEnabled]);

  const handleFlowToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = event.target.checked;
    
    try {
      setSyncing(true);
      setError(null);
      
      // Update backend first
      await EngineConfigService.setCrewAIFlowEnabled(newValue);
      
      // Update local state only after successful backend update
      setCrewAIFlowEnabled(newValue);
    } catch (err) {
      console.error('Failed to update flow configuration:', err);
      setError('Failed to save configuration to server');
      // Revert the toggle if backend update failed
      event.target.checked = !newValue;
    } finally {
      setSyncing(false);
    }
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 200 }}>
        <CircularProgress />
        <Typography variant="body2" sx={{ ml: 2 }}>
          Loading engine configuration...
        </Typography>
      </Box>
    );
  }

  return (
    <Box>
      <Box sx={{ 
        display: 'flex', 
        alignItems: 'center', 
        mb: 3
      }}>
        <EngineeringIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.2rem' }} />
        <Typography variant="h6" fontWeight="medium">
          Engines Configuration
        </Typography>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      <Alert 
        severity="info" 
        sx={{ mb: 3 }}
      >
        Configure execution engines and their features. Disabling features will hide related UI components.
      </Alert>

      {/* CrewAI Engine Section */}
      <Paper sx={{ p: 2, mb: 2 }} elevation={1}>
        <Box sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          mb: 2
        }}>
          <SmartToyIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.1rem' }} />
          <Typography variant="subtitle1" fontWeight="medium">
            CrewAI Engine
          </Typography>
        </Box>

        <Divider sx={{ mb: 2 }} />

        <Stack spacing={2}>
          <Box>
            <FormControlLabel
              control={
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <Switch
                    checked={crewAIFlowEnabled}
                    onChange={handleFlowToggle}
                    color="primary"
                    disabled={syncing}
                  />
                  {syncing && (
                    <CircularProgress size={16} sx={{ ml: 1 }} />
                  )}
                </Box>
              }
              label={
                <Box>
                  <Typography variant="body2" fontWeight="medium">
                    Enable Flow Feature
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Toggle flow execution capabilities, flow panels, and flow-related UI components
                  </Typography>
                </Box>
              }
            />
          </Box>

          {!crewAIFlowEnabled && (
            <Alert severity="warning" sx={{ mt: 1 }}>
              Flow feature is disabled. The following UI components will be hidden:
              <ul style={{ margin: '8px 0 0 16px', paddingLeft: '16px' }}>
                <li>Execute Flow button</li>
                <li>Flow Panel</li>
                <li>Add Flow button</li>
              </ul>
            </Alert>
          )}
        </Stack>
      </Paper>
    </Box>
  );
};

export default EnginesConfiguration; 