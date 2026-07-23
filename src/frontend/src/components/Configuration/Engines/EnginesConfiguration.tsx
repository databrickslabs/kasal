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
  CircularProgress,
  RadioGroup,
  Radio,
  FormControl,
  FormLabel,
  Select,
  MenuItem,
  InputLabel,
} from '@mui/material';
import EngineeringIcon from '@mui/icons-material/Engineering';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import InputIcon from '@mui/icons-material/Input';
import ChatIcon from '@mui/icons-material/Chat';
import { useFlowConfigStore } from '../../../store/flowConfig';
import { EngineConfigService } from '../../../api/EngineConfigService';
import { useCrewExecutionStore } from '../../../store/crewExecution';

const EnginesConfiguration: React.FC = () => {
  const {
    crewAIFlowEnabled,
    setCrewAIFlowEnabled
  } = useFlowConfigStore();
  const { inputMode, setInputMode } = useCrewExecutionStore();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [syncing, setSyncing] = useState(false);
  const [otelEnabled, setOtelEnabled] = useState(false);
  const [otelLogLevel, setOtelLogLevel] = useState('INFO');
  const [otelSyncing, setOtelSyncing] = useState(false);

  // Load initial state from backend
  useEffect(() => {
    const loadConfig = async () => {
      try {
        setLoading(true);
        const [flowResp, otelResp] = await Promise.all([
          EngineConfigService.getCrewAIFlowEnabled(),
          EngineConfigService.getOtelAppTelemetryConfig().catch(() => ({ otel_app_telemetry_enabled: false, otel_app_telemetry_log_level: 'INFO' })),
        ]);
        setCrewAIFlowEnabled(flowResp.flow_enabled);
        setOtelEnabled(otelResp.otel_app_telemetry_enabled);
        setOtelLogLevel(otelResp.otel_app_telemetry_log_level || 'INFO');
      } catch (err) {
        console.error('Failed to load engine configuration:', err);
        setError('Failed to load configuration from server');
      } finally {
        setLoading(false);
      }
    };

    loadConfig();
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

  const handleOtelToggle = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = event.target.checked;
    try {
      setOtelSyncing(true);
      setError(null);
      await EngineConfigService.setOtelAppTelemetryConfig({ enabled: newValue });
      setOtelEnabled(newValue);
    } catch (err) {
      console.error('Failed to update OTel App Telemetry:', err);
      setError('Failed to save OTel App Telemetry configuration');
    } finally {
      setOtelSyncing(false);
    }
  };

  const handleOtelLogLevelChange = async (event: { target: { value: string } }) => {
    const newLevel = event.target.value;
    try {
      setOtelSyncing(true);
      setError(null);
      await EngineConfigService.setOtelAppTelemetryConfig({ log_level: newLevel });
      setOtelLogLevel(newLevel);
    } catch (err) {
      console.error('Failed to update OTel log level:', err);
      setError('Failed to save OTel log level configuration');
    } finally {
      setOtelSyncing(false);
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

      {/* Input Variables Collection Mode */}
      <Paper elevation={1} sx={{ p: 3, mt: 3 }}>
        <Stack spacing={2}>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
            <SmartToyIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.2rem' }} />
            <Typography variant="subtitle1" fontWeight="medium">
              Input Variables Collection
            </Typography>
          </Box>

          <FormControl component="fieldset">
            <FormLabel component="legend">
              <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                Choose how to collect input variables when executing workflows with variables
              </Typography>
            </FormLabel>
            <RadioGroup
              value={inputMode}
              onChange={(e) => setInputMode(e.target.value as 'dialog' | 'chat')}
            >
              <FormControlLabel
                value="dialog"
                control={<Radio color="primary" />}
                label={
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <InputIcon sx={{ mr: 1, fontSize: '1rem' }} />
                    <Box>
                      <Typography variant="body2" fontWeight="medium">
                        Dialog Mode
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Show a popup dialog to collect all variable values before execution
                      </Typography>
                    </Box>
                  </Box>
                }
              />
              <FormControlLabel
                value="chat"
                control={<Radio color="primary" />}
                label={
                  <Box sx={{ display: 'flex', alignItems: 'center' }}>
                    <ChatIcon sx={{ mr: 1, fontSize: '1rem' }} />
                    <Box>
                      <Typography variant="body2" fontWeight="medium">
                        Chat Mode
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Collect variable values through conversational prompts in the chat (Coming Soon)
                      </Typography>
                    </Box>
                  </Box>
                }
              />
            </RadioGroup>
          </FormControl>

          <Alert severity="info" sx={{ mt: 2 }}>
            {inputMode === 'dialog'
              ? 'When variables are detected in your workflow, a dialog will appear to collect all values at once.'
              : 'When variables are detected, the chat will guide you through providing values one by one.'}
          </Alert>
        </Stack>
      </Paper>

      {/* App Telemetry (OpenTelemetry) Section */}
      <Paper sx={{ p: 2, mt: 3 }} elevation={1}>
        <Box sx={{
          display: 'flex',
          alignItems: 'center',
          mb: 2
        }}>
          <EngineeringIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.1rem' }} />
          <Typography variant="subtitle1" fontWeight="medium">
            App Telemetry (Preview)
          </Typography>
        </Box>

        <Divider sx={{ mb: 2 }} />

        <Stack spacing={2}>
          <Box>
            <FormControlLabel
              control={
                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                  <Switch
                    checked={otelEnabled}
                    onChange={handleOtelToggle}
                    color="primary"
                    disabled={otelSyncing}
                  />
                  {otelSyncing && (
                    <CircularProgress size={16} sx={{ ml: 1 }} />
                  )}
                </Box>
              }
              label={
                <Box>
                  <Typography variant="body2" fontWeight="medium">
                    Enable Structured OTel Log Export
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Send structured OpenTelemetry logs to Unity Catalog tables for monitoring and analysis.
                    Requires App Telemetry to be enabled in the Databricks App settings.
                  </Typography>
                </Box>
              }
            />
          </Box>

          {otelEnabled && (
            <>
              <FormControl size="small" sx={{ mt: 1, minWidth: 180 }}>
                <InputLabel id="otel-log-level-label">Log Level</InputLabel>
                <Select
                  labelId="otel-log-level-label"
                  value={otelLogLevel}
                  label="Log Level"
                  onChange={handleOtelLogLevelChange}
                  disabled={otelSyncing}
                >
                  <MenuItem value="DEBUG">DEBUG</MenuItem>
                  <MenuItem value="INFO">INFO</MenuItem>
                  <MenuItem value="WARNING">WARNING</MenuItem>
                  <MenuItem value="ERROR">ERROR</MenuItem>
                </Select>
                <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                  Minimum severity of log records exported via OTel
                </Typography>
              </FormControl>
              <Alert severity="info" sx={{ mt: 1 }}>
                Structured log records (with severity, trace context, and resource attributes) will be
                exported via OTLP to the telemetry destination configured in Databricks App settings.
                Logs are written to the <code>otel_logs</code> table in the configured Unity Catalog schema.
              </Alert>
            </>
          )}
        </Stack>
      </Paper>
    </Box>
  );
};

export default EnginesConfiguration;