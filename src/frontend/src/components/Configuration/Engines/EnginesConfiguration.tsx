import React, { useEffect, useState } from 'react';
import {
  Box,
  Chip,
  Typography,
  Switch,
  FormControlLabel,
  Paper,
  Alert,
  Stack,
  CircularProgress,
  RadioGroup,
  Radio,
  FormControl,
  Select,
  MenuItem,
  InputLabel,
} from '@mui/material';
import EngineeringIcon from '@mui/icons-material/Engineering';
import SmartToyIcon from '@mui/icons-material/SmartToy';
import InputIcon from '@mui/icons-material/Input';
import ChatIcon from '@mui/icons-material/Chat';
import { EngineConfigService } from '../../../api/config/EngineConfigService';
import { useCrewExecutionStore } from '../../../store/crewExecution';
import { useEventTriggersStore } from '../../../store/eventTriggers';
import HarnessSelector from './HarnessSelector';

const EnginesConfiguration: React.FC = () => {
  const { inputMode, setInputMode } = useCrewExecutionStore();
  // Event Triggers lives in a shared store so this toggle and the workflow
  // right-sidebar action stay in sync live (no refresh needed).
  const eventTriggersEnabled = useEventTriggersStore((s) => s.enabled);
  const setEventTriggersEnabledStore = useEventTriggersStore((s) => s.setEnabled);
  const loadEventTriggers = useEventTriggersStore((s) => s.load);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [otelEnabled, setOtelEnabled] = useState(false);
  const [otelLogLevel, setOtelLogLevel] = useState('INFO');
  const [otelSyncing, setOtelSyncing] = useState(false);
  const [eventTriggersSyncing, setEventTriggersSyncing] = useState(false);

  // Load initial state from backend
  useEffect(() => {
    const loadConfig = async () => {
      try {
        setLoading(true);
        const otelResp = await EngineConfigService.getOtelAppTelemetryConfig().catch(
          () => ({ otel_app_telemetry_enabled: false, otel_app_telemetry_log_level: 'INFO' }),
        );
        setOtelEnabled(otelResp.otel_app_telemetry_enabled);
        setOtelLogLevel(otelResp.otel_app_telemetry_log_level || 'INFO');
        await loadEventTriggers();
      } catch (err) {
        console.error('Failed to load engine configuration:', err);
        setError('Failed to load configuration from server');
      } finally {
        setLoading(false);
      }
    };

    loadConfig();
  }, [loadEventTriggers]);

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

  const handleEventTriggersToggle = async (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const newValue = event.target.checked;
    try {
      setEventTriggersSyncing(true);
      setError(null);
      await setEventTriggersEnabledStore(newValue);
    } catch (err) {
      console.error('Failed to update event triggers configuration:', err);
      setError('Failed to save event-trigger configuration');
    } finally {
      setEventTriggersSyncing(false);
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
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
        <EngineeringIcon sx={{ color: 'primary.main', fontSize: '1.2rem' }} />
        <Typography variant="h6" fontWeight={600}>
          Engines
        </Typography>
      </Box>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2.5 }}>
        Execution engines and their features — disabled features hide their UI.
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <Stack spacing={2}>
        {/* The DEFAULT harness. A run may name its own beside the model; this
            is what applies when it does not — scheduled and API runs. */}
        <HarnessSelector />

        {/* Input variables collection */}
        <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
            <SmartToyIcon sx={{ color: 'primary.main', fontSize: '1.1rem' }} />
            <Typography variant="subtitle1" fontWeight={600}>
              Input Variables
            </Typography>
          </Box>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
            How variable values are collected when a workflow declares them.
          </Typography>
          <RadioGroup
            row
            value={inputMode}
            onChange={(e) => setInputMode(e.target.value as 'dialog' | 'chat')}
          >
            <FormControlLabel
              value="dialog"
              control={<Radio size="small" color="primary" />}
              label={
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                  <InputIcon sx={{ fontSize: '1rem' }} />
                  <Typography variant="body2">Dialog — collect all values up front</Typography>
                </Box>
              }
            />
            <FormControlLabel
              value="chat"
              disabled
              control={<Radio size="small" color="primary" />}
              label={
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                  <ChatIcon sx={{ fontSize: '1rem' }} />
                  <Typography variant="body2">Conversational prompts</Typography>
                </Box>
              }
            />
          </RadioGroup>
        </Paper>

        {/* App telemetry (OTel) */}
        <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="subtitle1" fontWeight={600}>
              App Telemetry
            </Typography>
            <Chip size="small" label="Preview" variant="outlined" sx={{ height: 20 }} />
            {otelSyncing && <CircularProgress size={14} />}
            <Box sx={{ flex: 1 }} />
            <Switch
              checked={otelEnabled}
              onChange={handleOtelToggle}
              color="primary"
              disabled={otelSyncing}
              inputProps={{ 'aria-label': 'Enable structured OTel log export' }}
            />
          </Box>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.25 }}>
            Structured OTel logs to Unity Catalog (the <code>otel_logs</code> table) via the
            destination in Databricks App settings — which must have App Telemetry enabled.
          </Typography>
          {otelEnabled && (
            <FormControl size="small" sx={{ mt: 1.5, minWidth: 180 }}>
              <InputLabel id="otel-log-level-label">Log level</InputLabel>
              <Select
                labelId="otel-log-level-label"
                value={otelLogLevel}
                label="Log level"
                onChange={handleOtelLogLevelChange}
                disabled={otelSyncing}
              >
                <MenuItem value="DEBUG">DEBUG</MenuItem>
                <MenuItem value="INFO">INFO</MenuItem>
                <MenuItem value="WARNING">WARNING</MenuItem>
                <MenuItem value="ERROR">ERROR</MenuItem>
              </Select>
            </FormControl>
          )}
        </Paper>

        {/* Event triggers */}
        <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="subtitle1" fontWeight={600}>
              Event Triggers
            </Typography>
            <Chip size="small" label="Preview" variant="outlined" sx={{ height: 20 }} />
            {eventTriggersSyncing && <CircularProgress size={14} />}
            <Box sx={{ flex: 1 }} />
            <Switch
              checked={eventTriggersEnabled}
              onChange={handleEventTriggersToggle}
              color="primary"
              disabled={eventTriggersSyncing}
              inputProps={{ 'aria-label': 'Enable event triggers' }}
            />
          </Box>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.25 }}>
            A finished crew/flow can emit an event that triggers another.
            {eventTriggersEnabled
              ? ' The background consumer is draining the queue.'
              : ' Currently off — fired events stay queued until enabled.'}
          </Typography>
        </Paper>
      </Stack>
    </Box>
  );
};

export default EnginesConfiguration;
