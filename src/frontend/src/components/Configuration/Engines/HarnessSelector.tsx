import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Divider,
  FormControl,
  FormControlLabel,
  Paper,
  Radio,
  RadioGroup,
  Stack,
  Typography,
} from '@mui/material';
import MemoryIcon from '@mui/icons-material/Memory';
import {
  EngineConfigService,
  HarnessDescription,
} from '../../../api/config/EngineConfigService';

/**
 * Picks the agent runtime — Kasal's own or CrewAI — that new executions run on.
 *
 * Its own component rather than another section inside EnginesConfiguration:
 * that file is wiring plus JSX for feature toggles, and this carries real
 * state (availability, capabilities, the reason an engine is unselectable).
 *
 * Two things it is careful about, because both are what people get wrong when
 * they meet this screen:
 *
 * - **The switch is not retroactive.** A run resolves its engine once, when it
 *   is created, and carries it on its own row; switching here changes the NEXT
 *   execution, and a resume stays on whatever engine produced its checkpoint.
 * - **An engine that cannot run says why.** A disabled radio with no
 *   explanation is indistinguishable from a bug.
 */
const HarnessSelector: React.FC = () => {
  const [harness, setHarness] = useState<string>('kasal');
  const [options, setOptions] = useState<HarnessDescription[]>([]);
  const [loading, setLoading] = useState(true);
  const [switching, setSwitching] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const response = await EngineConfigService.getHarness();
      setHarness(response.harness);
      setOptions(response.harnesses ?? []);
      setError(null);
    } catch (err) {
      console.error('Failed to load the harness configuration:', err);
      setError('Failed to load the harness configuration from the server');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const handleChange = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const chosen = event.target.value;
    const previous = harness;
    try {
      setSwitching(true);
      setError(null);
      // Optimistic, then reconciled from the response: the server validates
      // that the engine can actually run before storing it, so its answer is
      // the one to trust.
      setHarness(chosen);
      const response = await EngineConfigService.setHarness(chosen);
      setHarness(response.harness);
      setOptions(response.harnesses ?? []);
    } catch (err) {
      console.error('Failed to change the default harness:', err);
      setError('Failed to change the default harness');
      setHarness(previous);
    } finally {
      setSwitching(false);
    }
  };

  const selected = options.find((option) => option.name === harness);

  return (
    <Paper sx={{ p: 2, mb: 2 }} elevation={1}>
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
        <MemoryIcon sx={{ mr: 1, color: 'primary.main', fontSize: '1.1rem' }} />
        <Typography variant="subtitle1" fontWeight="medium">
          Default Harness
        </Typography>
        {switching && <CircularProgress size={16} sx={{ ml: 1 }} />}
      </Box>

      <Divider sx={{ mb: 2 }} />

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading ? (
        <Box sx={{ display: 'flex', alignItems: 'center', py: 2 }}>
          <CircularProgress size={18} />
          <Typography variant="body2" sx={{ ml: 2 }}>
            Loading available harnesses…
          </Typography>
        </Box>
      ) : (
        <Stack spacing={2}>
          <Typography variant="caption" color="text.secondary">
            The harness a run uses when it does not choose one itself — which
            is what scheduled and API-triggered runs do, having no picker. In
            the designer and chat, pick the harness beside the model.
            <br />
            Changing this never affects a run already under way: each records
            its own harness, and a resume reuses it.
          </Typography>

          <FormControl>
            <RadioGroup value={harness} onChange={handleChange}>
              {options.map((option) => (
                <FormControlLabel
                  key={option.name}
                  value={option.name}
                  disabled={!option.available || switching}
                  control={<Radio color="primary" />}
                  label={
                    <Box sx={{ py: 0.5 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Typography variant="body2" fontWeight="medium">
                          {option.label || option.name}
                        </Typography>
                        {option.version && (
                          <Chip size="small" label={option.version} variant="outlined" />
                        )}
                      </Box>
                      {!option.available && option.unavailable_reason && (
                        <Typography variant="caption" color="error">
                          Unavailable: {option.unavailable_reason}
                        </Typography>
                      )}
                    </Box>
                  }
                />
              ))}
            </RadioGroup>
          </FormControl>

          {selected && selected.capabilities.length > 0 && (
            <Box>
              <Typography variant="caption" color="text.secondary">
                Supported by this harness
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 0.5 }}>
                {selected.capabilities.map((capability: string) => (
                  <Chip
                    key={capability}
                    size="small"
                    label={(capability as string).replace(/_/g, ' ')}
                    variant="outlined"
                  />
                ))}
              </Box>
            </Box>
          )}
        </Stack>
      )}
    </Paper>
  );
};

export default HarnessSelector;
