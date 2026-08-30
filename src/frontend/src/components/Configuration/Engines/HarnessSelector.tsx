import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Chip,
  CircularProgress,
  Paper,
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
    <Paper variant="outlined" sx={{ p: 2.5, borderRadius: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
        <MemoryIcon sx={{ color: 'primary.main', fontSize: '1.1rem' }} />
        <Typography variant="subtitle1" fontWeight={600}>
          Default Harness
        </Typography>
        {switching && <CircularProgress size={14} />}
      </Box>
      <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1.5 }}>
        Used by runs that don&apos;t pick one themselves (scheduled and API-triggered).
        Runs under way keep the harness they started with.
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 1.5 }}>
          {error}
        </Alert>
      )}

      {loading ? (
        <Box sx={{ display: 'flex', alignItems: 'center', py: 1.5 }}>
          <CircularProgress size={18} />
          <Typography variant="body2" sx={{ ml: 2 }}>
            Loading available harnesses…
          </Typography>
        </Box>
      ) : (
        <>
          {/* Selectable cards — click to choose; accent border marks the active one. */}
          <Box sx={{ display: 'flex', gap: 1.5, flexWrap: 'wrap' }}>
            {options.map((option) => {
              const isSelected = option.name === harness;
              const disabled = !option.available || switching;
              return (
                <Box
                  key={option.name}
                  component="button"
                  type="button"
                  disabled={disabled}
                  aria-pressed={isSelected}
                  onClick={() =>
                    handleChange({
                      target: { value: option.name },
                    } as React.ChangeEvent<HTMLInputElement>)
                  }
                  sx={{
                    flex: '1 1 180px',
                    maxWidth: 260,
                    textAlign: 'left',
                    px: 2,
                    py: 1.25,
                    borderRadius: 2,
                    cursor: disabled ? 'default' : 'pointer',
                    border: '1.5px solid',
                    borderColor: isSelected ? 'primary.main' : 'divider',
                    backgroundColor: isSelected ? 'action.selected' : 'background.paper',
                    opacity: disabled && !isSelected ? 0.55 : 1,
                    font: 'inherit',
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Typography variant="body2" fontWeight={600}>
                      {option.label || option.name}
                    </Typography>
                    {option.version && (
                      <Chip size="small" label={option.version} variant="outlined" sx={{ height: 20 }} />
                    )}
                  </Box>
                  {!option.available && option.unavailable_reason && (
                    <Typography variant="caption" color="error">
                      Unavailable: {option.unavailable_reason}
                    </Typography>
                  )}
                </Box>
              );
            })}
          </Box>

          {selected && selected.capabilities.length > 0 && (
            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1.5 }}>
              Supports: {selected.capabilities.map((c: string) => c.replace(/_/g, ' ')).join(' · ')}
            </Typography>
          )}
        </>
      )}
    </Paper>
  );
};

export default HarnessSelector;
