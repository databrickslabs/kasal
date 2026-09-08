import { Box, Typography, Tooltip, IconButton, FormControlLabel, Switch, Button, CircularProgress, TextField, FormControl, InputLabel, Select, MenuItem, FormHelperText } from '@mui/material';
import HelpOutlineIcon from '@mui/icons-material/HelpOutline';
import type { LLMGuardrailConfig } from '../../../../types/workflow/task';

interface Props {
  guardrail?: LLMGuardrailConfig | null;
  onToggle: (enabled: boolean) => void;
  onSuggest: () => void;
  suggesting: boolean;
  canSuggest: boolean;
  onChange: (field: keyof LLMGuardrailConfig, value: string) => void;
  models: { name: string }[];
  modelsLoading: boolean;
  maxRetries: number;
  onRetriesChange: (value: number) => void;
}

/** Task output validation, using the same neutral controls as the editor. */
export default function TaskGuardrailSection({ guardrail, onToggle, onSuggest, suggesting, canSuggest, onChange, models, modelsLoading, maxRetries, onRetriesChange }: Props) {
  return (
            <Box sx={{
              mt: 2,
              p: 2,
              backgroundColor: 'transparent',
              borderRadius: 3,
              border: '1px solid', borderColor: 'divider'
            }}>
              <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'text.secondary' }}>
                  LLM Guardrail
                </Typography>
                <Tooltip title="Uses an LLM agent to validate task output against criteria you define. This provides flexible, AI-powered validation.">
                  <IconButton size="small" sx={{ ml: 0.5 }}>
                    <HelpOutlineIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Box>

              <FormControlLabel
                control={
                  <Switch
                    checked={Boolean(guardrail)}
                    onChange={(e) => onToggle(e.target.checked)}
                    color="default"
                  />
                }
                label="Enable LLM Guardrail"
              />

              {guardrail && (
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'flex-end' }}>
                    <Button
                      size="small"
                      variant="outlined"
                      onClick={onSuggest}
                      disabled={suggesting || !canSuggest}
                      startIcon={suggesting ? <CircularProgress size={16} /> : undefined}
                    >
                      {suggesting ? 'Suggesting…' : 'Suggest criteria from task'}
                    </Button>
                  </Box>
                  <TextField
                    label="Validation Criteria"
                    value={guardrail.description || ''}
                    onChange={(e) => onChange('description', e.target.value)}
                    fullWidth
                    multiline
                    rows={2}
                    placeholder="Describe how the LLM should validate the task output..."
                    helperText="Describe the criteria the LLM will use to validate the task output, or click Suggest to generate it from the task description and expected output"
                  />
                  <FormControl fullWidth>
                    <InputLabel>Validation LLM Model</InputLabel>
                    <Select
                      value={guardrail.llm_model || ''}
                      onChange={(e) => onChange('llm_model', e.target.value)}
                      label="Validation LLM Model"
                      disabled={modelsLoading}
                      startAdornment={modelsLoading ? <CircularProgress size={20} sx={{ mr: 1 }} /> : null}
                    >
                      <MenuItem value="">
                        <em>Use the model selected for the run (default)</em>
                      </MenuItem>
                      {models.map((model) => (
                        <MenuItem key={model.name} value={model.name}>
                          {model.name}
                        </MenuItem>
                      ))}
                    </Select>
                    <FormHelperText>
                      Defaults to the model selected for the run (the chat input model).
                      Pick a specific model to override.
                    </FormHelperText>
                  </FormControl>
                  <TextField
                    label="Max retries on validation failure"
                    type="number"
                    value={maxRetries}
                    onChange={(e) => {
                      const parsed = Math.max(0, Math.min(10, parseInt(e.target.value, 10) || 0));
                      onRetriesChange(parsed);
                    }}
                    fullWidth
                    inputProps={{ min: 0, max: 10 }}
                    helperText="How many times the task is retried if the guardrail rejects the output (default 3)"
                  />
                </Box>
              )}
            </Box>

  );
}
