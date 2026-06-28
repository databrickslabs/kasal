import React, { useState, useCallback } from 'react';
import Box from '@mui/material/Box';
import { DetectedVariable } from '../../utils/variableDetector';
import { buttonResetSx, inputResetSx } from '../../chatSx';

/**
 * Inline input-variables prompt, rendered IN the chat flow (same style as the
 * Genie space selection on crew cards) — replaces the old modal dialog. The
 * crew run is parked until the user fills the placeholders and hits Run.
 */

const SENSITIVE_PATTERNS = [
  /secret/i,
  /password/i,
  /passwd/i,
  /token/i,
  /api[_-]?key/i,
  /credential/i,
  /private[_-]?key/i,
  /access[_-]?key/i,
];

export function isSensitive(name: string): boolean {
  return SENSITIVE_PATTERNS.some((p) => p.test(name));
}

// Survives re-renders/remounts within the session, mirroring the genie
// selection store: once a prompt has run, it stays disabled.
const variablesPromptStore = new Map<string, { submitted: boolean }>();

interface InputVariablesPromptProps {
  variables: DetectedVariable[];
  messageId: string;
  onSubmit?: (inputs: Record<string, string>) => void;
}

const InputVariablesPrompt: React.FC<InputVariablesPromptProps> = ({
  variables,
  messageId,
  onSubmit,
}) => {
  const [values, setValues] = useState<Record<string, string>>({});
  const [visibleFields, setVisibleFields] = useState<Record<string, boolean>>({});
  const [submitted, setSubmitted] = useState(
    () => variablesPromptStore.get(messageId)?.submitted ?? false,
  );

  const handleChange = useCallback((name: string, value: string) => {
    setValues((prev) => ({ ...prev, [name]: value }));
  }, []);

  const toggleVisibility = useCallback((name: string) => {
    setVisibleFields((prev) => ({ ...prev, [name]: !prev[name] }));
  }, []);

  const requiredFilled = variables.every(
    (v) => !v.required || Boolean(values[v.name]?.trim()),
  );

  // The Run button is disabled until every required variable is filled, so no
  // per-field validation is needed here — only trimmed non-empty values ship.
  const handleRun = () => {
    const inputs: Record<string, string> = {};
    for (const [k, v] of Object.entries(values)) {
      if (v.trim()) inputs[k] = v.trim();
    }
    setSubmitted(true);
    variablesPromptStore.set(messageId, { submitted: true });
    onSubmit?.(inputs);
  };

  return (
    <Box sx={{ pt: 0.5, '& > * + *': { mt: 1 } }}>
      <Box component="p" sx={{ fontSize: 12, px: 0.5, color: 'text.disabled' }}>
        This crew uses{' '}
        {/* Styled as a <span> (not <code>) so the chat's global inline-code rule
            doesn't override the token background; the code-red text + monospace
            are reproduced here. */}
        <Box
          component="span"
          sx={{
            px: 0.5,
            py: 0.25,
            borderRadius: '4px',
            fontSize: 11,
            fontFamily: 'monospace',
            fontWeight: 500,
            backgroundColor: (t) => t.chat.bgSecondary,
            color: (t) => (t.palette.mode === 'dark' ? '#FF7A6B' : '#D42E1B'),
          }}
        >
          &#123;variable&#125;
        </Box>{' '}
        placeholders. Provide values below to run.
      </Box>

      {variables.map((v) => {
        const sensitive = isSensitive(v.name);
        const showValue = visibleFields[v.name] || false;
        return (
          <Box key={v.name}>
            <Box
              component="label"
              sx={{
                display: 'block',
                fontSize: 10,
                fontWeight: 600,
                textTransform: 'uppercase',
                letterSpacing: '0.05em',
                mb: 0.5,
                px: 0.5,
                color: 'text.disabled',
              }}
            >
              {v.name.replace(/[_-]/g, ' ')}
              {v.required && <Box component="span" sx={{ color: '#ef4444' }}> *</Box>}
            </Box>
            <Box sx={{ position: 'relative' }}>
              <Box
                component="input"
                type={sensitive && !showValue ? 'password' : 'text'}
                value={values[v.name] || ''}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleChange(v.name, e.target.value)}
                placeholder={`Enter ${v.name.replace(/[_-]/g, ' ')}`}
                disabled={submitted}
                sx={{
                  ...inputResetSx,
                  width: '100%',
                  borderRadius: '8px',
                  px: 1.5,
                  py: 1,
                  fontSize: 14,
                  transition: 'border-color 0.15s, background-color 0.15s',
                  backgroundColor: 'background.paper',
                  color: 'text.primary',
                  border: 1,
                  borderColor: 'divider',
                  pr: sensitive ? '2.5rem' : undefined,
                  '&:disabled': { opacity: 0.5 },
                }}
              />
              {sensitive && (
                <Box
                  component="button"
                  type="button"
                  onClick={() => toggleVisibility(v.name)}
                  title={showValue ? 'Hide' : 'Show'}
                  sx={{
                    ...buttonResetSx,
                    position: 'absolute',
                    right: 8,
                    top: '50%',
                    transform: 'translateY(-50%)',
                    width: 24,
                    height: 24,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    borderRadius: '4px',
                    transition: 'opacity 0.15s',
                    color: 'text.disabled',
                    '&:hover': { opacity: 0.7 },
                  }}
                >
                  {showValue ? (
                    <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M3.98 8.223A10.477 10.477 0 001.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.45 10.45 0 0112 4.5c4.756 0 8.773 3.162 10.065 7.498a10.523 10.523 0 01-4.293 5.774M6.228 6.228L3 3m3.228 3.228l3.65 3.65m7.894 7.894L21 21m-3.228-3.228l-3.65-3.65m0 0a3 3 0 10-4.243-4.243m4.242 4.242L9.88 9.88" />
                    </Box>
                  ) : (
                    <Box component="svg" sx={{ width: 16, height: 16 }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z" />
                      <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                    </Box>
                  )}
                </Box>
              )}
            </Box>
          </Box>
        );
      })}

      {/* Same affordance as the Genie space run button: full-width, subtle,
          disabled until ready and after it fires. */}
      <Box
        component="button"
        type="button"
        onClick={handleRun}
        disabled={!requiredFilled || submitted}
        sx={{
          ...buttonResetSx,
          width: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: 1,
          borderRadius: '8px',
          px: 1.5,
          py: 1,
          fontSize: 14,
          fontWeight: 500,
          transition: 'all 0.15s',
          backgroundColor: (t) => t.chat.bgSecondary,
          color: 'text.secondary',
          border: 1,
          borderColor: 'divider',
          '&:hover': { opacity: 0.8 },
          '&:disabled': { opacity: 0.5, cursor: 'not-allowed' },
        }}
      >
        <Box component="svg" sx={{ width: 16, height: 16 }} fill="currentColor" viewBox="0 0 24 24">
          <path d="M8 5v14l11-7z" />
        </Box>
        {submitted
          ? 'Running…'
          : requiredFilled
            ? 'Run crew'
            : 'Fill in the variables to run'}
      </Box>
    </Box>
  );
};

export default InputVariablesPrompt;
