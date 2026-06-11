import React, { useState, useCallback } from 'react';
import { DetectedVariable } from '../../utils/variableDetector';

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
  const [errors, setErrors] = useState<Record<string, boolean>>({});
  const [visibleFields, setVisibleFields] = useState<Record<string, boolean>>({});
  const [submitted, setSubmitted] = useState(
    () => variablesPromptStore.get(messageId)?.submitted ?? false,
  );

  const handleChange = useCallback((name: string, value: string) => {
    setValues((prev) => ({ ...prev, [name]: value }));
    if (value) {
      setErrors((prev) => {
        const next = { ...prev };
        delete next[name];
        return next;
      });
    }
  }, []);

  const toggleVisibility = useCallback((name: string) => {
    setVisibleFields((prev) => ({ ...prev, [name]: !prev[name] }));
  }, []);

  const requiredFilled = variables.every(
    (v) => !v.required || Boolean(values[v.name]?.trim()),
  );

  const handleRun = () => {
    const newErrors: Record<string, boolean> = {};
    let hasError = false;
    for (const v of variables) {
      if (v.required && !values[v.name]?.trim()) {
        newErrors[v.name] = true;
        hasError = true;
      }
    }
    if (hasError) {
      setErrors(newErrors);
      return;
    }
    const inputs: Record<string, string> = {};
    for (const [k, v] of Object.entries(values)) {
      if (v.trim()) inputs[k] = v.trim();
    }
    setSubmitted(true);
    variablesPromptStore.set(messageId, { submitted: true });
    onSubmit?.(inputs);
  };

  return (
    <div className="pt-1 space-y-2">
      <p className="text-xs px-1" style={{ color: 'var(--text-muted)' }}>
        This crew uses{' '}
        <code
          className="px-1 py-0.5 rounded text-[11px]"
          style={{ backgroundColor: 'var(--bg-secondary)' }}
        >
          &#123;variable&#125;
        </code>{' '}
        placeholders. Provide values below to run.
      </p>

      {variables.map((v) => {
        const sensitive = isSensitive(v.name);
        const showValue = visibleFields[v.name] || false;
        return (
          <div key={v.name}>
            <label
              className="block text-[10px] font-semibold uppercase tracking-wider mb-1 px-1"
              style={{ color: errors[v.name] ? '#ef4444' : 'var(--text-muted)' }}
            >
              {v.name.replace(/[_-]/g, ' ')}
              {v.required && <span style={{ color: '#ef4444' }}> *</span>}
            </label>
            <div className="relative">
              <input
                type={sensitive && !showValue ? 'password' : 'text'}
                value={values[v.name] || ''}
                onChange={(e) => handleChange(v.name, e.target.value)}
                placeholder={`Enter ${v.name.replace(/[_-]/g, ' ')}`}
                disabled={submitted}
                className="w-full rounded-lg px-3 py-2 text-sm outline-none transition-colors disabled:opacity-50"
                style={{
                  backgroundColor: 'var(--bg-input)',
                  color: 'var(--text-primary)',
                  border: errors[v.name]
                    ? '1px solid #ef4444'
                    : '1px solid var(--border-color)',
                  paddingRight: sensitive ? '2.5rem' : undefined,
                }}
              />
              {sensitive && (
                <button
                  type="button"
                  onClick={() => toggleVisibility(v.name)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 w-6 h-6 flex items-center justify-center rounded transition-colors hover:opacity-70"
                  style={{ color: 'var(--text-muted)' }}
                  title={showValue ? 'Hide' : 'Show'}
                >
                  {showValue ? (
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M3.98 8.223A10.477 10.477 0 001.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.45 10.45 0 0112 4.5c4.756 0 8.773 3.162 10.065 7.498a10.523 10.523 0 01-4.293 5.774M6.228 6.228L3 3m3.228 3.228l3.65 3.65m7.894 7.894L21 21m-3.228-3.228l-3.65-3.65m0 0a3 3 0 10-4.243-4.243m4.242 4.242L9.88 9.88" />
                    </svg>
                  ) : (
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z" />
                      <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                    </svg>
                  )}
                </button>
              )}
            </div>
            {errors[v.name] && (
              <p className="text-[11px] mt-0.5 px-1" style={{ color: '#ef4444' }}>
                This variable is required
              </p>
            )}
          </div>
        );
      })}

      {/* Same affordance as the Genie space run button: full-width, subtle,
          disabled until ready and after it fires. */}
      <button
        type="button"
        onClick={handleRun}
        disabled={!requiredFilled || submitted}
        className="w-full flex items-center justify-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-all hover:opacity-80 disabled:opacity-50 disabled:cursor-not-allowed"
        style={{
          backgroundColor: 'var(--bg-secondary)',
          color: 'var(--text-secondary)',
          border: '1px solid var(--border-color)',
        }}
      >
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
          <path d="M8 5v14l11-7z" />
        </svg>
        {submitted
          ? 'Running…'
          : requiredFilled
            ? 'Run crew'
            : 'Fill in the variables to run'}
      </button>
    </div>
  );
};

export default InputVariablesPrompt;
