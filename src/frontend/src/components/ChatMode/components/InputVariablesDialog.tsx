import React, { useState, useEffect, useCallback } from 'react';
import { DetectedVariable } from '../utils/variableDetector';

interface InputVariablesDialogProps {
  open: boolean;
  variables: DetectedVariable[];
  onConfirm: (inputs: Record<string, string>) => void;
  onCancel: () => void;
}

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

function isSensitive(name: string): boolean {
  return SENSITIVE_PATTERNS.some((p) => p.test(name));
}

const InputVariablesDialog: React.FC<InputVariablesDialogProps> = ({
  open,
  variables,
  onConfirm,
  onCancel,
}) => {
  const [values, setValues] = useState<Record<string, string>>({});
  const [errors, setErrors] = useState<Record<string, boolean>>({});
  const [visibleFields, setVisibleFields] = useState<Record<string, boolean>>({});

  // Reset state when dialog opens with new variables
  useEffect(() => {
    if (open) {
      const init: Record<string, string> = {};
      for (const v of variables) {
        init[v.name] = '';
      }
      setValues(init);
      setErrors({});
      setVisibleFields({});
    }
  }, [open, variables]);

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

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();

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

      // Only include non-empty values
      const inputs: Record<string, string> = {};
      for (const [k, v] of Object.entries(values)) {
        if (v.trim()) inputs[k] = v.trim();
      }
      onConfirm(inputs);
    },
    [variables, values, onConfirm],
  );

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50"
        onClick={onCancel}
      />

      {/* Dialog */}
      <div
        className="relative w-full max-w-lg mx-4 rounded-xl shadow-2xl overflow-hidden"
        style={{
          backgroundColor: 'var(--bg-primary)',
          border: '1px solid var(--border-color)',
        }}
      >
        {/* Header */}
        <div
          className="flex items-center justify-between px-5 py-4"
          style={{ borderBottom: '1px solid var(--border-color)' }}
        >
          <div className="flex items-center gap-2.5">
            <div
              className="w-7 h-7 rounded-lg flex items-center justify-center text-white"
              style={{ backgroundColor: 'var(--accent)' }}
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4.745 3A23.933 23.933 0 003 12c0 3.183.62 6.22 1.745 9M19.5 3c.967 2.78 1.5 5.817 1.5 9s-.533 6.22-1.5 9M8.25 8.885l1.444-.89a.75.75 0 011.105.402l2.402 7.206a.75.75 0 001.104.401l1.445-.889" />
              </svg>
            </div>
            <h3
              className="font-semibold text-sm"
              style={{ color: 'var(--text-primary)' }}
            >
              Input Variables
            </h3>
            <span
              className="text-[11px] px-2 py-0.5 rounded-full"
              style={{
                backgroundColor: 'var(--bg-secondary)',
                color: 'var(--text-muted)',
              }}
            >
              {variables.length} required
            </span>
          </div>
          <button
            onClick={onCancel}
            className="w-7 h-7 rounded-lg flex items-center justify-center transition-colors hover:opacity-70"
            style={{ color: 'var(--text-muted)' }}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <form onSubmit={handleSubmit}>
          <div className="px-5 py-4 space-y-3 max-h-[60vh] overflow-y-auto">
            <p className="text-xs mb-3" style={{ color: 'var(--text-muted)' }}>
              This crew uses <code className="px-1 py-0.5 rounded text-[11px]" style={{ backgroundColor: 'var(--bg-secondary)' }}>&#123;variable&#125;</code> placeholders.
              Provide values below before running.
            </p>

            {variables.map((v) => {
              const sensitive = isSensitive(v.name);
              const showValue = visibleFields[v.name] || false;
              return (
                <div key={v.name}>
                  <label
                    className="block text-[11px] font-semibold uppercase tracking-wider mb-1"
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
                      className="w-full rounded-lg px-3 py-2 text-sm outline-none transition-colors"
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
                    <p className="text-[11px] mt-0.5" style={{ color: '#ef4444' }}>
                      This variable is required
                    </p>
                  )}
                </div>
              );
            })}
          </div>

          {/* Footer */}
          <div
            className="flex items-center justify-end gap-2 px-5 py-3"
            style={{ borderTop: '1px solid var(--border-color)' }}
          >
            <button
              type="button"
              onClick={onCancel}
              className="px-4 py-2 rounded-lg text-sm font-medium transition-colors hover:opacity-80"
              style={{
                color: 'var(--text-secondary)',
                border: '1px solid var(--border-color)',
              }}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 rounded-lg text-sm font-medium text-white transition-all hover:opacity-90"
              style={{ backgroundColor: 'var(--accent)' }}
            >
              Run with variables
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default InputVariablesDialog;
