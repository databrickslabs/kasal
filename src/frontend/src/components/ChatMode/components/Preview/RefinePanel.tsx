import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Theme,
  THEME_PRESETS,
  DEFAULT_THEME,
  FONT_OPTIONS,
  OptionValue,
  OptionSpec,
  optionSpecs,
  optionVal,
  buildPartialDirective,
} from '../../../Configuration/uiConfigShared';
import type { UiTheme } from '../../utils/uiDocument';

interface RefinePanelProps {
  /** Detected deliverable key (presentation, dashboard, album, …). */
  deliverable: string;
  /** Friendly, non-technical noun for the title ("Photo album", "Data view", …). */
  deliverableLabel: string;
  /** The artifact's current branding, used to seed the fine-tune controls. */
  currentTheme?: UiTheme;
  /** Apply a deterministic Look change (instant, no AI). */
  onApplyStyle: (theme: Theme) => void;
  /** Send an AI instruction to regenerate the content (the existing refine path). */
  onRefine: (instruction: string) => void;
  /** Close the panel. */
  onClose: () => void;
}

const THEME_KEYS = ['accent', 'background', 'surface', 'text', 'heading', 'muted', 'font', 'density'] as const;

/** Seed a full editable palette from whatever branding the artifact carries,
 *  falling back to the Default palette for any token the artifact omits. */
function seedTheme(current?: UiTheme): Theme {
  const picked: Partial<Theme> = {};
  if (current) {
    for (const k of THEME_KEYS) {
      const v = current[k];
      if (v !== undefined) (picked as Record<string, unknown>)[k] = v;
    }
  }
  return { ...DEFAULT_THEME, ...picked };
}

const COLOR_FIELDS: { key: keyof Theme; label: string }[] = [
  { key: 'accent', label: 'Accent' },
  { key: 'background', label: 'Background' },
  { key: 'surface', label: 'Cards' },
  { key: 'heading', label: 'Headings' },
  { key: 'text', label: 'Text' },
  { key: 'muted', label: 'Subtle' },
];

/**
 * In-preview "Customize" panel. Two ways to refine a rendered deliverable, kept
 * deliberately simple for a non-technical business user:
 *   • Look    — one-click style presets + an optional fine-tune. Applies INSTANTLY
 *               and deterministically (no AI, no crew run).
 *   • Content — friendly per-deliverable settings (slide count, chart, tone…) that
 *               need AI to regenerate, plus a free-text "describe a change" box.
 */
const RefinePanel: React.FC<RefinePanelProps> = ({
  deliverable,
  deliverableLabel,
  currentTheme,
  onApplyStyle,
  onRefine,
  onClose,
}) => {
  // Working palette — presets replace it, fine-tune edits patch it. A ref mirrors
  // it so patches merge onto the latest value without a side effect inside setState.
  const [draft, setDraft] = useState<Theme>(() => seedTheme(currentTheme));
  const draftRef = useRef(draft);
  draftRef.current = draft;
  const [fineOpen, setFineOpen] = useState(false);
  // Only the content options the user actually changed (so the AI directive is
  // scoped to their intent rather than restating every default).
  const [changed, setChanged] = useState<Record<string, OptionValue>>({});
  const [freeText, setFreeText] = useState('');

  const specs = useMemo(() => optionSpecs(deliverable), [deliverable]);

  // Debounce the live "apply" so dragging a color picker doesn't thrash the
  // store / IndexedDB write (and re-render the heavy renderer) on every
  // intermediate value. clearTimeout(undefined) is a no-op, so no guard needed.
  const applyTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(applyTimer.current), []);

  // Presets are discrete clicks → apply at once; fine-tune edits stream → debounce.
  const applyNow = (theme: Theme) => {
    clearTimeout(applyTimer.current);
    onApplyStyle(theme);
  };
  const applyDebounced = (theme: Theme) => {
    clearTimeout(applyTimer.current);
    applyTimer.current = setTimeout(() => onApplyStyle(theme), 140);
  };

  const applyPreset = (theme: Theme) => {
    setDraft(theme);
    applyNow(theme);
  };

  const patchDraft = (patch: Partial<Theme>) => {
    const next = { ...draftRef.current, ...patch };
    setDraft(next);
    applyDebounced(next);
  };

  const setChangedOpt = (key: string, value: OptionValue) =>
    setChanged((prev) => ({ ...prev, [key]: value }));

  const directive = buildPartialDirective(deliverable, changed);

  // The "Update with AI" button is disabled while `directive` is empty, so this
  // only fires with a real directive — no extra guard needed.
  const submitContent = () => {
    onRefine(directive);
    onClose();
  };

  const submitFreeText = () => {
    const trimmed = freeText.trim();
    if (!trimmed) return;
    onRefine(trimmed);
    setFreeText('');
    onClose();
  };

  const renderOptionControl = (s: OptionSpec) => {
    const value = optionVal(changed, s);
    if (s.kind === 'switch') {
      return (
        <label key={s.key} className="flex items-center gap-2 cursor-pointer text-xs" style={{ color: 'var(--text-secondary)' }}>
          <input
            type="checkbox"
            checked={value as boolean}
            onChange={(e) => setChangedOpt(s.key, e.target.checked)}
            style={{ accentColor: 'var(--accent)' }}
          />
          {s.label}
        </label>
      );
    }
    if (s.kind === 'number') {
      return (
        <label key={s.key} className="flex flex-col gap-1 text-xs" style={{ color: 'var(--text-secondary)' }}>
          {s.label}
          <input
            type="number"
            value={value as number}
            min={s.min}
            max={s.max}
            step={s.step ?? 1}
            onChange={(e) => setChangedOpt(s.key, Number(e.target.value))}
            className="w-20 rounded-md px-2 py-1 text-sm outline-none"
            style={{ backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}
          />
        </label>
      );
    }
    return (
      <label key={s.key} className="flex flex-col gap-1 text-xs" style={{ color: 'var(--text-secondary)' }}>
        {s.label}
        <select
          value={value as string}
          onChange={(e) => setChangedOpt(s.key, e.target.value)}
          className="rounded-md px-2 py-1 text-sm outline-none"
          style={{ backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}
        >
          {s.choices.map((c) => (
            <option key={c.value} value={c.value}>{c.label}</option>
          ))}
        </select>
      </label>
    );
  };

  const sectionLabel: React.CSSProperties = {
    color: 'var(--text-primary)',
    fontWeight: 600,
  };

  return (
    <div
      className="flex flex-col gap-4 px-4 py-3.5 flex-shrink-0 overflow-auto"
      style={{ borderBottom: '1px solid var(--border-color)', backgroundColor: 'var(--bg-secondary)', maxHeight: '60vh' }}
      data-testid="refine-panel"
    >
      <div className="text-sm" style={{ color: 'var(--text-primary)' }}>
        Customize this <span style={{ fontWeight: 700 }}>{deliverableLabel}</span>
      </div>

      {/* ---- Look: instant, deterministic ---- */}
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2">
          <span className="text-xs uppercase tracking-wide" style={sectionLabel}>Look</span>
          <span className="text-[10px]" style={{ color: 'var(--text-muted)' }}>Applies instantly</span>
        </div>
        <div className="flex flex-wrap gap-1.5">
          {THEME_PRESETS.map((p) => (
            <button
              key={p.key}
              onClick={() => applyPreset({ ...p.theme })}
              className="flex items-center gap-1.5 h-7 px-2.5 rounded-full text-xs font-medium transition-colors hover:opacity-80"
              style={{ color: 'var(--text-secondary)', backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}
              title={`Apply the ${p.label} style`}
            >
              <span className="w-3 h-3 rounded-full" style={{ backgroundColor: p.theme.accent, border: '1px solid rgba(128,128,128,0.4)' }} />
              {p.label}
            </button>
          ))}
        </div>

        <button
          onClick={() => setFineOpen((v) => !v)}
          className="self-start text-xs font-medium flex items-center gap-1 hover:opacity-80"
          style={{ color: 'var(--accent)' }}
        >
          <svg className={`w-3 h-3 transition-transform ${fineOpen ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </svg>
          Fine-tune colors & font
        </button>

        {fineOpen && (
          <div className="flex flex-col gap-3 pt-1">
            <div className="flex flex-wrap gap-3">
              {COLOR_FIELDS.map((f) => (
                <label key={f.key} className="flex flex-col items-center gap-1 text-[11px]" style={{ color: 'var(--text-secondary)' }}>
                  <input
                    type="color"
                    aria-label={f.label}
                    value={draft[f.key] as string}
                    onChange={(e) => patchDraft({ [f.key]: e.target.value } as Partial<Theme>)}
                    className="w-9 h-9 rounded cursor-pointer p-0"
                    style={{ border: '1px solid var(--border-color)', background: 'none' }}
                  />
                  {f.label}
                </label>
              ))}
            </div>
            <div className="flex flex-wrap gap-3">
              <label className="flex flex-col gap-1 text-xs" style={{ color: 'var(--text-secondary)' }}>
                Font
                <select
                  value={draft.font}
                  onChange={(e) => patchDraft({ font: e.target.value as Theme['font'] })}
                  className="rounded-md px-2 py-1 text-sm outline-none"
                  style={{ backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}
                >
                  {FONT_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>{o.label}</option>
                  ))}
                </select>
              </label>
              <label className="flex flex-col gap-1 text-xs" style={{ color: 'var(--text-secondary)' }}>
                Spacing
                <select
                  value={draft.density}
                  onChange={(e) => patchDraft({ density: e.target.value as Theme['density'] })}
                  className="rounded-md px-2 py-1 text-sm outline-none"
                  style={{ backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}
                >
                  <option value="comfortable">Comfortable</option>
                  <option value="compact">Compact</option>
                </select>
              </label>
            </div>
          </div>
        )}
      </div>

      {/* ---- Content: needs AI ---- */}
      {specs.length > 0 && (
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <span className="text-xs uppercase tracking-wide" style={sectionLabel}>Content</span>
            <span
              className="text-[9px] px-1.5 py-0.5 rounded-full font-semibold uppercase tracking-wide"
              style={{ color: 'var(--accent)', backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}
            >
              Uses AI
            </span>
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-2.5 items-end">
            {specs.map(renderOptionControl)}
          </div>
          <button
            onClick={submitContent}
            disabled={!directive}
            className="self-start mt-0.5 px-3 py-1.5 rounded-lg text-sm font-medium text-white transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            style={{ backgroundColor: 'var(--accent)' }}
            title={directive ? 'Regenerate with these changes' : 'Change a setting above to enable'}
          >
            Update with AI
          </button>
        </div>
      )}

      {/* ---- Free-text refine (always available) ---- */}
      <div className="flex flex-col gap-1.5">
        <span className="text-[11px]" style={{ color: 'var(--text-muted)' }}>Or describe a change</span>
        <div className="flex items-center gap-2">
          <input
            value={freeText}
            onChange={(e) => setFreeText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') submitFreeText();
              if (e.key === 'Escape') onClose();
            }}
            placeholder="e.g. add a chart comparing Q3 vs Q4…"
            className="flex-1 rounded-lg px-3 py-1.5 text-sm outline-none"
            style={{ backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}
          />
          <button
            onClick={submitFreeText}
            disabled={!freeText.trim()}
            className="px-3 py-1.5 rounded-lg text-sm font-medium text-white transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            style={{ backgroundColor: 'var(--accent)' }}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
};

export default RefinePanel;
