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
import type { UiTheme } from '../../../Configuration/uiConfigShared';

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
  // Which preset is currently applied (for the selected-chip highlight). Seeded
  // by matching the artifact's accent; cleared when a fine-tune edit diverges.
  const [selectedPreset, setSelectedPreset] = useState<string | null>(
    () => THEME_PRESETS.find((p) => p.theme.accent === seedTheme(currentTheme).accent)?.key ?? null,
  );
  // Only the content options the user actually changed (so the AI directive is
  // scoped to their intent rather than restating every default).
  const [changed, setChanged] = useState<Record<string, OptionValue>>({});
  const [freeText, setFreeText] = useState('');

  const specs = useMemo(() => optionSpecs(deliverable), [deliverable]);
  // Split content options so the layout reads as a tidy form: labelled fields
  // (number / select) align in a 2-column grid; on/off switches stack as a clean
  // checklist below — instead of one flex-wrap row that spreads them unevenly.
  const fieldSpecs = useMemo(() => specs.filter((s) => s.kind !== 'switch'), [specs]);
  const switchSpecs = useMemo(() => specs.filter((s) => s.kind === 'switch'), [specs]);

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
    setSelectedPreset(null); // a manual color/font edit no longer matches a preset
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
        <label key={s.key} className="flex items-center gap-2.5 cursor-pointer text-[13px] py-0.5" style={{ color: 'var(--text-secondary)' }}>
          <input
            type="checkbox"
            checked={value as boolean}
            onChange={(e) => setChangedOpt(s.key, e.target.checked)}
            className="w-4 h-4 flex-shrink-0 cursor-pointer"
            style={{ accentColor: 'var(--accent)' }}
          />
          {s.label}
        </label>
      );
    }
    if (s.kind === 'number') {
      return (
        <label key={s.key} className="flex flex-col gap-1 text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>
          {s.label}
          <input
            type="number"
            value={value as number}
            min={s.min}
            max={s.max}
            step={s.step ?? 1}
            onChange={(e) => setChangedOpt(s.key, Number(e.target.value))}
            className="w-full rounded-md px-2.5 py-1.5 text-sm outline-none"
            style={{ backgroundColor: 'var(--bg-input)', color: 'var(--text-primary)', border: '1px solid var(--border-color)' }}
          />
        </label>
      );
    }
    return (
      <label key={s.key} className="flex flex-col gap-1 text-[11px] font-medium" style={{ color: 'var(--text-muted)' }}>
        {s.label}
        <select
          value={value as string}
          onChange={(e) => setChangedOpt(s.key, e.target.value)}
          className="w-full rounded-md px-2.5 py-1.5 text-sm outline-none"
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

  // Primary AI button styling. Enabled → a solid accent CTA. Disabled → a clean
  // borderless ghost (no fill, no box) so it reads as inactive without the ugly
  // grey/white square a filled-but-disabled button left on the panel; NOT a faded
  // accent either (a red at low opacity smears into a washed-out pink).
  const primaryBtnStyle = (enabled: boolean): React.CSSProperties =>
    enabled
      ? { backgroundColor: 'var(--accent)', color: '#fff', borderColor: 'var(--accent)' }
      : { backgroundColor: 'transparent', color: 'var(--text-muted)', borderColor: 'transparent' };

  return (
    <div
      className="flex flex-col px-5 py-4 flex-shrink-0 overflow-auto"
      style={{ borderBottom: '1px solid var(--border-color)', backgroundColor: 'var(--bg-secondary)', maxHeight: '60vh' }}
      data-testid="refine-panel"
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="text-sm" style={{ color: 'var(--text-primary)' }}>
          Customize this <span style={{ fontWeight: 700 }}>{deliverableLabel}</span>
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close customize"
          title="Close"
          className="w-6 h-6 rounded-md flex items-center justify-center transition-colors hover:opacity-70 flex-shrink-0"
          style={{ color: 'var(--text-muted)' }}
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      {/* ---- Look: instant, deterministic ---- */}
      <div className="flex flex-col gap-2.5">
        <div className="flex items-center gap-2">
          <span className="text-[11px] uppercase tracking-wider" style={sectionLabel}>Look</span>
          <span className="text-[10px]" style={{ color: 'var(--text-muted)' }}>Applies instantly</span>
        </div>
        <div className="flex flex-wrap gap-1">
          {THEME_PRESETS.map((p) => {
            const active = selectedPreset === p.key;
            return (
              <button
                key={p.key}
                onClick={() => {
                  setSelectedPreset(p.key);
                  applyPreset({ ...p.theme });
                }}
                className="flex items-center gap-2 h-8 px-2.5 rounded-lg text-[13px] font-medium transition-colors hover:bg-[var(--bg-rail-hover)]"
                style={{
                  color: active ? 'var(--text-primary)' : 'var(--text-secondary)',
                  backgroundColor: active ? 'var(--bg-active-chip)' : 'transparent',
                  boxShadow: active ? 'inset 0 0 0 1.5px var(--accent)' : 'none',
                }}
                title={`Apply the ${p.label} style`}
                aria-pressed={active}
              >
                <span
                  className="w-3.5 h-3.5 rounded-full flex-shrink-0"
                  style={{ backgroundColor: p.theme.accent, boxShadow: 'inset 0 0 0 1px rgba(0,0,0,0.18)' }}
                />
                {p.label}
              </button>
            );
          })}
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
        <div
          className="flex flex-col gap-3 mt-4 pt-4"
          style={{ borderTop: '1px solid var(--border-color)' }}
        >
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wider" style={sectionLabel}>Content</span>
            <span
              className="text-[9px] px-1.5 py-0.5 rounded-full font-semibold uppercase tracking-wide"
              style={{ color: 'var(--accent)', backgroundColor: 'var(--bg-primary)', border: '1px solid var(--border-color)' }}
            >
              Uses AI
            </span>
          </div>
          {fieldSpecs.length > 0 && (
            <div className="grid grid-cols-2 gap-x-4 gap-y-3">
              {fieldSpecs.map(renderOptionControl)}
            </div>
          )}
          {switchSpecs.length > 0 && (
            <div className="flex flex-col gap-1">
              {switchSpecs.map(renderOptionControl)}
            </div>
          )}
          <button
            onClick={submitContent}
            disabled={!directive}
            className="self-start mt-1 px-3.5 py-1.5 rounded-lg text-sm font-medium border transition-colors hover:opacity-90 disabled:cursor-not-allowed"
            style={primaryBtnStyle(!!directive)}
            title={directive ? 'Regenerate with these changes' : 'Change a setting above to enable'}
          >
            Update with AI
          </button>
        </div>
      )}

      {/* ---- Free-text refine (always available) ---- */}
      <div
        className="flex flex-col gap-1.5 mt-4 pt-4"
        style={{ borderTop: '1px solid var(--border-color)' }}
      >
        <span className="text-[11px] uppercase tracking-wider" style={sectionLabel}>Describe a change</span>
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
            className="px-3.5 py-1.5 rounded-lg text-sm font-medium border transition-colors hover:opacity-90 disabled:cursor-not-allowed"
            style={primaryBtnStyle(!!freeText.trim())}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
};

export default RefinePanel;
