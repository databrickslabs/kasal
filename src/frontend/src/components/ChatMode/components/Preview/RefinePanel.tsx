import React, { useEffect, useMemo, useRef, useState } from 'react';
import Box from '@mui/material/Box';
import type { Theme as MuiTheme } from '@mui/material/styles';
import type { SystemStyleObject } from '@mui/system';
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
import { buttonResetSx, inputResetSx } from '../../chatSx';

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

// Shared field styling for the panel's number inputs and selects.
const fieldSx: SystemStyleObject<MuiTheme> = {
  ...inputResetSx,
  borderRadius: '6px',
  px: 1,
  py: 0.5,
  fontSize: 14,
  backgroundColor: 'background.paper',
  color: 'text.primary',
  border: 1,
  borderColor: 'divider',
};
const sectionLabelSx = {
  fontSize: 12,
  textTransform: 'uppercase',
  letterSpacing: '0.025em',
  color: 'text.primary',
  fontWeight: 600,
} as const;
// The accent-coloured "Send"/"Update with AI" pill buttons.
const accentBtnSx: SystemStyleObject<MuiTheme> = {
  ...buttonResetSx,
  px: 1.5,
  py: 0.75,
  borderRadius: '8px',
  fontSize: 14,
  fontWeight: 500,
  color: '#fff',
  transition: 'all 0.15s',
  backgroundColor: 'primary.main',
  '&:disabled': { opacity: 0.4, cursor: 'not-allowed' },
};

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
        <Box component="label" key={s.key} sx={{ display: 'flex', alignItems: 'center', gap: 1, cursor: 'pointer', fontSize: 12, color: 'text.secondary' }}>
          <Box
            component="input"
            type="checkbox"
            checked={value as boolean}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setChangedOpt(s.key, e.target.checked)}
            sx={{ accentColor: (t) => t.palette.primary.main }}
          />
          {s.label}
        </Box>
      );
    }
    if (s.kind === 'number') {
      return (
        <Box component="label" key={s.key} sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, fontSize: 12, color: 'text.secondary' }}>
          {s.label}
          <Box
            component="input"
            type="number"
            value={value as number}
            min={s.min}
            max={s.max}
            step={s.step ?? 1}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setChangedOpt(s.key, Number(e.target.value))}
            sx={{ ...fieldSx, width: 80 }}
          />
        </Box>
      );
    }
    return (
      <Box component="label" key={s.key} sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, fontSize: 12, color: 'text.secondary' }}>
        {s.label}
        <Box
          component="select"
          value={value as string}
          onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setChangedOpt(s.key, e.target.value)}
          sx={fieldSx}
        >
          {s.choices.map((c) => (
            <option key={c.value} value={c.value}>{c.label}</option>
          ))}
        </Box>
      </Box>
    );
  };

  return (
    <Box
      data-testid="refine-panel"
      sx={{
        display: 'flex',
        flexDirection: 'column',
        gap: 2,
        px: 2,
        py: 1.75,
        flexShrink: 0,
        overflow: 'auto',
        borderBottom: 1,
        borderColor: 'divider',
        backgroundColor: (t) => t.chat.bgSecondary,
        maxHeight: '60vh',
      }}
    >
      <Box sx={{ fontSize: 14, color: 'text.primary' }}>
        Customize this <Box component="span" sx={{ fontWeight: 700 }}>{deliverableLabel}</Box>
      </Box>

      {/* ---- Look: instant, deterministic ---- */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box component="span" sx={sectionLabelSx}>Look</Box>
          <Box component="span" sx={{ fontSize: 10, color: 'text.disabled' }}>Applies instantly</Box>
        </Box>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75 }}>
          {THEME_PRESETS.map((p) => (
            <Box
              component="button"
              key={p.key}
              onClick={() => applyPreset({ ...p.theme })}
              title={`Apply the ${p.label} style`}
              sx={{
                ...buttonResetSx,
                display: 'flex',
                alignItems: 'center',
                gap: 0.75,
                height: 28,
                px: 1.25,
                borderRadius: '9999px',
                fontSize: 12,
                fontWeight: 500,
                transition: 'opacity 0.15s',
                color: 'text.secondary',
                backgroundColor: 'background.default',
                border: 1,
                borderColor: 'divider',
                '&:hover': { opacity: 0.8 },
              }}
            >
              <Box component="span" sx={{ width: 12, height: 12, borderRadius: '9999px', backgroundColor: p.theme.accent, border: '1px solid rgba(128,128,128,0.4)' }} />
              {p.label}
            </Box>
          ))}
        </Box>

        <Box
          component="button"
          onClick={() => setFineOpen((v) => !v)}
          sx={{ ...buttonResetSx, alignSelf: 'flex-start', fontSize: 12, fontWeight: 500, display: 'flex', alignItems: 'center', gap: 0.5, color: 'primary.main', '&:hover': { opacity: 0.8 } }}
        >
          <Box component="svg" sx={{ width: 12, height: 12, transition: 'transform 0.15s', transform: fineOpen ? 'rotate(90deg)' : 'none' }} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </Box>
          Fine-tune colors &amp; font
        </Box>

        {fineOpen && (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5, pt: 0.5 }}>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1.5 }}>
              {COLOR_FIELDS.map((f) => (
                <Box component="label" key={f.key} sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 0.5, fontSize: 11, color: 'text.secondary' }}>
                  <Box
                    component="input"
                    type="color"
                    aria-label={f.label}
                    value={draft[f.key] as string}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => patchDraft({ [f.key]: e.target.value } as Partial<Theme>)}
                    sx={{ ...inputResetSx, width: 36, height: 36, borderRadius: '4px', cursor: 'pointer', p: 0, border: 1, borderColor: 'divider', background: 'none' }}
                  />
                  {f.label}
                </Box>
              ))}
            </Box>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1.5 }}>
              <Box component="label" sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, fontSize: 12, color: 'text.secondary' }}>
                Font
                <Box
                  component="select"
                  value={draft.font}
                  onChange={(e: React.ChangeEvent<HTMLSelectElement>) => patchDraft({ font: e.target.value as Theme['font'] })}
                  sx={fieldSx}
                >
                  {FONT_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>{o.label}</option>
                  ))}
                </Box>
              </Box>
              <Box component="label" sx={{ display: 'flex', flexDirection: 'column', gap: 0.5, fontSize: 12, color: 'text.secondary' }}>
                Spacing
                <Box
                  component="select"
                  value={draft.density}
                  onChange={(e: React.ChangeEvent<HTMLSelectElement>) => patchDraft({ density: e.target.value as Theme['density'] })}
                  sx={fieldSx}
                >
                  <option value="comfortable">Comfortable</option>
                  <option value="compact">Compact</option>
                </Box>
              </Box>
            </Box>
          </Box>
        )}
      </Box>

      {/* ---- Content: needs AI ---- */}
      {specs.length > 0 && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Box component="span" sx={sectionLabelSx}>Content</Box>
            <Box
              component="span"
              sx={{ fontSize: 9, px: 0.75, py: 0.25, borderRadius: '9999px', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.025em', color: 'primary.main', backgroundColor: 'background.default', border: 1, borderColor: 'divider' }}
            >
              Uses AI
            </Box>
          </Box>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', columnGap: 2, rowGap: 1.25, alignItems: 'flex-end' }}>
            {specs.map(renderOptionControl)}
          </Box>
          <Box
            component="button"
            onClick={submitContent}
            disabled={!directive}
            title={directive ? 'Regenerate with these changes' : 'Change a setting above to enable'}
            sx={{ ...accentBtnSx, alignSelf: 'flex-start', mt: 0.25 }}
          >
            Update with AI
          </Box>
        </Box>
      )}

      {/* ---- Free-text refine (always available) ---- */}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.75 }}>
        <Box component="span" sx={{ fontSize: 11, color: 'text.disabled' }}>Or describe a change</Box>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Box
            component="input"
            value={freeText}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setFreeText(e.target.value)}
            onKeyDown={(e: React.KeyboardEvent) => {
              if (e.key === 'Enter') submitFreeText();
              if (e.key === 'Escape') onClose();
            }}
            placeholder="e.g. add a chart comparing Q3 vs Q4…"
            sx={{ ...inputResetSx, flex: 1, borderRadius: '8px', px: 1.5, py: 0.75, fontSize: 14, backgroundColor: 'background.paper', color: 'text.primary', border: 1, borderColor: 'divider' }}
          />
          <Box
            component="button"
            onClick={submitFreeText}
            disabled={!freeText.trim()}
            sx={accentBtnSx}
          >
            Send
          </Box>
        </Box>
      </Box>
    </Box>
  );
};

export default RefinePanel;
