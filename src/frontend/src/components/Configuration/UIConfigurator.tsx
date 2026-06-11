import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Switch,
  FormControlLabel,
  Select,
  MenuItem,
  TextField,
  Button,
  Alert,
  CircularProgress,
  Divider,
  FormControl,
  InputLabel,
  Chip,
} from '@mui/material';
import { UIConfigService, UIConfig, UIConfigUpdate } from '../../api/UIConfigService';

type CatalogType = 'minimal' | 'basic' | 'custom';

/** A full branding palette. The renderer (UiRenderer.tsx) maps these onto the
 *  stage background, accent, text colors and font; the agent embeds the matching
 *  palette as the surface `theme` (see ui_emission.py). */
interface Theme {
  accent: string;
  background: string;
  surface: string;
  text: string;
  heading: string;
  muted: string;
  font: 'sans' | 'serif' | 'rounded' | 'mono';
  density: 'comfortable' | 'compact';
}

// Deliverable types that can be branded/configured independently. Keys mirror the
// backend (_THEME_LABELS in ui_emission.py) and the chat format options.
// "default" is the base palette applied to anything without its own.
const DELIVERABLE_TYPES = [
  { key: 'default', label: 'Default' },
  { key: 'dashboard', label: 'Dashboard' },
  { key: 'presentation', label: 'Presentation' },
  { key: 'genie', label: 'Genie' },
  { key: 'mindmap', label: 'Mindmap' },
  { key: 'album', label: 'Album' },
  { key: 'quiz', label: 'Quiz' },
  { key: 'report', label: 'Report' },
] as const;
type DeliverableKey = (typeof DELIVERABLE_TYPES)[number]['key'];

const FONT_OPTIONS: { value: Theme['font']; label: string }[] = [
  { value: 'sans', label: 'Sans-serif (modern)' },
  { value: 'serif', label: 'Serif (editorial)' },
  { value: 'rounded', label: 'Rounded (friendly)' },
  { value: 'mono', label: 'Monospace (technical)' },
];

// CSS stacks for the live preview — kept in sync with FONT_STACK in UiRenderer.tsx.
const FONT_CSS: Record<Theme['font'], string> = {
  sans: 'Inter, system-ui, sans-serif',
  serif: 'Georgia, "Times New Roman", serif',
  rounded: '"Nunito", "Quicksand", system-ui, sans-serif',
  mono: '"JetBrains Mono", Menlo, monospace',
};

// One-click starting palettes.
const THEME_PRESETS: { key: string; label: string; theme: Theme }[] = [
  { key: 'default', label: 'Default', theme: { accent: '#2272B4', background: '#FFFFFF', surface: '#F8FAFC', text: '#0F172A', heading: '#0F172A', muted: '#64748B', font: 'sans', density: 'comfortable' } },
  { key: 'dark', label: 'Dark', theme: { accent: '#38BDF8', background: '#0F172A', surface: '#1E293B', text: '#E2E8F0', heading: '#F8FAFC', muted: '#94A3B8', font: 'sans', density: 'comfortable' } },
  { key: 'vibrant', label: 'Vibrant', theme: { accent: '#7C3AED', background: '#FFFFFF', surface: '#F5F3FF', text: '#1E1B4B', heading: '#6D28D9', muted: '#6B7280', font: 'rounded', density: 'comfortable' } },
  { key: 'minimal', label: 'Minimal', theme: { accent: '#111827', background: '#FFFFFF', surface: '#FFFFFF', text: '#111827', heading: '#000000', muted: '#9CA3AF', font: 'sans', density: 'compact' } },
  { key: 'corporate', label: 'Corporate', theme: { accent: '#1E3A5F', background: '#FFFFFF', surface: '#F1F5F9', text: '#1E293B', heading: '#0F2540', muted: '#64748B', font: 'serif', density: 'comfortable' } },
  { key: 'playful', label: 'Playful', theme: { accent: '#F97316', background: '#FFFBEB', surface: '#FEF3C7', text: '#7C2D12', heading: '#C2410C', muted: '#B45309', font: 'rounded', density: 'comfortable' } },
  // Mirrors the renderer's built-in presentation deck identity (UiRenderer
  // DECK_THEME_VARS) so users can pin it as an explicit palette and tweak it.
  { key: 'databricks-deck', label: 'Databricks Deck', theme: { accent: '#FF3621', background: '#0E1B21', surface: '#16272F', text: '#E8EEF2', heading: '#FFFFFF', muted: '#8FA3AD', font: 'sans', density: 'comfortable' } },
];

const DEFAULT_THEME: Theme = THEME_PRESETS[0].theme;

const normalizeTheme = (t: Partial<Theme> | undefined): Theme => ({ ...DEFAULT_THEME, ...(t || {}) });

/* ------------------------------------------------------------------ */
/*  Per-deliverable settings (type-specific, beyond the palette)       */
/* ------------------------------------------------------------------ */

type OptionValue = string | number | boolean;
type OptionSpec =
  | { kind: 'select'; key: string; label: string; choices: { value: string; label: string }[]; default: string; phrase: (v: string) => string }
  | { kind: 'number'; key: string; label: string; min: number; max: number; step?: number; default: number; phrase: (v: number) => string }
  | { kind: 'switch'; key: string; label: string; default: boolean; phrase: (v: boolean) => string };

// Each option carries a `phrase()` so the directive text sent to the crew lives
// next to its control (single source of truth — the backend just appends it).
const TYPE_OPTIONS: Record<string, OptionSpec[]> = {
  dashboard: [
    { kind: 'number', key: 'tilesPerRow', label: 'KPI tiles per row', min: 2, max: 4, default: 3, phrase: (v) => `lay out KPI Stat tiles ${v} per row` },
    { kind: 'select', key: 'chart', label: 'Preferred chart', default: 'auto', choices: [{ value: 'auto', label: 'Auto' }, { value: 'bar', label: 'Bar' }, { value: 'line', label: 'Line' }, { value: 'pie', label: 'Pie' }], phrase: (v) => (v === 'auto' ? 'pick the chart type that best fits each metric' : `prefer ${v} charts`) },
    { kind: 'switch', key: 'deltas', label: 'Show deltas / trends on tiles', default: true, phrase: (v) => (v ? 'show a delta/trend on each Stat tile' : 'omit deltas on Stat tiles') },
  ],
  presentation: [
    { kind: 'number', key: 'slides', label: 'Target slide count', min: 3, max: 20, default: 8, phrase: (v) => `aim for about ${v} slides` },
    { kind: 'number', key: 'bullets', label: 'Max bullets per slide', min: 2, max: 6, default: 4, phrase: (v) => `at most ${v} bullet points per slide` },
    { kind: 'switch', key: 'titleSlide', label: 'Open with a title slide', default: true, phrase: (v) => (v ? 'open with a dedicated title slide' : 'skip the title slide') },
    { kind: 'switch', key: 'summarySlide', label: 'End with a summary slide', default: true, phrase: (v) => (v ? 'end with a summary / takeaways slide' : 'do not add a summary slide') },
  ],
  genie: [
    { kind: 'select', key: 'chart', label: 'Default chart', default: 'auto', choices: [{ value: 'auto', label: 'Auto' }, { value: 'bar', label: 'Bar' }, { value: 'line', label: 'Line' }, { value: 'pie', label: 'Pie' }, { value: 'none', label: 'Table only' }], phrase: (v) => (v === 'none' ? 'show the result as a Table only (no chart)' : v === 'auto' ? 'add a chart when it aids understanding' : `visualize the result with a ${v} chart`) },
    { kind: 'number', key: 'maxRows', label: 'Max table rows', min: 5, max: 100, step: 5, default: 20, phrase: (v) => `show at most ${v} rows in the Table` },
    { kind: 'switch', key: 'showSql', label: 'Show the SQL query', default: false, phrase: (v) => (v ? 'include the SQL query used (as a caption)' : 'do not surface the SQL query') },
  ],
  mindmap: [
    { kind: 'number', key: 'depth', label: 'Max depth', min: 2, max: 5, default: 3, phrase: (v) => `nest the tree up to ${v} levels deep` },
    { kind: 'number', key: 'branches', label: 'Main branches', min: 3, max: 8, default: 5, phrase: (v) => `give the central topic about ${v} main branches` },
    { kind: 'switch', key: 'icons', label: 'Use icons / emoji on nodes', default: false, phrase: (v) => (v ? 'prefix node labels with a relevant emoji' : 'keep node labels plain text') },
  ],
  album: [
    { kind: 'select', key: 'layout', label: 'Layout', default: 'grid', choices: [{ value: 'grid', label: 'Grid' }, { value: 'carousel', label: 'One per screen (carousel)' }], phrase: (v) => (v === 'carousel' ? 'lay the album out as a full-width carousel showing ONE image per screen that scrolls left→right — set the Album component’s "layout" to "carousel"' : 'lay the album out as a responsive grid (Album "layout":"grid")') },
    { kind: 'switch', key: 'captions', label: 'Show captions', default: true, phrase: (v) => (v ? 'give every image a short caption' : 'omit image captions') },
    { kind: 'number', key: 'maxImages', label: 'Max images', min: 4, max: 30, step: 2, default: 12, phrase: (v) => `include at most ${v} images` },
  ],
  quiz: [
    { kind: 'number', key: 'questions', label: 'Number of questions', min: 3, max: 20, default: 5, phrase: (v) => `write exactly ${v} questions` },
    { kind: 'select', key: 'difficulty', label: 'Difficulty', default: 'mixed', choices: [{ value: 'easy', label: 'Easy' }, { value: 'medium', label: 'Medium' }, { value: 'hard', label: 'Hard' }, { value: 'mixed', label: 'Mixed' }], phrase: (v) => (v === 'mixed' ? 'mix easy, medium and hard questions' : `pitch questions at a ${v} difficulty`) },
    { kind: 'number', key: 'choices', label: 'Options per question', min: 2, max: 5, default: 4, phrase: (v) => `give each question ${v} answer options` },
  ],
  report: [
    { kind: 'select', key: 'length', label: 'Length', default: 'standard', choices: [{ value: 'brief', label: 'Brief' }, { value: 'standard', label: 'Standard' }, { value: 'detailed', label: 'Detailed' }], phrase: (v) => `keep the report ${v} in length` },
    { kind: 'select', key: 'tone', label: 'Tone', default: 'neutral', choices: [{ value: 'neutral', label: 'Neutral' }, { value: 'executive', label: 'Executive' }, { value: 'technical', label: 'Technical' }, { value: 'casual', label: 'Casual' }], phrase: (v) => `write in a ${v} tone` },
    { kind: 'switch', key: 'execSummary', label: 'Include an executive summary', default: true, phrase: (v) => (v ? 'lead with an executive summary card' : 'no executive summary') },
    { kind: 'switch', key: 'sources', label: 'Include sources / citations', default: true, phrase: (v) => (v ? 'list sources / citations at the end' : 'do not include a sources list') },
  ],
};

const optionSpecs = (type: string): OptionSpec[] => TYPE_OPTIONS[type] || [];

const optionVal = (opts: Record<string, OptionValue> | undefined, s: OptionSpec): OptionValue => {
  const v = opts?.[s.key];
  return v === undefined ? s.default : v;
};

/** Turn a type's option values into the human directive appended to the crew. */
const buildDirective = (type: string, opts: Record<string, OptionValue> | undefined): string => {
  const specs = optionSpecs(type);
  if (specs.length === 0) return '';
  const clauses = specs.map((s) => (s.phrase as (x: OptionValue) => string)(optionVal(opts, s)));
  const joined = clauses.join('; ');
  return joined.charAt(0).toUpperCase() + joined.slice(1) + '.';
};

// Components available in each built-in catalog (shown as chips).
const CATALOG_COMPONENTS: Record<Exclude<CatalogType, 'custom'>, string[]> = {
  minimal: ['Text', 'Row', 'Column', 'Button', 'TextField'],
  basic: [
    'Text', 'Row', 'Column', 'Card', 'List', 'Divider', 'Image', 'Icon',
    'Badge', 'Button', 'TextField', 'CheckBox', 'Slider', 'ChoicePicker',
    'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide', 'Album', 'Mindmap',
  ],
};

// Rich starting point prefilled into the custom-catalog editor. Declares the
// full component set with prop hints so authors can extend from a real example.
const SAMPLE_CUSTOM_CATALOG = JSON.stringify(
  {
    catalogId: 'kasal.custom.v1',
    title: 'Custom UI Catalog',
    description: 'Components agents may use. Extend or trim to taste.',
    components: {
      Text: { props: { text: 'string', variant: ['h1', 'h2', 'h3', 'h4', 'h5', 'body', 'caption'] } },
      Column: { props: { children: 'id[]', justify: ['start', 'center', 'end', 'spaceBetween'], align: ['start', 'center', 'end', 'stretch'] } },
      Row: { props: { children: 'id[]', justify: ['start', 'center', 'end', 'spaceBetween'], align: ['start', 'center', 'end', 'stretch'] } },
      Card: { props: { title: 'string?', children: 'id[]' } },
      List: { props: { children: 'id[]' } },
      Divider: { props: {} },
      Image: { props: { url: 'string', alt: 'string?' } },
      Icon: { props: { name: 'string' } },
      Badge: { props: { text: 'string', tone: ['good', 'warn', 'bad', 'neutral'] } },
      Button: { props: { child: 'id' } },
      TextField: { props: { label: 'string', value: 'binding' } },
      CheckBox: { props: { label: 'string', value: 'binding(boolean)' } },
      Slider: { props: { label: 'string', min: 'number', max: 'number', value: 'binding' } },
      ChoicePicker: { props: { label: 'string', options: '[{ label, value }]', value: 'binding' }, note: 'single choice — quizzes/forms' },
      Dashboard: { props: { children: 'id[]' }, note: 'responsive KPI/card grid' },
      Stat: { props: { label: 'string', value: 'string|number', delta: 'string?', tone: ['good', 'warn', 'bad', 'neutral'] } },
      Chart: { props: { chartType: ['bar', 'line', 'pie'], title: 'string?', data: '[{ label, value }]' } },
      Table: { props: { columns: '[string]', rows: '[[cell, …], …]' }, note: 'data / Genie results' },
      Quiz: { props: { title: 'string?', questions: '[{ question, options:[string], answer: index }]' }, note: 'interactive scored quiz' },
      Slides: { props: { children: 'slideId[]' }, note: 'navigable deck' },
      Slide: { props: { title: 'string?', children: 'id[]' } },
    },
  },
  null,
  2,
);

/** A labeled color swatch input. */
const ColorField: React.FC<{ label: string; value: string; onChange: (v: string) => void }> = ({ label, value, onChange }) => (
  <TextField
    label={label}
    type="color"
    size="small"
    value={value}
    onChange={(e) => onChange(e.target.value)}
    sx={{ width: 92 }}
    InputLabelProps={{ shrink: true }}
  />
);

/** Live preview of a palette — a miniature themed surface (heading, body, two
 *  stat tiles, an accent button) so the choices are visible before saving. */
const PalettePreview: React.FC<{ theme: Theme }> = ({ theme }) => {
  const fontFamily = FONT_CSS[theme.font];
  const tile = (label: string, value: string, valueColor: string) => (
    <Box sx={{ flex: 1, background: theme.surface, borderRadius: 1.5, p: 1.25, border: `1px solid ${theme.muted}33` }}>
      <Typography sx={{ color: theme.muted, fontSize: 10, fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase', fontFamily }}>{label}</Typography>
      <Typography sx={{ color: valueColor, fontSize: '1.25rem', fontWeight: 800, fontFamily }}>{value}</Typography>
    </Box>
  );
  return (
    <Box sx={{ borderRadius: 2, overflow: 'hidden', border: '1px solid', borderColor: 'divider' }}>
      <Box sx={{ background: theme.background, p: 2, fontFamily }}>
        <Typography sx={{ color: theme.heading, fontWeight: 800, fontSize: '1.15rem', mb: 0.5, fontFamily }}>
          Heading sample
        </Typography>
        <Typography sx={{ color: theme.text, fontSize: '0.85rem', mb: 1.5, fontFamily }}>
          Body copy renders in this color.{' '}
          <Box component="span" sx={{ color: theme.muted }}>Muted secondary text.</Box>
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, mb: 1.5 }}>
          {tile('Revenue', '$2.4M', theme.text)}
          {tile('Active users', '18.2k', theme.accent)}
        </Box>
        <Box sx={{ display: 'inline-block', background: theme.accent, color: '#fff', px: 1.5, py: 0.5, borderRadius: 1, fontSize: '0.8rem', fontWeight: 700, fontFamily }}>
          Accent button
        </Box>
      </Box>
    </Box>
  );
};

/**
 * Per-workspace "UI Configurator" configuration.
 *
 * When enabled, crews in this workspace emit a structured, design-system UI
 * (rendered consistently in the chat preview) instead of arbitrary HTML. The
 * structured format conforms to the A2UI protocol (see THIRD_PARTY_NOTICES).
 * Each deliverable type (dashboard, presentation, genie, mindmap, album, quiz,
 * report) has its own palette AND its own type-specific settings.
 */
const UIConfigurator: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  const [enabled, setEnabled] = useState(false);
  const [catalogType, setCatalogType] = useState<CatalogType>('minimal');
  const [catalogJson, setCatalogJson] = useState('');
  // Per-deliverable palettes. `default` is always present; other keys exist only
  // when that type has its own palette (absence = inherits Default).
  const [themes, setThemes] = useState<Record<string, Theme>>({ default: { ...DEFAULT_THEME } });
  // Per-deliverable type-specific settings (keyed by type, then option key).
  const [options, setOptions] = useState<Record<string, Record<string, OptionValue>>>({});
  const [activeType, setActiveType] = useState<DeliverableKey>('default');

  useEffect(() => {
    let active = true;
    UIConfigService.getConfig()
      .then((cfg: UIConfig) => {
        if (!active) return;
        setEnabled(cfg.enabled);
        setCatalogType((cfg.catalog_type as CatalogType) || 'minimal');
        setCatalogJson(cfg.catalog_json || '');
        // Hydrate palettes: prefer a `themes` map; else migrate the legacy
        // single {accent, density} style into the Default palette.
        let loaded: Record<string, Theme> = { default: { ...DEFAULT_THEME } };
        if (cfg.style_json) {
          try {
            const s = JSON.parse(cfg.style_json);
            if (s && typeof s.themes === 'object' && s.themes) {
              loaded = {};
              for (const [k, v] of Object.entries(s.themes)) {
                loaded[k] = normalizeTheme(v as Partial<Theme>);
              }
              if (!loaded.default) loaded.default = { ...DEFAULT_THEME };
            } else if (s) {
              loaded.default = normalizeTheme({ accent: s.accent, density: s.density });
            }
            if (s && typeof s.options === 'object' && s.options) {
              setOptions(s.options as Record<string, Record<string, OptionValue>>);
            }
          } catch {
            /* keep defaults */
          }
        }
        setThemes(loaded);
      })
      .catch(() => active && setError('Failed to load the UI Configurator configuration.'))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, []);

  const handleSave = useCallback(async () => {
    setSaving(true);
    setError(null);
    setSaved(false);
    // Validate custom catalog JSON before saving.
    if (enabled && catalogType === 'custom' && catalogJson.trim()) {
      try {
        JSON.parse(catalogJson);
      } catch {
        setError('Custom catalog is not valid JSON.');
        setSaving(false);
        return;
      }
    }
    // Derive a directive sentence per type from its settings — the backend
    // appends these verbatim, so all option phrasing stays in this file.
    const directives: Record<string, string> = {};
    for (const type of Object.keys(TYPE_OPTIONS)) {
      directives[type] = buildDirective(type, options[type]);
    }
    // Mirror the Default accent/density at the top level for backward compat
    // with the legacy single-color reader; the full maps live under `themes`.
    const style_json = JSON.stringify({
      accent: themes.default.accent,
      density: themes.default.density,
      themes,
      options,
      directives,
    });
    const payload: UIConfigUpdate = {
      enabled,
      catalog_type: catalogType,
      catalog_json: catalogType === 'custom' ? catalogJson : null,
      style_json,
    };
    try {
      await UIConfigService.updateConfig(payload);
      setSaved(true);
    } catch {
      setError('Failed to save. You may need workspace-admin permissions.');
    } finally {
      setSaving(false);
    }
  }, [enabled, catalogType, catalogJson, themes, options]);

  // The palette currently being edited, and whether the active type is themed.
  const activeTheme: Theme = themes[activeType] || themes.default;
  const isCustomized = activeType === 'default' || !!themes[activeType];
  const activeSpecs = optionSpecs(activeType);

  const patchActive = (patch: Partial<Theme>) => {
    setThemes((prev) => ({ ...prev, [activeType]: { ...(prev[activeType] || prev.default), ...patch } }));
    setSaved(false);
  };

  const setCustomize = (on: boolean) => {
    setThemes((prev) => {
      const next = { ...prev };
      if (on) next[activeType] = { ...(prev[activeType] || prev.default) };
      else delete next[activeType];
      return next;
    });
    setSaved(false);
  };

  const patchOption = (key: string, value: OptionValue) => {
    setOptions((prev) => ({ ...prev, [activeType]: { ...(prev[activeType] || {}), [key]: value } }));
    setSaved(false);
  };

  const renderOptionControl = (s: OptionSpec) => {
    const value = optionVal(options[activeType], s);
    if (s.kind === 'switch') {
      return (
        <FormControlLabel
          key={s.key}
          control={<Switch size="small" checked={value as boolean} onChange={(e) => patchOption(s.key, e.target.checked)} />}
          label={<Typography variant="body2">{s.label}</Typography>}
        />
      );
    }
    if (s.kind === 'number') {
      return (
        <TextField
          key={s.key}
          label={s.label}
          type="number"
          size="small"
          value={value as number}
          onChange={(e) => patchOption(s.key, Number(e.target.value))}
          inputProps={{ min: s.min, max: s.max, step: s.step ?? 1 }}
          sx={{ width: 170 }}
        />
      );
    }
    return (
      <FormControl key={s.key} size="small" sx={{ minWidth: 180 }}>
        <InputLabel id={`opt-${s.key}`}>{s.label}</InputLabel>
        <Select labelId={`opt-${s.key}`} label={s.label} value={value as string} onChange={(e) => patchOption(s.key, e.target.value)}>
          {s.choices.map((c) => (
            <MenuItem key={c.value} value={c.value}>{c.label}</MenuItem>
          ))}
        </Select>
      </FormControl>
    );
  };

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
        <CircularProgress size={28} />
      </Box>
    );
  }

  const activeLabel = DELIVERABLE_TYPES.find((d) => d.key === activeType)?.label;

  return (
    <Box sx={{ maxWidth: 680 }}>
      <Typography variant="h6" gutterBottom>
        UI Configurator
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        When enabled, crews in this workspace produce a structured, on-brand UI that renders
        consistently in the chat preview — instead of arbitrary, ad-hoc HTML. Branding and
        per-type settings are configured below. Disabled by default.
      </Typography>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {saved && <Alert severity="success" sx={{ mb: 2 }}>Saved.</Alert>}

      <FormControlLabel
        control={<Switch checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />}
        label="Enable predefined UI for this workspace"
      />

      {enabled && (
        <Box sx={{ mt: 2 }}>
          <Divider sx={{ mb: 2 }} />

          <FormControl fullWidth size="small" sx={{ mb: 2 }}>
            <InputLabel id="ui-catalog-label">Component catalog</InputLabel>
            <Select
              labelId="ui-catalog-label"
              label="Component catalog"
              value={catalogType}
              onChange={(e) => {
                const next = e.target.value as CatalogType;
                setCatalogType(next);
                // Prefill the custom editor with a rich starting catalog.
                if (next === 'custom' && !catalogJson.trim()) {
                  setCatalogJson(SAMPLE_CUSTOM_CATALOG);
                }
              }}
            >
              <MenuItem value="minimal">Minimal — 5 essentials (Text, Row, Column, Button, TextField)</MenuItem>
              <MenuItem value="basic">Basic — 13 components (cards, lists, images, badges, inputs…)</MenuItem>
              <MenuItem value="custom">Custom — bring your own catalog JSON</MenuItem>
            </Select>
          </FormControl>

          {catalogType !== 'custom' && (
            <Box sx={{ mb: 2 }}>
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                Components agents may use in this catalog:
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                {CATALOG_COMPONENTS[catalogType].map((c) => (
                  <Chip key={c} label={c} size="small" variant="outlined" />
                ))}
              </Box>
            </Box>
          )}

          {catalogType === 'custom' && (
            <TextField
              label="Custom catalog JSON"
              multiline
              minRows={10}
              fullWidth
              size="small"
              value={catalogJson}
              onChange={(e) => setCatalogJson(e.target.value)}
              placeholder={SAMPLE_CUSTOM_CATALOG}
              sx={{ mb: 2, fontFamily: 'monospace' }}
              InputProps={{ sx: { fontFamily: 'monospace', fontSize: '0.8rem' } }}
            />
          )}

          <Divider sx={{ mb: 2 }} />

          <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
            Branding &amp; per-type settings
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1.5 }}>
            Pick a deliverable to configure its own settings and palette. Types left on the
            Default palette inherit Default branding.
          </Typography>

          {/* Deliverable type selector — a dot shows the configured accent. */}
          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75, mb: 2 }}>
            {DELIVERABLE_TYPES.map(({ key, label }) => {
              const marked = key === 'default' || !!themes[key] || !!options[key];
              const dotColor = (themes[key] || themes.default).accent;
              return (
                <Chip
                  key={key}
                  label={label}
                  size="small"
                  onClick={() => setActiveType(key)}
                  color={activeType === key ? 'primary' : 'default'}
                  variant={activeType === key ? 'filled' : 'outlined'}
                  icon={
                    marked ? (
                      <Box sx={{ width: 9, height: 9, borderRadius: '50%', bgcolor: dotColor, ml: 0.75 }} />
                    ) : undefined
                  }
                />
              );
            })}
          </Box>

          {/* Type-specific settings (not shown for Default). */}
          {activeType !== 'default' && activeSpecs.length > 0 && (
            <Box sx={{ mb: 2 }}>
              <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
                {activeLabel} settings
              </Typography>
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, alignItems: 'center' }}>
                {activeSpecs.map(renderOptionControl)}
              </Box>
            </Box>
          )}

          {activeType !== 'default' && <Divider sx={{ mb: 2 }} />}

          <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
            {activeType === 'default' ? 'Default palette' : `${activeLabel} palette`}
          </Typography>

          {/* Per-type "use own palette" toggle (not shown for Default itself). */}
          {activeType !== 'default' && (
            <FormControlLabel
              sx={{ mb: 1 }}
              control={<Switch size="small" checked={!!themes[activeType]} onChange={(e) => setCustomize(e.target.checked)} />}
              label={<Typography variant="body2">Give {activeLabel} its own palette</Typography>}
            />
          )}

          {!isCustomized && (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2, fontStyle: 'italic' }}>
              {activeType === 'presentation'
                ? 'Presentations render with the built-in Databricks deck theme (deep-teal stage, orange accent, animated slides). Turn on the switch above to set custom branding instead.'
                : `Inherits the Default palette. Turn on the switch above to give ${activeLabel} its own branding.`}
            </Typography>
          )}

          {isCustomized && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              {/* Presets */}
              <Box>
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                  Start from a preset:
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {THEME_PRESETS.map((p) => (
                    <Chip key={p.key} label={p.label} size="small" variant="outlined" onClick={() => patchActive({ ...p.theme })} />
                  ))}
                  {activeType !== 'default' && (
                    <Chip label="Copy from Default" size="small" variant="outlined" onClick={() => patchActive({ ...themes.default })} />
                  )}
                </Box>
              </Box>

              {/* Color palette */}
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1.5 }}>
                <ColorField label="Accent" value={activeTheme.accent} onChange={(v) => patchActive({ accent: v })} />
                <ColorField label="Background" value={activeTheme.background} onChange={(v) => patchActive({ background: v })} />
                <ColorField label="Surface" value={activeTheme.surface} onChange={(v) => patchActive({ surface: v })} />
                <ColorField label="Heading" value={activeTheme.heading} onChange={(v) => patchActive({ heading: v })} />
                <ColorField label="Text" value={activeTheme.text} onChange={(v) => patchActive({ text: v })} />
                <ColorField label="Muted" value={activeTheme.muted} onChange={(v) => patchActive({ muted: v })} />
              </Box>

              {/* Typography + density */}
              <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
                <FormControl size="small" sx={{ minWidth: 220 }}>
                  <InputLabel id="ui-font-label">Font</InputLabel>
                  <Select
                    labelId="ui-font-label"
                    label="Font"
                    value={activeTheme.font}
                    onChange={(e) => patchActive({ font: e.target.value as Theme['font'] })}
                  >
                    {FONT_OPTIONS.map((f) => (
                      <MenuItem key={f.value} value={f.value}>{f.label}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <FormControl size="small" sx={{ minWidth: 180 }}>
                  <InputLabel id="ui-density-label">Density</InputLabel>
                  <Select
                    labelId="ui-density-label"
                    label="Density"
                    value={activeTheme.density}
                    onChange={(e) => patchActive({ density: e.target.value as Theme['density'] })}
                  >
                    <MenuItem value="comfortable">Comfortable</MenuItem>
                    <MenuItem value="compact">Compact</MenuItem>
                  </Select>
                </FormControl>
              </Box>

              {/* Live preview */}
              <Box>
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                  Preview:
                </Typography>
                <PalettePreview theme={activeTheme} />
              </Box>
            </Box>
          )}
        </Box>
      )}

      <Box sx={{ mt: 3 }}>
        <Button variant="contained" onClick={handleSave} disabled={saving}>
          {saving ? 'Saving…' : 'Save'}
        </Button>
      </Box>
    </Box>
  );
};

export default UIConfigurator;
