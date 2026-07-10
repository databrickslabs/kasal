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
  Collapse,
  Chip,
} from '@mui/material';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import { UIConfigService, UIConfig, UIConfigUpdate } from '../../api/UIConfigService';
// Branding palettes + per-deliverable settings live in a shared module so the
// in-preview "Customize" panel reuses the exact same specs (single source of truth).
import {
  Theme,
  DeliverableKey,
  DELIVERABLE_TYPES,
  FONT_OPTIONS,
  FONT_CSS,
  THEME_PRESETS,
  DEFAULT_THEME,
  normalizeTheme,
  OptionValue,
  OptionSpec,
  TYPE_OPTIONS,
  optionSpecs,
  optionVal,
  buildDirective,
} from './uiConfigShared';

type CatalogType = 'minimal' | 'full' | 'custom';

// Components available in each built-in catalog (shown as chips). These MUST match
// the shared A2UI renderer's registry (src/shared/a2ui) and the backend's
// MINIMAL_COMPONENTS / catalog.json — they are the components the composer is
// allowed to emit and the renderer can actually draw.
const CATALOG_COMPONENTS: Record<Exclude<CatalogType, 'custom'>, string[]> = {
  // Mirrors backend MINIMAL_COMPONENTS — structure + prose, no rich surfaces.
  minimal: ['Markdown', 'Text', 'Heading', 'List', 'Table', 'Divider', 'Row', 'Column', 'Card', 'Image'],
  // The full shared catalog (catalog.json) — adds KPIs, charts, slides, mindmap, quiz.
  full: [
    'Markdown', 'Text', 'Heading', 'Image', 'Card', 'KeyValue', 'List', 'Table',
    'Divider', 'Row', 'Column', 'Grid', 'Chart', 'SlideDeck', 'Slide', 'Mindmap', 'Quiz',
    'Flashcards', 'Map',
  ],
};

// Rich starting point prefilled into the custom-catalog editor. Mirrors the shared
// catalog.json shape ({ version, surfaceKinds, components:{ Name:{ summary, props }}})
// that the backend composer reads and the shared renderer draws — so a saved custom
// catalog is a valid edit of the real contract. Trim components to constrain agents.
const SAMPLE_CUSTOM_CATALOG = JSON.stringify(
  {
    version: '1.0',
    description: 'Components agents may use. Extend or trim to taste. Names must match the shared renderer.',
    surfaceKinds: ['conversation', 'document', 'presentation', 'dashboard', 'mindmap', 'quiz', 'flashcards', 'map'],
    components: {
      Markdown: { summary: 'A block of GitHub-flavored markdown.', props: { content: 'string(binding)' } },
      Text: { summary: 'A short run of plain text.', props: { text: 'string(binding)', variant: ['body', 'caption', 'label'] } },
      Heading: { summary: 'A section heading.', props: { text: 'string(binding)', level: 'int(1-6)' } },
      Image: { summary: 'An image with optional caption.', props: { src: 'string(binding)', alt: 'string?', caption: 'string?' } },
      Card: { summary: 'A titled container grouping children.', props: { title: 'string?', children: 'id[]' } },
      KeyValue: { summary: 'A label/value pair; good for KPIs.', props: { label: 'string(binding)', value: 'string(binding)' } },
      List: { summary: 'An ordered or unordered list of strings.', props: { items: 'array(binding)', ordered: 'bool?' } },
      Table: { summary: 'A data table.', props: { columns: '[string]', rows: '[[cell, …]](binding)' } },
      Divider: { summary: 'A horizontal rule / separator.', props: {} },
      Row: { summary: 'Lays out children horizontally.', props: { children: 'id[]', gap: 'int?' } },
      Column: { summary: 'Lays out children vertically.', props: { children: 'id[]', gap: 'int?' } },
      Grid: { summary: 'Responsive grid with a column count.', props: { children: 'id[]', columns: 'int' } },
      Chart: { summary: 'A bar/line/pie chart.', props: { chartType: ['bar', 'line', 'pie'], data: 'array(binding)', xKey: 'string', yKeys: '[string]', title: 'string?' } },
      SlideDeck: { summary: 'Presentation container; children are Slides. Root for surfaceKind "presentation".', props: { children: 'slideId[]' } },
      Slide: { summary: 'One slide. variant: title|stats|quote|content|section.', props: { variant: 'string', kicker: 'string?', title: 'string?', subtitle: 'string?', children: 'id[]' } },
      Mindmap: { summary: 'A mindmap/tree. Root for surfaceKind "mindmap".', props: { root: '{ id, label, description?, children:[node] }(binding)' } },
      Quiz: { summary: 'Interactive multiple-choice quiz. Root for surfaceKind "quiz".', props: { title: 'string?', questions: '[{ question, options:[string], answer:int, explanation? }](binding)' } },
      Flashcards: { summary: 'Anki-style flippable study deck. Root for surfaceKind "flashcards".', props: { title: 'string?', cards: '[{ front, back, hint? }](binding)' } },
      Map: { summary: 'Geographic map plotting lat/lng points. Root for surfaceKind "map".', props: { title: 'string?', points: '[{ lat, lng, label?, value? }](binding)' } },
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
  const [catalogType, setCatalogType] = useState<CatalogType>('full');
  const [catalogJson, setCatalogJson] = useState('');
  // Collapsed by default: the catalog's component list is reference info, shown as a
  // compact count row that expands to a chip grid on demand.
  const [showCatalogComps, setShowCatalogComps] = useState(false);
  // Per-deliverable palettes. `default` is always present; other keys exist only
  // when that type has its own palette (absence = inherits Default).
  const [themes, setThemes] = useState<Record<string, Theme>>({ default: { ...DEFAULT_THEME } });
  // Per-deliverable type-specific settings (keyed by type, then option key).
  const [options, setOptions] = useState<Record<string, Record<string, OptionValue>>>({});
  const [activeType, setActiveType] = useState<DeliverableKey>('default');

  useEffect(() => {
    let active = true;
    // Force a fresh fetch: this editor is the writer, so it must open on the true
    // server state rather than a palette cached by an earlier chat surface.
    UIConfigService.getConfig(true)
      .then((cfg: UIConfig) => {
        if (!active) return;
        setEnabled(cfg.enabled);
        // Legacy rows may carry the old "basic" value — treat it as "full".
        const ct = (cfg.catalog_type as string) === 'basic' ? 'full' : cfg.catalog_type;
        setCatalogType((ct as CatalogType) || 'full');
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
      setError('Failed to save. You may need teamspace-admin permissions.');
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
        When enabled, crews in this teamspace produce a structured, on-brand UI that renders
        consistently in the chat preview — instead of arbitrary, ad-hoc HTML. Branding and
        per-type settings are configured below. Disabled by default.
      </Typography>

      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
      {saved && <Alert severity="success" sx={{ mb: 2 }}>Saved.</Alert>}

      <FormControlLabel
        control={<Switch checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />}
        label="Enable predefined UI for this teamspace"
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
              <MenuItem value="minimal">Minimal — essentials (Markdown, Text, Heading, List, Table, Card, Row/Column…)</MenuItem>
              <MenuItem value="full">Full — full set (adds KPIs, Charts, Slides, Mindmap, Quiz…)</MenuItem>
              <MenuItem value="custom">Custom — bring your own catalog JSON</MenuItem>
            </Select>
          </FormControl>

          {/* The catalog's components are reference info, not a selector — show a
              compact, professional count row that expands to a chip grid on demand
              (stays tidy as the catalog grows). */}
          {catalogType !== 'custom' && (
            <Box sx={{ mb: 2 }}>
              <Box
                onClick={() => setShowCatalogComps((s) => !s)}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  cursor: 'pointer',
                  px: 1.25,
                  py: 0.75,
                  borderRadius: 1,
                  border: '1px solid',
                  borderColor: 'divider',
                  bgcolor: 'action.hover',
                  '&:hover': { bgcolor: 'action.selected' },
                }}
              >
                <Typography variant="body2" color="text.secondary">
                  Components agents may use
                  <Box component="span" sx={{ ml: 1, color: 'text.primary', fontWeight: 600 }}>
                    {CATALOG_COMPONENTS[catalogType].length}
                  </Box>
                </Typography>
                <KeyboardArrowDownIcon
                  fontSize="small"
                  sx={{
                    color: 'text.secondary',
                    transform: showCatalogComps ? 'rotate(180deg)' : 'none',
                    transition: 'transform 150ms',
                  }}
                />
              </Box>
              <Collapse in={showCatalogComps}>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.75, pt: 1 }}>
                  {CATALOG_COMPONENTS[catalogType].map((c) => (
                    <Chip key={c} label={c} size="small" variant="outlined" />
                  ))}
                </Box>
              </Collapse>
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

          {/* Deliverable type selector — a dropdown (was a chip row) so it scales as
              we add more deliverable types. The dot shows the configured accent. */}
          <FormControl size="small" fullWidth sx={{ mb: 2 }}>
            <InputLabel id="ui-deliverable-label">Deliverable</InputLabel>
            <Select
              labelId="ui-deliverable-label"
              label="Deliverable"
              value={activeType}
              onChange={(e) => setActiveType(e.target.value as DeliverableKey)}
              renderValue={(val) => {
                const item = DELIVERABLE_TYPES.find((d) => d.key === val);
                const dot = (themes[val as string] || themes.default).accent;
                return (
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ width: 9, height: 9, borderRadius: '50%', bgcolor: dot }} />
                    {item?.label ?? val}
                  </Box>
                );
              }}
            >
              {DELIVERABLE_TYPES.map(({ key, label }) => {
                const marked = key === 'default' || !!themes[key] || !!options[key];
                const dotColor = (themes[key] || themes.default).accent;
                return (
                  <MenuItem key={key} value={key}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Box
                        sx={{
                          width: 9,
                          height: 9,
                          borderRadius: '50%',
                          bgcolor: marked ? dotColor : 'transparent',
                          border: marked ? 'none' : '1px solid',
                          borderColor: 'divider',
                        }}
                      />
                      {label}
                    </Box>
                  </MenuItem>
                );
              })}
            </Select>
          </FormControl>

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
                ? 'Presentations render with the built-in Studio deck theme (deep-teal stage, orange accent, animated slides). Turn on the switch above to set custom branding instead.'
                : `Inherits the Default palette. Turn on the switch above to give ${activeLabel} its own branding.`}
            </Typography>
          )}

          {isCustomized && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              {/* Presets — a dropdown (was a chip row) so it scales as we add more
                  presets. Fire-and-forget: picking one applies it and the control
                  resets to the placeholder. */}
              <FormControl size="small" sx={{ minWidth: 240 }}>
                <InputLabel id="ui-preset-label" shrink>
                  Start from a preset
                </InputLabel>
                <Select
                  labelId="ui-preset-label"
                  label="Start from a preset"
                  value=""
                  displayEmpty
                  renderValue={() => 'Start from a preset…'}
                  onChange={(e) => {
                    const key = e.target.value as string;
                    if (key === '__default__') {
                      patchActive({ ...themes.default });
                      return;
                    }
                    const preset = THEME_PRESETS.find((p) => p.key === key);
                    if (preset) patchActive({ ...preset.theme });
                  }}
                >
                  {THEME_PRESETS.map((p) => (
                    <MenuItem key={p.key} value={p.key}>
                      {p.label}
                    </MenuItem>
                  ))}
                  {activeType !== 'default' && (
                    <MenuItem value="__default__">Copy from Default</MenuItem>
                  )}
                </Select>
              </FormControl>

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
