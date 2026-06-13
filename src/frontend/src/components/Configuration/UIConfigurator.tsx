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

type CatalogType = 'minimal' | 'basic' | 'custom';

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
                ? 'Presentations render with the built-in Studio deck theme (deep-teal stage, orange accent, animated slides). Turn on the switch above to set custom branding instead.'
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
