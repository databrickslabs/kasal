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

interface StyleConfig {
  accent?: string;
  density?: 'comfortable' | 'compact';
}

const DEFAULT_STYLE: StyleConfig = { accent: '#2272B4', density: 'comfortable' };

// Components available in each built-in catalog (shown as chips).
const CATALOG_COMPONENTS: Record<Exclude<CatalogType, 'custom'>, string[]> = {
  minimal: ['Text', 'Row', 'Column', 'Button', 'TextField'],
  basic: [
    'Text', 'Row', 'Column', 'Card', 'List', 'Divider', 'Image', 'Icon',
    'Badge', 'Button', 'TextField', 'CheckBox', 'Slider', 'ChoicePicker',
    'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide',
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

/**
 * Per-workspace "UI Configurator" configuration.
 *
 * When enabled, crews in this workspace emit a structured, design-system UI
 * (rendered consistently in the chat preview) instead of arbitrary HTML. The
 * structured format conforms to the A2UI protocol (see THIRD_PARTY_NOTICES);
 * the UX here is intentionally UI-centric, not protocol-specific.
 */
const UIConfigurator: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  const [enabled, setEnabled] = useState(false);
  const [catalogType, setCatalogType] = useState<CatalogType>('minimal');
  const [catalogJson, setCatalogJson] = useState('');
  const [style, setStyle] = useState<StyleConfig>(DEFAULT_STYLE);

  useEffect(() => {
    let active = true;
    UIConfigService.getConfig()
      .then((cfg: UIConfig) => {
        if (!active) return;
        setEnabled(cfg.enabled);
        setCatalogType((cfg.catalog_type as CatalogType) || 'minimal');
        setCatalogJson(cfg.catalog_json || '');
        if (cfg.style_json) {
          try {
            setStyle({ ...DEFAULT_STYLE, ...JSON.parse(cfg.style_json) });
          } catch {
            setStyle(DEFAULT_STYLE);
          }
        }
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
    const payload: UIConfigUpdate = {
      enabled,
      catalog_type: catalogType,
      catalog_json: catalogType === 'custom' ? catalogJson : null,
      style_json: JSON.stringify(style),
    };
    try {
      await UIConfigService.updateConfig(payload);
      setSaved(true);
    } catch {
      setError('Failed to save. You may need workspace-admin permissions.');
    } finally {
      setSaving(false);
    }
  }, [enabled, catalogType, catalogJson, style]);

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
        <CircularProgress size={28} />
      </Box>
    );
  }

  return (
    <Box sx={{ maxWidth: 640 }}>
      <Typography variant="h6" gutterBottom>
        UI Configurator
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        When enabled, crews in this workspace produce a structured, on-brand UI that renders
        consistently in the chat preview — instead of arbitrary, ad-hoc HTML. Disabled by default.
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

          <Typography variant="subtitle2" sx={{ mt: 1, mb: 1 }}>
            Style
          </Typography>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', mb: 1 }}>
            <TextField
              label="Accent color"
              type="color"
              size="small"
              value={style.accent || DEFAULT_STYLE.accent}
              onChange={(e) => setStyle((s) => ({ ...s, accent: e.target.value }))}
              sx={{ width: 120 }}
            />
            <FormControl size="small" sx={{ minWidth: 180 }}>
              <InputLabel id="ui-density-label">Density</InputLabel>
              <Select
                labelId="ui-density-label"
                label="Density"
                value={style.density || 'comfortable'}
                onChange={(e) => setStyle((s) => ({ ...s, density: e.target.value as StyleConfig['density'] }))}
              >
                <MenuItem value="comfortable">Comfortable</MenuItem>
                <MenuItem value="compact">Compact</MenuItem>
              </Select>
            </FormControl>
          </Box>
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
