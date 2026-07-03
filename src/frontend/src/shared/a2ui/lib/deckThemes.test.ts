import { describe, it, expect } from 'vitest';
import {
  hexToHslChannels,
  themeToDeck,
  themeToTokens,
  getDeckTheme,
  seriesFromAccent,
  readableTextOn,
  DEFAULT_DECK_THEME_ID,
  type Palette,
} from './deckThemes';

const HEX = /^#[0-9a-f]{6}$/i;

const LIGHT: Palette = {
  accent: '#2272B4',
  background: '#FFFFFF',
  surface: '#F8FAFC',
  text: '#0F172A',
  heading: '#0F172A',
  muted: '#64748B',
};
const DARK: Palette = {
  accent: '#38BDF8',
  background: '#0F172A',
  surface: '#1E293B',
  text: '#E2E8F0',
  heading: '#F8FAFC',
  muted: '#94A3B8',
};

describe('hexToHslChannels', () => {
  it('converts pure white/black to channel triples', () => {
    expect(hexToHslChannels('#FFFFFF')).toBe('0 0% 100%');
    expect(hexToHslChannels('#000000')).toBe('0 0% 0%');
  });
  it('expands 3-digit hex', () => {
    expect(hexToHslChannels('#fff')).toBe('0 0% 100%');
  });
  it('emits "H S% L%" shape for a color', () => {
    expect(hexToHslChannels('#2272B4')).toMatch(/^\d{1,3} \d{1,3}% \d{1,3}%$/);
  });
  it('falls back safely on invalid input (no throw)', () => {
    expect(hexToHslChannels('not-a-color')).toBe('0 0% 50%');
    expect(hexToHslChannels('')).toBe('0 0% 50%');
  });
});

describe('themeToDeck', () => {
  it('maps a light palette and flags it not-dark', () => {
    const d = themeToDeck(LIGHT);
    expect(d.bg).toBe('#FFFFFF');
    expect(d.title).toBe('#0F172A');
    expect(d.accent).toBe('#2272B4');
    expect(d.kicker).toBe('#2272B4');
    expect(d.dark).toBe(false);
    expect(d.stage).toContain('#FFFFFF'); // gradient from background
  });
  it('flags a dark palette as dark', () => {
    expect(themeToDeck(DARK).dark).toBe(true);
  });
});

describe('themeToTokens', () => {
  it('emits the full --a2-* token set as channel strings', () => {
    const t = themeToTokens(LIGHT);
    for (const key of [
      '--a2-background', '--a2-foreground', '--a2-card', '--a2-card-foreground',
      '--a2-primary', '--a2-primary-foreground', '--a2-muted', '--a2-muted-foreground',
      '--a2-border', '--a2-ring',
    ]) {
      expect(t[key]).toMatch(/^\d{1,3} \d{1,3}% \d{1,3}%$/);
    }
  });
  it('primary tracks the accent; background tracks the palette background', () => {
    const t = themeToTokens(LIGHT);
    expect(t['--a2-primary']).toBe(hexToHslChannels(LIGHT.accent));
    expect(t['--a2-background']).toBe(hexToHslChannels(LIGHT.background));
    // primary-foreground is the background (contrast heuristic)
    expect(t['--a2-primary-foreground']).toBe(hexToHslChannels(LIGHT.background));
  });
});

describe('getDeckTheme', () => {
  it('returns the default theme for unknown ids', () => {
    expect(getDeckTheme('nope').id).toBe(DEFAULT_DECK_THEME_ID);
    expect(getDeckTheme(undefined).id).toBe(DEFAULT_DECK_THEME_ID);
  });
});

describe('seriesFromAccent', () => {
  it('leads with the accent itself, so single-series charts stay on brand', () => {
    expect(seriesFromAccent('#2272B4', 4)[0]).toBe('#2272B4');
  });
  it('returns exactly `count` valid hex colors', () => {
    const s = seriesFromAccent('#2272B4', 5);
    expect(s).toHaveLength(5);
    for (const c of s) expect(c).toMatch(HEX);
  });
  it('produces distinct hues beyond the accent (not a single flat color)', () => {
    const s = seriesFromAccent('#2272B4', 4);
    expect(new Set(s).size).toBe(4);
  });
  it('handles empty and non-positive counts', () => {
    expect(seriesFromAccent('#2272B4', 0)).toEqual([]);
    expect(seriesFromAccent('#2272B4', -3)).toEqual([]);
  });
  it('falls back to a usable palette when the accent is unparseable', () => {
    const s = seriesFromAccent('not-a-color', 3);
    expect(s).toHaveLength(3);
    for (const c of s) expect(c).toMatch(HEX);
  });
});

describe('readableTextOn', () => {
  it('returns white on a dark accent and dark ink on a light accent', () => {
    expect(readableTextOn('#0F172A')).toBe('#ffffff');
    expect(readableTextOn('#FDE68A')).toBe('#0f172a');
  });
  it('falls back to white on invalid input (no throw)', () => {
    expect(readableTextOn('nope')).toBe('#ffffff');
  });
});
