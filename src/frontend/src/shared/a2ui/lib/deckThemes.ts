import { createContext } from 'react'

// A single presentation theme applied to EVERY slide in a deck (no per-slide
// color rotation). Variety comes from per-slide LAYOUT (title / stats / quote /
// content), not color. Users pick a theme via the deck's Customize button; the
// choice also drives the PowerPoint export.
export interface DeckTheme {
  id: string
  name: string
  stage: string // CSS background for the on-screen slide (may be a gradient)
  bg: string // solid hex (PowerPoint slide background + fallback)
  fg: string // body text
  title: string // title color
  kicker: string // eyebrow / topic label + accent text
  accent: string // accent bar, stat numbers
  muted: string // subtitle / secondary text
  panel: string // inner card background (stat tiles)
  panelBorder: string
  dark: boolean
}

export const DECK_THEMES: DeckTheme[] = [
  {
    id: 'midnight',
    name: 'Midnight',
    stage: 'linear-gradient(135deg, #0b1020 0%, #1b2347 100%)',
    bg: '#10162e',
    fg: '#e6ecff',
    title: '#ffffff',
    kicker: '#8aa2ff',
    accent: '#5a8cff',
    muted: 'rgba(255,255,255,0.6)',
    panel: 'rgba(255,255,255,0.06)',
    panelBorder: 'rgba(255,255,255,0.14)',
    dark: true,
  },
  {
    id: 'ember',
    name: 'Ember',
    stage: 'radial-gradient(ellipse at 50% 30%, #16313b 0%, #0a141a 72%)',
    bg: '#0b161c',
    fg: '#dfe7ec',
    title: '#ffffff',
    kicker: '#ff6a4d',
    accent: '#ff3621',
    muted: 'rgba(255,255,255,0.58)',
    panel: 'rgba(255,255,255,0.06)',
    panelBorder: 'rgba(255,255,255,0.14)',
    dark: true,
  },
  {
    id: 'slate',
    name: 'Slate',
    stage: 'linear-gradient(135deg, #0f172a 0%, #1e293b 100%)',
    bg: '#0f172a',
    fg: '#e2e8f0',
    title: '#ffffff',
    kicker: '#38bdf8',
    accent: '#0ea5e9',
    muted: 'rgba(255,255,255,0.55)',
    panel: 'rgba(255,255,255,0.06)',
    panelBorder: 'rgba(255,255,255,0.14)',
    dark: true,
  },
  {
    id: 'light',
    name: 'Light',
    stage: '#ffffff',
    bg: '#ffffff',
    fg: '#293040',
    title: '#0f172a',
    kicker: '#2563eb',
    accent: '#2563eb',
    muted: '#6b7280',
    panel: '#f4f6f8',
    panelBorder: '#e6e8eb',
    dark: false,
  },
]

export const DECK_THEME_KEY = 'kasal.deckTheme'
export const DEFAULT_DECK_THEME_ID = 'midnight'

export function getDeckTheme(id: string | null | undefined): DeckTheme {
  return DECK_THEMES.find((t) => t.id === id) ?? DECK_THEMES[0]
}

// The active deck theme flows from the surface toolbar (where Customize lives)
// down to the Slide / KeyValue renderers.
export const DeckThemeContext = createContext<DeckTheme>(DECK_THEMES[0])

/* ------------------------------------------------------------------ */
/*  Workspace palette → renderer theme mapping                         */
/* ------------------------------------------------------------------ */

// A UIConfigurator branding palette (the source of truth). Structural subset of
// the configurator's `Theme` — declared locally so this module stays portable
// (the exported app has no access to Kasal's Configuration types).
export interface Palette {
  accent: string
  background: string
  surface: string
  text: string
  heading: string
  muted: string
  font?: string
  density?: string
}

const _ch = (hex: string): [number, number, number] | null => {
  const h = (hex || '').replace('#', '').trim()
  const full = h.length === 3 ? h.split('').map((c) => c + c).join('') : h
  if (!/^[0-9a-fA-F]{6}$/.test(full)) return null
  return [
    parseInt(full.slice(0, 2), 16),
    parseInt(full.slice(2, 4), 16),
    parseInt(full.slice(4, 6), 16),
  ]
}

/** hex (#RRGGBB / #RGB) → "H S% L%" channels for `hsl(var(--a2-*) / <alpha>)`. */
export function hexToHslChannels(hex: string): string {
  const rgb = _ch(hex)
  if (!rgb) return '0 0% 50%'
  const [r, g, b] = rgb.map((v) => v / 255) as [number, number, number]
  const max = Math.max(r, g, b)
  const min = Math.min(r, g, b)
  const l = (max + min) / 2
  const d = max - min
  let hue = 0
  let s = 0
  if (d !== 0) {
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min)
    if (max === r) hue = (g - b) / d + (g < b ? 6 : 0)
    else if (max === g) hue = (b - r) / d + 2
    else hue = (r - g) / d + 4
    hue /= 6
  }
  return `${Math.round(hue * 360)} ${Math.round(s * 100)}% ${Math.round(l * 100)}%`
}

/** hex + alpha → rgba() (used for derived borders that need transparency). */
function withAlpha(hex: string, alpha: number): string {
  const rgb = _ch(hex)
  if (!rgb) return `rgba(127,127,127,${alpha})`
  return `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${alpha})`
}

function isDarkHex(hex: string): boolean {
  const rgb = _ch(hex)
  if (!rgb) return false
  // Perceived luminance (sRGB) — < 0.5 reads as a dark stage.
  const [r, g, b] = rgb
  return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255 < 0.5
}

/** Map a workspace branding palette → a DeckTheme (drives the on-screen deck AND
 *  the PowerPoint export, which both read DeckThemeContext). */
export function themeToDeck(p: Palette): DeckTheme {
  return {
    id: 'workspace',
    name: 'Workspace',
    stage: `linear-gradient(135deg, ${p.background} 0%, ${p.surface} 100%)`,
    bg: p.background,
    fg: p.text,
    title: p.heading,
    kicker: p.accent,
    accent: p.accent,
    muted: p.muted,
    panel: p.surface,
    panelBorder: withAlpha(p.muted, 0.3),
    dark: isDarkHex(p.background),
  }
}

/** Map a workspace branding palette → the `--a2-*` CSS custom properties the
 *  shadcn components (Card/Table/Markdown/Chart/Button) consume. Apply as an
 *  inline style on the renderer's wrapper so document/dashboard surfaces pick up
 *  workspace branding too. */
export function themeToTokens(p: Palette): Record<string, string> {
  const bg = hexToHslChannels(p.background)
  const fg = hexToHslChannels(p.text)
  const surface = hexToHslChannels(p.surface)
  const accent = hexToHslChannels(p.accent)
  const muted = hexToHslChannels(p.muted)
  return {
    '--a2-background': bg,
    '--a2-foreground': fg,
    '--a2-card': surface,
    '--a2-card-foreground': fg,
    '--a2-popover': surface,
    '--a2-popover-foreground': fg,
    '--a2-primary': accent,
    '--a2-primary-foreground': bg,
    '--a2-secondary': surface,
    '--a2-secondary-foreground': fg,
    '--a2-muted': surface,
    '--a2-muted-foreground': muted,
    '--a2-accent': surface,
    '--a2-accent-foreground': fg,
    '--a2-border': muted,
    '--a2-input': muted,
    '--a2-ring': accent,
  }
}
