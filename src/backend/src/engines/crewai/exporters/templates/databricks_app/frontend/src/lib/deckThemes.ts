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
