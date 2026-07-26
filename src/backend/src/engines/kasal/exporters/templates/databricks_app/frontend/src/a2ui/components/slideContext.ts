/**
 * Slide position context.
 *
 * Lives in its own module because both the primitives (Markdown, KeyValue) and
 * the slide renderers read it — importing it from `slides` would make the two
 * modules mutually dependent.
 */
import { createContext } from 'react'

// Slide layout context: which slide is showing + that we're inside a deck (so
// KeyValue renders as a themed stat tile). The deck THEME comes from
// DeckThemeContext (one theme for the whole deck — variety is by LAYOUT below).
export const SlideCtx = createContext<{ idx: number; total: number; inDeck: boolean }>({
  idx: 0,
  total: 1,
  inDeck: false,
})
