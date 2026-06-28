/**
 * Guards for the presentation deck identity (UI Configuration ↔ renderer).
 *
 * The Databricks "Studio" deck identity (orange accent, deep-teal stage) now lives
 * as the configurator's Studio palette preset and is applied to the deck by the
 * SHARED A2UI renderer via themeToDeck (the legacy renderer's hardcoded
 * DECK_THEME_VARS was retired with it). These must not drift apart.
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const configuratorSrc = readFileSync(resolve(__dirname, 'UIConfigurator.tsx'), 'utf-8');
// THEME_PRESETS (incl. the Studio preset) live in the shared specs module.
const sharedSrc = readFileSync(resolve(__dirname, 'uiConfigShared.ts'), 'utf-8');
// The deck theme is owned by the shared A2UI renderer: a workspace palette is
// mapped to a DeckTheme via themeToDeck.
const deckThemesSrc = readFileSync(
  resolve(__dirname, '../../shared/a2ui/lib/deckThemes.ts'),
  'utf-8',
);

describe('presentation deck identity', () => {
  it('configurator offers the Studio preset with the Databricks accent + teal stage', () => {
    expect(sharedSrc).toContain("label: 'Studio'");
    expect(sharedSrc).toContain("accent: '#FF3621'");
  });

  it('the Presentation tab explains the built-in Studio deck default', () => {
    expect(configuratorSrc).toContain('built-in Studio deck theme');
  });

  it('the shared renderer maps a workspace palette into the deck theme', () => {
    expect(deckThemesSrc).toContain('export function themeToDeck');
    // accent + stage (background→surface gradient) are carried into the deck.
    expect(deckThemesSrc).toContain('accent: p.accent');
    expect(deckThemesSrc).toContain('${p.background}');
  });
});
