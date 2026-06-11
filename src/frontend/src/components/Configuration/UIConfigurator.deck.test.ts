/**
 * Guards for the presentation deck identity (UI Configuration ↔ renderer).
 *
 * The renderer owns the built-in Databricks deck theme; the configurator
 * mirrors it as the "Databricks Deck" preset and explains the default on the
 * Presentation tab. These must not drift apart.
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

const configuratorSrc = readFileSync(resolve(__dirname, 'UIConfigurator.tsx'), 'utf-8');
const rendererSrc = readFileSync(
  resolve(__dirname, '../ChatMode/components/Preview/UiRenderer.tsx'),
  'utf-8',
);

describe('presentation deck identity', () => {
  it('renderer defines deck default tokens with the Databricks accent', () => {
    expect(rendererSrc).toContain('DECK_THEME_VARS');
    expect(rendererSrc).toContain("'--ui-accent': '#FF3621'");
    expect(rendererSrc).toContain('#162A34'); // deep-teal stage
  });

  it('configurator offers the matching Databricks Deck preset', () => {
    expect(configuratorSrc).toContain("label: 'Databricks Deck'");
    expect(configuratorSrc).toContain("accent: '#FF3621'");
  });

  it('the Presentation tab explains the built-in deck default', () => {
    expect(configuratorSrc).toContain('built-in Databricks deck theme');
  });

  it('slide entrance animation is wired (renderer class + css keyframes)', () => {
    expect(rendererSrc).toContain("className=\"ui-slide-enter\"");
    const css = readFileSync(resolve(__dirname, '../ChatMode/chat.css'), 'utf-8');
    expect(css).toContain('@keyframes kasalSlideRise');
    expect(css).toContain('prefers-reduced-motion');
  });
});
