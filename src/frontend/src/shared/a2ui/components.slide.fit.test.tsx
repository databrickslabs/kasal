import { describe, it, expect, vi, beforeAll } from 'vitest';
import { render, screen } from '@testing-library/react';
import { A2UIRenderer } from './A2UIRenderer';
import { FitBox } from './components/slideFit';
import type { Surface } from './types';

// Slides are a FIXED 1280x720 canvas, so an overfull body must SHRINK, never
// scroll and never bleed. A scrollbar looks fine on screen and then silently
// drops everything below the fold from the downloaded PDF/PPTX; a bleed lands the
// body on top of the sources footer. Both were live bugs.
//
// jsdom reports every element as 0x0, so the real shrink FACTOR cannot be
// asserted here — that is a layout-engine behaviour. What is assertable, and what
// the bugs actually were, is structural: no scroll container in a slide body, and
// no content lost when it is too tall.

beforeAll(() => {
  // FitBox observes its box for late font/image reflow.
  if (typeof ResizeObserver === 'undefined') {
    vi.stubGlobal(
      'ResizeObserver',
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      },
    );
  }
});

const longBullets = Array.from({ length: 14 }, (_, i) => `- Bullet number ${i + 1}`).join('\n');

const deckOf = (slide: Record<string, unknown>, extra: Record<string, unknown>[] = []): Surface => ({
  surfaceKind: 'presentation',
  root: 'deck',
  components: [
    { id: 'deck', component: 'SlideDeck', children: ['s'] },
    { id: 's', component: 'Slide', ...slide } as never,
    ...(extra as never[]),
  ],
});

// Every variant that owns a body region. `overflow-auto` on any of them is the
// bug: it is a scrollbar in an artefact that cannot scroll.
const BODY_VARIANTS = [
  'content',
  'two-column',
  'visual',
  'agenda',
  'comparison',
  'kpi-split',
  'boxes',
  'split',
];

describe('slide bodies shrink to fit rather than scroll', () => {
  it.each(BODY_VARIANTS)('%s has no scrollable region', (variant) => {
    const { container } = render(
      <A2UIRenderer
        payload={deckOf(
          {
            variant,
            title: 'Overfull',
            sources: [{ label: 'A source' }],
            children: ['m1', 'm2'],
          },
          [
            { id: 'm1', component: 'Markdown', content: longBullets },
            { id: 'm2', component: 'Markdown', content: longBullets },
          ],
        )}
      />,
    );
    // A scroll container inside a slide means content is reachable only by
    // scrolling — impossible in the PDF/PPTX the reader downloads.
    expect(container.querySelectorAll('.a2-slide .overflow-auto')).toHaveLength(0);
  });

  it.each(BODY_VARIANTS)('%s keeps all its content when overfull', (variant) => {
    render(
      <A2UIRenderer
        payload={deckOf(
          { variant, title: 'Overfull', children: ['m1'] },
          [{ id: 'm1', component: 'Markdown', content: longBullets }],
        )}
      />,
    );
    // Shrinking keeps every word on the slide; clipping or scrolling would not.
    expect(screen.getByText('Bullet number 1')).toBeTruthy();
    expect(screen.getByText('Bullet number 14')).toBeTruthy();
  });
});

describe('FitBox', () => {
  it('clips at its own boundary so it cannot bleed over the sources footer', () => {
    const { container } = render(<FitBox>content</FitBox>);
    expect(container.firstElementChild?.className).toContain('overflow-hidden');
  });

  it('leaves content unscaled when it already fits', () => {
    // jsdom heights are 0, which reads as "fits" — so this pins the no-op path:
    // a slide that fits must not be transformed at all.
    const { container } = render(<FitBox>short</FitBox>);
    const inner = container.firstElementChild?.firstElementChild as HTMLElement;
    expect(inner.style.transform).toBe('');
  });

  it('compensates its width for the transform so text reflows before shrinking', () => {
    // Without `width: 100/k %` the content would keep the pre-scale measure and
    // get squeezed into a narrow strip instead of using the full column.
    const { container } = render(<FitBox>short</FitBox>);
    const inner = container.firstElementChild?.firstElementChild as HTMLElement;
    expect(inner.style.width).toBe('100%');
    expect(inner.style.transformOrigin).toBe('top left');
  });
});
