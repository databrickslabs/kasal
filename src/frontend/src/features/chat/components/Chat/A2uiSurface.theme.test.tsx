/**
 * A surface's own `theme` (which the model can send) reaches CSS only as hex
 * colours. Real renderer and theme mapping here; A2uiSurface.test.tsx stubs both.
 */
import { describe, it, expect, vi } from 'vitest';
import { render } from '@testing-library/react';
import { A2uiSurface } from './A2uiSurface';

vi.mock('../../hooks/useA2uiThemes', () => ({ useA2uiThemes: () => null }));

const remote = 'url(https://images.example.org/x.png?d=1)';

describe('A2uiSurface theme', () => {
  it('drops a theme colour that is not hex before it reaches any style', () => {
    const surface = {
      surfaceKind: 'document',
      root: 'al',
      theme: { accent: '#2272B4', background: remote, surface: remote, text: '#111111', heading: '#111111', muted: remote },
      dataModel: { pics: ['/assets/a.png'] },
      components: [{ id: 'al', component: 'Album', title: 'Gallery', items: { path: '/pics' } }],
    };
    const { container } = render(<A2uiSurface surface={surface as never} />);
    expect(container.querySelector('img[src="/assets/a.png"]')).not.toBeNull();
    const styles = [container as HTMLElement, ...Array.from(container.querySelectorAll<HTMLElement>('[style]'))]
      .map((el) => el.getAttribute('style') || '')
      .join('\n');
    expect(styles).not.toMatch(/url\(|example\.org/);
  });
});
