/**
 * Documentation with the real markdown renderer (Documentation.test.tsx stubs
 * it out): a doc's own images resolve and render, a remote one waits for a
 * click, and links behave as before.
 */
import { render, screen } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import Documentation from './Documentation';

vi.mock('react-router-dom', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router-dom')>()),
  useNavigate: () => vi.fn(),
  useLocation: () => ({ pathname: '/docs', search: '', hash: '', state: null, key: 'test' }),
}));

vi.mock('mermaid', () => ({ default: { initialize: vi.fn(), run: vi.fn() } }));

const remote = 'https://images.example.org/badge.png?d=1';
const doc = `# Guide\n\n![flow](images/flow.png) ![badge](${remote})\n\n[upstream](https://example.com/page)`;
const originalFetch = global.fetch;

beforeEach(() => {
  global.requestAnimationFrame = vi.fn() as unknown as typeof requestAnimationFrame;
  global.fetch = vi.fn().mockResolvedValue({ ok: true, text: () => Promise.resolve(doc) }) as unknown as typeof fetch;
});

afterEach(() => {
  global.fetch = originalFetch;
});

describe('Documentation images', () => {
  it('renders the doc\'s own images and holds a remote one back until clicked', async () => {
    const { container } = render(<Documentation />);
    expect(await screen.findByRole('link', { name: 'Load image' })).toHaveAttribute('href', remote);
    expect(container.querySelector(`img[src="${remote}"]`)).toBeNull();
    expect(container.querySelector('img[src="/docs/images/flow.png"]')).not.toBeNull();
    const link = screen.getByRole('link', { name: 'upstream' });
    expect(link).toHaveAttribute('href', 'https://example.com/page');
    expect(link).toHaveAttribute('target', '_blank');
  });
});
