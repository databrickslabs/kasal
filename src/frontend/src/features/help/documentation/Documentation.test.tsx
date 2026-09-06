/**
 * Unit tests for Documentation.loadDocument's HTML-shell guard.
 *
 * A dev/SPA server answers an unknown /docs/*.md path with the app's index.html
 * shell (HTTP 200) rather than a 404. loadDocument now detects that shell
 * (a leading <!doctype html> or a <div id="root">) and renders the
 * "Document Not Found" message instead of the raw HTML.
 *
 * These tests render the component with a mocked global.fetch and assert on the
 * markdown content that reaches the renderer. react-markdown / remark-gfm /
 * mermaid are ESM-only and irrelevant to the guard, so they're stubbed to
 * passthroughs (the same pattern used in ShowResult.test.tsx). The markdown
 * stub renders docContent verbatim, so we can assert on its text directly.
 */
import { render, screen, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import Documentation from './Documentation';

// Documentation uses react-router's useNavigate/useLocation; these tests don't
// exercise navigation, so stub the hooks to avoid needing a Router wrapper.
vi.mock('react-router-dom', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router-dom')>()),
  useNavigate: () => vi.fn(),
  useLocation: () => ({ pathname: '/docs', search: '', hash: '', state: null, key: 'test' }),
}));

// react-markdown 9.x is ESM-only; mock it to a passthrough that renders the
// raw markdown string so we can assert on the loaded content directly.
vi.mock('react-markdown', () => ({
  default: ({ children }: { children: string }) => <div data-testid="md">{children}</div>,
}));

vi.mock('remark-gfm', () => ({ default: () => {} }));

// mermaid only runs inside an effect after content loads; stub it out.
vi.mock('mermaid', () => ({
  default: {
    initialize: vi.fn(),
    run: vi.fn(),
  },
}));

const originalFetch = global.fetch;

beforeEach(() => {
  vi.clearAllMocks();
  // requestAnimationFrame is used by the mermaid effect; provide a no-op.
  global.requestAnimationFrame = vi.fn() as unknown as typeof requestAnimationFrame;
});

afterEach(() => {
  global.fetch = originalFetch;
});

const mockFetch = (body: string, ok = true) => {
  global.fetch = vi.fn().mockResolvedValue({
    ok,
    text: () => Promise.resolve(body),
  }) as unknown as typeof fetch;
};

describe('Documentation loadDocument HTML-shell guard', () => {
  it('renders "Document Not Found" when the response is a <!doctype html> shell', async () => {
    mockFetch('<!doctype html>\n<html><head></head><body><div id="root"></div></body></html>');

    render(<Documentation />);

    await waitFor(() => {
      expect(screen.getByTestId('md')).toBeInTheDocument();
    });

    const rendered = screen.getByTestId('md').textContent || '';
    expect(rendered).toContain('Document Not Found');
    // The raw HTML shell must NOT be rendered as the document.
    expect(rendered).not.toContain('<!doctype html>');
    expect(rendered).not.toContain('<div id="root">');
  });

  it('renders "Document Not Found" when the response contains <div id="root"> (no doctype)', async () => {
    // Leading doctype absent, but the SPA root div is present.
    mockFetch('<html><body><div id="root"></div><script src="/bundle.js"></script></body></html>');

    render(<Documentation />);

    await waitFor(() => {
      expect(screen.getByTestId('md')).toBeInTheDocument();
    });

    const rendered = screen.getByTestId('md').textContent || '';
    expect(rendered).toContain('Document Not Found');
    expect(rendered).not.toContain('<div id="root">');
  });

  it('detects the shell case-insensitively (<!DOCTYPE HTML> with leading whitespace)', async () => {
    mockFetch('   \n<!DOCTYPE HTML>\n<HTML></HTML>');

    render(<Documentation />);

    await waitFor(() => {
      expect(screen.getByTestId('md')).toBeInTheDocument();
    });

    const rendered = screen.getByTestId('md').textContent || '';
    expect(rendered).toContain('Document Not Found');
  });

  it('renders real markdown content when the response is a genuine document', async () => {
    const realDoc = '# Why Kasal\n\nKasal is an AI agent orchestration platform.';
    mockFetch(realDoc);

    render(<Documentation />);

    await waitFor(() => {
      expect(screen.getByTestId('md')).toBeInTheDocument();
    });

    const rendered = screen.getByTestId('md').textContent || '';
    expect(rendered).toContain('Why Kasal');
    expect(rendered).toContain('AI agent orchestration platform');
    // Real content must NOT be replaced by the not-found message.
    expect(rendered).not.toContain('Document Not Found');
  });

  it('renders "Document Not Found" when the fetch is not ok (e.g. 404)', async () => {
    mockFetch('not found', false);

    render(<Documentation />);

    await waitFor(() => {
      expect(screen.getByTestId('md')).toBeInTheDocument();
    });

    const rendered = screen.getByTestId('md').textContent || '';
    expect(rendered).toContain('Document Not Found');
  });

  it('renders an error message when fetch rejects', async () => {
    global.fetch = vi.fn().mockRejectedValue(new Error('boom')) as unknown as typeof fetch;

    render(<Documentation />);

    await waitFor(() => {
      expect(screen.getByTestId('md')).toBeInTheDocument();
    });

    const rendered = screen.getByTestId('md').textContent || '';
    expect(rendered).toContain('Error Loading Document');
  });
});
