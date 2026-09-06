import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { CrewOutputRenderer, looksLikeHtmlDocument } from './CrewOutputRenderer';

const theme = createTheme();
const renderOutput = (content: string, maxHeight?: number | string) =>
  render(
    <ThemeProvider theme={theme}>
      <CrewOutputRenderer content={content} maxHeight={maxHeight} />
    </ThemeProvider>
  );

describe('looksLikeHtmlDocument', () => {
  it('detects full HTML documents', () => {
    expect(looksLikeHtmlDocument('<!DOCTYPE html><html><body>x</body></html>')).toBe(true);
    expect(looksLikeHtmlDocument('  <html lang="en"><body></body></html>')).toBe(true);
    expect(looksLikeHtmlDocument('<body>hi</body>')).toBe(true);
  });

  it('treats markdown / plain text as not-HTML', () => {
    expect(looksLikeHtmlDocument('| a | b |\n|---|---|\n| 1 | 2 |')).toBe(false);
    expect(looksLikeHtmlDocument('# Heading\n\nsome **bold** text')).toBe(false);
    expect(looksLikeHtmlDocument('')).toBe(false);
    // Inline tags in prose should not trigger full-document HTML handling.
    expect(looksLikeHtmlDocument('use the <b>save</b> tool')).toBe(false);
  });
});

describe('CrewOutputRenderer', () => {
  it('renders a GitHub-flavored markdown table as a real <table> (not raw text)', () => {
    const md = [
      '| Feature | Goal |',
      '|---|---|',
      '| Vector Search | Retrieval grounding |',
    ].join('\n');

    const { container } = renderOutput(md);

    const table = container.querySelector('table');
    expect(table).not.toBeNull();
    // Header cells and a data cell are rendered as table elements.
    expect(container.querySelectorAll('th').length).toBe(2);
    expect(screen.getByText('Vector Search')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Feature' })).toBeInTheDocument();
    // The literal markdown pipe syntax must NOT appear as visible text.
    expect(container.textContent).not.toContain('|---|');
  });

  it('renders headings and bold markdown rather than raw markers', () => {
    const { container } = renderOutput('# Title\n\nsome **bold** word');
    expect(container.querySelector('h1')).not.toBeNull();
    expect(container.querySelector('strong')).not.toBeNull();
    expect(container.textContent).not.toContain('**bold**');
  });

  it('renders an HTML document inside a sandboxed iframe', () => {
    const html = '<!DOCTYPE html><html><body><h1>Deck</h1></body></html>';
    const { container } = renderOutput(html);

    const iframe = container.querySelector('iframe');
    expect(iframe).not.toBeNull();
    expect(iframe!.getAttribute('srcdoc')).toContain('<h1>Deck</h1>');
    // Sandbox with no allow-scripts (empty sandbox attribute present).
    expect(iframe!.getAttribute('sandbox')).toBe('');
    // Markdown path is not used for HTML.
    expect(container.querySelector('table')).toBeNull();
  });

  it('does not render an iframe for markdown content', () => {
    const { container } = renderOutput('plain markdown, no html');
    expect(container.querySelector('iframe')).toBeNull();
  });

  it('renders markdown links as new-tab anchors', () => {
    const { container } = renderOutput('See [Databricks](https://example.com) docs');
    const link = container.querySelector('a');
    expect(link).not.toBeNull();
    expect(link!.getAttribute('href')).toBe('https://example.com');
    expect(link!.getAttribute('target')).toBe('_blank');
    expect(link!.getAttribute('rel')).toContain('noopener');
  });

  it('accepts a CSS string maxHeight (full-screen mode)', () => {
    // Should not throw with a non-numeric height; markdown still renders.
    const { container } = renderOutput('# ok', 'calc(100vh - 160px)');
    expect(container.querySelector('h1')).not.toBeNull();
  });
});
