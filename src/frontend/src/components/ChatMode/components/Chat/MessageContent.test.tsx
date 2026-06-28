import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import MessageContent from './MessageContent';

// Styling is applied via the chat MUI theme (sx), so these tests assert on the
// rendered structure (branch testids + real markdown elements), not Tailwind
// classes. `message-plain` = the plain-text branch, `message-markdown` = the
// ReactMarkdown branch.
describe('MessageContent', () => {
  it('renders plain text in the plain-text branch when content has no markdown', () => {
    const { container } = render(<MessageContent content="just plain text" />);

    const p = screen.getByTestId('message-plain');
    expect(p.tagName.toLowerCase()).toBe('p');
    expect(p.textContent).toBe('just plain text');
    // Should not render the markdown branch.
    expect(container.querySelector('[data-testid="message-markdown"]')).toBeNull();
  });

  it('renders markdown content in the markdown branch using ReactMarkdown', () => {
    const { container } = render(<MessageContent content="# Heading" />);

    expect(screen.getByTestId('message-markdown')).toBeInTheDocument();
    // ReactMarkdown should produce an actual heading element.
    const heading = container.querySelector('h1');
    expect(heading).not.toBeNull();
    expect(heading?.textContent).toBe('Heading');
    // The plain-text fallback should not be present.
    expect(container.querySelector('[data-testid="message-plain"]')).toBeNull();
  });

  it('renders markdown code blocks (GFM enabled) for fenced code content', () => {
    const md = '```\nconst x = 1;\n```';
    const { container } = render(<MessageContent content={md} />);

    expect(screen.getByTestId('message-markdown')).toBeInTheDocument();
    const code = container.querySelector('pre code');
    expect(code).not.toBeNull();
    expect(code?.textContent).toContain('const x = 1;');
  });

  it('renders empty plain content as an empty paragraph', () => {
    const { container } = render(<MessageContent content="" />);

    const p = screen.getByTestId('message-plain');
    expect(p.tagName.toLowerCase()).toBe('p');
    expect(p.textContent).toBe('');
    expect(container.querySelector('[data-testid="message-markdown"]')).toBeNull();
  });

  it('renders bold markdown in the markdown branch', () => {
    render(<MessageContent content="**bold text**" />);
    const strong = screen.getByText('bold text');
    expect(strong.tagName.toLowerCase()).toBe('strong');
  });
});
