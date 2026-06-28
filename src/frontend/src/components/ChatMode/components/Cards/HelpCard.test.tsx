/**
 * Unit tests for HelpCard — renders help content as markdown inside a styled card.
 */
import { render, screen } from '@testing-library/react';
import { vi, describe, it, expect } from 'vitest';
import HelpCard from './HelpCard';

// react-markdown 9.x is ESM-only; mock it to a simple passthrough.
vi.mock('react-markdown', () => ({
  default: ({ children }: { children: string }) => <div data-testid="md">{children}</div>,
}));

vi.mock('remark-gfm', () => ({ default: () => {} }));

describe('HelpCard', () => {
  it('renders the provided content through ReactMarkdown', () => {
    render(<HelpCard content="# Help\nUse /run to start" />);
    const md = screen.getByTestId('md');
    expect(md).toHaveTextContent('# Help');
    expect(md).toHaveTextContent('Use /run to start');
  });

  it('renders an empty string content without crashing', () => {
    render(<HelpCard content="" />);
    expect(screen.getByTestId('md')).toBeInTheDocument();
  });

  it('applies the card container styling', () => {
    const { container } = render(<HelpCard content="hello" />);
    const card = container.querySelector('.rounded-xl');
    expect(card).toBeInTheDocument();
    // jsdom does not resolve CSS custom properties via toHaveStyle, so assert on
    // the inline style attribute directly.
    const style = card?.getAttribute('style') || '';
    expect(style).toContain('var(--bg-input)');
    expect(style).toContain('var(--border-color)');
    expect(card?.querySelector('.prose')).toBeInTheDocument();
  });
});
