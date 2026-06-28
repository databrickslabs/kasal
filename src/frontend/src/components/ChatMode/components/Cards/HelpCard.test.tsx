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

  it('renders the markdown inside the card container', () => {
    render(<HelpCard content="hello" />);
    // The card wrapper contains the rendered markdown. Styling is applied via the
    // chat MUI theme (sx), so assert structure/behaviour, not Tailwind classes.
    const card = screen.getByTestId('help-card');
    expect(card).toBeInTheDocument();
    expect(card).toContainElement(screen.getByTestId('md'));
  });
});
