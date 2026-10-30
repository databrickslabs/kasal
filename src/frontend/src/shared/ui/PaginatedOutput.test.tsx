import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import PaginatedOutput from './PaginatedOutput';

// Trace output (TraceTimelineContent, LlmEventDetails) renders through here as
// markdown, so it gets the chat answer's image policy.
const remote = 'https://images.example.org/c.png?d=1';
const md = `Result: ![chart](${remote}) ![logo](/logo192.png)\n\n[the source](https://example.com/page)`;

const renderOutput = (content: string) =>
  render(
    <ThemeProvider theme={createTheme()}>
      <PaginatedOutput content={content} showCopyButton={false} />
    </ThemeProvider>,
  );

describe('PaginatedOutput images', () => {
  it('shows a remote image as a placeholder and makes no request until clicked', () => {
    const { container } = renderOutput(md);
    expect(container.querySelector(`img[src="${remote}"]`)).toBeNull();
    expect(screen.getByRole('link', { name: 'Load image' })).toHaveAttribute('href', remote);
  });

  it('renders same-origin images directly and leaves links unchanged', () => {
    const { container } = renderOutput(md);
    expect(container.querySelector('img[src="/logo192.png"]')).not.toBeNull();
    const link = screen.getByRole('link', { name: 'the source' });
    expect(link).toHaveAttribute('href', 'https://example.com/page');
    expect(link).not.toHaveAttribute('target');
  });
});
