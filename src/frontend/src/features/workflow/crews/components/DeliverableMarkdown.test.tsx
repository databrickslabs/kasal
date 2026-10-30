import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import DeliverableMarkdown from './DeliverableMarkdown';

// The eval deliverable in the Optimize dialog (CrewOptimizeDialog.test.tsx
// stubs the markdown renderer, so the rendering itself is checked here).
const remote = 'https://images.example.org/c.png?d=1';

describe('DeliverableMarkdown', () => {
  it('shows a remote image as a placeholder and makes no request until clicked', () => {
    const { container } = render(<DeliverableMarkdown content={`![chart](${remote}) ![logo](/logo192.png)`} />);
    expect(container.querySelector(`img[src="${remote}"]`)).toBeNull();
    expect(screen.getByRole('link', { name: 'Load image' })).toHaveAttribute('href', remote);
    expect(container.querySelector('img[src="/logo192.png"]')).not.toBeNull();
  });

  it('keeps links opening in a new tab, and renders tables', () => {
    render(<DeliverableMarkdown content={'[src](https://example.com/page)\n\n| a | b |\n|---|---|\n| 1 | 2 |'} />);
    const link = screen.getByRole('link', { name: 'src' });
    expect(link).toHaveAttribute('href', 'https://example.com/page');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noreferrer');
    expect(screen.getByRole('table')).toBeInTheDocument();
  });
});
